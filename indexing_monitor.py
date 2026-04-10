#!/usr/bin/env python3
"""Independent GSC indexing monitor backed by Google Sheets.

This worker is separate from the SEO validator UI/reporting flow.
It discovers fresh URLs from configured news sitemaps and keeps polling
Google Search Console until each article is first seen as indexed.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import requests

UTC = dt.timezone.utc
IST = dt.timezone(dt.timedelta(hours=5, minutes=30))
DEFAULT_CONFIG_PATH = Path(__file__).with_name("indexing_monitor_config.local.json")


def today_ist_midnight(reference: Optional[dt.datetime] = None) -> dt.datetime:
    current = reference.astimezone(IST) if reference else dt.datetime.now(tz=IST)
    return current.replace(hour=0, minute=0, second=0, microsecond=0)


def default_cutoff_datetime() -> dt.datetime:
    return today_ist_midnight().astimezone(UTC)


GSC_SITE_DAILY_LIMIT = 2000
GSC_SITE_MINUTE_LIMIT = 600


SUMMARY_HEADERS = [
    "date",
    "url",
    "sitemap_published_date",
    "Google_Last_Crawl_At",
    "first_checked_at",
    "last_checked_at",
    "check_count",
    "current_status",
    "first_indexed_seen_at",
    "last_non_indexed_seen_at",
    "estimated_indexed_at",
    "indexing_latency_minutes",
    "gsc_daily_limit",
    "configured_hourly_cap",
    "gsc_coverage_state",
    "gsc_page_fetch_state",
]
LOG_HEADERS = [
    "url",
    "checked_at",
    "status",
    "verdict",
    "coverage_state",
    "indexing_state",
    "page_fetch_state",
    "robots_state",
    "last_crawl_time",
    "error",
]
STATE_HEADERS = [
    "property_key",
    "last_sitemap_check_at",
    "gsc_hour_bucket",
    "gsc_checks_this_hour",
    "gsc_quota_backoff_until",
]
STATE_SHEET = "_monitor_state"
NAMESPACES = {
    "sm": "http://www.sitemaps.org/schemas/sitemap/0.9",
    "news": "http://www.google.com/schemas/sitemap-news/0.9",
}


@dataclass
class PropertyConfig:
    key: str
    summary_sheet: str
    log_sheet: str
    gsc_site_url: str
    sitemap_urls: List[str]
    discovery_interval_minutes: int
    max_gsc_checks_per_hour: Optional[int] = None
    max_gsc_checks_per_run: Optional[int] = None
    max_new_urls_per_run: Optional[int] = None
    allow_lastmod_fallback: bool = False


def load_json_config(config_path: Path) -> Dict[str, object]:
    with config_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def parse_iso_datetime(value: str) -> Optional[dt.datetime]:
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        parsed = dt.datetime.fromisoformat(raw)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    except Exception:
        return None


def format_iso_datetime(value: dt.datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat()


def format_sheet_datetime(value: dt.datetime) -> str:
    return value.astimezone(IST).replace(microsecond=0).isoformat()


def display_datetime_value(value: str) -> str:
    parsed = parse_iso_datetime(value)
    if parsed:
        return format_sheet_datetime(parsed)
    return value


def parse_publication_datetime(value: str) -> Optional[dt.datetime]:
    parsed = parse_iso_datetime(value)
    if parsed:
        return parsed
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        return dt.datetime.strptime(raw, "%Y-%m-%d").replace(tzinfo=IST).astimezone(UTC)
    except Exception:
        return None


def parse_cutoff_datetime(value: str) -> dt.datetime:
    raw = (value or "").strip()
    if not raw:
        return default_cutoff_datetime()
    if raw.lower() in {"today", "today_ist", "today-only", "today_only"}:
        return default_cutoff_datetime()
    parsed = parse_iso_datetime(raw)
    if parsed:
        return parsed
    for fmt in ("%Y-%m-%d %H:%M", "%d-%m-%Y %H:%M", "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y"):
        try:
            parsed_value = dt.datetime.strptime(raw, fmt)
            return parsed_value.replace(tzinfo=IST).astimezone(UTC)
        except ValueError:
            continue
    raise ValueError(f"Unsupported cutoff date format: {value}")


def midpoint_iso(start_value: str, end_value: str) -> str:
    start_dt = parse_iso_datetime(start_value)
    end_dt = parse_iso_datetime(end_value)
    if not start_dt or not end_dt:
        return display_datetime_value(end_value)
    midpoint = start_dt + (end_dt - start_dt) / 2
    return format_sheet_datetime(midpoint)


def indexing_latency_minutes(published_value: str, indexed_value: str) -> str:
    published_dt = parse_publication_datetime(published_value)
    indexed_dt = parse_iso_datetime(indexed_value)
    if not published_dt or not indexed_dt:
        return ""
    latency = int((indexed_dt - published_dt).total_seconds() // 60)
    return str(max(latency, 0))


def status_bucket(status: str, error: str) -> str:
    if is_quota_exceeded_error(error):
        return "Quota Exceeded"
    if error:
        return "Error"
    raw = (status or "").strip()
    if raw:
        return raw
    return "Unknown"


def build_google_credentials(service_account_path: Optional[str]):
    try:
        from google.oauth2 import service_account
    except Exception as exc:  # pragma: no cover - dependency/runtime guard
        raise RuntimeError(f"google-auth is required: {exc}") from exc

    scopes = [
        "https://www.googleapis.com/auth/webmasters.readonly",
        "https://www.googleapis.com/auth/spreadsheets",
    ]

    if service_account_path:
        if not os.path.exists(service_account_path):
            raise FileNotFoundError(f"Service account JSON not found: {service_account_path}")
        return service_account.Credentials.from_service_account_file(
            service_account_path,
            scopes=scopes,
        )

    raw_json = os.environ.get("GSC_SERVICE_ACCOUNT_JSON", "").strip()
    if raw_json:
        payload = json.loads(raw_json)
        return service_account.Credentials.from_service_account_info(payload, scopes=scopes)

    google_application_credentials = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if google_application_credentials:
        if not os.path.exists(google_application_credentials):
            raise FileNotFoundError(
                f"GOOGLE_APPLICATION_CREDENTIALS not found: {google_application_credentials}"
            )
        return service_account.Credentials.from_service_account_file(
            google_application_credentials,
            scopes=scopes,
        )

    raise RuntimeError(
        "No service-account credentials found. Provide service_account_json_path in config, "
        "GSC_SERVICE_ACCOUNT_JSON, or GOOGLE_APPLICATION_CREDENTIALS."
    )


def build_services(credentials):
    try:
        from googleapiclient.discovery import build
    except Exception as exc:  # pragma: no cover - dependency/runtime guard
        raise RuntimeError(f"google-api-python-client is required: {exc}") from exc

    sheets = build("sheets", "v4", credentials=credentials, cache_discovery=False)
    gsc = build("searchconsole", "v1", credentials=credentials, cache_discovery=False)
    return sheets, gsc


def is_permission_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return "does not have permission" in message or "insufficient permission" in message


def get_spreadsheet_metadata(sheets_service, spreadsheet_id: str) -> Dict[str, object]:
    return (
        sheets_service.spreadsheets()
        .get(spreadsheetId=spreadsheet_id)
        .execute()
    )


def ensure_sheet(sheets_service, spreadsheet_id: str, sheet_title: str) -> None:
    metadata = get_spreadsheet_metadata(sheets_service, spreadsheet_id)
    titles = {sheet["properties"]["title"] for sheet in metadata.get("sheets", [])}
    if sheet_title in titles:
        return
    body = {"requests": [{"addSheet": {"properties": {"title": sheet_title}}}]}
    sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body=body,
    ).execute()


def read_values(sheets_service, spreadsheet_id: str, range_name: str) -> List[List[str]]:
    response = (
        sheets_service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=range_name)
        .execute()
    )
    return response.get("values", [])


def ensure_headers(
    sheets_service,
    spreadsheet_id: str,
    sheet_title: str,
    headers: List[str],
) -> None:
    ensure_sheet(sheets_service, spreadsheet_id, sheet_title)
    existing = read_values(sheets_service, spreadsheet_id, f"{sheet_title}!1:1")
    if existing and existing[0] == headers:
        return
    sheets_service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"{sheet_title}!A1",
        valueInputOption="RAW",
        body={"values": [headers]},
    ).execute()


def append_rows(
    sheets_service,
    spreadsheet_id: str,
    sheet_title: str,
    rows: List[List[str]],
) -> None:
    if not rows:
        return
    sheets_service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=f"{sheet_title}!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": rows},
    ).execute()


def rewrite_sheet(
    sheets_service,
    spreadsheet_id: str,
    sheet_title: str,
    rows: List[List[str]],
) -> None:
    sheets_service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=f"{sheet_title}!A:Z",
        body={},
    ).execute()
    if rows:
        sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_title}!A1",
            valueInputOption="RAW",
            body={"values": rows},
        ).execute()


def update_row(
    sheets_service,
    spreadsheet_id: str,
    sheet_title: str,
    row_number: int,
    row_values: List[str],
) -> None:
    end_column = chr(ord("A") + len(row_values) - 1)
    sheets_service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"{sheet_title}!A{row_number}:{end_column}{row_number}",
        valueInputOption="RAW",
        body={"values": [row_values]},
    ).execute()


def sheet_records_with_rows(
    sheets_service,
    spreadsheet_id: str,
    sheet_title: str,
    headers: List[str],
) -> List[Tuple[int, Dict[str, str]]]:
    values = read_values(sheets_service, spreadsheet_id, f"{sheet_title}!A:Z")
    if not values:
        return []
    data_rows = values[1:]
    records: List[Tuple[int, Dict[str, str]]] = []
    for offset, row in enumerate(data_rows, start=2):
        record = {header: row[index] if index < len(row) else "" for index, header in enumerate(headers)}
        records.append((offset, record))
    return records


def load_state_map(sheets_service, spreadsheet_id: str) -> Dict[str, Tuple[int, Dict[str, str]]]:
    ensure_headers(sheets_service, spreadsheet_id, STATE_SHEET, STATE_HEADERS)
    return {
        record["property_key"]: (row_number, record)
        for row_number, record in sheet_records_with_rows(
            sheets_service, spreadsheet_id, STATE_SHEET, STATE_HEADERS
        )
        if record.get("property_key")
    }


def upsert_state_record(
    sheets_service,
    spreadsheet_id: str,
    property_key: str,
    state_map: Dict[str, Tuple[int, Dict[str, str]]],
    updates: Dict[str, str],
) -> None:
    current_row_number, current_record = state_map.get(property_key, (0, {header: "" for header in STATE_HEADERS}))
    merged = {header: current_record.get(header, "") for header in STATE_HEADERS}
    merged.update(updates)
    merged["property_key"] = property_key
    row_values = [merged.get(header, "") for header in STATE_HEADERS]

    if current_row_number:
        update_row(sheets_service, spreadsheet_id, STATE_SHEET, current_row_number, row_values)
        state_map[property_key] = (current_row_number, merged)
    else:
        append_rows(sheets_service, spreadsheet_id, STATE_SHEET, [row_values])
        refreshed = load_state_map(sheets_service, spreadsheet_id)
        state_map.clear()
        state_map.update(refreshed)


def load_summary_map(
    sheets_service,
    spreadsheet_id: str,
    sheet_title: str,
) -> Dict[str, Tuple[int, Dict[str, str]]]:
    ensure_headers(sheets_service, spreadsheet_id, sheet_title, SUMMARY_HEADERS)
    return {
        record["url"]: (row_number, record)
        for row_number, record in sheet_records_with_rows(
            sheets_service, spreadsheet_id, sheet_title, SUMMARY_HEADERS
        )
        if record.get("url")
    }


def sort_summary_sheet(
    sheets_service,
    spreadsheet_id: str,
    sheet_title: str,
) -> None:
    records = sheet_records_with_rows(sheets_service, spreadsheet_id, sheet_title, SUMMARY_HEADERS)
    if not records:
        return

    sorted_records = sorted(
        (record for _, record in records if record.get("url")),
        key=lambda record: (
            record.get("date", ""),
            parse_publication_datetime(record.get("sitemap_published_date", ""))
            or dt.datetime.min.replace(tzinfo=UTC),
        ),
        reverse=True,
    )
    rewrite_sheet(
        sheets_service,
        spreadsheet_id,
        sheet_title,
        [SUMMARY_HEADERS] + [build_summary_update_row(record) for record in sorted_records],
    )


def sort_log_sheet(
    sheets_service,
    spreadsheet_id: str,
    sheet_title: str,
) -> None:
    records = sheet_records_with_rows(sheets_service, spreadsheet_id, sheet_title, LOG_HEADERS)
    if not records:
        return

    sorted_records = sorted(
        (record for _, record in records if record.get("url")),
        key=lambda record: (
            parse_iso_datetime(record.get("checked_at", "")) or dt.datetime.min.replace(tzinfo=UTC),
            record.get("url", ""),
        ),
        reverse=True,
    )
    rewrite_sheet(
        sheets_service,
        spreadsheet_id,
        sheet_title,
        [LOG_HEADERS] + [[record.get(header, "") for header in LOG_HEADERS] for record in sorted_records],
    )


def current_hour_bucket(now: dt.datetime) -> str:
    bucket = now.astimezone(IST).replace(minute=0, second=0, microsecond=0)
    return format_sheet_datetime(bucket)


def is_quota_exceeded_error(error: str) -> bool:
    message = (error or "").lower()
    return "quota exceeded" in message or "httperror 429" in message or "429" in message


def quota_backoff_due(state_record: Dict[str, str], now: dt.datetime) -> bool:
    blocked_until = parse_iso_datetime(state_record.get("gsc_quota_backoff_until", ""))
    if not blocked_until:
        return True
    return now >= blocked_until


def set_quota_backoff(
    state_record: Dict[str, str],
    now: dt.datetime,
    minutes: int = 60,
) -> Dict[str, str]:
    updated = dict(state_record)
    updated["gsc_quota_backoff_until"] = format_sheet_datetime(now + dt.timedelta(minutes=minutes))
    return updated


def property_can_run_gsc(property_config: PropertyConfig, state_record: Dict[str, str], now: dt.datetime) -> bool:
    if not quota_backoff_due(state_record, now):
        return False
    hourly_limit = property_config.max_gsc_checks_per_hour
    if not hourly_limit:
        return True
    bucket = current_hour_bucket(now)
    current_bucket = state_record.get("gsc_hour_bucket", "")
    if current_bucket != bucket:
        return True
    current_count = int(state_record.get("gsc_checks_this_hour", "0") or "0")
    return current_count < hourly_limit


def increment_property_hourly_count(
    property_config: PropertyConfig,
    state_record: Dict[str, str],
    now: dt.datetime,
) -> Dict[str, str]:
    bucket = current_hour_bucket(now)
    current_bucket = state_record.get("gsc_hour_bucket", "")
    current_count = int(state_record.get("gsc_checks_this_hour", "0") or "0")
    if current_bucket != bucket:
        current_count = 0
    current_count += 1
    updated = dict(state_record)
    updated["gsc_hour_bucket"] = bucket
    updated["gsc_checks_this_hour"] = str(current_count)
    return updated


def property_discovery_due(state_record: Dict[str, str], interval_minutes: int, now: dt.datetime) -> bool:
    last_checked_at = parse_iso_datetime(state_record.get("last_sitemap_check_at", ""))
    if not last_checked_at:
        return True
    return (now - last_checked_at) >= dt.timedelta(minutes=interval_minutes)


def next_poll_interval_minutes(published_dt: Optional[dt.datetime], now: dt.datetime) -> int:
    if not published_dt:
        return 240
    if now - published_dt <= dt.timedelta(hours=1):
        return 10
    return 240


def row_due_for_gsc(record: Dict[str, str], now: dt.datetime) -> bool:
    if record.get("current_status") == "Indexed":
        return False
    if not record.get("url"):
        return False
    if not record.get("first_checked_at"):
        return True
    last_checked_at = parse_iso_datetime(record.get("last_checked_at", ""))
    if not last_checked_at:
        return True
    published_dt = parse_publication_datetime(record.get("sitemap_published_date", ""))
    interval = next_poll_interval_minutes(published_dt, now)
    return now - last_checked_at >= dt.timedelta(minutes=interval)


def build_summary_row(
    property_config: PropertyConfig,
    url: str,
    published_at: str,
    row_date: str,
) -> List[str]:
    values = {
        "date": row_date,
        "url": url,
        "sitemap_published_date": display_datetime_value(published_at),
        "Google_Last_Crawl_At": "",
        "first_checked_at": "",
        "last_checked_at": "",
        "check_count": "0",
        "current_status": "Pending",
        "first_indexed_seen_at": "",
        "last_non_indexed_seen_at": "",
        "estimated_indexed_at": "",
        "indexing_latency_minutes": "",
        "gsc_daily_limit": str(GSC_SITE_DAILY_LIMIT),
        "configured_hourly_cap": str(property_config.max_gsc_checks_per_hour or "No custom cap"),
        "gsc_coverage_state": "",
        "gsc_page_fetch_state": "",
    }
    return [values[header] for header in SUMMARY_HEADERS]


def build_summary_update_row(record: Dict[str, str]) -> List[str]:
    return [record.get(header, "") for header in SUMMARY_HEADERS]


def normalize_gsc_result(index_status: Dict[str, object]) -> Dict[str, str]:
    verdict = str(index_status.get("verdict", "") or "")
    coverage_state = str(index_status.get("coverageState", "") or "")
    indexing_state = str(index_status.get("indexingState", "") or "")
    page_fetch_state = str(index_status.get("pageFetchState", "") or "")
    robots_state = str(index_status.get("robotsTxtState", "") or "")

    if robots_state == "DISALLOWED" or page_fetch_state == "BLOCKED_ROBOTS_TXT":
        status = "Blocked by robots.txt"
    elif indexing_state in ("BLOCKED_BY_META_TAG", "BLOCKED_BY_HTTP_HEADER"):
        status = "Blocked by noindex"
    elif verdict == "PASS":
        status = "Indexed"
    elif verdict == "NEUTRAL":
        status = "Excluded"
    elif verdict == "FAIL":
        status = "Error"
    else:
        status = "Unknown"

    return {
        "status": status,
        "verdict": verdict,
        "coverage_state": coverage_state,
        "indexing_state": indexing_state,
        "page_fetch_state": page_fetch_state,
        "robots_state": robots_state,
        "last_crawl_time": display_datetime_value(str(index_status.get("lastCrawlTime", "") or "")),
        "error": "",
    }


def inspect_url(gsc_service, inspection_url: str, site_url: str) -> Dict[str, str]:
    try:
        response = (
            gsc_service.urlInspection()
            .index()
            .inspect(
                body={
                    "inspectionUrl": inspection_url,
                    "siteUrl": site_url,
                    "languageCode": "en-US",
                }
            )
            .execute()
        )
        index_status = response.get("inspectionResult", {}).get("indexStatusResult", {})
        if not isinstance(index_status, dict):
            return {
                "status": "Error",
                "verdict": "",
                "coverage_state": "",
                "indexing_state": "",
                "page_fetch_state": "",
                "robots_state": "",
                "last_crawl_time": "",
                "error": "No index status returned by GSC",
            }
        return normalize_gsc_result(index_status)
    except Exception as exc:
        return {
            "status": "Error",
            "verdict": "",
            "coverage_state": "",
            "indexing_state": "",
            "page_fetch_state": "",
            "robots_state": "",
            "last_crawl_time": "",
            "error": str(exc),
        }


def parse_news_sitemap(xml_text: str, allow_lastmod_fallback: bool = False) -> List[Tuple[str, str]]:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []

    def local_name(tag: str) -> str:
        if "}" in tag:
            return tag.rsplit("}", 1)[1]
        return tag

    def find_child_text(element: ET.Element, child_name: str) -> str:
        for child in list(element):
            if local_name(child.tag) == child_name and child.text:
                return child.text.strip()
        return ""

    def find_publication_date(element: ET.Element) -> str:
        for child in list(element):
            if local_name(child.tag) != "news":
                continue
            for grandchild in list(child):
                if local_name(grandchild.tag) == "publication_date" and grandchild.text:
                    return grandchild.text.strip()
        return ""

    def find_lastmod(element: ET.Element) -> str:
        for child in list(element):
            if local_name(child.tag) == "lastmod" and child.text:
                return child.text.strip()
        return ""

    entries: List[Tuple[str, str]] = []
    for url_node in list(root):
        if local_name(url_node.tag) != "url":
            continue
        loc = find_child_text(url_node, "loc")
        published = find_publication_date(url_node)
        if not published and allow_lastmod_fallback:
            published = find_lastmod(url_node)
        if loc and published:
            entries.append((loc, published))
    return entries


def fetch_sitemap_urls(
    session: requests.Session,
    sitemap_url: str,
    allow_lastmod_fallback: bool = False,
) -> List[Tuple[str, str]]:
    response = session.get(sitemap_url, timeout=30)
    response.raise_for_status()
    return parse_news_sitemap(response.text, allow_lastmod_fallback=allow_lastmod_fallback)


def discover_new_urls(
    session: requests.Session,
    property_config: PropertyConfig,
    summary_map: Dict[str, Tuple[int, Dict[str, str]]],
    cutoff_datetime: dt.datetime,
    now: dt.datetime,
) -> List[List[str]]:
    discovered: Dict[str, str] = {}
    for sitemap_url in property_config.sitemap_urls:
        for url, published in fetch_sitemap_urls(
            session,
            sitemap_url,
            allow_lastmod_fallback=property_config.allow_lastmod_fallback,
        ):
            published_dt = parse_publication_datetime(published)
            if not published_dt:
                continue
            if published_dt < cutoff_datetime:
                continue
            if url in summary_map and summary_map[url][1].get("current_status") == "Indexed":
                continue
            discovered[url] = published

    items = sorted(
        discovered.items(),
        key=lambda pair: parse_publication_datetime(pair[1]) or dt.datetime.min.replace(tzinfo=UTC),
        reverse=True,
    )
    if property_config.max_new_urls_per_run:
        items = items[: property_config.max_new_urls_per_run]

    rows: List[List[str]] = []
    for url, published in items:
        if url in summary_map:
            continue
        published_dt = parse_publication_datetime(published)
        row_date = (
            published_dt.astimezone(IST).date().isoformat()
            if published_dt
            else now.astimezone(IST).date().isoformat()
        )
        rows.append(build_summary_row(property_config, url, published, row_date))
    return rows


def update_summary_after_gsc(
    record: Dict[str, str],
    gsc_result: Dict[str, str],
    checked_at: str,
    property_config: PropertyConfig,
) -> Dict[str, str]:
    updated = dict(record)
    updated["gsc_daily_limit"] = updated.get("gsc_daily_limit") or str(GSC_SITE_DAILY_LIMIT)
    updated["configured_hourly_cap"] = updated.get("configured_hourly_cap") or str(
        property_config.max_gsc_checks_per_hour or "No custom cap"
    )
    updated["first_checked_at"] = updated.get("first_checked_at") or checked_at
    updated["last_checked_at"] = checked_at
    google_last_crawl_at = gsc_result.get("last_crawl_time", "") or updated.get("Google_Last_Crawl_At", "")
    updated["Google_Last_Crawl_At"] = google_last_crawl_at
    updated["check_count"] = str(int(updated.get("check_count", "0") or "0") + 1)
    updated["current_status"] = status_bucket(gsc_result.get("status", ""), gsc_result.get("error", ""))
    updated["gsc_coverage_state"] = gsc_result.get("coverage_state", "")
    updated["gsc_page_fetch_state"] = gsc_result.get("page_fetch_state", "")

    if updated["current_status"] == "Indexed":
        if not updated.get("first_indexed_seen_at"):
            updated["first_indexed_seen_at"] = checked_at
        estimated = midpoint_iso(updated.get("last_non_indexed_seen_at", ""), checked_at)
        updated["estimated_indexed_at"] = estimated
    else:
        updated["last_non_indexed_seen_at"] = checked_at

    latency_basis = google_last_crawl_at or updated.get("estimated_indexed_at", "")
    updated["indexing_latency_minutes"] = indexing_latency_minutes(
        updated.get("sitemap_published_date", ""),
        latency_basis,
    )

    return updated


def build_log_row(url: str, checked_at: str, gsc_result: Dict[str, str]) -> List[str]:
    values = {
        "url": url,
        "checked_at": checked_at,
        "status": status_bucket(gsc_result.get("status", ""), gsc_result.get("error", "")),
        "verdict": gsc_result.get("verdict", ""),
        "coverage_state": gsc_result.get("coverage_state", ""),
        "indexing_state": gsc_result.get("indexing_state", ""),
        "page_fetch_state": gsc_result.get("page_fetch_state", ""),
        "robots_state": gsc_result.get("robots_state", ""),
        "last_crawl_time": gsc_result.get("last_crawl_time", ""),
        "error": gsc_result.get("error", ""),
    }
    return [values[header] for header in LOG_HEADERS]


def clear_sheet_contents(sheets_service, spreadsheet_id: str, sheet_title: str) -> None:
    ensure_sheet(sheets_service, spreadsheet_id, sheet_title)
    sheets_service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=f"{sheet_title}!A:Z",
        body={},
    ).execute()


def reset_monitor_sheets(
    sheets_service,
    spreadsheet_id: str,
    properties: List[PropertyConfig],
) -> None:
    clear_sheet_contents(sheets_service, spreadsheet_id, STATE_SHEET)
    for property_config in properties:
        clear_sheet_contents(sheets_service, spreadsheet_id, property_config.summary_sheet)
        clear_sheet_contents(sheets_service, spreadsheet_id, property_config.log_sheet)
    ensure_headers(sheets_service, spreadsheet_id, STATE_SHEET, STATE_HEADERS)
    for property_config in properties:
        ensure_headers(sheets_service, spreadsheet_id, property_config.summary_sheet, SUMMARY_HEADERS)
        ensure_headers(sheets_service, spreadsheet_id, property_config.log_sheet, LOG_HEADERS)


def ensure_property_sheets(
    sheets_service,
    spreadsheet_id: str,
    properties: List[PropertyConfig],
) -> None:
    for property_config in properties:
        ensure_headers(sheets_service, spreadsheet_id, property_config.summary_sheet, SUMMARY_HEADERS)
        ensure_headers(sheets_service, spreadsheet_id, property_config.log_sheet, LOG_HEADERS)


def run_property_discovery(
    session: requests.Session,
    sheets_service,
    spreadsheet_id: str,
    property_config: PropertyConfig,
    state_map: Dict[str, Tuple[int, Dict[str, str]]],
    cutoff_datetime: dt.datetime,
    now: dt.datetime,
) -> int:
    state_row_number, state_record = state_map.get(
        property_config.key,
        (0, {header: "" for header in STATE_HEADERS}),
    )
    summary_map = load_summary_map(sheets_service, spreadsheet_id, property_config.summary_sheet)

    if property_discovery_due(state_record, property_config.discovery_interval_minutes, now):
        new_rows = discover_new_urls(session, property_config, summary_map, cutoff_datetime, now)
        append_rows(sheets_service, spreadsheet_id, property_config.summary_sheet, new_rows)
        sort_summary_sheet(sheets_service, spreadsheet_id, property_config.summary_sheet)
        state_record = dict(state_record)
        state_record["last_sitemap_check_at"] = format_sheet_datetime(now)
        upsert_state_record(
            sheets_service,
            spreadsheet_id,
            property_config.key,
            state_map,
            state_record,
        )
        state_row_number, state_record = state_map.get(
            property_config.key,
            (state_row_number, state_record),
        )
        return len(new_rows)
    return 0


def run_property_gsc(
    sheets_service,
    gsc_service,
    spreadsheet_id: str,
    property_config: PropertyConfig,
    state_map: Dict[str, Tuple[int, Dict[str, str]]],
    cutoff_datetime: dt.datetime,
    now: dt.datetime,
) -> Dict[str, int]:
    metrics = {"checked": 0, "indexed_now": 0}
    state_row_number, state_record = state_map.get(
        property_config.key,
        (0, {header: "" for header in STATE_HEADERS}),
    )
    summary_map = load_summary_map(sheets_service, spreadsheet_id, property_config.summary_sheet)
    due_records = [
        (row_number, record)
        for url, (row_number, record) in summary_map.items()
        if row_due_for_gsc(record, now)
        and (
            (parse_publication_datetime(record.get("sitemap_published_date", "")) or dt.datetime.min.replace(tzinfo=UTC))
            >= cutoff_datetime
        )
    ]
    due_records.sort(
        key=lambda item: parse_publication_datetime(item[1].get("sitemap_published_date", ""))
        or dt.datetime.min.replace(tzinfo=UTC),
        reverse=True,
    )
    due_records.sort(key=lambda item: int(item[1].get("check_count", "0") or "0") > 0)

    for row_number, record in due_records:
        if not property_can_run_gsc(property_config, state_record, now):
            break
        if property_config.max_gsc_checks_per_run and metrics["checked"] >= property_config.max_gsc_checks_per_run:
            break

        checked_at = format_sheet_datetime(now)
        gsc_result = inspect_url(gsc_service, record["url"], property_config.gsc_site_url)
        if is_quota_exceeded_error(gsc_result.get("error", "")):
            append_rows(
                sheets_service,
                spreadsheet_id,
                property_config.log_sheet,
                [build_log_row(record["url"], checked_at, gsc_result)],
            )
            sort_log_sheet(sheets_service, spreadsheet_id, property_config.log_sheet)
            metrics["checked"] += 1
            state_record = set_quota_backoff(state_record, now)
            upsert_state_record(
                sheets_service,
                spreadsheet_id,
                property_config.key,
                state_map,
                state_record,
            )
            break
        updated_record = update_summary_after_gsc(record, gsc_result, checked_at, property_config)
        update_row(
            sheets_service,
            spreadsheet_id,
            property_config.summary_sheet,
            row_number,
            build_summary_update_row(updated_record),
        )
        sort_summary_sheet(sheets_service, spreadsheet_id, property_config.summary_sheet)
        append_rows(
            sheets_service,
            spreadsheet_id,
            property_config.log_sheet,
            [build_log_row(record["url"], checked_at, gsc_result)],
        )
        sort_log_sheet(sheets_service, spreadsheet_id, property_config.log_sheet)
        metrics["checked"] += 1
        if updated_record.get("current_status") == "Indexed":
            metrics["indexed_now"] += 1

        state_record = increment_property_hourly_count(property_config, state_record, now)
        upsert_state_record(
            sheets_service,
            spreadsheet_id,
            property_config.key,
            state_map,
            state_record,
        )
        state_row_number, state_record = state_map.get(
            property_config.key,
            (state_row_number, state_record),
        )
    return metrics
def load_property_configs(config: Dict[str, object]) -> List[PropertyConfig]:
    items = config.get("properties", [])
    if not isinstance(items, list):
        raise ValueError("Config field 'properties' must be a list")

    properties: List[PropertyConfig] = []
    for item in items:
        if not isinstance(item, dict):
            raise ValueError("Each property config must be an object")
        properties.append(
            PropertyConfig(
                key=str(item["key"]),
                summary_sheet=str(item.get("summary_sheet", item["key"])),
                log_sheet=str(item.get("log_sheet", f"{item['key']}_log")),
                gsc_site_url=str(item["gsc_site_url"]),
                sitemap_urls=[str(url) for url in item.get("sitemap_urls", [])],
                discovery_interval_minutes=int(item["discovery_interval_minutes"]),
                max_gsc_checks_per_hour=(
                    int(item["max_gsc_checks_per_hour"])
                    if item.get("max_gsc_checks_per_hour") is not None
                    else None
                ),
                max_gsc_checks_per_run=(
                    int(item["max_gsc_checks_per_run"])
                    if item.get("max_gsc_checks_per_run") is not None
                    else None
                ),
                max_new_urls_per_run=(
                    int(item["max_new_urls_per_run"])
                    if item.get("max_new_urls_per_run") is not None
                    else None
                ),
                allow_lastmod_fallback=bool(item.get("allow_lastmod_fallback", False)),
            )
        )
    return properties


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Google Sheets + GSC indexing monitor")
    parser.add_argument(
        "--config",
        default=str(DEFAULT_CONFIG_PATH),
        help="Path to indexing monitor config JSON file.",
    )
    parser.add_argument(
        "--property",
        default="",
        help="Optional single property key to run.",
    )
    parser.add_argument(
        "--reset-sheet",
        action="store_true",
        help="Clear monitor tabs before running.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config_path = Path(args.config)
    if not config_path.exists():
        raise FileNotFoundError(
            f"Config file not found: {config_path}. Create indexing_monitor_config.local.json first."
        )

    config = load_json_config(config_path)
    cutoff_datetime = parse_cutoff_datetime(
        str(
            config.get(
                "cutoff_datetime",
                config.get("cutoff_date", "today_ist"),
            )
        )
    )
    now = dt.datetime.now(tz=UTC)
    cutoff_datetime = max(cutoff_datetime, today_ist_midnight(now).astimezone(UTC))
    spreadsheet_id = str(config["spreadsheet_id"])
    service_account_json_path = str(config.get("service_account_json_path", "") or "")

    credentials = build_google_credentials(service_account_json_path)
    sheets_service, gsc_service = build_services(credentials)
    properties = load_property_configs(config)
    if args.property:
        properties = [item for item in properties if item.key == args.property]
        if not properties:
            raise ValueError(f"No property config found for key: {args.property}")

    try:
        if args.reset_sheet:
            reset_monitor_sheets(sheets_service, spreadsheet_id, properties)

        ensure_headers(sheets_service, spreadsheet_id, STATE_SHEET, STATE_HEADERS)
        ensure_property_sheets(sheets_service, spreadsheet_id, properties)
        state_map = load_state_map(sheets_service, spreadsheet_id)
        session = requests.Session()
        session.headers.update({"User-Agent": "SchemaSitemapValidator/1.0"})
        summaries: List[str] = []

        for property_config in properties:
            try:
                discovered = run_property_discovery(
                    session,
                    sheets_service,
                    spreadsheet_id,
                    property_config,
                    state_map,
                    cutoff_datetime,
                    now,
                )
                summaries.append(f"{property_config.key}: discovered={discovered}")
            except Exception as exc:
                summaries.append(f"{property_config.key}: discovery_error={exc}")

        for property_config in properties:
            try:
                metrics = run_property_gsc(
                    sheets_service,
                    gsc_service,
                    spreadsheet_id,
                    property_config,
                    state_map,
                    cutoff_datetime,
                    now,
                )
            except Exception as exc:
                summaries.append(f"{property_config.key}: gsc_error={exc}")
                continue
            summaries.append(
                f"{property_config.key}: checked={metrics['checked']} indexed_now={metrics['indexed_now']}"
            )

        print("Indexing monitor completed")
        for summary in summaries:
            print(summary)
        return 0
    except Exception as exc:
        if is_permission_error(exc):
            raise RuntimeError(
                "Google Sheets access failed. Share spreadsheet "
                f"{spreadsheet_id} with the service account before rerunning."
            ) from exc
        raise


if __name__ == "__main__":
    raise SystemExit(main())
