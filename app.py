#!/usr/bin/env python3
"""Streamlit UI for schema-validator."""

from __future__ import annotations

import io
import json
import os
import re
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from xml.sax.saxutils import escape as xml_escape

import pandas as pd
import streamlit as st

import validator


APP_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT = APP_DIR / "report.html"
DEFAULT_DEPLOYED_GSC_JSON = Path(tempfile.gettempdir()) / "schema-validator-gsc-service-account.json"
DEFAULT_DEPLOYED_GSC_CACHE = Path(tempfile.gettempdir()) / "schema-validator-gsc-cache.json"
DEFAULT_PDF_FONT_PATH = APP_DIR / "assets" / "fonts" / "GotuDevanagari-Regular.ttf"
DEFAULT_PDF_FONT_NAME = "AuditUnicode"
DEFAULT_PDF_FONT_BOLD_NAME = "AuditUnicodeBold"
IST = timezone(timedelta(hours=5, minutes=30))
DEFAULT_LOGIN_SPREADSHEET_ID = "1-wGQoVKu0GqcsHJDT0pCIakEO-bIdXhzmV5ydn4kkNw"
DEFAULT_LOGIN_WORKSHEET_NAME = "seo audit tool login"


def parse_multiline(value: str) -> list[str]:
    if not value:
        return []
    value = value.replace(",", "\n")
    return [line.strip() for line in value.splitlines() if line.strip()]


def dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        output.append(value)
    return output


def classify_targets(values: list[str]) -> tuple[list[str], list[str], list[str]]:
    domains: list[str] = []
    sitemap_urls: list[str] = []
    page_urls: list[str] = []

    for raw in values:
        value = raw.strip()
        if not value:
            continue
        probe = value if "://" in value else f"https://{value}"
        parsed = validator.urlparse(probe)
        if not parsed.netloc:
            continue
        path = parsed.path or ""
        if path.endswith(".xml") or path.endswith(".xml.gz") or "sitemap" in path.lower():
            sitemap_urls.append(probe)
            continue
        if path in ("", "/"):
            domains.append(parsed.netloc)
            continue
        page_urls.append(probe)

    return dedupe(domains), dedupe(sitemap_urls), dedupe(page_urls)


def compute_domains(
    input_domains: list[str], sitemap_urls: list[str], page_urls: list[str]
) -> list[str]:
    domains = list(input_domains)
    for url in sitemap_urls + page_urls:
        parsed = validator.urlparse(url)
        if parsed.netloc:
            domains.append(parsed.netloc)
    return dedupe(domains)


def resolve_gsc_json_path() -> str:
    default_path = Path(validator.DEFAULT_GSC_JSON_PATH)
    if default_path.exists():
        return str(default_path)

    secret_candidates = []
    if "gsc_service_account" in st.secrets:
        secret_candidates.append(st.secrets["gsc_service_account"])
    if "gsc_service_account_json" in st.secrets:
        secret_candidates.append(st.secrets["gsc_service_account_json"])

    env_json = os.environ.get("GSC_SERVICE_ACCOUNT_JSON", "").strip()
    if env_json:
        secret_candidates.append(env_json)

    for candidate in secret_candidates:
        if not candidate:
            continue
        try:
            if isinstance(candidate, str):
                payload = json.loads(candidate)
            else:
                payload = dict(candidate)
            DEFAULT_DEPLOYED_GSC_JSON.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            return str(DEFAULT_DEPLOYED_GSC_JSON)
        except Exception:
            continue

    return ""


def resolve_gsc_cache_path() -> str:
    default_cache = Path(validator.DEFAULT_GSC_CACHE_PATH)
    default_parent = default_cache.parent
    if default_parent.exists() and os.access(default_parent, os.W_OK):
        return str(default_cache)
    return str(DEFAULT_DEPLOYED_GSC_CACHE)


def classify_gsc_bucket(result: validator.UrlCheckResult) -> str:
    if result.gsc_error:
        return "Error"
    if result.gsc_status == "Indexed":
        return "Indexed"
    if result.gsc_status == "Excluded":
        return "Excluded"
    if result.gsc_status in ("Blocked by robots.txt", "Blocked by noindex"):
        return "Blocked"
    if not result.gsc_status:
        return "No GSC Data"
    return "Other"


def build_gsc_rows(report: validator.Report) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for site in report.sites:
        for index, result in enumerate(site.urls):
            rows.append(
                {
                    "Row Key": f"{site.domain}::{index}",
                    "Site": site.domain,
                    "Site Report": site,
                    "URL": result.url,
                    "Result": result,
                    "HTTP": result.http_status or "-",
                    "GSC Category": classify_gsc_bucket(result),
                    "GSC Status": result.gsc_status or "-",
                    "Coverage State": result.gsc_coverage_state or "-",
                    "Indexing State": result.gsc_indexing_state or "-",
                    "Robots State": result.gsc_robots_state or "-",
                    "Page Fetch State": result.gsc_page_fetch_state or "-",
                    "Last Inspected": result.gsc_checked_at or "-",
                    "Last Crawl": result.gsc_last_crawl_time or "-",
                    "Google Canonical": result.gsc_google_canonical or "-",
                    "User Canonical": result.gsc_user_canonical or "-",
                    "GSC Property": result.gsc_property or "-",
                    "GSC Error": result.gsc_error or "-",
                }
            )
    return rows


def iter_report_urls(
    report: validator.Report,
) -> list[tuple[validator.SiteReport, validator.UrlCheckResult, int]]:
    rows: list[tuple[validator.SiteReport, validator.UrlCheckResult, int]] = []
    for site in report.sites:
        for index, result in enumerate(site.urls):
            rows.append((site, result, index))
    return rows


def resolve_login_spreadsheet_id() -> str:
    if "seo_audit_login_spreadsheet_id" in st.secrets:
        return str(st.secrets["seo_audit_login_spreadsheet_id"]).strip()
    if "login_history_spreadsheet_id" in st.secrets:
        return str(st.secrets["login_history_spreadsheet_id"]).strip()
    env_value = os.environ.get("SEO_AUDIT_LOGIN_SPREADSHEET_ID", "").strip()
    if env_value:
        return env_value
    return DEFAULT_LOGIN_SPREADSHEET_ID


def resolve_login_worksheet_name() -> str:
    if "seo_audit_login_worksheet_name" in st.secrets:
        return str(st.secrets["seo_audit_login_worksheet_name"]).strip()
    if "login_history_worksheet_name" in st.secrets:
        return str(st.secrets["login_history_worksheet_name"]).strip()
    env_value = os.environ.get("SEO_AUDIT_LOGIN_WORKSHEET_NAME", "").strip()
    if env_value:
        return env_value
    return DEFAULT_LOGIN_WORKSHEET_NAME


def resolve_login_service_account_json_path() -> str:
    secret_candidates = []
    if "seo_audit_login_service_account" in st.secrets:
        secret_candidates.append(st.secrets["seo_audit_login_service_account"])
    if "seo_audit_login_service_account_json" in st.secrets:
        secret_candidates.append(st.secrets["seo_audit_login_service_account_json"])
    if "gsc_service_account" in st.secrets:
        secret_candidates.append(st.secrets["gsc_service_account"])
    if "gsc_service_account_json" in st.secrets:
        secret_candidates.append(st.secrets["gsc_service_account_json"])

    env_json = os.environ.get("SEO_AUDIT_LOGIN_SERVICE_ACCOUNT_JSON", "").strip()
    if env_json:
        secret_candidates.append(env_json)

    for candidate in secret_candidates:
        if not candidate:
            continue
        try:
            if isinstance(candidate, str):
                payload = json.loads(candidate)
            else:
                payload = dict(candidate)
            DEFAULT_DEPLOYED_GSC_JSON.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            return str(DEFAULT_DEPLOYED_GSC_JSON)
        except Exception:
            continue

    return resolve_gsc_json_path()


def ist_now() -> datetime:
    return datetime.now(IST).replace(microsecond=0)


def iso_to_display(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "-"
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=IST)
        return parsed.astimezone(IST).replace(microsecond=0).isoformat()
    except Exception:
        return raw


def normalize_username(username: str) -> tuple[str, str]:
    value = str(username or "").strip().lower().replace(" ", "")
    if not value:
        return "", "Please enter your Jagran username."
    if "@" in value:
        return "", "Use only the username, without the email domain."
    if not re.fullmatch(r"[a-z0-9._-]+", value):
        return "", "Username can only contain letters, numbers, dot, underscore, or hyphen."
    return value, ""


def build_sheets_service(service_account_json_path: str):
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    credentials = service_account.Credentials.from_service_account_file(
        service_account_json_path,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return build("sheets", "v4", credentials=credentials, cache_discovery=False)


def sheet_range(worksheet_name: str, a1_range: str) -> str:
    escaped = str(worksheet_name).replace("'", "''")
    return f"'{escaped}'!{a1_range}"


def ensure_login_sheet(
    sheets_service,
    spreadsheet_id: str,
    worksheet_name: str,
) -> None:
    metadata = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    titles = {
        sheet["properties"]["title"]
        for sheet in metadata.get("sheets", [])
        if sheet.get("properties", {}).get("title")
    }
    if worksheet_name not in titles:
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": worksheet_name}}}]},
        ).execute()

    header_range = sheet_range(worksheet_name, "A1:C1")
    existing = (
        sheets_service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=header_range)
        .execute()
        .get("values", [])
    )
    if not existing:
        sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=sheet_range(worksheet_name, "A1"),
            valueInputOption="RAW",
            body={"values": [["date", "username", "logged_in_at"]]},
        ).execute()


def append_login_history_row(
    service_account_json_path: str,
    spreadsheet_id: str,
    worksheet_name: str,
    username: str,
    logged_in_at: str,
) -> None:
    if not service_account_json_path:
        raise RuntimeError("Login sheet sync skipped: no usable service-account JSON was resolved.")
    if not spreadsheet_id:
        raise RuntimeError("Login sheet sync skipped: no spreadsheet ID was resolved.")
    print(
        f"[seo-audit-login] writing login row | spreadsheet_id={spreadsheet_id} | worksheet={worksheet_name} | credentials={service_account_json_path}"
    )
    sheets_service = build_sheets_service(service_account_json_path)
    ensure_login_sheet(sheets_service, spreadsheet_id, worksheet_name)
    existing_rows = (
        sheets_service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=sheet_range(worksheet_name, "A:C"))
        .execute()
        .get("values", [])
    )
    target_row = [logged_in_at[:10], username, logged_in_at]
    for row in existing_rows[1:]:
        candidate = [
            row[0] if len(row) > 0 else "",
            row[1] if len(row) > 1 else "",
            row[2] if len(row) > 2 else "",
        ]
        if candidate == target_row:
            print("[seo-audit-login] duplicate login row detected; skipping append")
            return
    row = [target_row]
    response = sheets_service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=sheet_range(worksheet_name, "A1"),
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": row},
    ).execute()
    print(f"[seo-audit-login] append complete | updates={response.get('updates', {})}")


def friendly_login_sheet_error(raw_error: str) -> str:
    message = str(raw_error or "").strip()
    lowered = message.lower()
    if not message:
        return ""
    if "404" in lowered or "requested entity was not found" in lowered:
        return "Login sheet not found. Please recheck the spreadsheet ID and sheet access."
    if "403" in lowered or "permission" in lowered or "access denied" in lowered:
        return "Login sheet access denied. Please share the sheet with the service account."
    return "Login logging could not be completed. Please verify the sheet ID and service-account access."


def require_login(service_account_json_path: str, spreadsheet_id: str, worksheet_name: str) -> tuple[str, str]:
    username = str(st.session_state.get("logged_in_username", "")).strip().lower()
    logged_in_at = str(st.session_state.get("logged_in_at", "")).strip()
    login_sheet_synced = bool(st.session_state.get("login_sheet_synced", False))
    if username and logged_in_at:
        if not login_sheet_synced:
            try:
                append_login_history_row(
                    service_account_json_path,
                    spreadsheet_id,
                    worksheet_name,
                    username,
                    logged_in_at,
                )
                st.session_state["login_sheet_synced"] = True
                st.session_state.pop("login_sheet_error", None)
            except Exception as exc:
                st.session_state["login_sheet_error"] = str(exc)
        return username, logged_in_at

    st.markdown(
        """
<div class="header-bar">
  <div class="header-title">Schema & Sitemap Validator</div>
  <div class="header-sub">Log in with your Jagran username to access the audit dashboard.</div>
</div>
""",
        unsafe_allow_html=True,
    )
    with st.form("login_form", clear_on_submit=False):
        username_input = st.text_input("Username", placeholder="firstname.lastname")
        submitted = st.form_submit_button("Continue", use_container_width=True)

    if submitted:
        normalized, error = normalize_username(username_input)
        if error:
            st.error(error)
        else:
            logged_in_at = ist_now().isoformat()
            st.session_state["logged_in_username"] = normalized
            st.session_state["logged_in_at"] = logged_in_at
            st.session_state["login_sheet_synced"] = False
            try:
                append_login_history_row(
                    service_account_json_path,
                    spreadsheet_id,
                    worksheet_name,
                    normalized,
                    logged_in_at,
                )
                st.session_state["login_sheet_synced"] = True
                st.session_state.pop("login_sheet_error", None)
            except Exception as exc:
                st.session_state["login_sheet_error"] = str(exc)
            st.rerun()
    st.stop()


def render_app_header(username: str) -> None:
    title_col, action_col = st.columns([12, 1])
    with title_col:
        st.markdown(
            """
<div class="header-bar">
  <div class="header-title">Schema & Sitemap Validator</div>
  <div class="header-sub">One run, full SEO audit with schema + sitemap insights.</div>
</div>
""",
            unsafe_allow_html=True,
        )
    with action_col:
        st.markdown("<div style='height: 0.9rem;'></div>", unsafe_allow_html=True)
        if st.button("⎋", help=f"Log out ({username})", key="logout_icon_button", use_container_width=True):
            st.session_state.pop("logged_in_username", None)
            st.session_state.pop("logged_in_at", None)
            st.session_state.pop("login_sheet_synced", None)
            st.rerun()


def _pdf_modules():
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
        from reportlab.lib.units import inch
        from reportlab.pdfbase import pdfmetrics
        from reportlab.pdfbase.ttfonts import TTFont
        from reportlab.platypus import (
            ListFlowable,
            ListItem,
            PageBreak,
            Paragraph,
            SimpleDocTemplate,
            Spacer,
            Table,
            TableStyle,
        )
    except ImportError as exc:
        raise RuntimeError(
            "PDF export requires the 'reportlab' package. Please install the updated requirements."
        ) from exc

    return {
        "A4": A4,
        "colors": colors,
        "getSampleStyleSheet": getSampleStyleSheet,
        "ParagraphStyle": ParagraphStyle,
        "SimpleDocTemplate": SimpleDocTemplate,
        "Paragraph": Paragraph,
        "Spacer": Spacer,
        "Table": Table,
        "TableStyle": TableStyle,
        "ListFlowable": ListFlowable,
        "ListItem": ListItem,
        "PageBreak": PageBreak,
        "inch": inch,
        "pdfmetrics": pdfmetrics,
        "TTFont": TTFont,
    }


def _safe_text(value: object) -> str:
    if value is None:
        return "-"
    text = str(value).strip()
    return text if text else "-"


def _resolve_pdf_font_path() -> Path | None:
    candidates = [
        DEFAULT_PDF_FONT_PATH,
        Path("/System/Library/AssetsV2/com_apple_MobileAsset_Font7/3c0538e5098b7ae593a24c89e80fd5949e873ce1.asset/AssetData/GotuDevanagari-Regular.ttf"),
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def _register_pdf_fonts(modules: dict) -> dict[str, str]:
    pdfmetrics = modules["pdfmetrics"]
    TTFont = modules["TTFont"]
    font_path = _resolve_pdf_font_path()
    if not font_path:
        return {"regular": "Helvetica", "bold": "Helvetica-Bold"}

    registered = set(pdfmetrics.getRegisteredFontNames())
    if DEFAULT_PDF_FONT_NAME not in registered:
        pdfmetrics.registerFont(TTFont(DEFAULT_PDF_FONT_NAME, str(font_path)))
    if DEFAULT_PDF_FONT_BOLD_NAME not in registered:
        pdfmetrics.registerFont(TTFont(DEFAULT_PDF_FONT_BOLD_NAME, str(font_path)))
    return {"regular": DEFAULT_PDF_FONT_NAME, "bold": DEFAULT_PDF_FONT_BOLD_NAME}


def _pdf_paragraph_text(value: object) -> str:
    text = _safe_text(value)
    text = xml_escape(text)
    return text.replace("\n", "<br/>")


def _build_pdf_story_helpers():
    modules = _pdf_modules()
    fonts = _register_pdf_fonts(modules)
    styles = modules["getSampleStyleSheet"]()
    styles["Normal"].fontName = fonts["regular"]
    styles["BodyText"].fontName = fonts["regular"]
    styles["Title"].fontName = fonts["bold"]
    styles["Title"].fontSize = 22
    styles["Title"].leading = 26
    styles["Title"].textColor = modules["colors"].HexColor("#0f172a")
    styles["Heading2"].fontName = fonts["bold"]
    styles["Heading3"].fontName = fonts["bold"]
    styles["Heading3"].fontSize = 11
    styles["Heading3"].leading = 14
    styles["Heading3"].spaceBefore = 8
    styles["Heading3"].spaceAfter = 4
    styles.add(
        modules["ParagraphStyle"](
            name="SectionHeading",
            parent=styles["Heading2"],
            fontName=fonts["bold"],
            fontSize=13,
            leading=16,
            spaceBefore=10,
            spaceAfter=6,
            textColor=modules["colors"].HexColor("#0f172a"),
        )
    )
    styles.add(
        modules["ParagraphStyle"](
            name="LeadBody",
            parent=styles["BodyText"],
            fontName=fonts["regular"],
            fontSize=10,
            leading=14,
            textColor=modules["colors"].HexColor("#475569"),
            spaceAfter=8,
        )
    )
    styles.add(
        modules["ParagraphStyle"](
            name="SmallBody",
            parent=styles["BodyText"],
            fontName=fonts["regular"],
            fontSize=9,
            leading=12,
            textColor=modules["colors"].HexColor("#334155"),
        )
    )
    styles.add(
        modules["ParagraphStyle"](
            name="LabelBody",
            parent=styles["BodyText"],
            fontName=fonts["bold"],
            fontSize=9,
            leading=12,
            textColor=modules["colors"].HexColor("#0f172a"),
            spaceAfter=2,
        )
    )
    return modules, styles


def _pdf_section_title(story: list, modules: dict, styles, title: str, intro: str = "") -> None:
    story.append(modules["Paragraph"](_pdf_paragraph_text(title), styles["SectionHeading"]))
    if intro:
        story.append(modules["Paragraph"](_pdf_paragraph_text(intro), styles["LeadBody"]))


def _kv_table(story: list, modules: dict, styles, items: list[tuple[str, object]]) -> None:
    rows = [
        [
            modules["Paragraph"]("<b>Field</b>", styles["LabelBody"]),
            modules["Paragraph"]("<b>Value</b>", styles["LabelBody"]),
        ]
    ]
    for label, value in items:
        rows.append(
            [
                modules["Paragraph"](_pdf_paragraph_text(label), styles["LabelBody"]),
                modules["Paragraph"](_pdf_paragraph_text(value), styles["SmallBody"]),
            ]
        )
    table = modules["Table"](
        rows,
        colWidths=[1.75 * modules["inch"], 5.05 * modules["inch"]],
        repeatRows=1,
    )
    table.setStyle(
        modules["TableStyle"](
            [
                ("BACKGROUND", (0, 0), (-1, 0), modules["colors"].HexColor("#e2e8f0")),
                ("TEXTCOLOR", (0, 0), (-1, 0), modules["colors"].HexColor("#0f172a")),
                ("GRID", (0, 0), (-1, -1), 0.5, modules["colors"].HexColor("#cbd5e1")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("TOPPADDING", (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [
                    modules["colors"].white,
                    modules["colors"].HexColor("#f8fafc"),
                ]),
            ]
        )
    )
    story.append(table)
    story.append(modules["Spacer"](1, 0.16 * modules["inch"]))


def _detail_blocks(story: list, modules: dict, styles, items: list[tuple[str, object]]) -> None:
    if not items:
        story.append(modules["Paragraph"]("No detail available.", styles["SmallBody"]))
        return
    for index, (label, value) in enumerate(items):
        if index:
            story.append(modules["Spacer"](1, 0.06 * modules["inch"]))
        story.append(modules["Paragraph"](_pdf_paragraph_text(label), styles["LabelBody"]))
        story.append(modules["Paragraph"](_pdf_paragraph_text(value), styles["SmallBody"]))
    story.append(modules["Spacer"](1, 0.16 * modules["inch"]))


def _schema_label(name: str) -> str:
    return (
        str(name or "")
        .replace("_", " ")
        .replace("Of", "of")
        .replace("Url", "URL")
        .title()
    )


def _schema_value_pairs(obj: dict[str, str]) -> list[tuple[str, str]]:
    raw_pairs = str(obj.get("properties_used_values", "") or "")
    output: list[tuple[str, str]] = []
    for part in raw_pairs.split("||"):
        cleaned = part.strip()
        if not cleaned:
            continue
        if ":" in cleaned:
            key, value = cleaned.split(":", 1)
        else:
            key, value = cleaned, ""
        output.append((key.strip(), value.strip()))
    return output


def _schema_field_items(obj: dict[str, str], fields: list[str]) -> list[tuple[str, object]]:
    items: list[tuple[str, object]] = []
    for field in fields:
        value = obj.get(field, "")
        if not value and field not in {
            "properties_used",
            "allowed_properties",
            "missing_required",
            "missing_recommended",
            "source",
        }:
            continue
        if field == "properties_used":
            prop_pairs = _schema_value_pairs(obj)
            if prop_pairs:
                items.append(("Properties Used", "\n".join(f"{key}: {value}" for key, value in prop_pairs)))
            else:
                items.append(("Properties Used", value or "—"))
            continue
        if field == "allowed_properties":
            count = obj.get("allowed_properties_count", "")
            full_list = obj.get("allowed_properties_full", "") or value
            if full_list:
                prefix = f"{count} schema.org properties\n" if count else ""
                items.append(("Allowed Properties", prefix + full_list))
            else:
                items.append(("Allowed Properties", "—"))
            continue
        items.append((_schema_label(field), value or "—"))
    return items


def _append_schema_object_card(
    story: list,
    modules: dict,
    styles,
    title: str,
    obj: dict[str, str],
    fields: list[str],
) -> None:
    story.append(modules["Paragraph"](_pdf_paragraph_text(title), styles["Heading3"]))
    _detail_blocks(story, modules, styles, _schema_field_items(obj, fields))


def _append_schema_details_section(
    story: list,
    modules: dict,
    styles,
    result: validator.UrlCheckResult,
) -> None:
    if not (result.schema_objects or result.microdata_objects or result.rdfa_objects):
        story.append(modules["Paragraph"]("No structured data objects were captured.", styles["SmallBody"]))
        story.append(modules["Spacer"](1, 0.12 * modules["inch"]))
        return

    root_objects = [
        obj for obj in result.schema_objects if obj.get("source") in ("", None, "root")
    ]
    nested_objects = [
        obj for obj in result.schema_objects if obj.get("source") not in ("", None, "root")
    ]
    nested_by_parent: dict[str, list[dict[str, str]]] = {}
    for obj in nested_objects:
        source = obj.get("source", "")
        parent = source.split(".", 1)[0] if source else "Unknown"
        nested_by_parent.setdefault(parent, []).append(obj)

    def root_key(obj: dict[str, str]) -> str:
        type_field = obj.get("type", "")
        return type_field.split(",", 1)[0].strip() if type_field else "Unknown"

    if root_objects:
        story.append(
            modules["Paragraph"](
                _pdf_paragraph_text("JSON-LD Root Objects"),
                styles["Heading3"],
            )
        )
        story.append(modules["Spacer"](1, 0.04 * modules["inch"]))
        for root in root_objects:
            label_type = root.get("type", "Object")
            label_name = root.get("name", "")
            label = f"{label_type} — {label_name}" if label_name else label_type
            _append_schema_object_card(
                story,
                modules,
                styles,
                label,
                root,
                ["type"] + list(validator.SCHEMA_SUMMARY_FIELDS),
            )
            parent_key = root_key(root)
            nested = nested_by_parent.get(parent_key, [])
            for nested_obj in nested:
                nested_title = nested_obj.get("type", "Nested Object")
                _append_schema_object_card(
                    story,
                    modules,
                    styles,
                    f"Nested: {nested_title}",
                    nested_obj,
                    list(validator.NESTED_SCHEMA_FIELDS),
                )

    remaining_nested = []
    used_parents = {root_key(obj) for obj in root_objects}
    for parent, items in nested_by_parent.items():
        if parent not in used_parents:
            remaining_nested.extend(items)
    if remaining_nested:
        story.append(
            modules["Paragraph"](
                _pdf_paragraph_text("JSON-LD Nested Objects (Other)"),
                styles["Heading3"],
            )
        )
        for nested_obj in remaining_nested:
            nested_title = nested_obj.get("type", "Nested Object")
            _append_schema_object_card(
                story,
                modules,
                styles,
                nested_title,
                nested_obj,
                list(validator.NESTED_SCHEMA_FIELDS),
            )

    if result.microdata_objects:
        story.append(modules["Paragraph"](_pdf_paragraph_text("Microdata Objects"), styles["Heading3"]))
        for obj in result.microdata_objects:
            _append_schema_object_card(
                story,
                modules,
                styles,
                obj.get("type", "Microdata Object"),
                obj,
                ["type"] + list(validator.SCHEMA_SUMMARY_FIELDS),
            )

    if result.rdfa_objects:
        story.append(modules["Paragraph"](_pdf_paragraph_text("RDFa Objects"), styles["Heading3"]))
        for obj in result.rdfa_objects:
            _append_schema_object_card(
                story,
                modules,
                styles,
                obj.get("type", "RDFa Object"),
                obj,
                ["type"] + list(validator.SCHEMA_SUMMARY_FIELDS),
            )


def _bullet_list(story: list, modules: dict, styles, values: list[str]) -> None:
    cleaned = [value.strip() for value in values if str(value).strip()]
    if not cleaned:
        story.append(modules["Paragraph"]("None", styles["SmallBody"]))
        return
    story.append(
        modules["ListFlowable"](
            [
                modules["ListItem"](modules["Paragraph"](_pdf_paragraph_text(value), styles["SmallBody"]))
                for value in cleaned
            ],
            bulletType="bullet",
            start="circle",
            leftIndent=16,
        )
    )


def _content_summary_text(result: validator.UrlCheckResult) -> str:
    parts: list[str] = []
    if result.word_count:
        source = f" ({result.word_count_source})" if result.word_count_source else ""
        parts.append(f"{result.word_count} words{source}")
    parts.append(
        f"H1:{result.heading_h1_count} H2:{result.heading_h2_count} H3:{result.heading_h3_count}"
    )
    if result.feature_image_status:
        parts.append(f"Feature image alt: {result.feature_image_status}")
    return " | ".join(parts)


def _recommendations_for_result(result: validator.UrlCheckResult) -> list[str]:
    recommendations: list[str] = []
    recommendations.extend(result.seo_issues)
    recommendations.extend(result.seo_warnings)
    recommendations.extend(result.issues)
    recommendations.extend(result.warnings)
    deduped: list[str] = []
    seen: set[str] = set()
    for item in recommendations:
        cleaned = str(item).strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        deduped.append(cleaned)
    return deduped


def _meta_snapshot_rows(result: validator.UrlCheckResult) -> list[tuple[str, object]]:
    meta = result.seo_meta or {}
    preferred_keys = [
        "title",
        "meta_description",
        "canonical_url",
        "og:title",
        "og:description",
        "og:image",
        "twitter:title",
        "twitter:description",
        "twitter:image",
    ]
    rows: list[tuple[str, object]] = []
    for key in preferred_keys:
        if meta.get(key):
            rows.append((key, meta.get(key)))
    if not rows and meta:
        rows = sorted(meta.items())
    return rows


def _sitemap_snapshot_rows(site: validator.SiteReport) -> list[tuple[str, object]]:
    rows: list[tuple[str, object]] = []
    for index, sitemap in enumerate(site.sitemaps, start=1):
        status = sitemap.status_code if sitemap.status_code is not None else "-"
        seo_checks: list[str] = []
        if sitemap.urls_found:
            seo_checks.append(f"entries: {sitemap.urls_found}")
        if sitemap.changefreq_missing:
            seo_checks.append(f"changefreq missing: {sitemap.changefreq_missing}")
        if sitemap.lastmod_stale:
            seo_checks.append(f"stale lastmod: {sitemap.lastmod_stale}")
        if sitemap.news_entries:
            seo_checks.append(f"news entries: {sitemap.news_entries}")
        summary = f"Status {status}"
        if sitemap.kind:
            summary += f" | Type {sitemap.kind}"
        if seo_checks:
            summary += " | " + " | ".join(seo_checks)
        if sitemap.error:
            summary += f" | Error: {sitemap.error}"
        rows.append((f"Sitemap {index}", f"{sitemap.url}\n{summary}"))
    return rows


def _url_findings(result: validator.UrlCheckResult) -> tuple[list[str], list[str]]:
    issue_items = list(result.issues) + list(result.seo_issues)
    warning_items = list(result.warnings) + list(result.seo_warnings)
    deduped_issues: list[str] = []
    deduped_warnings: list[str] = []
    seen_issues: set[str] = set()
    seen_warnings: set[str] = set()
    for item in issue_items:
        cleaned = str(item).strip()
        if cleaned and cleaned not in seen_issues:
            seen_issues.add(cleaned)
            deduped_issues.append(cleaned)
    for item in warning_items:
        cleaned = str(item).strip()
        if cleaned and cleaned not in seen_warnings:
            seen_warnings.add(cleaned)
            deduped_warnings.append(cleaned)
    return deduped_issues, deduped_warnings


def _append_url_report_sections(
    story: list,
    modules: dict,
    styles,
    site: validator.SiteReport,
    result: validator.UrlCheckResult,
    *,
    heading: str,
    intro: str,
) -> None:
    issues, warnings = _url_findings(result)
    _pdf_section_title(story, modules, styles, heading, intro)
    _kv_table(
        story,
        modules,
        styles,
        [
            ("Site", site.domain),
            ("URL", result.url),
            ("HTTP Status", result.http_status),
            ("Indexability", result.indexability_status),
            ("Indexability Reasons", " | ".join(result.indexability_reasons) if result.indexability_reasons else "-"),
            ("Final URL", result.final_url),
            ("Canonical", result.seo_meta.get("canonical_url", "")),
            ("GSC Status", result.gsc_status),
            ("Coverage State", result.gsc_coverage_state),
            ("Page Fetch State", result.gsc_page_fetch_state),
            ("Robots State", result.gsc_robots_state),
            ("Last Crawl", result.gsc_last_crawl_time),
            ("Content Summary", _content_summary_text(result)),
            ("Feature Image Alt", result.feature_image_alt or result.feature_image_status),
            ("Schema Types", ", ".join(result.jsonld_types) if result.jsonld_types else "-"),
        ],
    )

    _pdf_section_title(
        story,
        modules,
        styles,
        "On-Page Metadata",
        "These are the most important on-page SEO fields captured for this URL.",
    )
    _detail_blocks(
        story,
        modules,
        styles,
        _meta_snapshot_rows(result) or [("Meta", "No meta fields captured")],
    )

    _pdf_section_title(
        story,
        modules,
        styles,
        "Issues",
        "These are the key blockers or implementation gaps found for this URL.",
    )
    _bullet_list(story, modules, styles, issues)
    story.append(modules["Spacer"](1, 0.12 * modules["inch"]))

    _pdf_section_title(
        story,
        modules,
        styles,
        "Warnings",
        "Warnings are lower-severity issues or optimisation opportunities for this URL.",
    )
    _bullet_list(story, modules, styles, warnings)
    story.append(modules["Spacer"](1, 0.12 * modules["inch"]))

    _pdf_section_title(
        story,
        modules,
        styles,
        "Recommended Actions",
        "Use these recommended actions to guide implementation or editorial fixes for this page.",
    )
    _bullet_list(story, modules, styles, _recommendations_for_result(result))
    story.append(modules["Spacer"](1, 0.12 * modules["inch"]))

    _pdf_section_title(
        story,
        modules,
        styles,
        "Schema Details",
        "This section expands the structured data captured for the URL, including root objects, nested objects, and key properties/values.",
    )
    _append_schema_details_section(story, modules, styles, result)


def build_url_pdf(site: validator.SiteReport, result: validator.UrlCheckResult) -> bytes:
    modules, styles = _build_pdf_story_helpers()
    buffer = io.BytesIO()
    doc = modules["SimpleDocTemplate"](
        buffer,
        pagesize=modules["A4"],
        leftMargin=36,
        rightMargin=36,
        topMargin=36,
        bottomMargin=36,
        title=result.url,
    )
    story: list = []
    story.append(modules["Paragraph"]("SEO Audit URL Report", styles["Title"]))
    story.append(
        modules["Paragraph"](
            _pdf_paragraph_text(
                "A focused audit for one URL covering crawlability, on-page SEO, structured data, and GSC status."
            ),
            styles["LeadBody"],
        )
    )
    story.append(modules["Paragraph"](_pdf_paragraph_text(result.url), styles["SmallBody"]))
    story.append(modules["Spacer"](1, 0.18 * modules["inch"]))
    _append_url_report_sections(
        story,
        modules,
        styles,
        site,
        result,
        heading="Executive Snapshot",
        intro="This section gives a complete overview of how this page is performing across crawlability, metadata, and structured data.",
    )

    doc.build(story)
    return buffer.getvalue()


def build_report_pdf(report: validator.Report, summary: dict[str, object]) -> bytes:
    modules, styles = _build_pdf_story_helpers()
    buffer = io.BytesIO()
    doc = modules["SimpleDocTemplate"](
        buffer,
        pagesize=modules["A4"],
        leftMargin=36,
        rightMargin=36,
        topMargin=36,
        bottomMargin=36,
        title="SEO Audit Report",
    )
    story: list = []
    story.append(modules["Paragraph"]("SEO Audit Report", styles["Title"]))
    story.append(
        modules["Paragraph"](
            _pdf_paragraph_text(
                "A consolidated report covering technical SEO, metadata, schema, sitemap quality, and GSC findings."
            ),
            styles["LeadBody"],
        )
    )
    story.append(
        modules["Paragraph"](
            _pdf_paragraph_text(f"Generated: {_safe_text(report.generated_at)}"),
            styles["SmallBody"],
        )
    )
    story.append(modules["Spacer"](1, 0.18 * modules["inch"]))

    _pdf_section_title(
        story,
        modules,
        styles,
        "Executive Summary",
        "This section rolls up the main audit scores and topline outcomes across all analysed URLs.",
    )
    _kv_table(
        story,
        modules,
        styles,
        [
            ("Overall Score", f"{summary.get('score', 0)}/100 ({summary.get('grade', '-')})"),
            ("SEO Score", f"{summary.get('seo_score', 0)}/100 ({summary.get('seo_grade', '-')})"),
            ("Schema Score", f"{summary.get('schema_score', 0)}/100 ({summary.get('schema_grade', '-')})"),
            ("Meta Coverage", f"{summary.get('meta_coverage_pct', 0)}%"),
            ("URLs Tested", sum(len(site.urls) for site in report.sites)),
            ("Sites Audited", len(report.sites)),
        ],
    )

    _pdf_section_title(
        story,
        modules,
        styles,
        "Highlights",
        "Quick takeaways from the run so a stakeholder can understand the state of the audit fast.",
    )
    _bullet_list(story, modules, styles, [str(item) for item in summary.get("highlights", [])])
    story.append(modules["Spacer"](1, 0.12 * modules["inch"]))

    _pdf_section_title(
        story,
        modules,
        styles,
        "Top Fixes",
        "These are the most repeated fixes across the audited set and are good candidates for prioritisation.",
    )
    fixes = [f"{message} ({count})" for message, count in summary.get("top_fixes", [])]
    _bullet_list(story, modules, styles, fixes)
    story.append(modules["Spacer"](1, 0.12 * modules["inch"]))

    for site_index, site in enumerate(report.sites):
        _pdf_section_title(
            story,
            modules,
            styles,
            f"Site Summary: {site.domain}",
            "This section shows the robots and audit context for the site, followed by page-level summaries.",
        )
        _kv_table(
            story,
            modules,
            styles,
            [
                ("Robots URL", site.robots_url),
                ("Robots Status", site.robots_status),
                ("Robots Error", site.robots_error),
                ("Sitemaps Checked", len(site.sitemaps)),
                ("URLs Audited", len(site.urls)),
            ],
        )
        if site.notes:
            _pdf_section_title(
                story,
                modules,
                styles,
                "Site Notes",
                "Run-level site notes captured during discovery and validation.",
            )
            _bullet_list(story, modules, styles, site.notes)
            story.append(modules["Spacer"](1, 0.12 * modules["inch"]))
        if site.sitemaps:
            _pdf_section_title(
                story,
                modules,
                styles,
                "Sitemap Snapshot",
                "This is a compact view of the sitemap files checked for this site and the important observations recorded against them.",
            )
            _kv_table(
                story,
                modules,
                styles,
                _sitemap_snapshot_rows(site),
            )
        story.append(modules["PageBreak"]())

        for result_index, result in enumerate(site.urls):
            story.append(modules["Paragraph"]("URL Detail", styles["SectionHeading"]))
            story.append(modules["Paragraph"](_pdf_paragraph_text(result.url), styles["Heading3"]))
            story.append(
                modules["Paragraph"](
                    _pdf_paragraph_text(
                        "This page mirrors the detailed audit view so the exported PDF can be shared independently with editorial, product, or engineering teams."
                    ),
                    styles["LeadBody"],
                )
            )
            _append_url_report_sections(
                story,
                modules,
                styles,
                site,
                result,
                heading="Page Audit Summary",
                intro="The sections below capture the same core findings shown in the app for this audited URL.",
            )
            if result_index < len(site.urls) - 1 or site_index < len(report.sites) - 1:
                story.append(modules["PageBreak"]())

    doc.build(story)
    return buffer.getvalue()


st.set_page_config(page_title="Schema & Sitemap Validator", layout="wide")

st.markdown(
    """
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600&family=Libre+Franklin:wght@500;600&display=swap');
html, body, [class*="css"] {
  font-family: 'IBM Plex Sans', sans-serif;
}
.stApp, .stMarkdown, .stMarkdown * {
  color: #0f172a;
}
.stApp {
  background: #f3f4f6;
}
.block-container {
  padding-top: 2.5rem;
  max-width: 1180px;
}
.stVerticalBlock {
  gap: 8px;
}
.stVerticalBlock > div {
  margin-top: 0;
  margin-bottom: 0;
}
.stAlert {
  margin-top: 8px;
}
.stForm {
  margin-top: 0;
}
div[data-testid="stForm"] {
  margin-top: 0;
}
.stForm > div {
  gap: 8px;
}
.header-bar {
  display: flex;
  flex-direction: column;
  gap: 6px;
  background: #ffffff;
  border: 1px solid #e5e7eb;
  border-radius: 16px;
  padding: 18px 22px;
  box-shadow: 0 12px 30px rgba(15, 23, 42, 0.08);
  margin-bottom: 18px;
  overflow: visible;
  margin-top: 6px;
}
.header-title {
  font-family: 'Libre Franklin', sans-serif;
  font-size: 24px;
  font-weight: 600;
  color: #0f172a;
  line-height: 1.25;
  padding-top: 2px;
}
.header-sub {
  font-size: 13px;
  color: #475569;
}
.section-title {
  font-family: 'Libre Franklin', sans-serif;
  font-size: 14px;
  font-weight: 600;
  letter-spacing: 0.02em;
  color: #111827;
  margin-bottom: 8px;
  text-transform: uppercase;
}
.field-label {
  font-size: 12px;
  font-weight: 600;
  color: #111827;
  opacity: 1;
  margin: 2px 0 4px;
  text-transform: uppercase;
  letter-spacing: 0.02em;
}
label[data-testid="stWidgetLabel"] {
  margin-bottom: 2px;
}
.stTextArea, .stTextInput, .stNumberInput, .stSelectbox, .stCheckbox {
  margin-bottom: 6px;
}
.stTextArea textarea {
  min-height: 72px;
}
.stSelectbox div[data-baseweb="select"] {
  border-radius: 10px;
}
.stSelectbox div[role="listbox"] {
  border-radius: 10px;
}
.stCheckbox {
  padding-top: 6px;
}
.stExpander {
  margin-top: 6px;
}
.stButton, .stDownloadButton {
  margin-top: 6px;
}
div[data-testid="column"]:nth-child(2) .stNumberInput {
  max-width: 260px;
}
div[data-testid="stForm"] {
  background: #ffffff;
  border: 1px solid #e5e7eb;
  border-radius: 18px;
  padding: 22px;
  box-shadow: 0 10px 24px rgba(15, 23, 42, 0.06);
}
.stTextArea textarea, .stTextInput input, .stNumberInput input {
  background: #ffffff;
  color: #111827;
  border: 1px solid #d1d5db;
  border-radius: 10px;
}
.stTextArea textarea:focus, .stTextInput input:focus, .stNumberInput input:focus {
  border-color: #1d4ed8;
  box-shadow: 0 0 0 3px rgba(29, 78, 216, 0.18);
}
.stRadio div[role="radiogroup"] {
  background: #f8fafc;
  border: 1px solid #e2e8f0;
  padding: 10px 12px;
  border-radius: 12px;
}
.stCheckbox label, .stRadio label {
  color: #1f2937 !important;
  font-weight: 500;
  opacity: 1 !important;
}
label[data-testid="stWidgetLabel"] {
  color: #111827 !important;
  font-weight: 600;
  opacity: 1 !important;
}
.stCheckbox label, .stSelectbox label {
  color: #111827 !important;
}
.stCheckbox * {
  opacity: 1 !important;
}
.stCheckbox label span {
  color: #111827 !important;
}
.stCheckbox label div, .stCheckbox label p {
  color: #111827 !important;
}
.stCheckbox label div[data-testid="stMarkdownContainer"] p {
  color: #111827 !important;
  opacity: 1 !important;
}
.stCaption, .stCaption * {
  color: #475569 !important;
  opacity: 1 !important;
}
.stButton>button, .stDownloadButton>button {
  background: #111827;
  color: #ffffff !important;
  -webkit-text-fill-color: #ffffff !important;
  text-shadow: 0 0 0 #ffffff;
  border-radius: 12px;
  padding: 0.7rem 1.2rem;
  border: none;
  width: 100%;
  font-weight: 600;
  opacity: 1 !important;
}
.stButton>button *, .stDownloadButton>button * {
  color: #ffffff !important;
  -webkit-text-fill-color: #ffffff !important;
  opacity: 1 !important;
}
.stButton>button span, .stButton>button p, .stButton>button div,
.stDownloadButton>button span, .stDownloadButton>button p, .stDownloadButton>button div {
  color: #ffffff !important;
  -webkit-text-fill-color: #ffffff !important;
}
.stButton button[kind], .stButton button[kind] *,
.stDownloadButton button[kind], .stDownloadButton button[kind] * {
  color: #ffffff !important;
  -webkit-text-fill-color: #ffffff !important;
  opacity: 1 !important;
}
div[data-testid="stForm"] .stButton > button,
div[data-testid="stForm"] .stButton > button *,
div[data-testid="stForm"] .stDownloadButton > button,
div[data-testid="stForm"] .stDownloadButton > button * {
  color: #ffffff !important;
  -webkit-text-fill-color: #ffffff !important;
  opacity: 1 !important;
}
div[data-testid="stFormSubmitButton"] button,
div[data-testid="stFormSubmitButton"] button * {
  color: #ffffff !important;
  -webkit-text-fill-color: #ffffff !important;
  opacity: 1 !important;
}
button[data-testid="baseButton-primary"],
button[data-testid="baseButton-primary"] * {
  color: #ffffff !important;
  -webkit-text-fill-color: #ffffff !important;
  opacity: 1 !important;
}
.stButton button,
.stButton button *,
.stDownloadButton button,
.stDownloadButton button * {
  color: #ffffff !important;
  -webkit-text-fill-color: #ffffff !important;
  text-shadow: 0 0 0 #ffffff;
  opacity: 1 !important;
}
.stButton button:disabled,
.stButton button:disabled *,
.stDownloadButton button:disabled,
.stDownloadButton button:disabled * {
  color: #ffffff !important;
  -webkit-text-fill-color: #ffffff !important;
  opacity: 1 !important;
}
.stButton>button:hover, .stDownloadButton>button:hover {
  background: #111827 !important;
  color: #ffffff !important;
  -webkit-text-fill-color: #ffffff !important;
  filter: none !important;
  box-shadow: none !important;
}
.stButton>button:active,
.stButton>button:focus,
.stButton>button:focus-visible,
.stDownloadButton>button:active,
.stDownloadButton>button:focus,
.stDownloadButton>button:focus-visible {
  background: #111827 !important;
  color: #ffffff !important;
  -webkit-text-fill-color: #ffffff !important;
  box-shadow: none !important;
  filter: none !important;
}
.stButton>button:disabled,
.stButton>button[disabled],
.stDownloadButton>button:disabled,
.stDownloadButton>button[disabled] {
  background: #9ca3af !important;
  color: #ffffff !important;
  -webkit-text-fill-color: #ffffff !important;
  cursor: not-allowed !important;
  filter: none !important;
}
.stButton>button:disabled:hover,
.stButton>button[disabled]:hover,
.stDownloadButton>button:disabled:hover,
.stDownloadButton>button[disabled]:hover {
  background: #9ca3af !important;
  color: #ffffff !important;
  -webkit-text-fill-color: #ffffff !important;
  filter: none !important;
}
.stExpander {
  background: #f8fafc;
  border-radius: 14px;
  border: 1px solid #e2e8f0;
  padding: 6px 8px;
}
.stForm div[data-testid="column"] {
  gap: 8px;
}
.stForm div[data-testid="stHorizontalBlock"] {
  gap: 12px;
}
div[data-testid="stMetric"] {
  background: #ffffff;
  border: 1px solid #e5e7eb;
  border-radius: 12px;
  padding: 12px;
  box-shadow: 0 6px 14px rgba(15, 23, 42, 0.05);
}
div[data-testid="stMetricLabel"] {
  color: #475569 !important;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.04em;
  font-size: 11px;
  opacity: 1 !important;
}
div[data-testid="stMetricValue"] {
  color: #0f172a !important;
  font-size: 26px;
  font-weight: 700;
}
div[data-testid="stMetric"] * {
  opacity: 1 !important;
}
.stMetric label, .stMetric span, .stMetric p {
  color: #475569 !important;
  opacity: 1 !important;
}
</style>
""",
    unsafe_allow_html=True,
)

schemaorg_ref_path = str(validator.DEFAULT_SCHEMAORG_REF_PATH)
schemaorg_data_path = str(validator.DEFAULT_SCHEMAORG_DATA_PATH)
rules_path = str(validator.DEFAULT_RULES_PATH)
gsc_json_path = resolve_gsc_json_path()
login_service_account_json_path = resolve_login_service_account_json_path()
login_spreadsheet_id = resolve_login_spreadsheet_id()
login_worksheet_name = resolve_login_worksheet_name()
gsc_cache_path = resolve_gsc_cache_path()
gsc_cache_ttl_hours = validator.DEFAULT_GSC_CACHE_TTL_HOURS
user_agent = validator.DEFAULT_USER_AGENT
output_path = str(DEFAULT_OUTPUT)
download_schemaorg = True

if "latest_report" not in st.session_state:
    st.session_state["latest_report"] = None
if "latest_summary" not in st.session_state:
    st.session_state["latest_summary"] = None
if "latest_run_config" not in st.session_state:
    st.session_state["latest_run_config"] = None
if "latest_run_requested_gsc" not in st.session_state:
    st.session_state["latest_run_requested_gsc"] = False
if "active_url_detail" not in st.session_state:
    st.session_state["active_url_detail"] = ""
if "logged_in_username" not in st.session_state:
    st.session_state["logged_in_username"] = ""
if "logged_in_at" not in st.session_state:
    st.session_state["logged_in_at"] = ""

logged_in_username, logged_in_at = require_login(
    login_service_account_json_path,
    login_spreadsheet_id,
    login_worksheet_name,
)
render_app_header(logged_in_username)


def run_validation_audit(
    *,
    targets_text_value: str,
    max_urls_value: int,
    use_schemaorg_value: bool,
    include_gsc: bool,
) -> None:
    targets = parse_multiline(targets_text_value)
    input_domains, sitemap_urls, page_urls = classify_targets(targets)
    domains = compute_domains(input_domains, sitemap_urls, page_urls)

    if not domains:
        st.error("Please enter at least one valid domain, sitemap URL, or page URL.")
        st.stop()

    if sitemap_urls:
        sitemap_mode = "explicit"
    elif input_domains:
        sitemap_mode = "robots"
    else:
        sitemap_mode = "disabled"

    rules = validator.load_schema_rules(rules_path)
    schemaorg_ref = {}
    if use_schemaorg_value:
        schemaorg_ref = validator.load_schemaorg_reference(schemaorg_ref_path, schemaorg_data_path)
        if download_schemaorg and not schemaorg_ref:
            downloaded = validator.download_schemaorg_data(
                validator.SCHEMAORG_DATA_URL, schemaorg_data_path
            )
            if downloaded:
                schemaorg_ref = validator.load_schemaorg_reference(
                    schemaorg_ref_path, schemaorg_data_path
                )

    sitemap_urls_by_domain = validator.group_by_domain(sitemap_urls)
    page_urls_by_domain = validator.group_by_domain(page_urls)
    effective_gsc_json_path = gsc_json_path if include_gsc else ""
    spinner_label = (
        "Running validation and fetching GSC status..."
        if include_gsc
        else "Running validation..."
    )

    with st.spinner(spinner_label):
        report = validator.build_report(
            domains=domains,
            max_urls=max_urls_value,
            user_agent=user_agent,
            rules=rules,
            rules_path=rules_path,
            schemaorg_ref=schemaorg_ref,
            schemaorg_ref_path=schemaorg_ref_path,
            sitemap_urls_by_domain=sitemap_urls_by_domain,
            page_urls_by_domain=page_urls_by_domain,
            sitemap_mode=sitemap_mode,
            gsc_json_path=effective_gsc_json_path,
            gsc_cache_path=gsc_cache_path,
            gsc_cache_ttl_hours=gsc_cache_ttl_hours,
        )
        validator.render_report(report, output_path)

    summary = validator.compute_executive_summary(report)
    st.session_state["latest_report"] = report
    st.session_state["latest_summary"] = summary
    st.session_state["latest_run_requested_gsc"] = include_gsc
    st.session_state["latest_run_config"] = {
        "targets_text": targets_text_value,
        "max_urls": max_urls_value,
        "use_schemaorg": use_schemaorg_value,
    }


def report_candidate_domains(report: validator.Report) -> list[str]:
    return [site.domain for site in report.sites]


def apply_gsc_result_to_url_result(
    result: validator.UrlCheckResult, gsc_result: dict[str, object], gsc_property: str
) -> None:
    result.gsc_property = gsc_property or str(gsc_result.get("property", "") or "")
    result.gsc_status = str(gsc_result.get("status", "") or "")
    result.gsc_verdict = str(gsc_result.get("verdict", "") or "")
    result.gsc_coverage_state = str(gsc_result.get("coverage_state", "") or "")
    result.gsc_indexing_state = str(gsc_result.get("indexing_state", "") or "")
    result.gsc_robots_state = str(gsc_result.get("robots_state", "") or "")
    result.gsc_page_fetch_state = str(gsc_result.get("page_fetch_state", "") or "")
    result.gsc_last_crawl_time = str(gsc_result.get("last_crawl_time", "") or "")
    result.gsc_google_canonical = str(gsc_result.get("google_canonical", "") or "")
    result.gsc_user_canonical = str(gsc_result.get("user_canonical", "") or "")
    result.gsc_sitemaps = list(gsc_result.get("sitemaps", []) or [])
    result.gsc_referring_urls = list(gsc_result.get("referring_urls", []) or [])
    result.gsc_error = str(gsc_result.get("error", "") or "")
    result.gsc_checked_at = str(gsc_result.get("checked_at", "") or "")


def fetch_on_demand_gsc_status(report: validator.Report, result: validator.UrlCheckResult) -> None:
    service, error = validator.build_gsc_service(gsc_json_path) if gsc_json_path else (None, "GSC JSON path not provided")
    gsc_result: dict[str, object]
    gsc_property = ""
    if service is None:
        gsc_result = {
            "status": "",
            "error": error or "GSC service unavailable",
            "checked_at": "",
        }
    else:
        gsc_property = validator.infer_gsc_property(result.url, report_candidate_domains(report))
        if not gsc_property:
            gsc_result = {
                "status": "",
                "property": "",
                "error": "No GSC property mapping configured for this domain",
                "checked_at": "",
            }
        else:
            gsc_result = validator.inspect_url_in_gsc(service, result.url, gsc_property)
            cache = validator.load_gsc_cache(gsc_cache_path)
            validator.set_cached_gsc_result(cache, gsc_property, result.url, gsc_result)
            validator.save_gsc_cache(gsc_cache_path, cache)

    apply_gsc_result_to_url_result(result, gsc_result, gsc_property)
    report.gsc_enabled = bool(service)
    st.session_state["latest_report"] = report
    st.session_state["latest_summary"] = validator.compute_executive_summary(report)
    st.session_state["latest_run_requested_gsc"] = True


def render_gsc_action_table(report: validator.Report) -> list[dict[str, object]]:
    gsc_rows = build_gsc_rows(report)
    if not gsc_rows:
        return []

    filter_options = ["All", "Indexed", "Excluded", "Blocked", "Error", "No GSC Data", "Other"]
    gsc_filter = st.selectbox("Filter URLs by GSC status", filter_options, index=0)
    if gsc_filter == "All":
        filtered_rows = gsc_rows
    else:
        filtered_rows = [row for row in gsc_rows if row["GSC Category"] == gsc_filter]

    st.caption(f"Showing {len(filtered_rows)} of {len(gsc_rows)} audited URLs")
    if not filtered_rows:
        st.info("No URLs match the selected GSC filter.")
        return []

    header_cols = st.columns([1.2, 4.3, 0.7, 1.2, 1.2, 1.5, 1.5, 1.2, 1.2], gap="small")
    header_labels = [
        "Site",
        "URL",
        "HTTP",
        "GSC Category",
        "GSC Status",
        "Coverage State",
        "Indexing State",
        "GSC",
        "PDF",
    ]
    for column, label in zip(header_cols, header_labels):
        column.markdown(f"**{label}**")

    for row in filtered_rows:
        site = row["Site Report"]
        result = row["Result"]
        row_key = str(row["Row Key"])
        button_label = "Refresh GSC" if (result.gsc_status or result.gsc_error) else "Fetch GSC"
        cols = st.columns([1.2, 4.3, 0.7, 1.2, 1.2, 1.5, 1.5, 1.2, 1.2], gap="small")
        cols[0].write(str(row["Site"]))
        cols[1].markdown(f"[{result.url}]({result.url})")
        cols[2].write(str(row["HTTP"]))
        cols[3].write(str(row["GSC Category"]))
        cols[4].write(str(row["GSC Status"]))
        cols[5].write(str(row["Coverage State"]))
        cols[6].write(str(row["Indexing State"]))
        if gsc_json_path:
            if cols[7].button(button_label, key=f"gsc_table_{row_key}", use_container_width=True):
                fetch_on_demand_gsc_status(st.session_state["latest_report"], result)
                st.rerun()
        else:
            cols[7].button("GSC Off", key=f"gsc_table_{row_key}", use_container_width=True, disabled=True)
        try:
            url_pdf = build_url_pdf(site, result)
            cols[8].download_button(
                "PDF",
                data=url_pdf,
                file_name=f"url-audit-{site.domain}-{row_key.split('::')[-1]}.pdf",
                mime="application/pdf",
                key=f"url_pdf_{row_key}",
                use_container_width=True,
            )
        except Exception:
            cols[8].button("PDF Off", key=f"url_pdf_off_{row_key}", use_container_width=True, disabled=True)
        st.divider()
    return filtered_rows


def render_url_detail_section(rows: list[dict[str, object]]) -> None:
    if not rows:
        return

    st.subheader("Per-URL Audit Detail")
    st.caption("Open any URL below to review the same audit findings in a readable format and download a URL-only PDF.")

    for row in rows:
        site = row["Site Report"]
        result = row["Result"]
        row_key = str(row["Row Key"])
        expander_label = result.url
        with st.expander(expander_label, expanded=False):
            top_cols = st.columns([2.4, 1.2, 1.4, 1.2, 1.4], gap="small")
            top_cols[0].markdown(f"**Site**  \n{site.domain}")
            top_cols[1].markdown(f"**HTTP**  \n{row['HTTP']}")
            top_cols[2].markdown(f"**Indexability**  \n{result.indexability_status or '-'}")
            top_cols[3].markdown(f"**GSC**  \n{result.gsc_status or '-'}")
            try:
                url_pdf = build_url_pdf(site, result)
                top_cols[4].download_button(
                    "Download URL PDF",
                    data=url_pdf,
                    file_name=f"url-audit-{site.domain}-{row_key.split('::')[-1]}.pdf",
                    mime="application/pdf",
                    key=f"url_pdf_detail_{row_key}",
                    use_container_width=True,
                )
            except Exception:
                top_cols[4].button(
                    "Download URL PDF",
                    key=f"url_pdf_detail_disabled_{row_key}",
                    use_container_width=True,
                    disabled=True,
                )

            summary_cols = st.columns(3, gap="small")
            summary_cols[0].markdown(
                f"**Content**  \n{_content_summary_text(result)}"
            )
            summary_cols[1].markdown(
                f"**Canonical**  \n{result.seo_meta.get('canonical_url', '-') or '-'}"
            )
            summary_cols[2].markdown(
                f"**Last Crawl**  \n{result.gsc_last_crawl_time or '-'}"
            )

            meta_rows = _meta_snapshot_rows(result)
            if meta_rows:
                st.markdown("**Metadata Snapshot**")
                st.table(pd.DataFrame(meta_rows, columns=["Field", "Value"]))

            findings_col1, findings_col2 = st.columns(2, gap="large")
            with findings_col1:
                st.markdown("**Issues**")
                if result.issues:
                    for issue in result.issues:
                        st.markdown(f"- {issue}")
                else:
                    st.markdown("- None")
            with findings_col2:
                st.markdown("**Warnings**")
                if result.warnings:
                    for warning in result.warnings:
                        st.markdown(f"- {warning}")
                else:
                    st.markdown("- None")

            schema_col1, schema_col2 = st.columns(2, gap="large")
            with schema_col1:
                st.markdown("**Schema Types**")
                if result.jsonld_types:
                    for schema_type in result.jsonld_types:
                        st.markdown(f"- {schema_type}")
                else:
                    st.markdown("- None")
            with schema_col2:
                st.markdown("**Recommended Actions**")
                recommendations = _recommendations_for_result(result)
                if recommendations:
                    for recommendation in recommendations[:10]:
                        st.markdown(f"- {recommendation}")
                else:
                    st.markdown("- No action items captured.")

with st.form("run_form"):
    col_a, col_b = st.columns([2.3, 1], gap="large")
    with col_a:
        st.markdown('<div class="section-title">Targets</div>', unsafe_allow_html=True)
        st.markdown(
            '<div class="field-label">Domains, sitemap URLs, or page URLs</div>',
            unsafe_allow_html=True,
        )
        targets_text = st.text_area(
            "Targets",
            value="",
            help="Enter one per line (domain, sitemap XML, or full page URL).",
            placeholder="jagran.com\nhttps://www.thedailyjagran.com/news-sitemap.xml\nhttps://www.thedailyjagran.com/world/example-article",
            height=160,
            label_visibility="collapsed",
        )
        st.caption(
            "We auto-detect sitemap URLs (XML / contains 'sitemap'). Domains use robots.txt discovery."
        )

    with col_b:
        st.markdown('<div class="section-title">Run Settings</div>', unsafe_allow_html=True)
        st.markdown('<div class="field-label">Max URLs per site</div>', unsafe_allow_html=True)
        max_urls = st.number_input(
            "Max URLs per site",
            min_value=1,
            max_value=500,
            value=validator.DEFAULT_MAX_URLS,
            step=1,
            label_visibility="collapsed",
        )
        use_schemaorg = st.checkbox(
            "Show allowed schema.org nodes",
            value=True,
        )
        st.caption("Run the audit first. You can fetch GSC status from the URL table in the results section.")
        run_button = st.form_submit_button("Run Validation", use_container_width=True)

if run_button:
    run_validation_audit(
        targets_text_value=targets_text,
        max_urls_value=max_urls,
        use_schemaorg_value=use_schemaorg,
        include_gsc=False,
    )

current_report = st.session_state.get("latest_report")
current_summary = st.session_state.get("latest_summary")
current_run_requested_gsc = bool(st.session_state.get("latest_run_requested_gsc"))
latest_run_config = st.session_state.get("latest_run_config")

if current_report and current_summary:
    if not gsc_json_path:
        st.info(
            "GSC is disabled in this deployment because no service-account JSON was found. "
            "Add it via Streamlit secrets to enable indexing checks."
        )
    elif current_run_requested_gsc and not current_report.gsc_enabled:
        st.warning(
            "A GSC request was attempted, but Search Console could not be queried. "
            "Please check the deployment credentials or property access."
        )
    if not current_report.schemaorg_ref_loaded:
        st.warning(
            "Schema.org properties reference not loaded. "
            "Check your internet connection or provide the schema.org JSON-LD file path."
        )

    summary = current_summary
    st.markdown("### Run Results")
    row1 = st.columns(4)
    row1[0].metric("Overall Score", f"{summary['score']}/100")
    row1[0].caption(f"Grade: {summary['grade']}")
    row1[1].metric("SEO Score", f"{summary['seo_score']}/100")
    row1[1].caption(f"Grade: {summary['seo_grade']}")
    row1[2].metric("Schema Score", f"{summary['schema_score']}/100")
    row1[2].caption(f"Grade: {summary['schema_grade']}")
    row1[3].metric("Meta Coverage", f"{summary['meta_coverage_pct']}%")
    row1[3].caption(f"Missing: {summary['meta_missing']}")

    row2 = st.columns(3)
    row2[0].metric("Canonical Mismatch", summary["canonical_mismatch"])
    row2[1].metric("Title/Desc Issues", summary["title_desc_issue_urls"])
    row2[2].metric("Schema Issues", summary["schema_issues"])

    row3 = st.columns(4)
    row3[0].metric("Indexable", summary.get("indexable_urls", 0))
    row3[1].metric("Blocked", summary.get("blocked_urls", 0))
    row3[2].metric("Uncertain", summary.get("uncertain_urls", 0))
    row3[3].metric("Redirected", summary.get("redirected_urls", 0))

    report_download_col, preview_hint_col = st.columns([1.4, 2.2])
    with report_download_col:
        try:
            report_pdf = build_report_pdf(current_report, summary)
            st.download_button(
                "Download Full Report (PDF)",
                data=report_pdf,
                file_name=f"seo-audit-report-{datetime.now(IST).strftime('%Y%m%d-%H%M%S')}.pdf",
                mime="application/pdf",
                key="download_full_report_pdf",
                use_container_width=True,
            )
        except Exception:
            st.button(
                "Download Full Report (PDF)",
                key="download_full_report_pdf_disabled",
                use_container_width=True,
                disabled=True,
            )
    with preview_hint_col:
        st.caption(
            "Use the PDF buttons in the URL table to download an individual URL audit. "
            "If PDF buttons are disabled, install the updated requirements."
        )

    gsc_rows = build_gsc_rows(current_report)
    if current_report.gsc_enabled:
        row4 = st.columns(4)
        row4[0].metric("GSC Indexed", summary.get("gsc_indexed_urls", 0))
        row4[1].metric("GSC Excluded", summary.get("gsc_excluded_urls", 0))
        row4[2].metric("GSC Blocked", summary.get("gsc_blocked_urls", 0))
        row4[3].metric("GSC Errors", summary.get("gsc_error_urls", 0))

        row5 = st.columns(2)
        row5[0].metric("Last GSC Inspection", summary.get("gsc_last_checked", "-"))
        row5[0].caption(f"Cache TTL: {gsc_cache_ttl_hours}h")
        row5[1].metric("Last GSC Crawl", summary.get("gsc_last_crawl", "-"))
        row5[1].caption("Latest crawl timestamp returned by Search Console")

    filtered_gsc_rows: list[dict[str, object]] = []
    if gsc_rows:
        filtered_gsc_rows = render_gsc_action_table(current_report)
        render_url_detail_section(filtered_gsc_rows)

    show_preview = st.checkbox("Show report preview", value=True, key="show_report_preview")
    if show_preview:
        with st.expander("Report preview", expanded=True):
            try:
                html_content = Path(output_path).read_text(encoding="utf-8")
                st.components.v1.html(html_content, height=900, scrolling=True)
            except Exception as exc:
                st.error(f"Could not load report HTML: {exc}")
else:
    st.info("Pick targets and click Run Validation to generate a report.")
