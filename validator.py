#!/usr/bin/env python3
"""Schema and sitemap validator for multiple domains.

Usage:
  python validator.py --output report.html --max-urls 20
"""

from __future__ import annotations

import argparse
import datetime as dt
import gzip
import html
import json
import os
import re
import sys
import time
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse, parse_qsl, urlencode, urljoin
from urllib.robotparser import RobotFileParser
import xml.etree.ElementTree as ET

import requests
from bs4 import BeautifulSoup
from bs4.element import Tag


DEFAULT_DOMAINS = [
    "jagran.com",
    "thedailyjagran.com",
    "jagranjosh.com",
    "onlymyhealth.com",
    "herzindagi.com",
]

DEFAULT_MAX_URLS = 20
DEFAULT_USER_AGENT = "SchemaSitemapValidator/1.0"
DEFAULT_RULES_PATH = os.path.join(os.path.dirname(__file__), "schema_rules.json")
DEFAULT_SCHEMAORG_REF_PATH = os.path.join(
    os.path.dirname(__file__), "schemaorg_properties.json"
)
DEFAULT_SCHEMAORG_DATA_PATH = os.path.join(
    os.path.dirname(__file__), "schemaorg-current-https.jsonld"
)
DEFAULT_GSC_JSON_PATH = "/Users/kartikaykhosla/Documents/article-analyzer/bscadmanager-227c5bb67c18.json"
DEFAULT_GSC_CACHE_PATH = "/Users/kartikaykhosla/Documents/article-analyzer/gsc_inspection_cache.json"
DEFAULT_GSC_CACHE_TTL_HOURS = 24
SCHEMAORG_DATA_URL = "https://schema.org/version/latest/schemaorg-current-https.jsonld"
DEFAULT_GSC_PROPERTY_MAP = {
    "jagran.com": "https://www.jagran.com/",
    "www.jagran.com": "https://www.jagran.com/",
    "jagranjosh.com": "https://www.jagranjosh.com/",
    "www.jagranjosh.com": "https://www.jagranjosh.com/",
    "onlymyhealth.com": "https://www.onlymyhealth.com/",
    "www.onlymyhealth.com": "https://www.onlymyhealth.com/",
    "thedailyjagran.com": "https://www.thedailyjagran.com/",
    "www.thedailyjagran.com": "https://www.thedailyjagran.com/",
    "herzindagi.com": "https://www.herzindagi.com/",
    "www.herzindagi.com": "https://www.herzindagi.com/",
}
TITLE_LENGTH_MIN = 30
TITLE_LENGTH_MAX = 60
DESCRIPTION_LENGTH_MIN = 120
DESCRIPTION_LENGTH_MAX = 160
WORD_COUNT_WARNING_THRESHOLD = 150
SITEMAP_STALE_DAYS = 365
VALID_CHANGEFREQ = {"always", "hourly", "daily", "weekly", "monthly", "yearly", "never"}
CONTENT_ROOT_SELECTORS = [
    "article",
    "main article",
    "[itemprop='articleBody']",
    "[itemprop='mainContentOfPage']",
    ".article-body",
    ".articleBody",
    ".story-body",
    ".storyBody",
    ".entry-content",
    ".post-content",
    ".content-body",
    ".article-content",
    "main",
]
IMAGE_SOURCE_ATTRS = (
    "src",
    "data-src",
    "data-lazy-src",
    "data-original",
    "data-image",
    "data-fallback-src",
)
GENERIC_ALT_VALUES = {
    "image",
    "photo",
    "featured image",
    "feature image",
    "thumbnail",
    "hero image",
    "img",
}
SCHEMA_SUMMARY_FIELDS = [
    "name",
    "headline",
    "description",
    "url",
    "mainEntityOfPage",
    "datePublished",
    "dateModified",
    "author",
    "publisher",
    "image",
    "inLanguage",
    "breadcrumb",
    "isPartOf",
    "properties_used",
    "allowed_properties",
    "missing_required",
    "missing_recommended",
    "source",
]

NESTED_SCHEMA_FIELDS = [
    "type",
    "name",
    "headline",
    "url",
    "author",
    "publisher",
    "properties_used",
    "missing_required",
    "missing_recommended",
    "source",
]

SEO_FIELDS = [
    ("Title", "title"),
    ("Meta Description", "meta_description"),
    ("Canonical URL", "canonical"),
    ("Canonical Match", "canonical_match"),
    ("OG Title", "og:title"),
    ("OG Description", "og:description"),
    ("OG Image", "og:image"),
    ("OG URL", "og:url"),
    ("OG Type", "og:type"),
    ("OG Site Name", "og:site_name"),
    ("Twitter Card", "twitter:card"),
    ("Twitter Title", "twitter:title"),
    ("Twitter Description", "twitter:description"),
    ("Twitter Image", "twitter:image"),
    ("Twitter Site", "twitter:site"),
    ("Facebook App ID", "fb:app_id"),
    ("Facebook Pages", "fb:pages"),
]

TRACKING_PARAMS = {
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "fbclid",
    "gclid",
    "igshid",
}

SEO_LENGTH_RULES = {
    "title": (TITLE_LENGTH_MIN, TITLE_LENGTH_MAX),
    "meta_description": (DESCRIPTION_LENGTH_MIN, DESCRIPTION_LENGTH_MAX),
}

SEO_COVERAGE_KEYS = [
    "title",
    "meta_description",
    "canonical",
    "og:title",
    "og:description",
    "og:image",
    "og:url",
    "og:type",
    "og:site_name",
    "twitter:card",
    "twitter:title",
    "twitter:description",
    "twitter:image",
    "twitter:site",
    "fb:app_id",
    "fb:pages",
]


@dataclass
class SitemapFetchResult:
    url: str
    status_code: Optional[int]
    error: Optional[str]
    kind: Optional[str] = None
    urls_found: int = 0
    sitemaps_found: int = 0
    lastmod_missing: int = 0
    lastmod_invalid: int = 0
    lastmod_stale: int = 0
    changefreq_missing: int = 0
    changefreq_invalid: int = 0
    priority_invalid: int = 0
    news_entries: int = 0
    news_missing_publication: int = 0
    news_publication_names: List[str] = field(default_factory=list)
    entry_samples: List[Dict[str, str]] = field(default_factory=list)


@dataclass
class UrlCheckResult:
    url: str
    http_status: Optional[int]
    fetch_error: Optional[str]
    content_type: Optional[str]
    skipped_by_robots: bool
    indexability_status: str = ""
    indexability_reasons: List[str] = field(default_factory=list)
    robots_meta: str = ""
    x_robots_tag: str = ""
    final_url: str = ""
    redirect_chain: List[str] = field(default_factory=list)
    soft_404: bool = False
    hreflang_status: str = ""
    hreflang_issues: List[str] = field(default_factory=list)
    pagination: str = ""
    auth_blocked: str = ""
    duplicate_canonical: bool = False
    canonical_target_count: int = 0
    jsonld_blocks: int = 0
    jsonld_types: List[str] = field(default_factory=list)
    microdata_items: int = 0
    rdfa_elements: int = 0
    issues: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    schema_objects: List[Dict[str, str]] = field(default_factory=list)
    microdata_objects: List[Dict[str, str]] = field(default_factory=list)
    rdfa_objects: List[Dict[str, str]] = field(default_factory=list)
    seo_meta: Dict[str, str] = field(default_factory=dict)
    seo_issues: List[str] = field(default_factory=list)
    seo_warnings: List[str] = field(default_factory=list)
    word_count: int = 0
    word_count_source: str = ""
    heading_h1_count: int = 0
    heading_h2_count: int = 0
    heading_h3_count: int = 0
    heading_structure: List[str] = field(default_factory=list)
    feature_image_url: str = ""
    feature_image_alt: str = ""
    feature_image_status: str = ""
    gsc_property: str = ""
    gsc_status: str = ""
    gsc_verdict: str = ""
    gsc_coverage_state: str = ""
    gsc_indexing_state: str = ""
    gsc_robots_state: str = ""
    gsc_page_fetch_state: str = ""
    gsc_last_crawl_time: str = ""
    gsc_google_canonical: str = ""
    gsc_user_canonical: str = ""
    gsc_sitemaps: List[str] = field(default_factory=list)
    gsc_referring_urls: List[str] = field(default_factory=list)
    gsc_error: str = ""
    gsc_checked_at: str = ""


@dataclass
class SiteReport:
    domain: str
    robots_url: str
    robots_status: Optional[int]
    robots_error: Optional[str]
    sitemaps: List[SitemapFetchResult] = field(default_factory=list)
    urls: List[UrlCheckResult] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)


@dataclass
class Report:
    generated_at: str
    max_urls_per_site: int
    user_agent: str
    rules_path: str
    schemaorg_ref_path: str
    schemaorg_ref_loaded: bool
    schemaorg_types: int
    gsc_enabled: bool
    gsc_json_path: str
    gsc_cache_path: str
    sites: List[SiteReport]


def load_schema_rules(path: str) -> Dict[str, dict]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        return {"types": {}}
    except Exception as exc:
        print(f"Warning: failed to load schema rules from {path}: {exc}")
        return {"types": {}}

    if not isinstance(data, dict) or "types" not in data:
        return {"types": {}}
    return data


def normalize_schema_id(value: str) -> Optional[str]:
    if not value:
        return None
    if value.startswith("schema:"):
        return value.split("schema:", 1)[1]
    if "schema.org/" in value:
        return value.split("schema.org/")[-1].split("#")[-1]
    if value.startswith("http://schema.org/"):
        return value.split("http://schema.org/")[-1]
    return None


def load_schemaorg_reference(ref_path: str, data_path: Optional[str]) -> Dict[str, List[str]]:
    cached: Dict[str, List[str]] = {}
    if ref_path and os.path.exists(ref_path):
        try:
            with open(ref_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                cached = {k: v for k, v in data.items() if isinstance(v, list)}
                if cached:
                    return cached
        except Exception as exc:
            print(f"Warning: failed to load schema.org reference from {ref_path}: {exc}")

    if not data_path:
        return cached
    if not os.path.exists(data_path):
        print(f"Warning: schema.org data file not found: {data_path}")
        return {}

    try:
        with open(data_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as exc:
        print(f"Warning: failed to read schema.org data file: {exc}")
        return {}

    graph = data.get("@graph", [])
    if not isinstance(graph, list):
        print("Warning: schema.org data missing @graph array.")
        return {}

    type_parents: Dict[str, List[str]] = {}
    property_domains: Dict[str, List[str]] = {}

    for item in graph:
        if not isinstance(item, dict):
            continue
        item_type = item.get("@type")
        item_id = item.get("@id")
        name = normalize_schema_id(item_id) if isinstance(item_id, str) else None
        if not name:
            continue
        if item_type == "rdfs:Class":
            parents_raw = item.get("rdfs:subClassOf", [])
            parents: List[str] = []
            if isinstance(parents_raw, dict):
                parents_raw = [parents_raw]
            if isinstance(parents_raw, list):
                for parent in parents_raw:
                    if isinstance(parent, dict):
                        parent_id = parent.get("@id")
                        parent_name = normalize_schema_id(parent_id) if isinstance(parent_id, str) else None
                        if parent_name:
                            parents.append(parent_name)
            type_parents[name] = parents
        if item_type == "rdf:Property":
            domains_raw = item.get("schema:domainIncludes", [])
            domains: List[str] = []
            if isinstance(domains_raw, dict):
                domains_raw = [domains_raw]
            if isinstance(domains_raw, list):
                for domain in domains_raw:
                    if isinstance(domain, dict):
                        domain_id = domain.get("@id")
                        domain_name = normalize_schema_id(domain_id) if isinstance(domain_id, str) else None
                        if domain_name:
                            domains.append(domain_name)
            property_domains[name] = domains

    ancestor_cache: Dict[str, List[str]] = {}

    def ancestors(type_name: str) -> List[str]:
        if type_name in ancestor_cache:
            return ancestor_cache[type_name]
        parents = type_parents.get(type_name, [])
        full: List[str] = []
        for parent in parents:
            full.append(parent)
            full.extend(ancestors(parent))
        ancestor_cache[type_name] = list(dict.fromkeys(full))
        return ancestor_cache[type_name]

    ref: Dict[str, List[str]] = {}
    for type_name in type_parents.keys():
        allowed: List[str] = []
        lineage = set([type_name] + ancestors(type_name))
        for prop, domains in property_domains.items():
            if set(domains) & lineage:
                allowed.append(prop)
        ref[type_name] = sorted(set(allowed))

    if ref_path:
        try:
            with open(ref_path, "w", encoding="utf-8") as f:
                json.dump(ref, f, indent=2, sort_keys=True)
        except Exception as exc:
            print(f"Warning: failed to write schema.org reference cache: {exc}")

    return ref


def download_schemaorg_data(url: str, dest_path: str) -> bool:
    try:
        resp = requests.get(url, timeout=60, stream=True)
        if resp.status_code >= 400:
            print(f"Warning: failed to download schema.org data (HTTP {resp.status_code})")
            return False
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        with open(dest_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
        return True
    except Exception as exc:
        print(f"Warning: failed to download schema.org data: {exc}")
        return False


def normalize_list(values: Optional[List[str]]) -> List[str]:
    if not values:
        return []
    out: List[str] = []
    for value in values:
        if value is None:
            continue
        parts = [v.strip() for v in value.split(",")]
        for part in parts:
            if part:
                if "://" not in part:
                    part = "https://" + part
                out.append(part)
    return out


def extract_domain(url: str) -> Optional[str]:
    if not url:
        return None
    if "://" not in url:
        url = "https://" + url
    parsed = urlparse(url)
    if parsed.netloc:
        return parsed.netloc.lower()
    return None


def normalize_url_for_compare(raw_url: str) -> str:
    if not raw_url:
        return ""
    if "://" not in raw_url:
        raw_url = "https://" + raw_url
    parsed = urlparse(raw_url)
    scheme = (parsed.scheme or "https").lower()
    netloc = parsed.netloc.lower()
    if ":" in netloc:
        host, port = netloc.split(":", 1)
        if (scheme == "https" and port == "443") or (scheme == "http" and port == "80"):
            netloc = host
    path = parsed.path or "/"
    if path != "/" and path.endswith("/"):
        path = path[:-1]
    params = []
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        if key.lower() in TRACKING_PARAMS:
            continue
        params.append((key, value))
    query = urlencode(sorted(params))
    normalized = f"{scheme}://{netloc}{path}"
    if query:
        normalized += f"?{query}"
    return normalized


def group_by_domain(urls: List[str]) -> Dict[str, List[str]]:
    grouped: Dict[str, List[str]] = {}
    for url in urls:
        domain = extract_domain(url)
        if not domain:
            continue
        grouped.setdefault(domain, []).append(url)
    return grouped


def normalize_gsc_domain(domain: str) -> str:
    domain = (domain or "").strip().lower()
    if domain.startswith("www."):
        return domain[4:]
    return domain


def infer_gsc_property(url: str, candidate_domains: List[str]) -> str:
    host = extract_domain(url) or ""
    raw_host = (host or "").strip().lower()
    host = normalize_gsc_domain(raw_host)
    if not raw_host:
        return ""
    if raw_host in DEFAULT_GSC_PROPERTY_MAP:
        return DEFAULT_GSC_PROPERTY_MAP[raw_host]
    if host in DEFAULT_GSC_PROPERTY_MAP:
        return DEFAULT_GSC_PROPERTY_MAP[host]

    candidates = sorted({normalize_gsc_domain(domain) for domain in candidate_domains if domain}, key=len, reverse=True)
    for domain in candidates:
        if host == domain or host.endswith("." + domain):
            mapped = DEFAULT_GSC_PROPERTY_MAP.get(domain)
            if mapped:
                return mapped

    return ""


def build_gsc_service(json_path: str):
    if not json_path:
        return None, "GSC JSON path not provided"
    if not os.path.exists(json_path):
        return None, f"GSC JSON not found: {json_path}"
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
    except Exception as exc:
        return None, f"GSC libraries not available: {exc}"

    try:
        credentials = service_account.Credentials.from_service_account_file(
            json_path,
            scopes=["https://www.googleapis.com/auth/webmasters.readonly"],
        )
        service = build("searchconsole", "v1", credentials=credentials, cache_discovery=False)
        return service, None
    except Exception as exc:
        return None, f"GSC auth failed: {exc}"


def load_gsc_cache(cache_path: str) -> Dict[str, Dict[str, object]]:
    if not cache_path or not os.path.exists(cache_path):
        return {}
    try:
        with open(cache_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return {str(k): v for k, v in data.items() if isinstance(v, dict)}
    except Exception:
        return {}
    return {}


def save_gsc_cache(cache_path: str, cache: Dict[str, Dict[str, object]]) -> None:
    if not cache_path:
        return
    os.makedirs(os.path.dirname(cache_path), exist_ok=True)
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2, sort_keys=True)


def gsc_cache_key(site_property: str, inspection_url: str) -> str:
    return f"{site_property}::{normalize_url_for_compare(inspection_url)}"


def parse_datetime_safe(value: str) -> Optional[dt.datetime]:
    if not value:
        return None
    try:
        raw = value
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        parsed = dt.datetime.fromisoformat(raw)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.timezone.utc)
        return parsed.astimezone(dt.timezone.utc)
    except Exception:
        return None


def get_cached_gsc_result(
    cache: Dict[str, Dict[str, object]],
    site_property: str,
    inspection_url: str,
    ttl_hours: int,
) -> Optional[Dict[str, object]]:
    key = gsc_cache_key(site_property, inspection_url)
    item = cache.get(key)
    if not item:
        return None
    checked_at = parse_datetime_safe(str(item.get("checked_at", "") or ""))
    if not checked_at:
        return None
    age = dt.datetime.now(dt.timezone.utc) - checked_at
    if age.total_seconds() > ttl_hours * 3600:
        return None
    return dict(item)


def set_cached_gsc_result(
    cache: Dict[str, Dict[str, object]],
    site_property: str,
    inspection_url: str,
    result: Dict[str, object],
) -> None:
    key = gsc_cache_key(site_property, inspection_url)
    cache[key] = dict(result)


def normalize_gsc_index_result(index_status: Dict[str, object]) -> Dict[str, object]:
    verdict = str(index_status.get("verdict", "") or "")
    coverage = str(index_status.get("coverageState", "") or "")
    indexing = str(index_status.get("indexingState", "") or "")
    robots = str(index_status.get("robotsTxtState", "") or "")
    fetch = str(index_status.get("pageFetchState", "") or "")

    status = "Unknown"
    if robots == "DISALLOWED" or fetch == "BLOCKED_ROBOTS_TXT":
        status = "Blocked by robots.txt"
    elif indexing in ("BLOCKED_BY_META_TAG", "BLOCKED_BY_HTTP_HEADER"):
        status = "Blocked by noindex"
    elif verdict == "PASS":
        status = "Indexed"
    elif verdict == "NEUTRAL":
        status = "Excluded"
    elif verdict == "FAIL":
        status = "Error"

    return {
        "status": status,
        "verdict": verdict,
        "coverage_state": coverage,
        "indexing_state": indexing,
        "robots_state": robots,
        "page_fetch_state": fetch,
        "last_crawl_time": str(index_status.get("lastCrawlTime", "") or ""),
        "google_canonical": str(index_status.get("googleCanonical", "") or ""),
        "user_canonical": str(index_status.get("userCanonical", "") or ""),
        "sitemaps": list(index_status.get("sitemap", []) or []),
        "referring_urls": list(index_status.get("referringUrls", []) or []),
        "error": "",
    }


def inspect_url_in_gsc(service, inspection_url: str, site_property: str) -> Dict[str, object]:
    if service is None:
        return {"status": "", "error": "GSC service unavailable"}
    try:
        body = {
            "inspectionUrl": inspection_url,
            "siteUrl": site_property,
            "languageCode": "en-US",
        }
        response = service.urlInspection().index().inspect(body=body).execute()
        inspection_result = response.get("inspectionResult", {})
        index_status = inspection_result.get("indexStatusResult", {})
        if not isinstance(index_status, dict):
            return {"status": "", "error": "GSC returned no index status result"}
        normalized = normalize_gsc_index_result(index_status)
        normalized["property"] = site_property
        normalized["checked_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
        return normalized
    except Exception as exc:
        return {
            "status": "",
            "property": site_property,
            "error": str(exc),
            "verdict": "",
            "coverage_state": "",
            "indexing_state": "",
            "robots_state": "",
            "page_fetch_state": "",
            "last_crawl_time": "",
            "google_canonical": "",
            "user_canonical": "",
            "sitemaps": [],
            "referring_urls": [],
            "checked_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        }


def fetch_url(session: requests.Session, url: str, user_agent: str, timeout: int = 20) -> Tuple[Optional[requests.Response], Optional[str]]:
    headers = {"User-Agent": user_agent}
    try:
        resp = session.get(url, headers=headers, timeout=timeout, allow_redirects=True)
        return resp, None
    except Exception as exc:
        return None, str(exc)


def get_robots(domain: str, session: requests.Session, user_agent: str) -> Tuple[RobotFileParser, str, Optional[int], Optional[str], List[str]]:
    robots_url = f"https://{domain}/robots.txt"
    resp, err = fetch_url(session, robots_url, user_agent)

    rp = RobotFileParser()
    sitemap_urls: List[str] = []

    if resp is None:
        rp.parse([])
        return rp, robots_url, None, err, sitemap_urls

    if resp.status_code >= 400:
        rp.parse([])
        return rp, robots_url, resp.status_code, None, sitemap_urls

    text = resp.text
    rp.parse(text.splitlines())
    for line in text.splitlines():
        if line.lower().startswith("sitemap:"):
            sitemap = line.split(":", 1)[1].strip()
            if sitemap:
                sitemap_urls.append(sitemap)

    return rp, robots_url, resp.status_code, None, sitemap_urls


def maybe_decompress(content: bytes, url: str, content_encoding: Optional[str]) -> bytes:
    if content_encoding and "gzip" in content_encoding.lower():
        return content
    if url.lower().endswith(".gz"):
        try:
            return gzip.decompress(content)
        except OSError:
            return content
    return content


def strip_ns(tag: str) -> str:
    return tag.split("}", 1)[-1] if "}" in tag else tag


def child_text(element: ET.Element, name: str) -> Optional[str]:
    for child in list(element):
        if strip_ns(child.tag) == name and child.text:
            return child.text.strip()
    return None


def parse_iso_date(value: str) -> Optional[dt.datetime]:
    if not value:
        return None
    raw = value.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        return dt.datetime.fromisoformat(raw)
    except ValueError:
        try:
            return dt.datetime.strptime(raw, "%Y-%m-%d")
        except ValueError:
            return None


def normalize_datetime(value: dt.datetime) -> dt.datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=dt.timezone.utc)
    return value.astimezone(dt.timezone.utc)


def parse_news_meta(url_node: ET.Element) -> Optional[Dict[str, str]]:
    for child in list(url_node):
        if strip_ns(child.tag) == "news":
            publication_name = None
            publication_date = None
            title = None
            for news_child in list(child):
                tag = strip_ns(news_child.tag)
                if tag == "publication":
                    publication_name = child_text(news_child, "name")
                elif tag == "publication_date":
                    publication_date = news_child.text.strip() if news_child.text else None
                elif tag == "title":
                    title = news_child.text.strip() if news_child.text else None
            return {
                "publication_name": publication_name or "",
                "publication_date": publication_date or "",
                "title": title or "",
            }
    return None


def parse_sitemap(xml_bytes: bytes) -> Tuple[str, List[Dict[str, str]]]:
    root = ET.fromstring(xml_bytes)
    tag = strip_ns(root.tag)
    if tag == "sitemapindex":
        entries: List[Dict[str, str]] = []
        for sitemap in root.findall(".//{*}sitemap"):
            loc = child_text(sitemap, "loc")
            if not loc:
                continue
            entry = {
                "loc": loc,
                "lastmod": child_text(sitemap, "lastmod") or "",
            }
            entries.append(entry)
        return "sitemapindex", entries
    if tag == "urlset":
        entries = []
        for url_node in root.findall(".//{*}url"):
            loc = child_text(url_node, "loc")
            if not loc:
                continue
            entry = {
                "loc": loc,
                "lastmod": child_text(url_node, "lastmod") or "",
                "changefreq": child_text(url_node, "changefreq") or "",
                "priority": child_text(url_node, "priority") or "",
            }
            news_meta = parse_news_meta(url_node)
            if news_meta:
                entry.update(
                    {
                        "news_publication_name": news_meta.get("publication_name", ""),
                        "news_publication_date": news_meta.get("publication_date", ""),
                        "news_title": news_meta.get("title", ""),
                    }
                )
            entries.append(entry)
        return "urlset", entries
    return "unknown", []


def same_domain(url: str, domain: str) -> bool:
    try:
        parsed = urlparse(url)
    except Exception:
        return False
    host = parsed.netloc.lower()
    return host == domain.lower() or host.endswith("." + domain.lower())


def flatten_jsonld(data) -> List[dict]:
    items: List[dict] = []

    def add(obj):
        if isinstance(obj, dict):
            items.append(obj)
            graph = obj.get("@graph")
            if isinstance(graph, list):
                for entry in graph:
                    add(entry)
        elif isinstance(obj, list):
            for entry in obj:
                add(entry)

    add(data)
    return items


def parse_jsonld_block(raw: str) -> Tuple[Optional[object], Optional[str], bool]:
    if not raw:
        return None, "Empty JSON-LD block", False
    try:
        return json.loads(raw), None, False
    except Exception as exc:
        first_error = str(exc)

    sanitized = re.sub(r"[\x00-\x1F]+", " ", raw)
    sanitized = re.sub(r"\s+", " ", sanitized).strip()
    try:
        return json.loads(sanitized), None, True
    except Exception:
        return None, first_error, False


def context_has_schema_org(context) -> bool:
    if not context:
        return False
    if isinstance(context, str):
        return "schema.org" in context
    if isinstance(context, list):
        return any(context_has_schema_org(item) for item in context)
    if isinstance(context, dict):
        vocab = context.get("@vocab")
        if isinstance(vocab, str) and "schema.org" in vocab:
            return True
        for value in context.values():
            if isinstance(value, str) and "schema.org" in value:
                return True
    return False


def is_schema_iri(value: str) -> bool:
    return isinstance(value, str) and "schema.org" in value


def normalize_schema_type(value: str) -> Optional[str]:
    if not isinstance(value, str):
        return None
    if "schema.org" in value:
        return value.split("schema.org/")[-1].split("#")[-1].strip()
    return value.strip()


def is_known_schema_type(
    type_name: str, rules: Dict[str, dict], schemaorg_ref: Optional[Dict[str, List[str]]]
) -> bool:
    if not type_name:
        return False
    if type_name in rules.get("types", {}):
        return True
    if schemaorg_ref and type_name in schemaorg_ref:
        return True
    return False


def extract_types(obj: dict) -> List[str]:
    types = obj.get("@type")
    if types is None:
        return []
    if isinstance(types, list):
        return [t for t in types if isinstance(t, str)]
    if isinstance(types, str):
        return [types]
    return []


def is_empty(value) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, list):
        return len(value) == 0
    if isinstance(value, dict):
        return len(value) == 0
    return False


def has_value_parts(value, parts: List[str]) -> bool:
    if not parts:
        return not is_empty(value)
    part = parts[0]
    if isinstance(value, list):
        return any(has_value_parts(item, parts) for item in value)
    if not isinstance(value, dict):
        return False
    if part not in value:
        return False
    return has_value_parts(value[part], parts[1:])


def has_value(obj: dict, prop: str) -> bool:
    if "|" in prop:
        return any(has_value(obj, p.strip()) for p in prop.split("|"))
    parts = prop.split(".")
    return has_value_parts(obj, parts)


def prop_present(prop_names: set, rule: str) -> bool:
    if "|" in rule:
        return any(prop_present(prop_names, part.strip()) for part in rule.split("|"))
    if "." in rule:
        rule = rule.split(".", 1)[0]
    return rule in prop_names


def stringify_node(value) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        for key in ("name", "headline", "url", "@id", "contentUrl"):
            if key in value and isinstance(value[key], str):
                return value[key].strip()
        return ""
    if isinstance(value, list):
        parts = [stringify_node(item) for item in value]
        return ", ".join([p for p in parts if p])
    return str(value)


def extract_names(value, id_map: Optional[Dict[str, dict]] = None) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        names = [extract_names(item, id_map) for item in value]
        return ", ".join([n for n in names if n])
    if isinstance(value, dict):
        if isinstance(value.get("name"), str):
            return value["name"].strip()
        if id_map and isinstance(value.get("@id"), str):
            ref = id_map.get(value["@id"])
            if ref and isinstance(ref.get("name"), str):
                return ref["name"].strip()
        return stringify_node(value)
    if isinstance(value, str):
        return value.strip()
    return str(value)


def extract_image(value) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        images = [extract_image(item) for item in value]
        return ", ".join([i for i in images if i])
    if isinstance(value, dict):
        for key in ("url", "contentUrl"):
            if isinstance(value.get(key), str):
                return value[key].strip()
        return stringify_node(value)
    if isinstance(value, str):
        return value.strip()
    return str(value)


def truncate(value: str, limit: int = 200) -> str:
    if len(value) <= limit:
        return value
    return value[: limit - 3] + "..."


def summarize_list(items: List[str], limit: int = 30) -> str:
    if not items:
        return ""
    items = [item for item in items if item]
    if len(items) <= limit:
        return ", ".join(items)
    extra = len(items) - limit
    return ", ".join(items[:limit]) + f" (+{extra} more)"


def parse_directives(value: str) -> List[str]:
    if not value:
        return []
    raw = re.split(r"[,\s]+", value.lower())
    return [item.strip() for item in raw if item.strip()]


def extract_robots_meta(soup: BeautifulSoup) -> Tuple[str, List[str]]:
    directives: List[str] = []
    raw_parts: List[str] = []
    for meta in soup.find_all("meta"):
        name = meta.get("name") or meta.get("http-equiv") or ""
        name = name.strip().lower()
        if name not in ("robots", "googlebot", "bingbot"):
            continue
        content = meta.get("content", "")
        if content:
            raw_parts.append(f"{name}: {content.strip()}")
            directives.extend(parse_directives(content))
    raw_value = "; ".join(raw_parts)
    return raw_value, sorted(set(directives))


def extract_hreflang(soup: BeautifulSoup) -> Tuple[str, List[str]]:
    links = soup.find_all("link", rel=True, hreflang=True)
    codes: List[str] = []
    issues: List[str] = []
    pattern = re.compile(r"^[a-zA-Z]{2,3}(-[a-zA-Z]{2})?$")
    for link in links:
        rels = link.get("rel") or []
        if isinstance(rels, str):
            rels = [rels]
        if not any(str(rel).lower() == "alternate" for rel in rels):
            continue
        code = str(link.get("hreflang", "")).strip()
        href = str(link.get("href", "")).strip()
        if not href:
            issues.append(f"hreflang {code or '(missing)'} missing href")
        if code:
            codes.append(code.lower())
            if code.lower() != "x-default" and not pattern.match(code):
                issues.append(f"invalid hreflang: {code}")
        else:
            issues.append("hreflang missing code")

    if not codes:
        return "Not present", []
    dupes = {code for code in codes if codes.count(code) > 1}
    for code in sorted(dupes):
        issues.append(f"duplicate hreflang: {code}")

    if issues:
        return f"Invalid ({len(issues)} issues)", issues
    return "Valid", []


def extract_pagination(soup: BeautifulSoup) -> str:
    prev_href = ""
    next_href = ""
    for link in soup.find_all("link", rel=True):
        rels = link.get("rel") or []
        if isinstance(rels, str):
            rels = [rels]
        rels = [str(rel).lower() for rel in rels]
        if "prev" in rels and link.get("href"):
            prev_href = link.get("href", "")
        if "next" in rels and link.get("href"):
            next_href = link.get("href", "")
    if prev_href or next_href:
        parts = []
        if prev_href:
            parts.append("rel=prev")
        if next_href:
            parts.append("rel=next")
        return ", ".join(parts)
    return "Not present"


def detect_access_block(soup: BeautifulSoup, status_code: int) -> str:
    if status_code in (401, 403, 451):
        return f"Blocked (HTTP {status_code})"
    text = soup.get_text(" ", strip=True).lower()
    snippet = text[:5000]
    phrases = [
        "access denied",
        "not authorized",
        "unauthorized",
        "forbidden",
        "sign in",
        "login",
        "subscribe to continue",
        "verify you are human",
        "captcha",
        "blocked in your region",
        "geo-blocked",
    ]
    for phrase in phrases:
        if phrase in snippet:
            return f"Suspected: {phrase}"
    return ""


def detect_soft_404(soup: BeautifulSoup) -> bool:
    title = ""
    if soup.title and soup.title.string:
        title = soup.title.string.strip().lower()
    text = soup.get_text(" ", strip=True).lower()
    snippet = text[:5000]
    title_hits = any(
        phrase in title
        for phrase in (
            "404",
            "page not found",
            "not found",
            "error 404",
            "does not exist",
            "gone",
        )
    )
    body_hits = any(
        phrase in snippet
        for phrase in (
            "page not found",
            "404",
            "error 404",
            "not found",
            "does not exist",
            "no longer available",
            "sorry, we can't",
            "sorry, we couldnt",
            "oops",
        )
    )
    if title_hits:
        return True
    if body_hits and len(text) < 3500:
        return True
    return False


def classify_indexability(
    *,
    skipped_by_robots: bool,
    http_status: Optional[int],
    content_type: Optional[str],
    meta_directives: List[str],
    x_directives: List[str],
    canonical_match: str,
    redirect_chain: List[str],
    final_url: str,
    soft_404: bool,
) -> Tuple[str, List[str]]:
    reasons: List[str] = []
    if skipped_by_robots:
        return "Blocked (robots.txt)", ["Blocked by robots.txt"]
    if http_status is None:
        return "Error", ["Fetch failed"]
    if http_status >= 400:
        return "Error", [f"HTTP {http_status}"]
    if content_type and "text/html" not in content_type.lower():
        return "Non-HTML", [f"Content-Type: {content_type}"]

    directives = set(meta_directives + x_directives)
    if "noindex" in directives or "none" in directives:
        return "Blocked (noindex)", ["noindex directive present"]

    status = "Indexable"
    if "nofollow" in directives:
        reasons.append("nofollow directive present")
    if "noarchive" in directives:
        reasons.append("noarchive directive present")
    if "nosnippet" in directives:
        reasons.append("nosnippet directive present")
    if "noimageindex" in directives:
        reasons.append("noimageindex directive present")

    if redirect_chain:
        status = "Redirected"
        reasons.append(f"Redirected to {final_url}")
    if canonical_match == "No":
        if status == "Indexable":
            status = "Canonicalized"
        reasons.append("Canonical points to a different URL")
    if soft_404:
        if status == "Indexable":
            status = "Uncertain (soft 404)"
        reasons.append("Soft 404 suspected (200 with not-found content)")

    if not reasons:
        reasons.append("No blocking signals detected")

    return status, reasons


def summarize_schema_object(
    obj: dict,
    types: List[str],
    rules: Dict[str, dict],
    id_map: Optional[Dict[str, dict]] = None,
    schemaorg_ref: Optional[Dict[str, List[str]]] = None,
    source: Optional[str] = None,
) -> Dict[str, str]:
    summary: Dict[str, str] = {}
    type_label = ", ".join(sorted(set(types))) if types else "Unknown"
    summary["type"] = type_label

    prop_names = [key for key in obj.keys() if not key.startswith("@")]
    if prop_names:
        summary["properties_used"] = truncate(", ".join(sorted(prop_names)), 300)
        prop_pairs: List[str] = []
        for prop in sorted(prop_names):
            value = stringify_node(obj.get(prop))
            if not value:
                continue
            prop_pairs.append(f"{prop}: {truncate(value, 160)}")
        if prop_pairs:
            summary["properties_used_values"] = " || ".join(prop_pairs)

    missing_required: List[str] = []
    missing_recommended: List[str] = []
    for t in types:
        rules_for_type = resolve_rules(t, rules)
        for prop in rules_for_type.get("required", []):
            if not has_value(obj, prop):
                missing_required.append(f"{t}:{prop}")
        for prop in rules_for_type.get("recommended", []):
            if not has_value(obj, prop):
                missing_recommended.append(f"{t}:{prop}")

    if missing_required:
        summary["missing_required"] = truncate(", ".join(sorted(set(missing_required))), 300)
    if missing_recommended:
        summary["missing_recommended"] = truncate(
            ", ".join(sorted(set(missing_recommended))), 300
        )

    if schemaorg_ref:
        allowed: List[str] = []
        for t in types:
            allowed.extend(schemaorg_ref.get(t, []))
        allowed = sorted(set(allowed))
        summary["allowed_properties"] = truncate(summarize_list(allowed, 30), 400)
        summary["allowed_properties_full"] = ", ".join(allowed)
        summary["allowed_properties_count"] = str(len(allowed))

    if source:
        summary["source"] = truncate(source, 200)

    for field in SCHEMA_SUMMARY_FIELDS:
        if field in ("author", "publisher"):
            raw_value = obj.get(field)
            if field == "author" and raw_value is None:
                raw_value = obj.get("creator") or obj.get("byline")
            value = extract_names(raw_value, id_map)
        elif field == "image":
            value = extract_image(obj.get(field))
        elif field in (
            "properties_used",
            "properties_used_values",
            "missing_required",
            "missing_recommended",
        ):
            value = summary.get(field, "")
        elif field == "allowed_properties":
            value = summary.get(field, "")
        elif field == "source":
            value = summary.get(field, "")
        else:
            value = stringify_node(obj.get(field))
        if value:
            summary[field] = truncate(value)
    return summary


def normalize_schema_prop(value: str) -> Optional[str]:
    if not value:
        return None
    value = value.strip()
    if value.startswith("schema:"):
        return value.split("schema:", 1)[1].strip()
    if "schema.org/" in value:
        return value.split("schema.org/")[-1].split("#")[-1].strip()
    if ":" in value:
        return None
    return value


def parse_space_values(attr_value: str) -> List[str]:
    if not attr_value:
        return []
    parts = []
    for item in attr_value.split():
        value = normalize_schema_prop(item)
        if value:
            parts.append(value)
    return parts


def extract_property_value(element: Tag) -> str:
    for attr in ("content", "href", "src", "resource", "value", "datetime"):
        if element.has_attr(attr):
            return str(element.get(attr)).strip()
    text = element.get_text(strip=True)
    return text


def collect_meta_authors(soup: BeautifulSoup) -> List[str]:
    candidates = set()
    meta_keys = {
        "author",
        "article:author",
        "parsely-author",
        "dc.creator",
        "dcterms.creator",
        "twitter:creator",
        "byl",
    }
    for meta in soup.find_all("meta"):
        key = meta.get("name") or meta.get("property") or meta.get("itemprop")
        if not key:
            continue
        if key.strip().lower() in meta_keys:
            content = meta.get("content")
            if content:
                candidates.add(content.strip())
    return sorted(candidates)


def get_meta_content(soup: BeautifulSoup, key: str, attr: str) -> str:
    tag = soup.find("meta", attrs={attr: key})
    if not tag:
        return ""
    content = tag.get("content")
    return content.strip() if content else ""


def extract_canonical(soup: BeautifulSoup) -> str:
    for link in soup.find_all("link", rel=True):
        rel = link.get("rel")
        rels = []
        if isinstance(rel, list):
            rels = [r.lower() for r in rel]
        elif isinstance(rel, str):
            rels = [rel.lower()]
        if "canonical" in rels:
            href = link.get("href")
            if href:
                return href.strip()
    return ""


def extract_seo_meta(soup: BeautifulSoup, page_url: str) -> Tuple[Dict[str, str], List[str], List[str]]:
    meta: Dict[str, str] = {}
    issues: List[str] = []
    warnings: List[str] = []

    title = soup.title.string.strip() if soup.title and soup.title.string else ""
    if title:
        meta["title"] = title
        if len(title) < TITLE_LENGTH_MIN:
            warnings.append(f"SEO: Title too short ({len(title)} chars)")
        elif len(title) > TITLE_LENGTH_MAX:
            warnings.append(f"SEO: Title too long ({len(title)} chars)")
    else:
        issues.append("SEO: Missing <title> tag")

    meta_desc = get_meta_content(soup, "description", "name")
    if meta_desc:
        meta["meta_description"] = meta_desc
        if len(meta_desc) < DESCRIPTION_LENGTH_MIN:
            warnings.append(f"SEO: Meta description too short ({len(meta_desc)} chars)")
        elif len(meta_desc) > DESCRIPTION_LENGTH_MAX:
            warnings.append(f"SEO: Meta description too long ({len(meta_desc)} chars)")
    else:
        warnings.append("SEO: Missing meta description")

    canonical = extract_canonical(soup)
    if canonical:
        if canonical.startswith("/"):
            canonical = urljoin(page_url, canonical)
        meta["canonical"] = canonical
    else:
        warnings.append("SEO: Missing canonical URL")

    og_keys = [
        "og:title",
        "og:description",
        "og:image",
        "og:url",
        "og:type",
        "og:site_name",
    ]
    for key in og_keys:
        value = get_meta_content(soup, key, "property")
        if value:
            meta[key] = value
        else:
            warnings.append(f"SEO: Missing {key}")

    twitter_keys = [
        "twitter:card",
        "twitter:title",
        "twitter:description",
        "twitter:image",
        "twitter:site",
    ]
    for key in twitter_keys:
        value = get_meta_content(soup, key, "name")
        if value:
            meta[key] = value
        else:
            warnings.append(f"SEO: Missing {key}")

    fb_keys = ["fb:app_id", "fb:pages"]
    for key in fb_keys:
        value = get_meta_content(soup, key, "property") or get_meta_content(soup, key, "name")
        if value:
            meta[key] = value
        else:
            warnings.append(f"SEO: Missing {key}")

    page_norm = normalize_url_for_compare(page_url)
    canonical_norm = normalize_url_for_compare(canonical) if canonical else ""
    if canonical_norm:
        meta["canonical_match"] = "Yes" if canonical_norm == page_norm else "No"
        if canonical_norm != page_norm:
            issues.append(
                f"SEO: Canonical URL differs from page URL (canonical: {canonical}, page: {page_url})"
            )
    else:
        meta["canonical_match"] = "N/A"

    og_url = meta.get("og:url", "")
    if og_url and og_url.startswith("/"):
        og_url = urljoin(page_url, og_url)
        meta["og:url"] = og_url
    if og_url:
        og_norm = normalize_url_for_compare(og_url)
        if canonical_norm and og_norm and og_norm != canonical_norm:
            warnings.append("SEO: og:url differs from canonical URL")
        elif og_norm and og_norm != page_norm:
            warnings.append("SEO: og:url differs from page URL")

    return meta, issues, warnings


def normalize_text_content(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


WORD_TOKEN_EDGE_CHARS = "\"'`.,!?;:()[]{}<>|/\\@#$%^&*_+=~\u0964\u0965\u2013\u2014\u2018\u2019\u201c\u201d\u2026"


def count_words(value: str) -> int:
    if not value:
        return 0
    normalized = normalize_text_content(html.unescape(value))
    if not normalized:
        return 0

    count = 0
    for token in normalized.split():
        cleaned = token.strip(WORD_TOKEN_EDGE_CHARS)
        if cleaned and any(char.isalnum() for char in cleaned):
            count += 1
    return count


def normalize_heading_text(value: str) -> str:
    normalized = normalize_text_content(value)
    normalized = re.sub(r"[^\w\s]", "", normalized, flags=re.UNICODE)
    return normalized.casefold()


def select_content_root(soup: BeautifulSoup) -> Optional[Tag]:
    candidates: List[Tuple[int, Tag]] = []
    seen_ids = set()
    for selector in CONTENT_ROOT_SELECTORS:
        for node in soup.select(selector):
            if not isinstance(node, Tag):
                continue
            node_id = id(node)
            if node_id in seen_ids:
                continue
            seen_ids.add(node_id)
            score = count_words(normalize_text_content(node.get_text(" ", strip=True)))
            if score:
                candidates.append((score, node))
    if candidates:
        candidates.sort(key=lambda item: item[0], reverse=True)
        return candidates[0][1]
    return soup.body if isinstance(soup.body, Tag) else None


def extract_schema_page_signals(html_text: str) -> Tuple[str, str, bool]:
    soup = BeautifulSoup(html_text, "html.parser")
    scripts = soup.find_all("script", attrs={"type": re.compile("ld\\+json", re.I)})
    best_headline = ""
    best_article_body = ""
    best_priority = -1
    is_live_blog = False

    for script in scripts:
        raw = script.string if script.string is not None else script.get_text()
        if not raw:
            continue
        raw = raw.strip()
        if not raw:
            continue
        data, error, _ = parse_jsonld_block(raw)
        if data is None:
            continue

        for obj in flatten_jsonld(data):
            if not isinstance(obj, dict):
                continue
            types = [normalize_schema_type(t) for t in extract_types(obj)]
            types = [t for t in types if t]
            if not types:
                continue
            if "LiveBlogPosting" in types:
                is_live_blog = True
            if not any(t in ("NewsArticle", "Article", "BlogPosting") for t in types):
                continue

            article_body = normalize_text_content(stringify_node(obj.get("articleBody")))
            headline = normalize_text_content(
                stringify_node(obj.get("headline") or obj.get("name"))
            )
            priority = 2 if "NewsArticle" in types else 1
            candidate_size = len(article_body)
            best_size = len(best_article_body)
            if priority > best_priority or (priority == best_priority and candidate_size > best_size):
                best_priority = priority
                best_article_body = article_body
                best_headline = headline

    return best_article_body, best_headline, is_live_blog


def extract_word_count(
    soup: BeautifulSoup,
    content_root: Optional[Tag],
    schema_article_body: str,
    is_live_blog: bool,
) -> Tuple[int, str]:
    if schema_article_body:
        return count_words(schema_article_body), "schema articleBody"
    scope = content_root if content_root is not None else soup
    text = normalize_text_content(scope.get_text(" ", strip=True))
    if is_live_blog:
        return count_words(text), "html content (live blog fallback)"
    return count_words(text), "html content"


def collect_heading_nodes(
    soup: BeautifulSoup, content_root: Optional[Tag], schema_headline: str
) -> List[Tag]:
    scope = content_root if content_root is not None else soup
    headings = [node for node in scope.find_all(["h1", "h2", "h3"]) if isinstance(node, Tag)]
    if not headings and scope is not soup:
        headings = [node for node in soup.find_all(["h1", "h2", "h3"]) if isinstance(node, Tag)]

    if any(node.name.lower() == "h1" for node in headings):
        return headings

    global_h1s = [node for node in soup.find_all("h1") if isinstance(node, Tag)]
    if not global_h1s:
        return headings

    preferred_h1: Optional[Tag] = None
    if schema_headline:
        schema_key = normalize_heading_text(schema_headline)
        for node in global_h1s:
            if normalize_heading_text(node.get_text(" ", strip=True)) == schema_key:
                preferred_h1 = node
                break
    if preferred_h1 is None and len(global_h1s) == 1:
        preferred_h1 = global_h1s[0]

    if preferred_h1 is None:
        return headings

    merged = [preferred_h1]
    seen = {id(preferred_h1)}
    for node in headings:
        node_id = id(node)
        if node_id in seen:
            continue
        seen.add(node_id)
        merged.append(node)
    return merged


def extract_heading_audit(
    soup: BeautifulSoup, content_root: Optional[Tag], schema_headline: str
) -> Tuple[int, int, int, List[str], List[str]]:
    headings = collect_heading_nodes(soup, content_root, schema_headline)

    counts = {"h1": 0, "h2": 0, "h3": 0}
    structure: List[str] = []
    issues: List[str] = []
    seen_issues = set()
    previous_level = 0
    empty_headings = 0

    for heading in headings:
        level = heading.name.lower()
        if level not in counts:
            continue
        counts[level] += 1
        heading_text = normalize_text_content(heading.get_text(" ", strip=True))
        if heading_text:
            structure.append(f"{level.upper()}: {truncate(heading_text, 140)}")
        else:
            empty_headings += 1

        level_number = int(level[1])
        if previous_level and level_number > previous_level + 1:
            issue = f"On-page: Heading hierarchy skips from H{previous_level} to H{level_number}"
            if issue not in seen_issues:
                issues.append(issue)
                seen_issues.add(issue)
        if heading_text:
            previous_level = level_number

    total_headings = sum(counts.values())
    if total_headings == 0:
        issues.append("On-page: No H1/H2/H3 headings found")
    elif counts["h1"] == 0:
        issues.append("On-page: Missing H1")
    elif counts["h1"] > 1:
        issues.append(f"On-page: Multiple H1 tags ({counts['h1']})")

    if empty_headings:
        issues.append(f"On-page: Empty heading tags ({empty_headings})")

    return counts["h1"], counts["h2"], counts["h3"], structure, issues


def extract_image_src(image: Tag, page_url: str) -> str:
    for attr in IMAGE_SOURCE_ATTRS:
        raw = image.get(attr)
        if raw and isinstance(raw, str):
            raw = raw.strip()
            if raw and not raw.startswith("data:"):
                return urljoin(page_url, raw)

    srcset = image.get("srcset") or image.get("data-srcset")
    if srcset and isinstance(srcset, str):
        first = srcset.split(",", 1)[0].strip().split(" ", 1)[0]
        if first and not first.startswith("data:"):
            return urljoin(page_url, first)
    return ""


def image_basename(value: str) -> str:
    if not value:
        return ""
    try:
        path = urlparse(value).path
    except Exception:
        path = value
    return os.path.basename(path).lower()


def normalize_image_url(value: str, page_url: str) -> str:
    if not value:
        return ""
    return urljoin(page_url, value.strip())


def classify_feature_image_alt(alt_text: str) -> str:
    normalized = normalize_text_content(alt_text).lower()
    if not normalized:
        return "Missing"
    if normalized in GENERIC_ALT_VALUES or len(normalized) < 5:
        return "Weak"
    return "Pass"


def extract_feature_image_audit(
    soup: BeautifulSoup,
    page_url: str,
    seo_meta: Dict[str, str],
    schema_objects: List[Dict[str, str]],
    content_root: Optional[Tag],
) -> Tuple[str, str, str]:
    candidate_urls: List[str] = []
    for value in (seo_meta.get("og:image", ""), seo_meta.get("twitter:image", "")):
        if value:
            candidate_urls.append(normalize_image_url(value, page_url))

    for obj in schema_objects:
        image_value = obj.get("image", "")
        if image_value:
            candidate_urls.append(normalize_image_url(image_value, page_url))

    deduped_candidates: List[str] = []
    seen_candidates = set()
    for value in candidate_urls:
        if not value:
            continue
        key = normalize_url_for_compare(value)
        if key in seen_candidates:
            continue
        seen_candidates.add(key)
        deduped_candidates.append(value)

    dom_scope = content_root if content_root is not None else soup
    dom_images = dom_scope.find_all("img")
    if not dom_images and dom_scope is not soup:
        dom_images = soup.find_all("img")

    matched_image: Optional[Tag] = None
    matched_src = ""
    candidate_keys = [normalize_url_for_compare(value) for value in deduped_candidates if value]
    candidate_basenames = {image_basename(value) for value in deduped_candidates if value}

    for image in dom_images:
        image_src = extract_image_src(image, page_url)
        if not image_src:
            continue
        image_key = normalize_url_for_compare(image_src)
        base_name = image_basename(image_src)
        if image_key in candidate_keys or (base_name and base_name in candidate_basenames):
            matched_image = image
            matched_src = image_src
            break

    if matched_image is None and dom_images:
        for image in dom_images:
            image_src = extract_image_src(image, page_url)
            if image_src:
                matched_image = image
                matched_src = image_src
                break

    if matched_image is not None:
        alt_text = normalize_text_content(str(matched_image.get("alt", "")))
        return matched_src, alt_text, classify_feature_image_alt(alt_text)

    if deduped_candidates:
        return deduped_candidates[0], "", "Not found in HTML"

    return "", "", "Not found"


def extract_author_info(value, id_map: Optional[Dict[str, dict]] = None) -> Tuple[List[str], bool]:
    names: List[str] = []
    has_typed = False

    def walk(val):
        nonlocal has_typed
        if val is None:
            return
        if isinstance(val, list):
            for item in val:
                walk(item)
            return
        if isinstance(val, dict):
            if id_map and isinstance(val.get("@id"), str):
                ref = id_map.get(val["@id"])
                if ref:
                    walk(ref)
            obj_type = val.get("@type")
            if isinstance(obj_type, list):
                obj_type = obj_type[0] if obj_type else None
            if isinstance(obj_type, str):
                norm = normalize_schema_type(obj_type)
                if norm in ("Person", "Organization"):
                    has_typed = True
            name = val.get("name")
            if isinstance(name, str) and name.strip():
                names.append(name.strip())
                return
            return
        if isinstance(val, str):
            if val.strip():
                names.append(val.strip())

    walk(value)
    names = list(dict.fromkeys(names))
    return names, has_typed

def collect_itemprops(root: Tag) -> List[Tag]:
    collected: List[Tag] = []

    def walk(node: Tag) -> None:
        for child in node.children:
            if not isinstance(child, Tag):
                continue
            if child.has_attr("itemscope") and child is not root:
                if child.has_attr("itemprop"):
                    collected.append(child)
                continue
            if child.has_attr("itemprop"):
                collected.append(child)
            walk(child)

    walk(root)
    return collected


def collect_rdfa_properties(root: Tag) -> List[Tag]:
    collected: List[Tag] = []

    def walk(node: Tag) -> None:
        for child in node.children:
            if not isinstance(child, Tag):
                continue
            if child.has_attr("typeof") and child is not root:
                if child.has_attr("property"):
                    collected.append(child)
                continue
            if child.has_attr("property"):
                collected.append(child)
            walk(child)

    walk(root)
    return collected


def collect_nested_schema_objects(
    obj: dict,
    rules: Dict[str, dict],
    id_map: Optional[Dict[str, dict]],
    schemaorg_ref: Optional[Dict[str, List[str]]],
    parent_path: str,
    max_depth: int = 4,
) -> List[Dict[str, str]]:
    collected: List[Dict[str, str]] = []
    seen: set = set()

    def fingerprint(summary: Dict[str, str]) -> str:
        return "|".join(
            [
                summary.get("type", ""),
                summary.get("name", ""),
                summary.get("url", ""),
                summary.get("headline", ""),
                summary.get("source", ""),
            ]
        )

    def walk(value, path: str, depth: int, include_self: bool) -> None:
        if depth <= 0:
            return
        if isinstance(value, list):
            for item in value:
                walk(item, path, depth - 1, True)
            return
        if not isinstance(value, dict):
            return

        types = extract_types(value)
        if include_self and types:
            summary = summarize_schema_object(
                value, types, rules, id_map, schemaorg_ref, source=path
            )
            key = fingerprint(summary)
            if key not in seen:
                collected.append(summary)
                seen.add(key)

        for key, child in value.items():
            if key.startswith("@"):
                continue
            child_path = f"{path}.{key}" if path else key
            walk(child, child_path, depth - 1, True)

    walk(obj, parent_path, max_depth, False)
    return collected


def resolve_rules(type_name: str, rules: Dict[str, dict], seen: Optional[set] = None) -> Dict[str, List[str]]:
    seen = seen or set()
    if type_name in seen:
        return {"required": [], "recommended": []}
    seen.add(type_name)

    types = rules.get("types", {})
    entry = types.get(type_name)
    if not isinstance(entry, dict):
        return {"required": [], "recommended": []}

    required = list(entry.get("required", []))
    recommended = list(entry.get("recommended", []))
    for parent in entry.get("extends", []):
        parent_rules = resolve_rules(parent, rules, seen)
        required = parent_rules["required"] + required
        recommended = parent_rules["recommended"] + recommended

    required = list(dict.fromkeys(required))
    recommended = list(dict.fromkeys(recommended))
    return {"required": required, "recommended": recommended}


def validate_faq(obj: dict) -> Tuple[List[str], List[str]]:
    issues: List[str] = []
    warnings: List[str] = []
    main = obj.get("mainEntity")
    if not main:
        issues.append("FAQPage missing mainEntity")
        return issues, warnings
    items = main if isinstance(main, list) else [main]
    for idx, entry in enumerate(items, start=1):
        if not isinstance(entry, dict):
            warnings.append(f"FAQPage mainEntity item {idx} is not an object")
            continue
        entry_type = entry.get("@type")
        if isinstance(entry_type, list):
            entry_type = entry_type[0] if entry_type else None
        if entry_type and normalize_schema_type(entry_type) != "Question":
            warnings.append(f"FAQPage mainEntity item {idx} is not a Question")
        if is_empty(entry.get("name")):
            warnings.append(f"FAQPage Question {idx} missing name")
        accepted = entry.get("acceptedAnswer")
        if not accepted:
            warnings.append(f"FAQPage Question {idx} missing acceptedAnswer")
            continue
        if isinstance(accepted, dict):
            if is_empty(accepted.get("text")) and is_empty(accepted.get("name")):
                warnings.append(f"FAQPage Question {idx} acceptedAnswer missing text")
    return issues, warnings


def validate_breadcrumb(obj: dict) -> Tuple[List[str], List[str]]:
    issues: List[str] = []
    warnings: List[str] = []
    items = obj.get("itemListElement")
    if not items:
        issues.append("BreadcrumbList missing itemListElement")
        return issues, warnings
    entries = items if isinstance(items, list) else [items]
    for idx, entry in enumerate(entries, start=1):
        if not isinstance(entry, dict):
            warnings.append(f"BreadcrumbList item {idx} is not an object")
            continue
        if is_empty(entry.get("position")):
            warnings.append(f"BreadcrumbList item {idx} missing position")
        item = entry.get("item")
        if not item:
            warnings.append(f"BreadcrumbList item {idx} missing item")
        elif isinstance(item, dict):
            if is_empty(item.get("name")):
                warnings.append(f"BreadcrumbList item {idx} missing item.name")
    return issues, warnings


def validate_howto(obj: dict) -> Tuple[List[str], List[str]]:
    issues: List[str] = []
    warnings: List[str] = []
    steps = obj.get("step")
    if not steps:
        issues.append("HowTo missing step")
        return issues, warnings
    entries = steps if isinstance(steps, list) else [steps]
    for idx, entry in enumerate(entries, start=1):
        if isinstance(entry, dict):
            if is_empty(entry.get("name")) and is_empty(entry.get("text")):
                warnings.append(f"HowTo step {idx} missing name/text")
        elif isinstance(entry, str):
            if not entry.strip():
                warnings.append(f"HowTo step {idx} is empty")
    return issues, warnings


def validate_schema_object(
    obj: dict,
    rules: Dict[str, dict],
    schema_context: bool,
    id_map: Optional[Dict[str, dict]] = None,
    meta_authors: Optional[List[str]] = None,
    schemaorg_ref: Optional[Dict[str, List[str]]] = None,
) -> Tuple[List[str], List[str], List[str]]:
    issues: List[str] = []
    warnings: List[str] = []
    jsonld_types: List[str] = []

    raw_types = extract_types(obj)
    if not raw_types:
        issues.append("JSON-LD object missing @type")
        return issues, warnings, jsonld_types

    normalized_types = []
    for t in raw_types:
        norm = normalize_schema_type(t)
        if norm:
            normalized_types.append(norm)
            jsonld_types.append(norm)

    has_schema_type = any(is_schema_iri(t) for t in raw_types)
    has_known_type = any(is_known_schema_type(t, rules, schemaorg_ref) for t in normalized_types)
    if not schema_context and not has_schema_type and not has_known_type:
        return issues, warnings, jsonld_types

    for t in normalized_types:
        rules_for_type = resolve_rules(t, rules)
        for prop in rules_for_type.get("required", []):
            if not has_value(obj, prop):
                issues.append(f"{t} missing required property '{prop}'")
        for prop in rules_for_type.get("recommended", []):
            if not has_value(obj, prop):
                warnings.append(f"{t} missing recommended property '{prop}'")

        if t in ("Article", "NewsArticle", "BlogPosting"):
            author_value = obj.get("author") or obj.get("creator") or obj.get("byline")
            author_names, has_typed_author = extract_author_info(author_value, id_map)
            meta_authors = meta_authors or []
            meta_blob = ", ".join(meta_authors)
            if not author_names:
                if meta_authors:
                    issues.append(
                        f"{t} missing author in schema (page meta has author: {meta_blob})"
                    )
                else:
                    warnings.append(f"{t} missing author in schema")
            else:
                if not has_typed_author:
                    issues.append(
                        f"{t} author should be Person/Organization with @type (found string/untyped)"
                    )
                if meta_authors:
                    if not any(name in meta_blob for name in author_names):
                        warnings.append(
                            f"{t} author differs from page meta author ({meta_blob})"
                        )

        if t == "FAQPage":
            faq_issues, faq_warnings = validate_faq(obj)
            issues.extend(faq_issues)
            warnings.extend(faq_warnings)
        if t == "BreadcrumbList":
            bc_issues, bc_warnings = validate_breadcrumb(obj)
            issues.extend(bc_issues)
            warnings.extend(bc_warnings)
        if t == "HowTo":
            how_issues, how_warnings = validate_howto(obj)
            issues.extend(how_issues)
            warnings.extend(how_warnings)

    return issues, warnings, jsonld_types


def extract_schemas(
    html_text: str, rules: Dict[str, dict], schemaorg_ref: Optional[Dict[str, List[str]]]
) -> Tuple[
    int,
    List[str],
    int,
    int,
    List[str],
    List[str],
    List[Dict[str, str]],
    List[Dict[str, str]],
    List[Dict[str, str]],
]:
    soup = BeautifulSoup(html_text, "html.parser")
    meta_authors = collect_meta_authors(soup)

    jsonld_blocks = 0
    jsonld_types: List[str] = []
    issues: List[str] = []
    warnings: List[str] = []
    schema_objects: List[Dict[str, str]] = []
    microdata_objects: List[Dict[str, str]] = []
    rdfa_objects: List[Dict[str, str]] = []

    scripts = soup.find_all("script", attrs={"type": re.compile("ld\\+json", re.I)})
    for script in scripts:
        raw = script.string if script.string is not None else script.get_text()
        if not raw:
            continue
        raw = raw.strip()
        if not raw:
            continue
        data, error, sanitized = parse_jsonld_block(raw)
        if data is None:
            issues.append(f"Invalid JSON-LD block: {error}")
            continue
        if sanitized:
            warnings.append("JSON-LD block sanitized before parsing")
        jsonld_blocks += 1
        root_context = data.get("@context") if isinstance(data, dict) else None
        if isinstance(data, dict) and "@context" not in data:
            warnings.append("JSON-LD block missing @context")

        id_map: Dict[str, dict] = {}
        for obj in flatten_jsonld(data):
            if isinstance(obj, dict) and isinstance(obj.get("@id"), str):
                id_map[obj["@id"]] = obj

        for obj in flatten_jsonld(data):
            if not isinstance(obj, dict):
                continue
            obj_context = obj.get("@context", root_context)
            schema_context = context_has_schema_org(obj_context)
            raw_types = extract_types(obj)
            has_schema_type = any(is_schema_iri(t) for t in raw_types)

            obj_issues, obj_warnings, obj_types = validate_schema_object(
                obj, rules, schema_context, id_map, meta_authors, schemaorg_ref
            )
            issues.extend(obj_issues)
            warnings.extend(obj_warnings)
            jsonld_types.extend(obj_types)
            if obj_types and (schema_context or has_schema_type):
                summary = summarize_schema_object(
                    obj, obj_types, rules, id_map, schemaorg_ref, source="root"
                )
                schema_objects.append(summary)

                nested = collect_nested_schema_objects(
                    obj,
                    rules,
                    id_map,
                    schemaorg_ref,
                    parent_path=obj_types[0] if obj_types else "root",
                )
                schema_objects.extend(nested)

    microdata_items = 0
    for item in soup.find_all(attrs={"itemscope": True}):
        microdata_items += 1
        itemtype_attr = item.get("itemtype") or ""
        types = parse_space_values(itemtype_attr)
        if not types:
            issues.append("Microdata itemscope missing itemtype")
        prop_elements = collect_itemprops(item)
        if not prop_elements:
            issues.append("Microdata itemscope missing itemprop")

        prop_names: set = set()
        prop_values: Dict[str, List[str]] = {}
        missing_required: List[str] = []
        missing_recommended: List[str] = []
        allowed_props: List[str] = []
        for prop_el in prop_elements:
            itemprop_attr = prop_el.get("itemprop") or ""
            props = parse_space_values(itemprop_attr)
            value = extract_property_value(prop_el)
            for prop in props:
                prop_names.add(prop)
                if value:
                    prop_values.setdefault(prop, []).append(value)

        for t in types:
            rules_for_type = resolve_rules(t, rules)
            for prop in rules_for_type.get("required", []):
                if not prop_present(prop_names, prop):
                    issues.append(f"Microdata {t} missing required property '{prop}'")
                    missing_required.append(f"{t}:{prop}")
            for prop in rules_for_type.get("recommended", []):
                if not prop_present(prop_names, prop):
                    warnings.append(f"Microdata {t} missing recommended property '{prop}'")
                    missing_recommended.append(f"{t}:{prop}")
            if schemaorg_ref:
                allowed_props.extend(schemaorg_ref.get(t, []))

        if types:
            allowed_props = sorted(set(allowed_props))
            summary_props = []
            for key in sorted(prop_values.keys()):
                values = ", ".join(prop_values[key][:2])
                summary_props.append(f"{key}={truncate(values, 120)}")
            microdata_objects.append(
                {
                    "type": ", ".join(types),
                    "properties": truncate("; ".join(summary_props), 400),
                    "missing_required": truncate(
                        ", ".join(sorted(set(missing_required))), 300
                    )
                    if missing_required
                    else "",
                    "missing_recommended": truncate(
                        ", ".join(sorted(set(missing_recommended))), 300
                    )
                    if missing_recommended
                    else "",
                    "allowed_properties": truncate(summarize_list(allowed_props, 30), 400)
                    if allowed_props
                    else "",
                }
            )

    rdfa_elements = len(soup.find_all(attrs={"typeof": True})) + len(
        soup.find_all(attrs={"property": True})
    )
    for element in soup.find_all(attrs={"typeof": True}):
        typeof_attr = element.get("typeof") or ""
        types = parse_space_values(typeof_attr)
        prop_elements = collect_rdfa_properties(element)

        prop_names: set = set()
        prop_values: Dict[str, List[str]] = {}
        missing_required: List[str] = []
        missing_recommended: List[str] = []
        allowed_props: List[str] = []
        for prop_el in prop_elements:
            prop_attr = prop_el.get("property") or ""
            props = parse_space_values(prop_attr)
            value = extract_property_value(prop_el)
            for prop in props:
                prop_names.add(prop)
                if value:
                    prop_values.setdefault(prop, []).append(value)

        for t in types:
            rules_for_type = resolve_rules(t, rules)
            for prop in rules_for_type.get("required", []):
                if not prop_present(prop_names, prop):
                    issues.append(f"RDFa {t} missing required property '{prop}'")
                    missing_required.append(f"{t}:{prop}")
            for prop in rules_for_type.get("recommended", []):
                if not prop_present(prop_names, prop):
                    warnings.append(f"RDFa {t} missing recommended property '{prop}'")
                    missing_recommended.append(f"{t}:{prop}")
            if schemaorg_ref:
                allowed_props.extend(schemaorg_ref.get(t, []))

        if types:
            allowed_props = sorted(set(allowed_props))
            summary_props = []
            for key in sorted(prop_values.keys()):
                values = ", ".join(prop_values[key][:2])
                summary_props.append(f"{key}={truncate(values, 120)}")
            rdfa_objects.append(
                {
                    "type": ", ".join(types),
                    "properties": truncate("; ".join(summary_props), 400),
                    "missing_required": truncate(
                        ", ".join(sorted(set(missing_required))), 300
                    )
                    if missing_required
                    else "",
                    "missing_recommended": truncate(
                        ", ".join(sorted(set(missing_recommended))), 300
                    )
                    if missing_recommended
                    else "",
                    "allowed_properties": truncate(summarize_list(allowed_props, 30), 400)
                    if allowed_props
                    else "",
                }
            )
    if rdfa_elements == 0:
        # Not necessarily an issue; many pages use JSON-LD only.
        pass

    return (
        jsonld_blocks,
        jsonld_types,
        microdata_items,
        rdfa_elements,
        issues,
        warnings,
        schema_objects,
        microdata_objects,
        rdfa_objects,
    )


def check_url(
    session: requests.Session,
    url: str,
    rp: RobotFileParser,
    user_agent: str,
    rules: Dict[str, dict],
    schemaorg_ref: Optional[Dict[str, List[str]]],
    gsc_service=None,
    gsc_candidate_domains: Optional[List[str]] = None,
    gsc_cache: Optional[Dict[str, Dict[str, object]]] = None,
    gsc_cache_ttl_hours: int = DEFAULT_GSC_CACHE_TTL_HOURS,
) -> UrlCheckResult:
    if not rp.can_fetch(user_agent, url):
        return UrlCheckResult(
            url=url,
            http_status=None,
            fetch_error=None,
            content_type=None,
            skipped_by_robots=True,
            indexability_status="Blocked (robots.txt)",
            indexability_reasons=["Blocked by robots.txt"],
            issues=["Blocked by robots.txt"],
        )

    resp, err = fetch_url(session, url, user_agent)
    if resp is None:
        return UrlCheckResult(
            url=url,
            http_status=None,
            fetch_error=err,
            content_type=None,
            skipped_by_robots=False,
            indexability_status="Error",
            indexability_reasons=[f"Fetch error: {err}"],
            issues=[f"Fetch error: {err}"],
        )

    content_type = resp.headers.get("Content-Type")
    if resp.status_code >= 400:
        return UrlCheckResult(
            url=url,
            http_status=resp.status_code,
            fetch_error=None,
            content_type=content_type,
            skipped_by_robots=False,
            indexability_status="Error",
            indexability_reasons=[f"HTTP {resp.status_code}"],
            issues=[f"HTTP {resp.status_code}"],
        )

    if content_type and "text/html" not in content_type.lower():
        return UrlCheckResult(
            url=url,
            http_status=resp.status_code,
            fetch_error=None,
            content_type=content_type,
            skipped_by_robots=False,
            indexability_status="Non-HTML",
            indexability_reasons=[f"Content-Type: {content_type}"],
            issues=[f"Skipped non-HTML content-type: {content_type}"],
        )

    (
        jsonld_blocks,
        jsonld_types,
        microdata_items,
        rdfa_elements,
        issues,
        warnings,
        schema_objects,
        microdata_objects,
        rdfa_objects,
    ) = extract_schemas(resp.text, rules, schemaorg_ref)

    soup = BeautifulSoup(resp.text, "html.parser")
    seo_meta, seo_issues, seo_warnings = extract_seo_meta(soup, url)
    schema_article_body, schema_headline, is_live_blog = extract_schema_page_signals(
        resp.text
    )
    content_root = select_content_root(soup)
    word_count, word_count_source = extract_word_count(
        soup, content_root, schema_article_body, is_live_blog
    )
    (
        heading_h1_count,
        heading_h2_count,
        heading_h3_count,
        heading_structure,
        heading_issues,
    ) = extract_heading_audit(soup, content_root, schema_headline)
    feature_image_url, feature_image_alt, feature_image_status = extract_feature_image_audit(
        soup,
        url,
        seo_meta,
        schema_objects,
        content_root,
    )
    robots_meta, meta_directives = extract_robots_meta(soup)
    x_robots_tag = resp.headers.get("X-Robots-Tag", "")
    x_directives = parse_directives(x_robots_tag)
    redirect_chain = [r.url for r in resp.history] if resp.history else []
    final_url = resp.url
    canonical_match = seo_meta.get("canonical_match", "")
    soft_404 = detect_soft_404(soup)
    hreflang_status, hreflang_issues = extract_hreflang(soup)
    pagination = extract_pagination(soup)
    auth_blocked = detect_access_block(soup, resp.status_code)
    indexability_status, indexability_reasons = classify_indexability(
        skipped_by_robots=False,
        http_status=resp.status_code,
        content_type=content_type,
        meta_directives=meta_directives,
        x_directives=x_directives,
        canonical_match=canonical_match,
        redirect_chain=redirect_chain,
        final_url=final_url,
        soft_404=soft_404,
    )
    issues.extend(seo_issues)
    warnings.extend(seo_warnings)
    if soft_404:
        warnings.append("Soft 404 suspected (200 status with not-found content)")
    if hreflang_issues:
        warnings.append("Hreflang issues: " + "; ".join(hreflang_issues[:3]))
    if auth_blocked:
        warnings.append(f"Access block suspected: {auth_blocked}")
    if word_count == 0:
        no_copy_issue = "On-page: No meaningful body copy detected"
        warnings.append(no_copy_issue)
        seo_warnings.append(no_copy_issue)
    elif word_count < WORD_COUNT_WARNING_THRESHOLD:
        low_word_count = f"On-page: Low word count ({word_count} words)"
        warnings.append(low_word_count)
        seo_warnings.append(low_word_count)
    for heading_issue in heading_issues:
        warnings.append(heading_issue)
        seo_warnings.append(heading_issue)
    if feature_image_status == "Missing":
        alt_issue = "On-page: Feature image missing alt text"
        warnings.append(alt_issue)
        seo_warnings.append(alt_issue)
    elif feature_image_status == "Weak":
        alt_issue = "On-page: Feature image alt text is weak"
        warnings.append(alt_issue)
        seo_warnings.append(alt_issue)

    gsc_result: Dict[str, object] = {}
    gsc_property = ""
    if gsc_service is not None:
        gsc_property = infer_gsc_property(url, gsc_candidate_domains or [])
        if not gsc_property:
            gsc_result = {
                "status": "",
                "property": "",
                "error": "No GSC property mapping configured for this domain",
                "verdict": "",
                "coverage_state": "",
                "indexing_state": "",
                "robots_state": "",
                "page_fetch_state": "",
                "last_crawl_time": "",
                "google_canonical": "",
                "user_canonical": "",
                "sitemaps": [],
                "referring_urls": [],
                "checked_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            }
        else:
            cached = get_cached_gsc_result(
                gsc_cache or {}, gsc_property, url, gsc_cache_ttl_hours
            )
            if cached is not None:
                gsc_result = cached
            else:
                gsc_result = inspect_url_in_gsc(gsc_service, url, gsc_property)
                if gsc_cache is not None:
                    set_cached_gsc_result(gsc_cache, gsc_property, url, gsc_result)
        gsc_error = str(gsc_result.get("error", "") or "")
        gsc_status = str(gsc_result.get("status", "") or "")
        if gsc_error:
            warnings.append(f"GSC: {gsc_error}")
            seo_warnings.append(f"GSC: {gsc_error}")
        elif gsc_status and gsc_status != "Indexed":
            warnings.append(f"GSC: {gsc_status}")
            seo_warnings.append(f"GSC: {gsc_status}")

    return UrlCheckResult(
        url=url,
        http_status=resp.status_code,
        fetch_error=None,
        content_type=content_type,
        skipped_by_robots=False,
        indexability_status=indexability_status,
        indexability_reasons=indexability_reasons,
        robots_meta=robots_meta,
        x_robots_tag=x_robots_tag,
        final_url=final_url,
        redirect_chain=redirect_chain,
        soft_404=soft_404,
        hreflang_status=hreflang_status,
        hreflang_issues=hreflang_issues,
        pagination=pagination,
        auth_blocked=auth_blocked,
        jsonld_blocks=jsonld_blocks,
        jsonld_types=jsonld_types,
        microdata_items=microdata_items,
        rdfa_elements=rdfa_elements,
        issues=issues,
        warnings=warnings,
        schema_objects=schema_objects,
        microdata_objects=microdata_objects,
        rdfa_objects=rdfa_objects,
        seo_meta=seo_meta,
        seo_issues=seo_issues,
        seo_warnings=seo_warnings,
        word_count=word_count,
        word_count_source=word_count_source,
        heading_h1_count=heading_h1_count,
        heading_h2_count=heading_h2_count,
        heading_h3_count=heading_h3_count,
        heading_structure=heading_structure,
        feature_image_url=feature_image_url,
        feature_image_alt=feature_image_alt,
        feature_image_status=feature_image_status,
        gsc_property=gsc_property or str(gsc_result.get("property", "") or ""),
        gsc_status=str(gsc_result.get("status", "") or ""),
        gsc_verdict=str(gsc_result.get("verdict", "") or ""),
        gsc_coverage_state=str(gsc_result.get("coverage_state", "") or ""),
        gsc_indexing_state=str(gsc_result.get("indexing_state", "") or ""),
        gsc_robots_state=str(gsc_result.get("robots_state", "") or ""),
        gsc_page_fetch_state=str(gsc_result.get("page_fetch_state", "") or ""),
        gsc_last_crawl_time=str(gsc_result.get("last_crawl_time", "") or ""),
        gsc_google_canonical=str(gsc_result.get("google_canonical", "") or ""),
        gsc_user_canonical=str(gsc_result.get("user_canonical", "") or ""),
        gsc_sitemaps=list(gsc_result.get("sitemaps", []) or []),
        gsc_referring_urls=list(gsc_result.get("referring_urls", []) or []),
        gsc_error=str(gsc_result.get("error", "") or ""),
        gsc_checked_at=str(gsc_result.get("checked_at", "") or ""),
    )


def gather_site(
    domain: str,
    max_urls: int,
    user_agent: str,
    rules: Dict[str, dict],
    schemaorg_ref: Optional[Dict[str, List[str]]],
    gsc_service=None,
    gsc_candidate_domains: Optional[List[str]] = None,
    gsc_cache: Optional[Dict[str, Dict[str, object]]] = None,
    gsc_cache_ttl_hours: int = DEFAULT_GSC_CACHE_TTL_HOURS,
    sitemap_urls: Optional[List[str]] = None,
    page_urls: Optional[List[str]] = None,
) -> SiteReport:
    session = requests.Session()

    rp, robots_url, robots_status, robots_error, robots_sitemaps = get_robots(
        domain, session, user_agent
    )

    if sitemap_urls is not None:
        sitemap_urls_final = sitemap_urls
    else:
        sitemap_urls_final = robots_sitemaps
        if not sitemap_urls_final:
            sitemap_urls_final = [f"https://{domain}/sitemap.xml"]

    site_report = SiteReport(
        domain=domain,
        robots_url=robots_url,
        robots_status=robots_status,
        robots_error=robots_error,
    )

    queue = list(sitemap_urls_final)
    seen = set()
    urls: List[str] = []

    while queue and len(urls) < max_urls:
        sitemap_url = queue.pop(0)
        if sitemap_url in seen:
            continue
        seen.add(sitemap_url)

        resp, err = fetch_url(session, sitemap_url, user_agent)
        if resp is None:
            site_report.sitemaps.append(
                SitemapFetchResult(url=sitemap_url, status_code=None, error=err)
            )
            continue

        if resp.status_code >= 400:
            site_report.sitemaps.append(
                SitemapFetchResult(
                    url=sitemap_url, status_code=resp.status_code, error=None
                )
            )
            continue

        xml_bytes = maybe_decompress(resp.content, resp.url, resp.headers.get("Content-Encoding"))
        try:
            kind, entries = parse_sitemap(xml_bytes)
        except Exception as exc:
            site_report.sitemaps.append(
                SitemapFetchResult(
                    url=sitemap_url,
                    status_code=resp.status_code,
                    error=f"Invalid XML: {exc}",
                )
            )
            continue

        if kind == "sitemapindex":
            locs = [entry.get("loc") for entry in entries if entry.get("loc")]
            queue.extend(locs)
            lastmod_missing = sum(1 for entry in entries if not entry.get("lastmod"))
            lastmod_invalid = sum(
                1
                for entry in entries
                if entry.get("lastmod") and not parse_iso_date(entry.get("lastmod", ""))
            )
            lastmod_stale = 0
            now = dt.datetime.now(dt.timezone.utc)
            for entry in entries:
                lastmod = entry.get("lastmod")
                if not lastmod:
                    continue
                parsed = parse_iso_date(lastmod)
                if not parsed:
                    continue
                parsed = normalize_datetime(parsed)
                if (now - parsed).days > SITEMAP_STALE_DAYS:
                    lastmod_stale += 1

            site_report.sitemaps.append(
                SitemapFetchResult(
                    url=sitemap_url,
                    status_code=resp.status_code,
                    error=None,
                    kind=kind,
                    sitemaps_found=len(locs),
                    lastmod_missing=lastmod_missing,
                    lastmod_invalid=lastmod_invalid,
                    lastmod_stale=lastmod_stale,
                    entry_samples=entries[:5],
                )
            )
            continue

        if kind == "urlset":
            new_urls = [
                entry.get("loc")
                for entry in entries
                if entry.get("loc") and same_domain(entry.get("loc"), domain)
            ]
            for u in new_urls:
                if u not in urls:
                    urls.append(u)
                if len(urls) >= max_urls:
                    break

            lastmod_missing = sum(1 for entry in entries if not entry.get("lastmod"))
            lastmod_invalid = sum(
                1
                for entry in entries
                if entry.get("lastmod") and not parse_iso_date(entry.get("lastmod", ""))
            )
            lastmod_stale = 0
            now = dt.datetime.now(dt.timezone.utc)
            for entry in entries:
                lastmod = entry.get("lastmod")
                if not lastmod:
                    continue
                parsed = parse_iso_date(lastmod)
                if not parsed:
                    continue
                parsed = normalize_datetime(parsed)
                if (now - parsed).days > SITEMAP_STALE_DAYS:
                    lastmod_stale += 1

            changefreq_missing = sum(1 for entry in entries if not entry.get("changefreq"))
            changefreq_invalid = sum(
                1
                for entry in entries
                if entry.get("changefreq")
                and entry.get("changefreq", "").strip().lower() not in VALID_CHANGEFREQ
            )
            priority_invalid = 0
            for entry in entries:
                if not entry.get("priority"):
                    continue
                try:
                    value = float(entry.get("priority"))
                    if value < 0.0 or value > 1.0:
                        priority_invalid += 1
                except ValueError:
                    priority_invalid += 1

            news_entries = 0
            news_missing_pub = 0
            publication_names = set()
            for entry in entries:
                if "news_publication_name" in entry or "news_publication_date" in entry:
                    news_entries += 1
                    name = entry.get("news_publication_name", "").strip()
                    if name:
                        publication_names.add(name)
                    else:
                        news_missing_pub += 1

            site_report.sitemaps.append(
                SitemapFetchResult(
                    url=sitemap_url,
                    status_code=resp.status_code,
                    error=None,
                    kind=kind,
                    urls_found=len(entries),
                    lastmod_missing=lastmod_missing,
                    lastmod_invalid=lastmod_invalid,
                    lastmod_stale=lastmod_stale,
                    changefreq_missing=changefreq_missing,
                    changefreq_invalid=changefreq_invalid,
                    priority_invalid=priority_invalid,
                    news_entries=news_entries,
                    news_missing_publication=news_missing_pub,
                    news_publication_names=sorted(publication_names),
                    entry_samples=entries[:5],
                )
            )
            continue

        site_report.sitemaps.append(
            SitemapFetchResult(
                url=sitemap_url,
                status_code=resp.status_code,
                error="Unknown sitemap type",
                kind=kind,
            )
        )

    if sitemap_urls_final and not urls:
        site_report.notes.append("No URLs found in sitemaps.")
    if sitemap_urls is not None:
        if sitemap_urls:
            site_report.notes.append("Using provided sitemap URLs.")
        else:
            site_report.notes.append("Sitemap crawling disabled; using only explicit URLs.")

    explicit_urls = page_urls or []
    for url in explicit_urls:
        if url not in urls:
            urls.append(url)

    for url in urls:
        result = check_url(
            session,
            url,
            rp,
            user_agent,
            rules,
            schemaorg_ref,
            gsc_service=gsc_service,
            gsc_candidate_domains=gsc_candidate_domains,
            gsc_cache=gsc_cache,
            gsc_cache_ttl_hours=gsc_cache_ttl_hours,
        )
        site_report.urls.append(result)

    return site_report


def sitemap_seo_summary(sm: SitemapFetchResult) -> str:
    parts = []
    if sm.kind == "urlset":
        if sm.lastmod_missing:
            parts.append(f"lastmod missing: {sm.lastmod_missing}")
        if sm.lastmod_invalid:
            parts.append(f"lastmod invalid: {sm.lastmod_invalid}")
        if sm.lastmod_stale:
            parts.append(f"lastmod stale(>{SITEMAP_STALE_DAYS}d): {sm.lastmod_stale}")
        if not sm.lastmod_missing and not sm.lastmod_invalid and not sm.lastmod_stale:
            parts.append("lastmod OK")
        if sm.changefreq_missing:
            parts.append(f"changefreq missing: {sm.changefreq_missing}")
        if sm.changefreq_invalid:
            parts.append(f"changefreq invalid: {sm.changefreq_invalid}")
        if sm.priority_invalid:
            parts.append(f"priority invalid: {sm.priority_invalid}")
        if sm.news_entries:
            parts.append(f"news entries: {sm.news_entries}")
            if sm.news_missing_publication:
                parts.append(f"news pub missing: {sm.news_missing_publication}")
    if sm.kind == "sitemapindex":
        if sm.lastmod_missing:
            parts.append(f"lastmod missing: {sm.lastmod_missing}")
        if sm.lastmod_invalid:
            parts.append(f"lastmod invalid: {sm.lastmod_invalid}")
        if sm.lastmod_stale:
            parts.append(f"lastmod stale(>{SITEMAP_STALE_DAYS}d): {sm.lastmod_stale}")
        if not sm.lastmod_missing and not sm.lastmod_invalid and not sm.lastmod_stale:
            parts.append("lastmod OK")
    return "; ".join(parts) if parts else "No SEO issues detected"


def sitemap_recommendations(sm: SitemapFetchResult) -> List[str]:
    recs: List[str] = []
    if sm.error:
        recs.append(f"Fix sitemap fetch/parsing error: {sm.error}")
        return recs
    if sm.status_code and sm.status_code >= 400:
        recs.append(f"Fix sitemap HTTP status {sm.status_code}")
        return recs

    if sm.kind == "urlset":
        if sm.lastmod_missing:
            recs.append(
                f"Add <lastmod> for {sm.lastmod_missing} URLs to improve crawl efficiency and freshness signals."
            )
        if sm.lastmod_invalid:
            recs.append(
                f"Correct {sm.lastmod_invalid} invalid <lastmod> values to ISO 8601 format."
            )
        if sm.lastmod_stale:
            recs.append(
                f"Review {sm.lastmod_stale} URLs with <lastmod> older than {SITEMAP_STALE_DAYS} days."
            )
        if sm.changefreq_invalid:
            recs.append(
                f"Fix {sm.changefreq_invalid} invalid <changefreq> values (allowed: {', '.join(sorted(VALID_CHANGEFREQ))})."
            )
        if sm.changefreq_missing:
            recs.append(
                f"Consider adding <changefreq> for {sm.changefreq_missing} URLs if you want explicit crawl hints."
            )
        if sm.priority_invalid:
            recs.append(
                f"Fix {sm.priority_invalid} invalid <priority> values (must be between 0.0 and 1.0)."
            )
        if sm.news_entries and sm.news_missing_publication:
            recs.append(
                f"Add <news:publication><news:name> for {sm.news_missing_publication} news entries."
            )
        if sm.news_entries and len(sm.news_publication_names) > 1:
            recs.append(
                "Use a consistent news publication name across all news sitemap entries."
            )

    if sm.kind == "sitemapindex":
        if sm.lastmod_missing:
            recs.append(
                f"Add <lastmod> for {sm.lastmod_missing} sitemap index entries to signal freshness."
            )
        if sm.lastmod_invalid:
            recs.append(
                f"Correct {sm.lastmod_invalid} invalid <lastmod> values in the sitemap index."
            )
        if sm.lastmod_stale:
            recs.append(
                f"Review {sm.lastmod_stale} index entries with <lastmod> older than {SITEMAP_STALE_DAYS} days."
            )
    return recs


def issue_to_fix(issue: str) -> str:
    if issue.startswith("JSON-LD object missing @type"):
        return "Add @type to all JSON-LD objects."
    if issue.startswith("JSON-LD block missing @context"):
        return "Add @context to JSON-LD blocks."
    if "missing required property" in issue:
        return issue.replace("missing required property", "add required property")
    if issue.startswith("Microdata itemscope missing itemtype"):
        return "Add itemtype to microdata itemscope elements."
    if issue.startswith("Microdata itemscope missing itemprop"):
        return "Ensure microdata itemscope elements include itemprop fields."
    if issue.startswith("RDFa"):
        return issue.replace("missing required property", "add required property")
    if issue.startswith("SEO: Missing <title>"):
        return "Add a descriptive <title> tag."
    if issue.startswith("SEO: Missing meta description"):
        return "Add a compelling meta description."
    if issue.startswith("SEO: Missing canonical"):
        return "Add a canonical URL tag."
    if issue.startswith("SEO: Canonical URL differs"):
        return "Align canonical URL with the primary page URL."
    if issue.startswith("SEO: Missing og:"):
        return "Add complete Open Graph tags (og:title, og:description, og:image, og:url, og:type, og:site_name)."
    if issue.startswith("SEO: Missing twitter:"):
        return "Add complete Twitter card tags (twitter:card, twitter:title, twitter:description, twitter:image, twitter:site)."
    if issue.startswith("SEO: Title too short"):
        return f"Expand the title to {TITLE_LENGTH_MIN}-{TITLE_LENGTH_MAX} characters."
    if issue.startswith("SEO: Title too long"):
        return f"Shorten the title to {TITLE_LENGTH_MIN}-{TITLE_LENGTH_MAX} characters."
    if issue.startswith("SEO: Meta description too short"):
        return f"Expand the meta description to {DESCRIPTION_LENGTH_MIN}-{DESCRIPTION_LENGTH_MAX} characters."
    if issue.startswith("SEO: Meta description too long"):
        return f"Shorten the meta description to {DESCRIPTION_LENGTH_MIN}-{DESCRIPTION_LENGTH_MAX} characters."
    if issue.startswith("On-page: Feature image missing alt text"):
        return "Add descriptive alt text to the feature image."
    if issue.startswith("On-page: Feature image alt text is weak"):
        return "Improve the feature image alt text so it describes the image clearly."
    if issue.startswith("On-page: Low word count"):
        return "Expand thin copy or confirm the page is intentionally short."
    if issue.startswith("On-page: No meaningful body copy detected"):
        return "Ensure the primary page body content is present in the HTML response."
    if issue.startswith("On-page: Missing H1"):
        return "Add one clear H1 that matches the page topic."
    if issue.startswith("On-page: Multiple H1"):
        return "Use a single primary H1 and demote extra headings."
    if issue.startswith("On-page: No H1/H2/H3 headings found"):
        return "Add a clear heading structure using H1, H2, and H3 where needed."
    if issue.startswith("On-page: Heading hierarchy skips"):
        return "Fix heading hierarchy so levels do not skip."
    if issue.startswith("On-page: Empty heading tags"):
        return "Remove empty headings or add meaningful heading text."
    if issue.startswith("HTTP "):
        return f"Resolve page errors ({issue})."
    if issue.startswith("Fetch error"):
        return "Fix page fetch errors (timeouts/DNS/blocked)."
    return f"Resolve: {issue}"


def score_grade(score: int) -> str:
    if score >= 90:
        return "Excellent"
    if score >= 75:
        return "Good"
    if score >= 60:
        return "Fair"
    return "Poor"


def compute_executive_summary(report: Report) -> Dict[str, object]:
    sitemap_issue_points = 0
    sitemap_rec_counter: Dict[str, int] = {}
    schema_fix_counter: Dict[str, int] = {}

    total_urls = sum(len(site.urls) for site in report.sites)
    total_sitemaps = sum(len(site.sitemaps) for site in report.sites)
    total_page_errors = 0
    total_schema_issues = 0
    total_schema_warnings = 0
    total_seo_issues = 0
    total_seo_warnings = 0
    canonical_mismatch = 0
    title_desc_issue_urls = 0
    meta_present = 0
    indexable_urls = 0
    blocked_urls = 0
    redirected_urls = 0
    canonicalized_urls = 0
    non_html_urls = 0
    error_urls = 0
    uncertain_urls = 0
    gsc_indexed_urls = 0
    gsc_excluded_urls = 0
    gsc_blocked_urls = 0
    gsc_error_urls = 0
    gsc_last_checked_values: List[dt.datetime] = []
    gsc_last_crawl_values: List[dt.datetime] = []

    for site in report.sites:
        for sm in site.sitemaps:
            if sm.error or (sm.status_code and sm.status_code >= 400):
                sitemap_issue_points += 5
            sitemap_issue_points += (
                sm.lastmod_missing
                + sm.lastmod_invalid
                + sm.lastmod_stale
                + sm.changefreq_invalid
                + sm.priority_invalid
                + sm.news_missing_publication
            )
            for rec in sitemap_recommendations(sm):
                sitemap_rec_counter[rec] = sitemap_rec_counter.get(rec, 0) + 1

        for result in site.urls:
            if result.fetch_error or (result.http_status and result.http_status >= 400):
                total_page_errors += 1
            total_seo_issues += len(result.seo_issues)
            total_seo_warnings += len(result.seo_warnings)
            schema_issue_count = max(len(result.issues) - len(result.seo_issues), 0)
            schema_warning_count = max(len(result.warnings) - len(result.seo_warnings), 0)
            total_schema_issues += schema_issue_count
            total_schema_warnings += schema_warning_count
            for issue in result.issues + result.warnings:
                fix = issue_to_fix(issue)
                schema_fix_counter[fix] = schema_fix_counter.get(fix, 0) + 1

            if result.seo_meta.get("canonical_match") == "No":
                canonical_mismatch += 1

            title = result.seo_meta.get("title", "")
            desc = result.seo_meta.get("meta_description", "")
            title_ok = (
                len(title) >= TITLE_LENGTH_MIN and len(title) <= TITLE_LENGTH_MAX
            ) if title else False
            desc_ok = (
                len(desc) >= DESCRIPTION_LENGTH_MIN and len(desc) <= DESCRIPTION_LENGTH_MAX
            ) if desc else False
            if not title_ok or not desc_ok:
                title_desc_issue_urls += 1

            for key in SEO_COVERAGE_KEYS:
                if result.seo_meta.get(key):
                    meta_present += 1

            status = result.indexability_status
            if status:
                if status == "Indexable":
                    indexable_urls += 1
                elif status.startswith("Blocked"):
                    blocked_urls += 1
                elif status == "Redirected":
                    redirected_urls += 1
                elif status == "Canonicalized":
                    canonicalized_urls += 1
                elif status == "Non-HTML":
                    non_html_urls += 1
                elif status == "Error":
                    error_urls += 1
                elif status.startswith("Uncertain"):
                    uncertain_urls += 1

            gsc_status = result.gsc_status
            if gsc_status == "Indexed":
                gsc_indexed_urls += 1
            elif gsc_status in ("Blocked by robots.txt", "Blocked by noindex"):
                gsc_blocked_urls += 1
            elif gsc_status == "Excluded":
                gsc_excluded_urls += 1
            elif gsc_status == "Error" or result.gsc_error:
                gsc_error_urls += 1
            checked_at = parse_datetime_safe(result.gsc_checked_at)
            if checked_at:
                gsc_last_checked_values.append(checked_at)
            crawl_at = parse_datetime_safe(result.gsc_last_crawl_time)
            if crawl_at:
                gsc_last_crawl_values.append(crawl_at)

    latest_gsc_checked = (
        max(gsc_last_checked_values).strftime("%Y-%m-%d %H:%M:%S UTC")
        if gsc_last_checked_values
        else ""
    )
    latest_gsc_crawl = (
        max(gsc_last_crawl_values).strftime("%Y-%m-%d %H:%M:%S UTC")
        if gsc_last_crawl_values
        else ""
    )

    meta_expected = total_urls * len(SEO_COVERAGE_KEYS) if total_urls else 0
    meta_coverage_pct = int(round((meta_present / meta_expected) * 100)) if meta_expected else 0
    meta_missing = meta_expected - meta_present if meta_expected else 0

    score = 100
    score -= min(60, sitemap_issue_points)
    score -= min(30, total_schema_issues)
    score -= min(10, total_schema_warnings)
    score = max(0, score)

    seo_score = 100
    seo_score -= min(60, total_seo_issues * 5 + canonical_mismatch * 3)
    seo_score -= min(25, total_seo_warnings * 2 + title_desc_issue_urls)
    seo_score -= min(15, max(0, 100 - meta_coverage_pct) // 5)
    seo_score = max(0, seo_score)

    schema_score = 100
    schema_score -= min(70, total_schema_issues * 6)
    schema_score -= min(30, total_schema_warnings * 2)
    schema_score = max(0, schema_score)

    grade = score_grade(score)
    seo_grade = score_grade(seo_score)
    schema_grade = score_grade(schema_score)

    top_fixes: List[Tuple[str, int]] = []
    for msg, count in sitemap_rec_counter.items():
        top_fixes.append((msg, count))
    for msg, count in schema_fix_counter.items():
        top_fixes.append((msg, count))
    top_fixes.sort(key=lambda item: item[1], reverse=True)
    top_fixes = top_fixes[:5]

    highlights = [
        f"Sites audited: {len(report.sites)}",
        f"Sitemaps checked: {total_sitemaps}",
        f"URLs tested: {total_urls}",
        f"Page errors: {total_page_errors}",
        f"Schema issues: {total_schema_issues}",
        f"Schema warnings: {total_schema_warnings}",
        f"SEO issues: {total_seo_issues}",
        f"SEO warnings: {total_seo_warnings}",
        f"Meta coverage: {meta_coverage_pct}% (missing {meta_missing})",
        f"GSC indexing: {gsc_indexed_urls} indexed, {gsc_excluded_urls} excluded, {gsc_blocked_urls} blocked, {gsc_error_urls} errors",
        "Indexability: "
        f"{indexable_urls} indexable, {blocked_urls} blocked, {uncertain_urls} uncertain, "
        f"{redirected_urls} redirected, {canonicalized_urls} canonicalized, "
        f"{error_urls} errors, {non_html_urls} non-HTML",
    ]

    return {
        "score": score,
        "grade": grade,
        "highlights": highlights,
        "top_fixes": top_fixes,
        "meta_coverage_pct": meta_coverage_pct,
        "meta_missing": meta_missing,
        "canonical_mismatch": canonical_mismatch,
        "title_desc_issue_urls": title_desc_issue_urls,
        "schema_issues": total_schema_issues,
        "indexable_urls": indexable_urls,
        "blocked_urls": blocked_urls,
        "uncertain_urls": uncertain_urls,
        "redirected_urls": redirected_urls,
        "canonicalized_urls": canonicalized_urls,
        "error_urls": error_urls,
        "non_html_urls": non_html_urls,
        "gsc_indexed_urls": gsc_indexed_urls,
        "gsc_excluded_urls": gsc_excluded_urls,
        "gsc_blocked_urls": gsc_blocked_urls,
        "gsc_error_urls": gsc_error_urls,
        "gsc_last_checked": latest_gsc_checked,
        "gsc_last_crawl": latest_gsc_crawl,
        "seo_score": seo_score,
        "seo_grade": seo_grade,
        "schema_score": schema_score,
        "schema_grade": schema_grade,
    }


def render_report(report: Report, output_path: str) -> None:
    def esc(value: str) -> str:
        return html.escape(value)

    def content_summary(result: UrlCheckResult) -> str:
        if result.fetch_error or result.http_status is None:
            return "Not checked"
        if result.http_status >= 400:
            return "Not checked"
        if result.content_type and "text/html" not in result.content_type.lower():
            return "Not checked"
        parts: List[str] = []
        if result.word_count:
            if result.word_count_source:
                parts.append(f"{result.word_count} words ({result.word_count_source})")
            else:
                parts.append(f"{result.word_count} words")
        if result.heading_h1_count or result.heading_h2_count or result.heading_h3_count:
            parts.append(
                f"H1:{result.heading_h1_count} H2:{result.heading_h2_count} H3:{result.heading_h3_count}"
            )
        else:
            parts.append("No H1/H2/H3")
        if result.feature_image_status:
            parts.append(f"Alt: {result.feature_image_status}")
        return " | ".join(parts)

    def is_heading_warning(message: str) -> bool:
        return message.startswith("On-page: Missing H1") or message.startswith(
            "On-page: Multiple H1"
        ) or message.startswith("On-page: Heading hierarchy skips") or message.startswith(
            "On-page: Empty heading"
        ) or message.startswith("On-page: No H1/H2/H3 headings found")

    total_urls = sum(len(site.urls) for site in report.sites)
    total_sitemaps = sum(len(site.sitemaps) for site in report.sites)
    total_issues = 0
    total_warnings = 0
    for site in report.sites:
        for sitemap in site.sitemaps:
            if sitemap.error or (sitemap.status_code and sitemap.status_code >= 400):
                total_issues += 1
        for url in site.urls:
            if url.fetch_error or url.http_status and url.http_status >= 400:
                total_issues += 1
            if url.issues:
                total_issues += len(url.issues)
            if url.warnings:
                total_warnings += len(url.warnings)

    lines: List[str] = []
    lines.append("<!doctype html>")
    lines.append("<html lang='en'>")
    lines.append("<head>")
    lines.append("<meta charset='utf-8'>")
    lines.append("<meta name='viewport' content='width=device-width, initial-scale=1'>")
    lines.append("<title>Schema & Sitemap Validation Report</title>")
    lines.append(
        "<style>"
        "body{font-family:Arial,Helvetica,sans-serif;margin:24px;background:#f7f7f8;color:#222;}"
        "h1{margin-bottom:4px;}"
        ".meta{color:#555;margin-bottom:16px;}"
        "table{border-collapse:collapse;width:100%;margin:12px 0;background:#fff;}"
        "th,td{border:1px solid #ddd;padding:8px;text-align:left;vertical-align:top;}"
        "th{background:#f0f0f0;}"
        ".badge{display:inline-block;padding:2px 6px;border-radius:10px;background:#e5e7eb;font-size:12px;}"
        ".ok{background:#d1fae5;}"
        ".warn{background:#fde68a;}"
        ".err{background:#fecaca;}"
        ".section{margin-top:24px;}"
        ".issues{color:#b91c1c;}"
        ".meta-table td{max-width:420px;word-break:break-word;}"
        ".cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px;margin:16px 0;}"
        ".card{background:#fff;border:1px solid #e5e7eb;border-radius:10px;padding:12px;box-shadow:0 1px 2px rgba(15,23,42,0.06);}"
        ".card .label{font-size:12px;text-transform:uppercase;color:#6b7280;letter-spacing:0.04em;}"
        ".card .value{font-size:22px;font-weight:700;color:#111827;margin-top:4px;}"
        ".card .sub{font-size:12px;color:#6b7280;margin-top:4px;}"
        ".meta-hint{font-size:12px;color:#6b7280;margin:6px 0 8px;}"
        ".schema-card{background:#fff;border:1px solid #e5e7eb;border-radius:10px;padding:10px;margin:8px 0;}"
        ".schema-title{font-weight:600;color:#111827;margin-bottom:6px;}"
        ".kv-table{width:100%;border-collapse:collapse;}"
        ".kv-table th{width:220px;text-align:left;color:#6b7280;font-weight:600;padding:6px;border-bottom:1px solid #eee;vertical-align:top;}"
        ".kv-table td{padding:6px;border-bottom:1px solid #eee;word-break:break-word;}"
        ".kv-list{margin:0;padding-left:16px;}"
        ".kv-list li{margin:2px 0;}"
        ".kv-subtable{width:100%;border-collapse:collapse;}"
        ".kv-subtable th{width:160px;text-align:left;color:#6b7280;font-weight:600;padding:4px 6px;border-bottom:1px dashed #eee;vertical-align:top;}"
        ".kv-subtable td{padding:4px 6px;border-bottom:1px dashed #eee;word-break:break-word;}"
        ".kv-details{margin-top:6px;}"
        ".kv-details summary{font-weight:600;color:#2563eb;cursor:pointer;}"
        ".kv-long{margin-top:6px;color:#334155;font-size:12px;line-height:1.4;white-space:pre-wrap;}"
        "details{background:#fff;border:1px solid #ddd;padding:8px;border-radius:6px;margin:6px 0;}"
        "summary{font-weight:bold;cursor:pointer;}"
        "</style>"
    )
    lines.append("</head>")
    lines.append("<body>")

    lines.append("<h1>Schema & Sitemap Validation Report</h1>")
    lines.append(
        f"<div class='meta'>Generated: {esc(report.generated_at)} | "
        f"Max URLs/site: {report.max_urls_per_site} | User-Agent: {esc(report.user_agent)} | "
        f"Rules: {esc(report.rules_path)} | Schema.org ref: {esc(report.schemaorg_ref_path)} | "
        f"Schema.org types: {report.schemaorg_types} | "
        f"GSC: {'enabled' if report.gsc_enabled else 'disabled'} | "
        f"GSC cache: {esc(report.gsc_cache_path)} | "
        f"Sitemap stale threshold: {SITEMAP_STALE_DAYS} days</div>"
    )

    summary = compute_executive_summary(report)
    lines.append("<div class='cards'>")
    lines.append(
        "<div class='card'>"
        "<div class='label'>Overall Score</div>"
        f"<div class='value'>{summary['score']}/100</div>"
        f"<div class='sub'>Grade: {esc(summary['grade'])}</div>"
        "</div>"
    )
    lines.append(
        "<div class='card'>"
        "<div class='label'>SEO Score</div>"
        f"<div class='value'>{summary['seo_score']}/100</div>"
        f"<div class='sub'>Grade: {esc(summary['seo_grade'])}</div>"
        "</div>"
    )
    lines.append(
        "<div class='card'>"
        "<div class='label'>Schema Score</div>"
        f"<div class='value'>{summary['schema_score']}/100</div>"
        f"<div class='sub'>Grade: {esc(summary['schema_grade'])}</div>"
        "</div>"
    )
    lines.append(
        "<div class='card'>"
        "<div class='label'>Meta Coverage</div>"
        f"<div class='value'>{summary['meta_coverage_pct']}%</div>"
        f"<div class='sub'>Missing: {summary['meta_missing']}</div>"
        "</div>"
    )
    lines.append(
        "<div class='card'>"
        "<div class='label'>Canonical Mismatch</div>"
        f"<div class='value'>{summary['canonical_mismatch']}</div>"
        "<div class='sub'>URLs</div>"
        "</div>"
    )
    lines.append(
        "<div class='card'>"
        "<div class='label'>Title/Description Issues</div>"
        f"<div class='value'>{summary['title_desc_issue_urls']}</div>"
        "<div class='sub'>URLs</div>"
        "</div>"
    )
    lines.append(
        "<div class='card'>"
        "<div class='label'>GSC Indexed</div>"
        f"<div class='value'>{summary['gsc_indexed_urls']}</div>"
        f"<div class='sub'>Blocked: {summary['gsc_blocked_urls']} · Excluded: {summary['gsc_excluded_urls']}</div>"
        "</div>"
    )
    lines.append("</div>")
    lines.append("<div class='section'>")
    lines.append("<h2>Executive Summary</h2>")
    lines.append("<table>")
    lines.append(
        "<tr><th>Overall Score</th><th>Overall Grade</th><th>SEO Score</th><th>SEO Grade</th><th>Schema Score</th><th>Schema Grade</th></tr>"
    )
    lines.append(
        f"<tr><td>{summary['score']}/100</td><td>{summary['grade']}</td>"
        f"<td>{summary['seo_score']}/100</td><td>{summary['seo_grade']}</td>"
        f"<td>{summary['schema_score']}/100</td><td>{summary['schema_grade']}</td></tr>"
    )
    lines.append("</table>")
    if report.gsc_enabled:
        lines.append("<div><strong>GSC Snapshot</strong></div>")
        lines.append("<table>")
        lines.append(
            "<tr><th>Indexed</th><th>Excluded</th><th>Blocked</th><th>Errors</th><th>Last Inspected</th><th>Last Crawl</th></tr>"
        )
        lines.append(
            f"<tr><td>{summary['gsc_indexed_urls']}</td>"
            f"<td>{summary['gsc_excluded_urls']}</td>"
            f"<td>{summary['gsc_blocked_urls']}</td>"
            f"<td>{summary['gsc_error_urls']}</td>"
            f"<td>{esc(str(summary.get('gsc_last_checked', '-') or '-'))}</td>"
            f"<td>{esc(str(summary.get('gsc_last_crawl', '-') or '-'))}</td></tr>"
        )
        lines.append("</table>")
    lines.append("<div><strong>Highlights</strong></div>")
    lines.append("<ul>")
    for item in summary["highlights"]:
        lines.append(f"<li>{esc(item)}</li>")
    lines.append("</ul>")
    lines.append("<div><strong>Top 5 SEO Fixes</strong></div>")
    if summary["top_fixes"]:
        lines.append("<table>")
        lines.append("<tr><th>Fix</th><th>Instances</th></tr>")
        for fix, count in summary["top_fixes"]:
            lines.append(f"<tr><td>{esc(fix)}</td><td>{count}</td></tr>")
        lines.append("</table>")
    else:
        lines.append("<div>No major fixes detected.</div>")
    lines.append("</div>")

    if not report.schemaorg_ref_loaded:
        lines.append("<div class='section'>")
        lines.append("<h3>Schema.org Reference</h3>")
        lines.append(
            "<div class='issues'>Schema.org properties reference not loaded. "
            "Provide --schemaorg-data (JSON-LD) or a cached properties file "
            "to show allowed/possible nodes per type.</div>"
        )
        lines.append("</div>")

    lines.append("<div class='section'>")
    lines.append("<h2>Summary</h2>")
    lines.append("<table>")
    lines.append("<tr><th>Sites</th><th>Sitemaps Checked</th><th>URLs Tested</th><th>Issues</th><th>Warnings</th></tr>")
    lines.append(
        f"<tr><td>{len(report.sites)}</td><td>{total_sitemaps}</td><td>{total_urls}</td><td>{total_issues}</td><td>{total_warnings}</td></tr>"
    )
    lines.append("</table>")
    lines.append("</div>")

    for site in report.sites:
        lines.append("<div class='section'>")
        lines.append(f"<h2>Site: {esc(site.domain)}</h2>")
        if site.notes:
            for note in site.notes:
                lines.append(f"<div class='issues'>{esc(note)}</div>")

        lines.append("<h3>Sitemaps</h3>")
        lines.append("<table>")
        lines.append("<tr><th>Sitemap</th><th>Status</th><th>Type</th><th>Entries</th><th>SEO Checks</th><th>Error</th></tr>")
        for sm in site.sitemaps:
            status = sm.status_code if sm.status_code is not None else "-"
            entries = sm.urls_found or sm.sitemaps_found or "-"
            error = sm.error or ""
            badge = "ok" if not error and (not sm.status_code or sm.status_code < 400) else "err"
            seo_summary = sitemap_seo_summary(sm)
            lines.append(
                "<tr>"
                f"<td>{esc(sm.url)}</td>"
                f"<td><span class='badge {badge}'>{status}</span></td>"
                f"<td>{esc(sm.kind or '-') }</td>"
                f"<td>{entries}</td>"
                f"<td>{esc(seo_summary)}</td>"
                f"<td class='issues'>{esc(error)}</td>"
                "</tr>"
            )
        lines.append("</table>")

        lines.append("<h3>Sitemap Recommendations</h3>")
        recommendations = []
        for sm in site.sitemaps:
            for rec in sitemap_recommendations(sm):
                recommendations.append(f"{sm.url}: {rec}")
        if recommendations:
            lines.append("<table>")
            lines.append("<tr><th>Sitemap</th><th>Recommendation</th></tr>")
            for rec in recommendations:
                if ": " in rec:
                    sm_url, text = rec.split(": ", 1)
                else:
                    sm_url, text = "-", rec
                lines.append(f"<tr><td>{esc(sm_url)}</td><td>{esc(text)}</td></tr>")
            lines.append("</table>")
        else:
            lines.append("<div>No sitemap recommendations.</div>")

        lines.append("<h3>URLs</h3>")
        if not site.urls:
            lines.append("<div>No URLs tested.</div>")
        else:
            lines.append("<table>")
            lines.append(
                "<tr><th>URL</th><th>HTTP</th><th>Indexability</th><th>GSC</th><th>Content</th><th>JSON-LD</th><th>Microdata</th><th>RDFa</th><th>JSON-LD Objects</th><th>Microdata Objects</th><th>RDFa Objects</th><th>Issues</th><th>Warnings</th></tr>"
            )
            for result in site.urls:
                http_status = result.http_status if result.http_status is not None else "-"
                issue_count = len(result.issues)
                warning_count = len(result.warnings)
                badge = "ok" if issue_count == 0 and warning_count == 0 else "warn"
                if result.fetch_error or (result.http_status and result.http_status >= 400):
                    badge = "err"

                jsonld_summary = f"{result.jsonld_blocks} blocks"
                if result.jsonld_types:
                    types_preview = ", ".join(sorted(set(result.jsonld_types))[:5])
                    jsonld_summary += f" ({esc(types_preview)})"

                nested_count = sum(
                    1
                    for obj in result.schema_objects
                    if obj.get("source") not in ("", None, "root")
                )
                root_count = len(result.schema_objects) - nested_count
                if nested_count:
                    jsonld_obj_display = f"{root_count} (nested {nested_count})"
                else:
                    jsonld_obj_display = f"{root_count}"

                lines.append(
                    "<tr>"
                    f"<td>{esc(result.url)}</td>"
                    f"<td><span class='badge {badge}'>{http_status}</span></td>"
                    f"<td>{esc(result.indexability_status or '-')}</td>"
                    f"<td>{esc(result.gsc_status or result.gsc_error or '-')}</td>"
                    f"<td>{esc(content_summary(result))}</td>"
                    f"<td>{jsonld_summary}</td>"
                    f"<td>{result.microdata_items}</td>"
                    f"<td>{result.rdfa_elements}</td>"
                    f"<td>{jsonld_obj_display}</td>"
                    f"<td>{len(result.microdata_objects)}</td>"
                    f"<td>{len(result.rdfa_objects)}</td>"
                    f"<td>{issue_count}</td>"
                    f"<td>{warning_count}</td>"
                    "</tr>"
                )
            lines.append("</table>")

            lines.append("<div>")
            for result in site.urls:
                if (
                    not result.issues
                    and not result.warnings
                    and not result.schema_objects
                    and not result.indexability_status
                ):
                    continue
                lines.append("<details>")
                lines.append(f"<summary>{esc(result.url)}</summary>")
                if result.issues or result.warnings:
                    lines.append("<table>")
                    lines.append("<tr><th>Type</th><th>Detail</th></tr>")
                    for issue in result.issues:
                        lines.append(f"<tr><td>Issue</td><td>{esc(issue)}</td></tr>")
                    for warning in result.warnings:
                        lines.append(f"<tr><td>Warning</td><td>{esc(warning)}</td></tr>")
                    lines.append("</table>")

                if result.gsc_status or result.gsc_error:
                    lines.append("<div><strong>GSC Indexing</strong></div>")
                    lines.append("<table>")
                    if result.gsc_property:
                        lines.append(
                            f"<tr><th>Property</th><td>{esc(result.gsc_property)}</td></tr>"
                        )
                    if result.gsc_status:
                        lines.append(
                            f"<tr><th>Status</th><td>{esc(result.gsc_status)}</td></tr>"
                        )
                    if result.gsc_checked_at:
                        lines.append(
                            f"<tr><th>Checked At</th><td>{esc(result.gsc_checked_at)}</td></tr>"
                        )
                    if result.gsc_error:
                        lines.append(
                            f"<tr><th>Error</th><td>{esc(result.gsc_error)}</td></tr>"
                        )
                    if result.gsc_verdict:
                        lines.append(
                            f"<tr><th>Verdict</th><td>{esc(result.gsc_verdict)}</td></tr>"
                        )
                    if result.gsc_coverage_state:
                        lines.append(
                            f"<tr><th>Coverage State</th><td>{esc(result.gsc_coverage_state)}</td></tr>"
                        )
                    if result.gsc_indexing_state:
                        lines.append(
                            f"<tr><th>Indexing State</th><td>{esc(result.gsc_indexing_state)}</td></tr>"
                        )
                    if result.gsc_robots_state:
                        lines.append(
                            f"<tr><th>Robots State</th><td>{esc(result.gsc_robots_state)}</td></tr>"
                        )
                    if result.gsc_page_fetch_state:
                        lines.append(
                            f"<tr><th>Page Fetch State</th><td>{esc(result.gsc_page_fetch_state)}</td></tr>"
                        )
                    if result.gsc_last_crawl_time:
                        lines.append(
                            f"<tr><th>Last Crawl Time</th><td>{esc(result.gsc_last_crawl_time)}</td></tr>"
                        )
                    if result.gsc_google_canonical:
                        lines.append(
                            f"<tr><th>Google Canonical</th><td>{esc(result.gsc_google_canonical)}</td></tr>"
                        )
                    if result.gsc_user_canonical:
                        lines.append(
                            f"<tr><th>User Canonical</th><td>{esc(result.gsc_user_canonical)}</td></tr>"
                        )
                    if result.gsc_sitemaps:
                        lines.append(
                            f"<tr><th>Sitemaps</th><td>{esc('; '.join(result.gsc_sitemaps))}</td></tr>"
                        )
                    if result.gsc_referring_urls:
                        lines.append(
                            f"<tr><th>Referring URLs</th><td>{esc('; '.join(result.gsc_referring_urls))}</td></tr>"
                        )
                    lines.append("</table>")

                if result.indexability_status:
                    lines.append("<div><strong>Indexability</strong></div>")
                    lines.append("<table>")
                    lines.append(
                        f"<tr><th>Status</th><td>{esc(result.indexability_status)}</td></tr>"
                    )
                    if result.indexability_reasons:
                        lines.append(
                            f"<tr><th>Reasons</th><td>{esc('; '.join(result.indexability_reasons))}</td></tr>"
                        )
                    if result.robots_meta:
                        lines.append(
                            f"<tr><th>Meta Robots</th><td>{esc(result.robots_meta)}</td></tr>"
                        )
                    if result.x_robots_tag:
                        lines.append(
                            f"<tr><th>X-Robots-Tag</th><td>{esc(result.x_robots_tag)}</td></tr>"
                        )
                    canonical = result.seo_meta.get("canonical", "") if result.seo_meta else ""
                    canonical_match = (
                        result.seo_meta.get("canonical_match", "") if result.seo_meta else ""
                    )
                    if canonical:
                        lines.append(f"<tr><th>Canonical</th><td>{esc(canonical)}</td></tr>")
                        if canonical_match:
                            lines.append(
                                f"<tr><th>Canonical Match</th><td>{esc(canonical_match)}</td></tr>"
                            )
                    if result.redirect_chain:
                        chain = " → ".join(result.redirect_chain + [result.final_url])
                        lines.append(
                            f"<tr><th>Redirects</th><td>{esc(chain)}</td></tr>"
                        )
                    elif result.final_url and result.final_url != result.url:
                        lines.append(
                            f"<tr><th>Final URL</th><td>{esc(result.final_url)}</td></tr>"
                        )
                    lines.append("</table>")

                def crawl_row(label: str, status: str, detail: str) -> str:
                    return (
                        f"<tr><th>{esc(label)}</th><td>{esc(status)}</td><td>{esc(detail)}</td></tr>"
                    )

                lines.append("<div><strong>Crawl Checks</strong></div>")
                lines.append("<table>")
                lines.append("<tr><th>Check</th><th>Status</th><th>Details</th></tr>")

                robots_status = "Allowed" if not result.skipped_by_robots else "Blocked"
                robots_detail = (
                    "Allowed by robots.txt"
                    if robots_status == "Allowed"
                    else "Disallowed by robots.txt"
                )
                lines.append(crawl_row("robots.txt", robots_status, robots_detail))

                http_status = (
                    str(result.http_status) if result.http_status is not None else "Not checked"
                )
                http_detail = (
                    "OK"
                    if result.http_status and 200 <= result.http_status < 300
                    else ""
                )
                lines.append(crawl_row("HTTP status", http_status, http_detail))

                has_response = result.http_status is not None
                is_html = (
                    True
                    if result.content_type and "text/html" in result.content_type.lower()
                    else False
                )

                if not has_response or not is_html:
                    meta_status = "Not checked"
                    meta_detail = "Page not fetched as HTML"
                else:
                    meta_status = "Pass" if not result.robots_meta else "Present"
                    meta_detail = result.robots_meta or "No meta robots tag"
                lines.append(crawl_row("Meta robots", meta_status, meta_detail))

                if not has_response:
                    x_status = "Not checked"
                    x_detail = "No response headers"
                else:
                    x_status = "Pass" if not result.x_robots_tag else "Present"
                    x_detail = result.x_robots_tag or "No X-Robots-Tag header"
                lines.append(crawl_row("X-Robots-Tag", x_status, x_detail))

                canonical = result.seo_meta.get("canonical", "") if result.seo_meta else ""
                canonical_match = (
                    result.seo_meta.get("canonical_match", "") if result.seo_meta else ""
                )
                if not has_response or not is_html:
                    can_status = "Not checked"
                    can_detail = "Page not fetched as HTML"
                elif canonical:
                    can_status = "Pass" if canonical_match != "No" else "Fail"
                    can_detail = (
                        "Matches" if canonical_match != "No" else "Points to a different URL"
                    )
                else:
                    can_status = "Info"
                    can_detail = "No canonical tag"
                lines.append(crawl_row("Canonical", can_status, can_detail))

                if result.redirect_chain:
                    redirect_status = "Info"
                    redirect_detail = " → ".join(result.redirect_chain + [result.final_url])
                elif not has_response:
                    redirect_status = "Not checked"
                    redirect_detail = "No response"
                else:
                    redirect_status = "Pass"
                    redirect_detail = "No redirects"
                lines.append(crawl_row("Redirects", redirect_status, redirect_detail))

                if not has_response or not is_html:
                    soft_status = "Not checked"
                    soft_detail = "Page not fetched as HTML"
                else:
                    soft_status = "Fail" if result.soft_404 else "Pass"
                    soft_detail = "Soft 404 suspected" if result.soft_404 else "Not detected"
                lines.append(crawl_row("Soft 404", soft_status, soft_detail))

                if not has_response or not is_html:
                    hreflang_status = "Not checked"
                    hreflang_detail = "Page not fetched as HTML"
                else:
                    hreflang_status = result.hreflang_status or "Not present"
                    hreflang_detail = (
                        "; ".join(result.hreflang_issues) if result.hreflang_issues else "OK"
                    )
                lines.append(crawl_row("Hreflang", hreflang_status, hreflang_detail))

                if not has_response or not is_html:
                    pagination = "Not checked"
                else:
                    pagination = result.pagination or "Not present"
                lines.append(crawl_row("Pagination (prev/next)", "Info", pagination))

                if not has_response or not is_html:
                    auth_status = "Not checked"
                    auth_detail = "Page not fetched as HTML"
                else:
                    auth_status = "Fail" if result.auth_blocked else "Pass"
                    auth_detail = result.auth_blocked or "No access block detected"
                lines.append(crawl_row("Auth/geo/login block", auth_status, auth_detail))

                if not has_response:
                    dup_status = "Not checked"
                    dup_detail = "No response"
                else:
                    dup_status = "Fail" if result.duplicate_canonical else "Pass"
                    if result.duplicate_canonical and result.canonical_target_count:
                        dup_detail = (
                            f"{result.canonical_target_count} URLs share the same canonical"
                        )
                    else:
                        dup_detail = "No duplicate canonical targets"
                lines.append(crawl_row("Duplicate canonicals", dup_status, dup_detail))

                lines.append("</table>")

                lines.append("<div><strong>Content Checks</strong></div>")
                lines.append("<table>")
                lines.append("<tr><th>Check</th><th>Status</th><th>Details</th></tr>")
                if not has_response or not is_html:
                    lines.append(
                        crawl_row("Word count", "Not checked", "Page not fetched as HTML")
                    )
                    lines.append(
                        crawl_row(
                            "Feature image alt text",
                            "Not checked",
                            "Page not fetched as HTML",
                        )
                    )
                    lines.append(
                        crawl_row(
                            "Heading counts",
                            "Not checked",
                            "Page not fetched as HTML",
                        )
                    )
                    lines.append(
                        crawl_row(
                            "Heading structure",
                            "Not checked",
                            "Page not fetched as HTML",
                        )
                    )
                else:
                    if result.word_count == 0:
                        word_status = "Review"
                    elif result.word_count < WORD_COUNT_WARNING_THRESHOLD:
                        word_status = "Low"
                    else:
                        word_status = "Pass"
                    word_detail = (
                        (
                            f"{result.word_count} words ({result.word_count_source})"
                            if result.word_count_source
                            else f"{result.word_count} words"
                        )
                        if result.word_count
                        else "No meaningful article copy detected"
                    )
                    lines.append(crawl_row("Word count", word_status, word_detail))

                    feature_status = result.feature_image_status or "Not found"
                    if result.feature_image_url:
                        feature_detail_parts = [result.feature_image_url]
                    else:
                        feature_detail_parts = []
                    if result.feature_image_alt:
                        feature_detail_parts.append(f"alt: {result.feature_image_alt}")
                    elif feature_status == "Missing":
                        feature_detail_parts.append("alt: missing")
                    elif feature_status == "Weak":
                        feature_detail_parts.append("alt: weak/generic")
                    elif feature_status == "Not found in HTML":
                        feature_detail_parts.append(
                            "Feature image found via meta/schema but not matched in page HTML"
                        )
                    elif feature_status == "Not found":
                        feature_detail_parts.append("No feature image detected")
                    lines.append(
                        crawl_row(
                            "Feature image alt text",
                            feature_status,
                            " | ".join(feature_detail_parts),
                        )
                    )

                    heading_counts = (
                        f"H1: {result.heading_h1_count} | "
                        f"H2: {result.heading_h2_count} | "
                        f"H3: {result.heading_h3_count}"
                    )
                    heading_status = "Pass"
                    if any(is_heading_warning(item) for item in result.warnings):
                        heading_status = "Review"
                    lines.append(crawl_row("Heading counts", heading_status, heading_counts))

                    if result.heading_structure:
                        structure_html = "<ul class='kv-list'>" + "".join(
                            f"<li>{esc(item)}</li>" for item in result.heading_structure
                        ) + "</ul>"
                    else:
                        structure_html = "No heading structure detected"
                    lines.append(
                        f"<tr><th>Heading structure</th><td>{esc(heading_status)}</td><td>{structure_html}</td></tr>"
                    )
                lines.append("</table>")

                if result.seo_meta:
                    lines.append("<div><strong>SEO Meta</strong></div>")
                    lines.append(
                        f"<div class='meta-hint'>Title target: {TITLE_LENGTH_MIN}-{TITLE_LENGTH_MAX} chars · "
                        f"Meta description target: {DESCRIPTION_LENGTH_MIN}-{DESCRIPTION_LENGTH_MAX} chars</div>"
                    )
                    lines.append("<table class='meta-table'>")
                    lines.append("<tr><th>Field</th><th>Value</th><th>Status</th><th>Length</th></tr>")
                    for label, key in SEO_FIELDS:
                        value = result.seo_meta.get(key, "")
                        status = "OK"
                        length_display = "-"
                        if key == "canonical_match":
                            if value == "No":
                                status = "Mismatch"
                            elif value == "N/A":
                                status = "N/A"
                        elif not value:
                            status = "Missing"
                        else:
                            if key in SEO_LENGTH_RULES:
                                min_len, max_len = SEO_LENGTH_RULES[key]
                                length_display = str(len(value))
                                if len(value) < min_len:
                                    status = "Short"
                                elif len(value) > max_len:
                                    status = "Long"
                            else:
                                length_display = str(len(value)) if value else "-"
                        lines.append(
                            f"<tr><td>{esc(label)}</td><td>{esc(value)}</td><td>{esc(status)}</td><td>{esc(length_display)}</td></tr>"
                        )
                    lines.append("</table>")

                if result.schema_objects:
                    lines.append("<div><strong>Schema Objects (JSON-LD)</strong></div>")

                    def kv_label(name: str) -> str:
                        return (
                            name.replace("_", " ")
                            .replace("Of", "of")
                            .replace("Url", "URL")
                            .title()
                        )

                    def render_kv_card(title: str, obj: Dict[str, str], fields: List[str]) -> None:
                        lines.append("<div class='schema-card'>")
                        lines.append(f"<div class='schema-title'>{esc(title)}</div>")
                        lines.append("<table class='kv-table'>")
                        for field in fields:
                            value = obj.get(field, "")
                            if not value and field not in (
                                "missing_required",
                                "missing_recommended",
                                "properties_used",
                                "properties_used_values",
                                "allowed_properties",
                                "source",
                            ):
                                continue
                            if field == "properties_used":
                                raw_pairs = obj.get("properties_used_values", "")
                                parts = [part.strip() for part in raw_pairs.split("||") if part.strip()]
                                if parts:
                                    rows = []
                                    for part in parts:
                                        if ":" in part:
                                            key, val = part.split(":", 1)
                                        else:
                                            key, val = part, ""
                                        rows.append(
                                            f"<tr><th>{esc(key.strip())}</th><td>{esc(val.strip())}</td></tr>"
                                        )
                                    display_html = (
                                        "<table class='kv-subtable'>"
                                        "<tr><th>Property</th><th>Value</th></tr>"
                                        + "".join(rows)
                                        + "</table>"
                                    )
                                else:
                                    list_parts = [part.strip() for part in value.split(",") if part.strip()]
                                    if list_parts:
                                        display_html = "<ul class='kv-list'>" + "".join(
                                            f"<li>{esc(part)}</li>" for part in list_parts
                                        ) + "</ul>"
                                    else:
                                        source_hint = obj.get("source", "")
                                        if source_hint and source_hint != "root":
                                            display_html = (
                                                f"No inline properties (referenced via {esc(source_hint)})."
                                            )
                                        else:
                                            display_html = "—"
                                lines.append(
                                    f"<tr><th>{esc(kv_label(field))}</th><td>{display_html}</td></tr>"
                                )
                            elif field == "allowed_properties":
                                full_list = obj.get("allowed_properties_full", "")
                                count = obj.get("allowed_properties_count", "")
                                if full_list:
                                    summary_line = (
                                        f"{esc(count)} properties (schema.org)"
                                        if count
                                        else "Schema.org properties list"
                                    )
                                    display_html = (
                                        f"{summary_line}"
                                        "<details class='kv-details'>"
                                        "<summary>Show full list</summary>"
                                        f"<div class='kv-long'>{esc(full_list)}</div>"
                                        "</details>"
                                    )
                                else:
                                    display_html = esc(value) if value else "—"
                                lines.append(
                                    f"<tr><th>{esc(kv_label(field))}</th><td>{display_html}</td></tr>"
                                )
                            else:
                                display = value if value else "—"
                                lines.append(
                                    f"<tr><th>{esc(kv_label(field))}</th><td>{esc(display)}</td></tr>"
                                )
                        lines.append("</table>")
                        lines.append("</div>")

                    root_objects = [
                        obj
                        for obj in result.schema_objects
                        if obj.get("source") in ("", None, "root")
                    ]
                    nested_objects = [
                        obj
                        for obj in result.schema_objects
                        if obj.get("source") not in ("", None, "root")
                    ]
                    nested_by_parent: Dict[str, List[Dict[str, str]]] = {}
                    for obj in nested_objects:
                        source = obj.get("source", "")
                        parent = source.split(".", 1)[0] if source else "Unknown"
                        nested_by_parent.setdefault(parent, []).append(obj)

                    def root_key(obj: Dict[str, str]) -> str:
                        type_field = obj.get("type", "")
                        return type_field.split(",", 1)[0].strip() if type_field else "Unknown"

                    if not root_objects and result.schema_objects:
                        for obj in result.schema_objects:
                            title = obj.get("type", "Schema Object")
                            render_kv_card(title, obj, ["type"] + SCHEMA_SUMMARY_FIELDS)
                    else:
                        for root in root_objects:
                            label_type = root.get("type", "Object")
                            label_name = root.get("name", "")
                            label = f"{label_type} — {label_name}" if label_name else label_type
                            render_kv_card(label, root, ["type"] + SCHEMA_SUMMARY_FIELDS)

                            parent_key = root_key(root)
                            nested = nested_by_parent.get(parent_key, [])
                            if nested:
                                lines.append(
                                    f"<div><em>Nested objects for {esc(parent_key)}</em></div>"
                                )
                                for obj in nested:
                                    title = obj.get("type", "Nested Object")
                                    render_kv_card(title, obj, NESTED_SCHEMA_FIELDS)

                        remaining = []
                        used_parents = {root_key(obj) for obj in root_objects}
                        for parent, items in nested_by_parent.items():
                            if parent not in used_parents:
                                remaining.extend(items)
                        if remaining:
                            lines.append("<div><em>Nested objects (other)</em></div>")
                            for obj in remaining:
                                title = obj.get("type", "Nested Object")
                                render_kv_card(title, obj, NESTED_SCHEMA_FIELDS)

                if result.microdata_objects:
                    lines.append("<div><strong>Schema Objects (Microdata)</strong></div>")
                    for obj in result.microdata_objects:
                        title = obj.get("type", "Microdata Object")
                        lines.append("<div class='schema-card'>")
                        lines.append(f"<div class='schema-title'>{esc(title)}</div>")
                        lines.append("<table class='kv-table'>")
                        for key in [
                            "type",
                            "properties",
                            "missing_required",
                            "missing_recommended",
                            "allowed_properties",
                        ]:
                            value = obj.get(key, "")
                            if not value and key not in (
                                "missing_required",
                                "missing_recommended",
                                "allowed_properties",
                            ):
                                continue
                            display = value if value else "—"
                            lines.append(
                                f"<tr><th>{esc(key.replace('_', ' ').title())}</th><td>{esc(display)}</td></tr>"
                            )
                        lines.append("</table>")
                        lines.append("</div>")

                if result.rdfa_objects:
                    lines.append("<div><strong>Schema Objects (RDFa)</strong></div>")
                    for obj in result.rdfa_objects:
                        title = obj.get("type", "RDFa Object")
                        lines.append("<div class='schema-card'>")
                        lines.append(f"<div class='schema-title'>{esc(title)}</div>")
                        lines.append("<table class='kv-table'>")
                        for key in [
                            "type",
                            "properties",
                            "missing_required",
                            "missing_recommended",
                            "allowed_properties",
                        ]:
                            value = obj.get(key, "")
                            if not value and key not in (
                                "missing_required",
                                "missing_recommended",
                                "allowed_properties",
                            ):
                                continue
                            display = value if value else "—"
                            lines.append(
                                f"<tr><th>{esc(key.replace('_', ' ').title())}</th><td>{esc(display)}</td></tr>"
                            )
                        lines.append("</table>")
                        lines.append("</div>")
                lines.append("</details>")
            lines.append("</div>")

        lines.append("</div>")

    lines.append("</body>")
    lines.append("</html>")

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def build_report(
    domains: List[str],
    max_urls: int,
    user_agent: str,
    rules: Dict[str, dict],
    rules_path: str,
    schemaorg_ref: Dict[str, List[str]],
    schemaorg_ref_path: str,
    sitemap_urls_by_domain: Dict[str, List[str]],
    page_urls_by_domain: Dict[str, List[str]],
    sitemap_mode: str,
    gsc_json_path: str = "",
    gsc_cache_path: str = DEFAULT_GSC_CACHE_PATH,
    gsc_cache_ttl_hours: int = DEFAULT_GSC_CACHE_TTL_HOURS,
) -> Report:
    sites: List[SiteReport] = []
    gsc_service, gsc_error = build_gsc_service(gsc_json_path) if gsc_json_path else (None, None)
    gsc_cache = load_gsc_cache(gsc_cache_path) if gsc_service else {}
    for domain in domains:
        if sitemap_mode == "robots":
            sitemap_urls = None
        elif sitemap_mode == "explicit":
            sitemap_urls = sitemap_urls_by_domain.get(domain, [])
        else:
            sitemap_urls = []

        page_urls = page_urls_by_domain.get(domain, [])
        site_report = gather_site(
            domain,
            max_urls,
            user_agent,
            rules,
            schemaorg_ref,
            gsc_service=gsc_service,
            gsc_candidate_domains=domains,
            gsc_cache=gsc_cache,
            gsc_cache_ttl_hours=gsc_cache_ttl_hours,
            sitemap_urls=sitemap_urls,
            page_urls=page_urls,
        )
        if gsc_error:
            site_report.notes.append(gsc_error)
        sites.append(site_report)
    report = Report(
        generated_at=dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        max_urls_per_site=max_urls,
        user_agent=user_agent,
        rules_path=rules_path,
        schemaorg_ref_path=schemaorg_ref_path,
        schemaorg_ref_loaded=bool(schemaorg_ref),
        schemaorg_types=len(schemaorg_ref),
        gsc_enabled=bool(gsc_service),
        gsc_json_path=gsc_json_path,
        gsc_cache_path=gsc_cache_path,
        sites=sites,
    )
    if gsc_service:
        save_gsc_cache(gsc_cache_path, gsc_cache)
    apply_duplicate_canonical_flags(report)
    return report


def apply_duplicate_canonical_flags(report: Report) -> None:
    canonical_map: Dict[str, List[UrlCheckResult]] = {}
    for site in report.sites:
        for result in site.urls:
            canonical = ""
            if result.seo_meta:
                canonical = result.seo_meta.get("canonical", "")
            if not canonical:
                canonical = result.final_url or result.url
            canonical_map.setdefault(canonical, []).append(result)

    for canonical, results in canonical_map.items():
        unique_urls = {r.url for r in results}
        if len(unique_urls) <= 1:
            continue
        for result in results:
            result.duplicate_canonical = True
            result.canonical_target_count = len(unique_urls)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate sitemaps and schemas.")
    parser.add_argument(
        "--domains",
        nargs="+",
        default=None,
        help="Domains to check (space-separated).",
    )
    parser.add_argument(
        "--max-urls",
        type=int,
        default=DEFAULT_MAX_URLS,
        help="Max URLs per site to validate.",
    )
    parser.add_argument(
        "--output",
        default="report.html",
        help="Output HTML report path.",
    )
    parser.add_argument(
        "--user-agent",
        default=DEFAULT_USER_AGENT,
        help="User-Agent string.",
    )
    parser.add_argument(
        "--rules",
        default=DEFAULT_RULES_PATH,
        help="Path to schema rules JSON.",
    )
    parser.add_argument(
        "--schemaorg-ref",
        default=DEFAULT_SCHEMAORG_REF_PATH,
        help="Path to schema.org properties cache JSON.",
    )
    parser.add_argument(
        "--schemaorg-data",
        default=DEFAULT_SCHEMAORG_DATA_PATH,
        help="Path to schema.org JSON-LD data file (optional, used to build cache).",
    )
    parser.add_argument(
        "--schemaorg-download",
        action="store_true",
        help="Force download schema.org JSON-LD data file if missing.",
    )
    parser.add_argument(
        "--schemaorg-no-download",
        action="store_true",
        help="Disable auto-download of schema.org JSON-LD data file.",
    )
    parser.add_argument(
        "--gsc-json",
        default=DEFAULT_GSC_JSON_PATH,
        help="Path to GSC service-account JSON file.",
    )
    parser.add_argument(
        "--gsc-cache",
        default=DEFAULT_GSC_CACHE_PATH,
        help="Path to persistent GSC inspection cache JSON file.",
    )
    parser.add_argument(
        "--gsc-cache-ttl-hours",
        type=int,
        default=DEFAULT_GSC_CACHE_TTL_HOURS,
        help="Reuse cached GSC inspection results younger than this many hours.",
    )
    parser.add_argument(
        "--sitemap-url",
        nargs="*",
        default=None,
        help="Sitemap URL(s) to use instead of robots discovery.",
    )
    parser.add_argument(
        "--page-url",
        nargs="*",
        default=None,
        help="Specific page URL(s) to validate.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    rules = load_schema_rules(args.rules)
    schemaorg_ref = load_schemaorg_reference(args.schemaorg_ref, args.schemaorg_data)
    if not schemaorg_ref and not args.schemaorg_no_download:
        downloaded = download_schemaorg_data(SCHEMAORG_DATA_URL, args.schemaorg_data)
        if downloaded:
            schemaorg_ref = load_schemaorg_reference(args.schemaorg_ref, args.schemaorg_data)

    sitemap_urls = normalize_list(args.sitemap_url)
    page_urls = normalize_list(args.page_url)
    sitemap_urls_by_domain = group_by_domain(sitemap_urls)
    page_urls_by_domain = group_by_domain(page_urls)

    if args.domains is None:
        inferred = sorted(set(sitemap_urls_by_domain.keys()) | set(page_urls_by_domain.keys()))
        if inferred:
            domains = inferred
        else:
            domains = DEFAULT_DOMAINS
    else:
        domains = args.domains
        inferred = set(sitemap_urls_by_domain.keys()) | set(page_urls_by_domain.keys())
        for domain in sorted(inferred):
            if domain not in domains:
                domains.append(domain)

    if sitemap_urls:
        sitemap_mode = "explicit"
    elif page_urls:
        sitemap_mode = "disabled"
    else:
        sitemap_mode = "robots"

    report = build_report(
        domains,
        args.max_urls,
        args.user_agent,
        rules,
        args.rules,
        schemaorg_ref,
        args.schemaorg_ref,
        sitemap_urls_by_domain,
        page_urls_by_domain,
        sitemap_mode,
        args.gsc_json,
        args.gsc_cache,
        args.gsc_cache_ttl_hours,
    )
    render_report(report, args.output)
    print(f"Report written to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
