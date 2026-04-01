#!/usr/bin/env python3
"""Streamlit UI for schema-validator."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

import pandas as pd
import streamlit as st

import validator


APP_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT = APP_DIR / "report.html"
DEFAULT_DEPLOYED_GSC_JSON = Path(tempfile.gettempdir()) / "schema-validator-gsc-service-account.json"
DEFAULT_DEPLOYED_GSC_CACHE = Path(tempfile.gettempdir()) / "schema-validator-gsc-cache.json"


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


def build_gsc_rows(report: validator.Report) -> list[dict[str, str | int]]:
    rows: list[dict[str, str | int]] = []
    for site in report.sites:
        for result in site.urls:
            rows.append(
                {
                    "Site": site.domain,
                    "URL": result.url,
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
.stButton {
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
.stButton>button {
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
.stButton>button * {
  color: #ffffff !important;
  -webkit-text-fill-color: #ffffff !important;
  opacity: 1 !important;
}
.stButton>button span, .stButton>button p, .stButton>button div {
  color: #ffffff !important;
  -webkit-text-fill-color: #ffffff !important;
}
.stButton button[kind], .stButton button[kind] * {
  color: #ffffff !important;
  -webkit-text-fill-color: #ffffff !important;
  opacity: 1 !important;
}
div[data-testid="stForm"] .stButton > button,
div[data-testid="stForm"] .stButton > button * {
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
.stButton button * {
  color: #ffffff !important;
  -webkit-text-fill-color: #ffffff !important;
  text-shadow: 0 0 0 #ffffff;
  opacity: 1 !important;
}
.stButton button:disabled,
.stButton button:disabled * {
  color: #ffffff !important;
  -webkit-text-fill-color: #ffffff !important;
  opacity: 1 !important;
}
.stButton>button:hover {
  background: #111827 !important;
  color: #ffffff !important;
  -webkit-text-fill-color: #ffffff !important;
  filter: none !important;
  box-shadow: none !important;
}
.stButton>button:active,
.stButton>button:focus,
.stButton>button:focus-visible {
  background: #111827 !important;
  color: #ffffff !important;
  -webkit-text-fill-color: #ffffff !important;
  box-shadow: none !important;
  filter: none !important;
}
.stButton>button:disabled,
.stButton>button[disabled] {
  background: #9ca3af !important;
  color: #ffffff !important;
  -webkit-text-fill-color: #ffffff !important;
  cursor: not-allowed !important;
  filter: none !important;
}
.stButton>button:disabled:hover,
.stButton>button[disabled]:hover {
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

st.markdown(
    """
<div class="header-bar">
  <div class="header-title">Schema & Sitemap Validator</div>
  <div class="header-sub">One run, full SEO audit with schema + sitemap insights.</div>
</div>
""",
    unsafe_allow_html=True,
)

schemaorg_ref_path = str(validator.DEFAULT_SCHEMAORG_REF_PATH)
schemaorg_data_path = str(validator.DEFAULT_SCHEMAORG_DATA_PATH)
rules_path = str(validator.DEFAULT_RULES_PATH)
gsc_json_path = resolve_gsc_json_path()
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
    st.session_state["active_url_detail"] = result.url


def render_url_detail_block(site: validator.SiteReport, result: validator.UrlCheckResult, index: int) -> None:
    detail_key = f"{site.domain}::{index}"
    expanded = st.session_state.get("active_url_detail") == result.url
    label = result.url if len(result.url) <= 120 else f"{result.url[:117]}..."
    with st.expander(label, expanded=expanded):
        top = st.columns(4)
        top[0].metric("HTTP", str(result.http_status or "-"))
        top[1].metric("Indexability", result.indexability_status or "-")
        top[2].metric("Word Count", str(result.word_count or 0))
        top[3].metric(
            "Headings",
            f"H1:{result.heading_h1_count} H2:{result.heading_h2_count} H3:{result.heading_h3_count}",
        )

        st.caption(f"Feature image alt: {result.feature_image_status or '-'}")

        button_label = "Refresh GSC Status" if (result.gsc_status or result.gsc_error) else "Request GSC Status"
        if gsc_json_path:
            if st.button(button_label, key=f"gsc_{detail_key}", use_container_width=True):
                fetch_on_demand_gsc_status(st.session_state["latest_report"], result)
                st.rerun()
        else:
            st.caption("GSC is not configured in this deployment.")

        basic_rows = [
            {"Field": "URL", "Value": result.url},
            {"Field": "Final URL", "Value": result.final_url or "-"},
            {"Field": "Meta Title", "Value": result.seo_meta.get("title", "-")},
            {"Field": "Meta Description", "Value": result.seo_meta.get("description", "-")},
            {"Field": "Canonical", "Value": result.seo_meta.get("canonical", "-")},
            {"Field": "Feature Image Alt", "Value": result.feature_image_alt or "-"},
            {"Field": "Schema Types", "Value": ", ".join(result.jsonld_types) if result.jsonld_types else "-"},
        ]
        st.table(pd.DataFrame(basic_rows))

        if result.gsc_status or result.gsc_error or result.gsc_checked_at:
            st.markdown("##### GSC Details")
            gsc_rows = [
                {"Field": "Status", "Value": result.gsc_status or "-"},
                {"Field": "Property", "Value": result.gsc_property or "-"},
                {"Field": "Checked At", "Value": result.gsc_checked_at or "-"},
                {"Field": "Verdict", "Value": result.gsc_verdict or "-"},
                {"Field": "Coverage State", "Value": result.gsc_coverage_state or "-"},
                {"Field": "Indexing State", "Value": result.gsc_indexing_state or "-"},
                {"Field": "Robots State", "Value": result.gsc_robots_state or "-"},
                {"Field": "Page Fetch State", "Value": result.gsc_page_fetch_state or "-"},
                {"Field": "Last Crawl Time", "Value": result.gsc_last_crawl_time or "-"},
                {"Field": "Google Canonical", "Value": result.gsc_google_canonical or "-"},
                {"Field": "User Canonical", "Value": result.gsc_user_canonical or "-"},
                {"Field": "Error", "Value": result.gsc_error or "-"},
            ]
            st.table(pd.DataFrame(gsc_rows))

        if result.issues or result.warnings or result.seo_issues or result.seo_warnings:
            st.markdown("##### Audit Notes")
            notes = []
            for item in result.issues:
                notes.append({"Type": "Issue", "Detail": item})
            for item in result.warnings:
                notes.append({"Type": "Warning", "Detail": item})
            for item in result.seo_issues:
                notes.append({"Type": "SEO Issue", "Detail": item})
            for item in result.seo_warnings:
                notes.append({"Type": "SEO Warning", "Detail": item})
            st.table(pd.DataFrame(notes))

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
        st.caption("Run the audit first. You can request GSC status per URL from the results section.")
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

        gsc_rows = build_gsc_rows(current_report)
        if gsc_rows:
            filter_options = ["All", "Indexed", "Excluded", "Blocked", "Error", "No GSC Data", "Other"]
            gsc_filter = st.selectbox("Filter URLs by GSC status", filter_options, index=0)
            if gsc_filter == "All":
                filtered_rows = gsc_rows
            else:
                filtered_rows = [row for row in gsc_rows if row["GSC Category"] == gsc_filter]
            st.caption(f"Showing {len(filtered_rows)} of {len(gsc_rows)} audited URLs")
            if filtered_rows:
                filtered_df = pd.DataFrame(filtered_rows)
                st.dataframe(
                    filtered_df,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "URL": st.column_config.LinkColumn("URL"),
                    },
                )
            else:
                st.info("No URLs match the selected GSC filter.")

    st.markdown("### Per-URL Actions")
    st.caption("Use the button inside each URL card when you want live GSC status for that specific page.")
    for site in current_report.sites:
        st.markdown(f"#### {site.domain}")
        for index, result in enumerate(site.urls):
            render_url_detail_block(site, result, index)

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
