#!/usr/bin/env python3
"""Streamlit UI for schema-validator."""

from __future__ import annotations

from pathlib import Path

import streamlit as st

import validator


APP_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT = APP_DIR / "report.html"


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
user_agent = validator.DEFAULT_USER_AGENT
output_path = str(DEFAULT_OUTPUT)
download_schemaorg = True

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
        run_button = st.form_submit_button("Run Validation", use_container_width=True)

if run_button:
    targets = parse_multiline(targets_text)
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
    if use_schemaorg:
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

    with st.spinner("Running validation..."):
        report = validator.build_report(
            domains=domains,
            max_urls=max_urls,
            user_agent=user_agent,
            rules=rules,
            rules_path=rules_path,
            schemaorg_ref=schemaorg_ref,
            schemaorg_ref_path=schemaorg_ref_path,
            sitemap_urls_by_domain=sitemap_urls_by_domain,
            page_urls_by_domain=page_urls_by_domain,
            sitemap_mode=sitemap_mode,
        )
        validator.render_report(report, output_path)

    # Report is saved locally; suppress banner to keep UI clean.
    if use_schemaorg and not schemaorg_ref:
        st.warning(
            "Schema.org properties reference not loaded. "
            "Check your internet connection or provide the schema.org JSON-LD file path."
        )
    summary = validator.compute_executive_summary(report)
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

    show_preview = st.checkbox("Show report preview", value=True)
    if show_preview:
        with st.expander("Report preview", expanded=True):
            try:
                html_content = Path(output_path).read_text(encoding="utf-8")
                st.components.v1.html(html_content, height=900, scrolling=True)
            except Exception as exc:
                st.error(f"Could not load report HTML: {exc}")
else:
    st.info("Pick targets and click Run Validation to generate a report.")
