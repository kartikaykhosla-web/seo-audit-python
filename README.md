# Schema & Sitemap Validator

Validates sitemap XMLs and extracts structured data (JSON-LD, Microdata, RDFa) for a limited set of URLs per site.

## What it does
- Reads `robots.txt` and respects disallow rules.
- Discovers sitemaps from `robots.txt`, or falls back to `/sitemap.xml`.
- Parses sitemap indexes and URL sitemaps (including `.gz`).
- Validates up to 20 URLs per site (configurable).
- Generates a readable HTML report.
- Audits sitemap fields (`lastmod`, `changefreq`, `priority`, news publication metadata) and provides SEO recommendations.
- Extracts schema values into a structured table for each URL.
- Includes an executive summary with an SEO health score and top fixes.
- Validates JSON-LD, Microdata, and RDFa against the schema rules.
- Audits on-page SEO tags (title, meta description, canonical, Open Graph, Twitter, Facebook).
- Adds SEO length benchmarks (title and meta description) and separate SEO vs schema scores.

## Quick start
```bash
cd schema-validator
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

python validator.py --output report.html --max-urls 20
```

## Run the UI
```bash
cd schema-validator
streamlit run app.py
```

## Custom domains
```bash
python validator.py --domains jagran.com thedailyjagran.com jagranjosh.com onlymyhealth.com herzindagi.com --output report.html
```

## Validate specific URLs
```bash
python validator.py --page-url https://www.jagran.com/some-article https://www.onlymyhealth.com/some-page
```

## Use explicit sitemaps
```bash
python validator.py --sitemap-url https://www.jagran.com/sitemap.xml https://www.onlymyhealth.com/sitemap.xml
```

## Sitemap vs URL mode
- If you pass `--page-url` without `--sitemap-url`, sitemap crawling is skipped and only those pages are validated.
- If you pass both `--page-url` and `--sitemap-url`, the report includes both sets.
- If you pass neither, sitemaps are discovered via `robots.txt` (default).

## Schema.org reference properties
To include full “allowed/possible” nodes per schema type in the report:
1. Download the schema.org JSON-LD data file (schemaorg-current-https.jsonld).
2. Build the local cache once:
```bash
python validator.py --schemaorg-data /path/to/schemaorg-current-https.jsonld --output report.html
```
This creates `schemaorg_properties.json` which is reused in future runs.
Optional: auto-download the schema.org file for you:
```bash
python validator.py --schemaorg-download --output report.html
```
To disable auto-download:
```bash
python validator.py --schemaorg-no-download --output report.html
```

## Notes
- Schema rules live in `schema_rules.json` and control required vs recommended properties.
- You can pass a custom rules file: `python validator.py --rules /path/to/rules.json`.
- JSON-LD types that match schema.org are validated against these rules; missing required fields are shown as Issues and missing recommended fields as Warnings.

## Indexing Monitor
This repo also includes an independent Google Sheets + GSC indexing monitor:

- Discovers new article URLs from configured news sitemaps
- Uses only `news:publication_date` from the sitemap
- Ignores articles older than the configured cutoff date / datetime
- Writes a summary tab and a log tab per property/language
- Polls GSC until a URL is first seen as indexed

### Files
- `indexing_monitor.py`
- `indexing_monitor_config.example.json`
- `indexing_monitor_config.local.json` (local only, ignored by git)

### Local run
```bash
cd schema-validator
python indexing_monitor.py --config indexing_monitor_config.local.json
```

### Optional: run only one property
```bash
python indexing_monitor.py --config indexing_monitor_config.local.json --property thedailyjagran.com
```

### Summary tab columns
- `date`
- `url`
- `sitemap_published_date`
- `Google_Last_Crawl_At`
- `first_checked_at`
- `last_checked_at`
- `check_count`
- `current_status`
- `first_indexed_seen_at`
- `last_non_indexed_seen_at`
- `estimated_indexed_at`
- `indexing_latency_minutes`
- `gsc_coverage_state`
- `gsc_page_fetch_state`

### Log tab columns
- `url`
- `checked_at`
- `status`
- `verdict`
- `coverage_state`
- `indexing_state`
- `page_fetch_state`
- `robots_state`
- `last_crawl_time`
- `error`

### Cutoff behavior
- You can set a fixed ISO datetime such as `2026-04-01T15:30:00+05:30`
- Or use `today_ist` to make the worker pick only articles published from `00:00 IST` of the current day onward
- GSC polling also skips rows older than the active cutoff, so previous-day URLs do not keep consuming quota

### Sorting behavior
- Summary tabs are automatically sorted after writes
- Primary sort: Column A (`date`) descending
- Secondary sort: `sitemap_published_date` descending

### Sitemap date source
- Default behavior uses `news:publication_date` only
- Individual properties can opt into `lastmod` fallback with `allow_lastmod_fallback: true`
- `jagranreviews.com` uses this fallback because its sitemap exposes `lastmod` instead of `news:publication_date`
