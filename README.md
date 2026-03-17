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
