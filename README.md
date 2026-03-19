# YellowPages Vietnam Scraper

A two-phase Python scraper that extracts structured business data from [yellowpages.vn](https://www.yellowpages.vn) based on industry keyword and location.

---

## Features

- **Phase 1 – List Scraping**: Paginates through search results and collects all company detail URLs
- **Phase 2 – Deep Scraping**: Visits each URL and extracts a full company profile
- **Auto page detection**: Reads `div#paging` to find the total number of pages automatically
- **Anti-block measures**: Random User-Agent rotation + randomized delays between requests
- **Graceful error handling**: Try/except on every request — one failed page never stops the run
- **Clean CSV output**: Exported as `utf-8-sig` (Excel-compatible Vietnamese encoding)

---

## Project Structure

```
yellowpages-scraper/
├── yellowpages_scraper.py   # Main scraper (CLI entry point)
├── demo.ipynb               # Jupyter notebook walkthrough
├── results.csv              # Output file (generated on run)
└── README.md
```

---

## Installation

```bash
pip install requests beautifulsoup4 pandas
```

---

## Usage

### Command Line

```bash
# Minimal — keyword only (scrapes nationwide, auto-detects all pages)
python yellowpages_scraper.py "May mac"

# With location filter
python yellowpages_scraper.py "May mac" --where "Binh Duong"

# Limit to 3 pages and save to a custom file
python yellowpages_scraper.py "May mac" -w "Binh Duong" -p 3 -o output.csv

# Full help
python yellowpages_scraper.py --help
```

### Arguments

| Argument | Short | Required | Default | Description |
|---|---|---|---|---|
| `keyword` | — | **Yes** | — | Industry / search keyword |
| `--where` | `-w` | No | `""` (nationwide) | Location filter |
| `--max-pages` | `-p` | No | auto-detect | Max pages to scrape |
| `--output` | `-o` | No | `results.csv` | Output CSV filename |
| `--delay-min` | — | No | `2.0` | Min delay between requests (sec) |
| `--delay-max` | — | No | `5.0` | Max delay between requests (sec) |

### As a Python module

```python
from yellowpages_scraper import YellowPagesScraper

scraper = YellowPagesScraper(
    keyword     = "May mac",
    where       = "Binh Duong",
    max_pages   = 3,          # omit for auto-detect
    output_file = "output.csv",
)

df = scraper.run()
print(df.head())
```

---

## Output Columns

| Column | Description |
|---|---|
| Company Name | Official business name |
| Address | Registered address |
| Phone | Primary phone number |
| Website | Company website |
| Email | Contact email |
| Tax Code | Vietnamese business tax ID |
| Business Type | e.g. Manufacturer, Trader |
| Year Established | Year the company was founded |
| Target Market | e.g. Nationwide, Export |
| Main Customers | e.g. Hospitals, Schools |
| No. of Employees | Headcount range |
| Industry | Search keyword used |
| Detail URL | Source page URL |

---

## How It Works

```
Phase 1                          Phase 2
┌─────────────────────────┐     ┌────────────────────────┐
│ /srch/[where]/[kw].html │ ──▶ │ company-detail.html    │
│ page=1 → extract URLs   │     │ parse div.hoso_pc      │
│ page=2 → extract URLs   │     │ └─ .hoso_left  (label) │
│ ...                     │     │ └─ .hoso_right (value) │
└─────────────────────────┘     └────────────────────────┘
         ↓                              ↓
   list of URLs                  Company objects
                                        ↓
                                   results.csv
```

---

## Notes

- Scraping is subject to yellowpages.vn's terms of service. Use responsibly.
- If pages return empty results, the scraper stops pagination automatically.
- HTML selectors may need updating if the site structure changes.

---

