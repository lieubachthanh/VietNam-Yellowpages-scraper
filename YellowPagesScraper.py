import requests
from bs4 import BeautifulSoup
import pandas as pd
import html
import time
import random
import logging
import argparse
import re
from urllib.parse import urljoin
from dataclasses import dataclass, asdict
from typing import Optional

# ── Logging configuration ────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("YPScraper")

BASE_URL = "https://www.yellowpages.vn"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
]


# ── Data model ───────────────────────────────────────────────────────────────
@dataclass
class Company:
    ten_cong_ty:        str = ""   # Company name
    dia_chi:            str = ""   # Address
    so_dien_thoai:      str = ""   # Phone number
    website:            str = ""   # Website URL
    email:              str = ""   # Email address
    ma_so_thue:         str = ""   # Tax code
    loai_hinh:          str = ""   # Business type
    nam_thanh_lap:      str = ""   # Year established
    thi_truong:         str = ""   # Target market
    khach_hang_chinh:   str = ""   # Main customers
    so_luong_nhan_vien: str = ""   # Number of employees
    nganh_nghe:         str = ""   # Industry / search keyword
    detail_url:         str = ""   # Source URL


# ── Scraper ──────────────────────────────────────────────────────────────────
class YellowPagesScraper:

    def __init__(self, keyword, where="", max_pages=None,
                 delay_min=1.5, delay_max=4.0, output_file="results.csv"):
        self.keyword     = keyword
        self.where       = where
        self.max_pages   = max_pages   # None = auto-detect from div#paging
        self.delay_min   = delay_min
        self.delay_max   = delay_max
        self.output_file = output_file
        self.session     = self._build_session()
        self.results: list[Company] = []

    # ── Helpers ──────────────────────────────────────────────────────────────

    def _build_session(self):
        """Create a persistent HTTP session with shared browser-like headers."""
        s = requests.Session()
        s.headers.update({
            "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection":      "keep-alive",
        })
        return s

    def _random_headers(self):
        """Return headers with a random User-Agent to reduce bot-detection risk."""
        return {"User-Agent": random.choice(USER_AGENTS)}

    def _sleep(self):
        """Random delay between requests to avoid overwhelming the server."""
        time.sleep(random.uniform(self.delay_min, self.delay_max))

    def _get(self, url):
        """
        Perform a GET request and return a BeautifulSoup object.
        Returns None on any error so the pipeline continues gracefully.
        """
        try:
            resp = self.session.get(url, headers=self._random_headers(), timeout=15)
            resp.raise_for_status()
            resp.encoding = "utf-8"
            return BeautifulSoup(html.unescape(resp.text), "html.parser")
        except requests.exceptions.HTTPError as e:
            log.warning(f"  HTTP {e.response.status_code} -> {url}")
        except requests.exceptions.ConnectionError:
            log.warning(f"  Connection error -> {url}")
        except requests.exceptions.Timeout:
            log.warning(f"  Request timed out -> {url}")
        except Exception as e:
            log.warning(f"  Unexpected error [{e}] -> {url}")
        return None

    # ── Phase 1: Collect company URLs ────────────────────────────────────────

    def _build_search_url(self, page):
        """
        Build a paginated search URL.
        Pattern: /srch/[where]/[keyword].html?page=N
                 /srch/[keyword].html           (no location filter)
        """
        where_slug   = self.where.strip().replace(" ", "_")
        keyword_slug = self.keyword.strip().replace(" ", "_")
        path = (
            f"/srch/{where_slug}/{keyword_slug}.html"
            if where_slug
            else f"/srch/{keyword_slug}.html"
        )
        url = BASE_URL + path
        if page > 1:
            url += f"?page={page}"
        return url

    def _get_max_page(self, soup):
        """
        Auto-detect total pages from div#paging.

        Expected HTML:
          <div id="paging">
            <a href="?page=1">First</a>
            <a href="?page=1" class="page_active">1</a>
            ...  <a href="?page=7">7</a>   <- this is the max
            <a href="?page=2">Next</a>
          </div>

        Reads all href="?page=N" attributes and returns the largest N.
        """
        paging = soup.select_one("div#paging")
        if not paging:
            log.info("  div#paging not found -- defaulting to 1 page.")
            return 1
        max_p = 1
        for a in paging.select("a[href]"):
            m = re.search(r"[?&]page=(\d+)", a["href"])
            if m:
                max_p = max(max_p, int(m.group(1)))
        log.info(f"  Detected div#paging -> total {max_p} page(s).")
        return max_p

    def _extract_company_urls(self, soup):
        """Extract all company detail URLs from a single search result page."""
        urls = []
        for tag in soup.select("h2.fs-5.pb-0.text-capitalize a", limit=50):
            href = tag.get("href", "")
            if href:
                full = urljoin(BASE_URL, href)
                if full not in urls:
                    urls.append(full)
        return urls

    def collect_company_urls(self):
        """
        Phase 1 -- Paginate through search results and collect company URLs.
        Fetches page 1 upfront to auto-detect max_pages and reuse the soup.
        """
        all_urls = []
        url_p1  = self._build_search_url(1)
        log.info(f"  Fetching page 1 -> {url_p1}")
        soup_p1 = self._get(url_p1)
        if soup_p1 is None:
            log.error("Failed to load the first search result page.")
            return []

        # Resolve max_pages if not provided
        if self.max_pages is None:
            self.max_pages = self._get_max_page(soup_p1)

        log.info(
            f"[PHASE 1] keyword='{self.keyword}' | "
            f"location='{self.where or '(nationwide)'}' | "
            f"max_pages={self.max_pages}"
        )

        for page in range(1, self.max_pages + 1):
            # Reuse already-fetched page 1 soup to avoid an extra request
            soup = soup_p1 if page == 1 else self._get(self._build_search_url(page))
            if soup is None:
                log.warning(f"  Skipping page {page} due to fetch error.")
                self._sleep()
                continue

            if page > 1:
                log.info(f"  Page {page}/{self.max_pages} -> {self._build_search_url(page)}")

            page_urls = self._extract_company_urls(soup)
            log.info(f"     -> Found {len(page_urls)} companies.")

            if not page_urls:
                log.info("  No more results -- stopping pagination.")
                break

            for u in page_urls:
                if u not in all_urls:
                    all_urls.append(u)

            if page < self.max_pages:
                self._sleep()

        log.info(f"[PHASE 1] Done -- {len(all_urls)} unique URLs collected.")
        return all_urls

    # ── Phase 2: Extract company details ─────────────────────────────────────

    @staticmethod
    def _safe_text(soup, *selectors):
        """Try selectors in order; return the first non-empty text found."""
        for sel in selectors:
            tag = soup.select_one(sel)
            if tag:
                return tag.get_text(separator=" ", strip=True)
        return ""

    def _extract_detail(self, url):
        """
        Phase 2 -- Parse a company detail page into a Company object.

        Profile block (div.hoso_pc) HTML:
          <div class="hoso_pc">
            <div class="mt-3 h-auto clearfix">   <- one row per field
              <div class="hoso_left"> Field name: </div>  <- label
              <div class="hoso_right"> Value      </div>  <- value
            </div> ...
          </div>

        Step 1: Iterate all div.mt-3 rows once -> build {label: value} dict.
        Step 2: Map dict values directly onto Company fields.
        """
        co   = Company(detail_url=url, nganh_nghe=self.keyword)
        soup = self._get(url)
        if soup is None:
            return co

        # -- Basic info from page header --------------------------------------
        co.ten_cong_ty = self._safe_text(soup, "h1.fs-4.text-capitalize")
        co.dia_chi     = self._safe_text(soup, "div.mt-3.h-auto.clearfix p.m-0.pb-2")

        phone_tag = soup.select_one("a[href^='tel:']")
        if phone_tag:
            co.so_dien_thoai = phone_tag.get("href", "").replace("tel:", "").strip()

        web_tag = soup.select_one("a.text-success")
        if web_tag:
            co.website = web_tag.get("href", "").strip()

        email_tag = soup.select_one("a[href^='mailto:']")
        if email_tag:
            co.email = email_tag.get("href", "").replace("mailto:", "").strip()

        # -- Company profile block (div.hoso_pc) ------------------------------
        hoso_block = soup.select_one("div.hoso_pc")
        if hoso_block:
            # Step 1: single-pass traversal -> build label/value dict
            info = {}
            for row in hoso_block.select("div.mt-3"):
                label_el = row.select_one(".hoso_left")
                value_el = row.select_one(".hoso_right")
                if not label_el or not value_el:
                    continue
                label = label_el.get_text(strip=True).rstrip(":")
                value = value_el.get_text(strip=True)
                if label and value:
                    info[label] = value
            log.debug(f"  [hoso_pc] {info}")

            # Step 2: assign sequentially to Company fields
            co.ten_cong_ty        = info.get("Tên công ty",          "")
            co.loai_hinh          = info.get("Loại hình kinh doanh", "")
            co.ma_so_thue         = info.get("Mã số thuế",           "")
            co.nam_thanh_lap      = info.get("Năm thành lập",        "")
            co.thi_truong         = info.get("Thị trường chính",     "")
            co.khach_hang_chinh   = info.get("Khách hàng chính",     "")
            co.so_luong_nhan_vien = info.get("Số lượng nhân viên",   "")

        return co

    # ── Pipeline ──────────────────────────────────────────────────────────────

    def run(self):
        """Run the full two-phase pipeline and export results to CSV."""
        # Phase 1: collect URLs
        company_urls = self.collect_company_urls()
        if not company_urls:
            log.error("No URLs found. Check your keyword and location.")
            return pd.DataFrame()

        # Phase 2: scrape details
        log.info(f"\n[PHASE 2] Processing {len(company_urls)} companies ...")
        for idx, url in enumerate(company_urls, 1):
            log.info(f"  [{idx:>4}/{len(company_urls)}] {url}")
            self.results.append(self._extract_detail(url))
            self._sleep()

        # Export -- column order matches Company dataclass field order
        df = pd.DataFrame([asdict(c) for c in self.results])
        df.columns = [
            "Company Name", "Address", "Phone", "Website", "Email",
            "Tax Code", "Business Type", "Year Established",
            "Target Market", "Main Customers", "No. of Employees",
            "Industry", "Detail URL",
        ]
        # Preserve leading zeros: Phone (e.g. 0274...) and Tax Code (e.g. 0313...)
        # Wrap values in Excel text-formula ="value" so the leading zero is never stripped.
        for col in ["Phone", "Tax Code"]:
            df[col] = df[col].astype(str).str.strip().apply(
                lambda v: f'="{v}"' if v and v != "nan" else ""
            )
        df.to_csv(self.output_file, index=False, encoding="utf-8-sig")
        log.info(f"\n[DONE] Saved {len(df)} records -> '{self.output_file}'")
        return df


# ── CLI ──────────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        prog="yellowpages_scraper",
        description="Scrape business data from yellowpages.vn",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=(
            "Examples:\n"
            '  python yellowpages_scraper.py "May mac"\n'
            '  python yellowpages_scraper.py "May mac" -w "Binh Duong"\n'
            '  python yellowpages_scraper.py "May mac" -w "Binh Duong" -p 3 -o out.csv'
        ),
    )
    parser.add_argument(
        "keyword", type=str,
        help='(Required) Industry / search keyword.\n  e.g. "May mac"'
    )
    parser.add_argument(
        "--where", "-w",
        type=str, default="", metavar="LOCATION",
        help='(Optional) Target location.\n  e.g. "Binh Duong"  (default: nationwide)'
    )
    parser.add_argument(
        "--max-pages", "-p",
        type=int, default=None, dest="max_pages", metavar="N",
        help="(Optional) Max pages to scrape.\n  Default: auto-detect from div#paging."
    )
    parser.add_argument(
        "--output", "-o",
        type=str, default="results.csv", dest="output_file", metavar="FILE",
        help="(Optional) Output CSV filename.\n  Default: results.csv"
    )
    parser.add_argument(
        "--delay-min",
        type=float, default=2.0, metavar="SEC",
        help="(Optional) Min delay between requests (seconds). Default: 2.0"
    )
    parser.add_argument(
        "--delay-max",
        type=float, default=5.0, metavar="SEC",
        help="(Optional) Max delay between requests (seconds). Default: 5.0"
    )
    args = parser.parse_args()
    if not args.keyword.strip():
        parser.error("keyword cannot be empty.")
    return args


if __name__ == "__main__":
    args = parse_args()
    scraper = YellowPagesScraper(
        keyword     = args.keyword,
        where       = args.where,
        max_pages   = args.max_pages,
        delay_min   = args.delay_min,
        delay_max   = args.delay_max,
        output_file = args.output_file,
    )
    scraper.run()