import time
import re
import pandas as pd
from curl_cffi import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.imobiliare.ro/vanzare-apartamente/bucuresti"

MAX_PROPERTIES = 10_000
MAX_SEARCH_PAGES = 3000
OUTPUT_FILE = "imobiliare_listing_links.csv"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "ro-RO,ro;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://www.imobiliare.ro/",
}

session = requests.Session()


def fetch_page(url, retries=3):
    for attempt in range(1, retries + 1):
        try:
            resp = session.get(
                url,
                headers=HEADERS,
                timeout=30,
                impersonate="chrome120"
            )
            resp.raise_for_status()
            return BeautifulSoup(resp.text, "html.parser")
        except Exception as e:
            print(f"⚠ Attempt {attempt}/{retries} failed for {url}: {e}")
            time.sleep(2 * attempt)
    return None


def build_search_page_url(base_url, page):
    if page <= 1:
        return base_url
    separator = "&" if "?" in base_url else "?"
    return f"{base_url}{separator}page={page}"


def normalize_offer_url(href):
    if not href:
        return None

    full = href if href.startswith("http") else "https://www.imobiliare.ro" + href
    full = full.split("#", 1)[0].split("?", 1)[0]

    if re.match(r"^https://www\.imobiliare\.ro/oferta/[^/]+$", full):
        return full

    return None


def get_listing_links(soup, max_links=None):
    links = []

    for a in soup.find_all("a", href=True):
        full = normalize_offer_url(a["href"])

        if full and full not in links:
            links.append(full)

        if max_links is not None and len(links) >= max_links:
            break

    return links


def collect_links():
    links = []
    seen_links = set()

    print("Collecting property links...")

    for page in range(1, MAX_SEARCH_PAGES + 1):
        if len(links) >= MAX_PROPERTIES:
            break

        page_url = build_search_page_url(BASE_URL, page)
        print(f"Fetching search page {page}: {page_url}")

        soup = fetch_page(page_url)
        if not soup:
            print(f"⚠ Failed to fetch page {page}, stopping.")
            break

        remaining = MAX_PROPERTIES - len(links)
        page_links = get_listing_links(soup, max_links=remaining)

        before_count = len(links)

        for link in page_links:
            if link not in seen_links:
                seen_links.add(link)
                links.append(link)

        added = len(links) - before_count
        print(f"Found {len(page_links)} candidates, added {added} new")

        if added == 0:
            print("No new links found, stopping pagination.")
            break

        time.sleep(0.5)

    print(f"Collected {len(links)} unique links total")

    df = pd.DataFrame({"url": links})
    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")

    print(f"Saved links to {OUTPUT_FILE}")


if __name__ == "__main__":
    collect_links()