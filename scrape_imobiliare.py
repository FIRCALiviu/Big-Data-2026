import pandas as pd
import time
import re
import json
import requests
from bs4 import BeautifulSoup

# ── CONFIG ─────────────────────────────────────────
BASE_URL = "https://www.imobiliare.ro/vanzare-apartamente"

MAX_PROPERTIES = 10
MAX_SEARCH_PAGES = 30
OUTPUT_FILE = "storia_apartments.csv"
# ───────────────────────────────────────────────────

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ro-RO,ro;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.storia.ro/",
}

session = requests.Session()
session.headers.update(HEADERS)


def clean(text):
    return re.sub(r"\s+", " ", text).strip() if text else "N/A"


def fetch_page(url, retries=3):
    """Fetch a URL with retries, returning a BeautifulSoup object or None."""
    for attempt in range(1, retries + 1):
        try:
            resp = session.get(url, timeout=15)
            resp.raise_for_status()
            return BeautifulSoup(resp.text, "html.parser")
        except requests.RequestException as e:
            print(f"  ⚠ Attempt {attempt}/{retries} failed for {url}: {e}")
            time.sleep(2 * attempt)
    return None


def build_search_page_url(base_url, page):
    """Build paginated URL for search results."""
    if page <= 1:
        return base_url
    separator = "&" if "?" in base_url else "?"
    return f"{base_url}{separator}page={page}"


def normalize_offer_url(href):
    """Normalize and validate listing URLs to include only /ro/oferta/ pages."""
    if not href:
        return None

    full = href if href.startswith("http") else "https://www.imobiliare.ro" + href
    full = full.split("#", 1)[0].split("?", 1)[0]

    if re.match(r"^https://www\.imobiliare\.ro/oferta/[^/]+$", full):
        return full
    return None


def get_listing_links(soup, max_links=None):
    """Extract listing links from one search page."""
    links = []
    for a in soup.find_all("a", href=True):
        full = normalize_offer_url(a["href"])
        if full and full not in links:
            links.append(full)
        if max_links is not None and len(links) >= max_links:
            break
    return links


def get_price_from_soup(soup):
    return soup.select_one("span.text-xl.font-semibold.leading-none.md\\:text-xxl.md\\:font-extrabold").get_text(strip=True)

def get_rooms_from_soup(soup):
    boxes = soup.select( ".swiper-item.flex.items-center.gap-2.rounded-lg.border.border-grey-300.px-3.py-2")
    for b in boxes:
        element = b.select_one(
        "span.whitespace-nowrap.text-xs.text-grey-700.md\\:text-sm")
        if element and element.get_text(strip=True)=="Nr. cam.:":
            camere = b.select_one("span.whitespace-nowrap.text-sm.font-semibold")
            return camere.get_text(strip = True)
    return "N/A"

def get_construction_material(soup):
    labels = soup.select(".flex.w-full.justify-between.gap-x-2.border-b.border-gray-200.py-3.md\\:gap-x-8")
    for label in labels : 
        name_label = label.select_one("span.flex.shrink-0.whitespace-nowrap.text-sm.font-normal.text-grey-550.md\\:text-base")
        if name_label.get_text(strip=True).lower() == "structură rezistență:" :
            value = label.select_one("span.flex.flex-wrap.text-right.text-sm.font-bold.text-grey-550.md\\:justify-end.md\\:text-base") 
            return value.get_text(strip=True).lower()
    return "N/A"
def get_surface_from_soup(soup):
    boxes = soup.select( ".swiper-item.flex.items-center.gap-2.rounded-lg.border.border-grey-300.px-3.py-2")
    for b in boxes:
        element = b.select_one(
        "span.whitespace-nowrap.text-xs.text-grey-700.md\\:text-sm")
        if element and element.get_text(strip=True)=="Sup. utilă:":
            surface = b.select_one("span.whitespace-nowrap.text-sm.font-semibold")
            return surface.get_text(strip = True)
    return "N/A"

def parse_year(year):
    try : 
        i = year.index("(")
        return int(year[:i])
    except:
        return int(year)
def get_year_built(soup):
    boxes = soup.select( ".swiper-item.flex.items-center.gap-2.rounded-lg.border.border-grey-300.px-3.py-2")
    for b in boxes:
        element = b.select_one(
        "span.whitespace-nowrap.text-xs.text-grey-700.md\\:text-sm")
        if element and element.get_text(strip=True)=="An constr.:":
            year = b.select_one("span.whitespace-nowrap.text-sm.font-semibold")
            return parse_year(  year.get_text(strip = True) )
    return "N/A"

def get_floor_from_soup(soup):
    boxes = soup.select( ".swiper-item.flex.items-center.gap-2.rounded-lg.border.border-grey-300.px-3.py-2")
    for b in boxes:
        element = b.select_one(
        "span.whitespace-nowrap.text-xs.text-grey-700.md\\:text-sm")
        if element and element.get_text(strip=True)=="Etaj:":
            etaj = b.select_one("span.whitespace-nowrap.text-sm.font-semibold")
            return etaj.get_text(strip = True)
    return "N/A"




def get_elevator(soup): # we assume that if it isn't found, it doesn't have one
    elements = soup.select(".text-content.rounded-full.rounded-full.border.border-grey-300.bg-transparent.bg-white.px-2.py-1.text-base")
    for element in elements:
        if element.get_text(strip=True).lower()=='lift' :
            return 'da'
    return 'nu'


def get_city_from_soup(soup):
    city = soup.find_all('p', attrs={'data-cy': 'listing-address'})
    for addrs in city:
        span = addrs.find('span')
        if span:
            span.decompose()
        string = (addrs.get_text(strip=True))
        if string:
            return string 



    return "N/A"

def get_nr_bathrooms(soup):
    labels = soup.select(".flex.w-full.justify-between.gap-x-2.border-b.border-gray-200.py-3.md\\:gap-x-8")
    for label in labels : 
        name_label = label.select_one("span.flex.shrink-0.whitespace-nowrap.text-sm.font-normal.text-grey-550.md\\:text-base")
        if name_label.get_text(strip=True).lower() == "nr. băi:" :
            value = label.select_one("span.flex.flex-wrap.text-right.text-sm.font-bold.text-grey-550.md\\:justify-end.md\\:text-base") 
            return value.get_text(strip=True).lower()
    return "N/A"

def get_latitude_longitude(soup):
    link = soup.find("a", href=re.compile(r"www\.google\.com/maps\?\w+"))
    coordinates = link.get('href')[41:]
    x,y = coordinates.split(",")
    return float(x),float(y)

import re
from bs4 import BeautifulSoup

def clean(text: str) -> str:
    """Normalize whitespace and strip."""
    return re.sub(r"\s+", " ", text or "").strip()






def scrape():
    listings = []

    # ── Step 1: collect links with pagination ─────────────────────
    print("Collecting property links...")
    links = []
    seen_links = set()

    for page in range(1, MAX_SEARCH_PAGES + 1):
        if len(links) >= MAX_PROPERTIES:
            break

        page_url = build_search_page_url(BASE_URL, page)
        print(f"  Fetching search page {page}: {page_url}")
        soup = fetch_page(page_url)
        if not soup:
            print(f"  ⚠ Failed to fetch page {page}, stopping pagination.")
            break

        remaining = MAX_PROPERTIES - len(links)
        page_links = get_listing_links(soup, max_links=remaining)
        before_count = len(links)

        for link in page_links:
            if link not in seen_links:
                seen_links.add(link)
                links.append(link)

        added = len(links) - before_count
        print(f"    found {len(page_links)} candidates, added {added} new")

        if added == 0:
            print("  No new links on this page, stopping pagination.")
            break

    print(f"  Collected {len(links)} unique links total")

    # ── Step 2: scrape each detail page ───────────────────────────
    for i, link in enumerate(links, 1):
        print(f"\n[{i}/{len(links)}] {link}")
        try:
            detail_soup = fetch_page(link)
            if not detail_soup:
                raise ValueError("Failed to fetch detail page")

            surface = get_surface_from_soup(detail_soup)
            rooms   = get_rooms_from_soup(detail_soup)
            floor   = get_floor_from_soup(detail_soup)
            price   = get_price_from_soup(detail_soup)
            city    = get_city_from_soup(detail_soup)

            year_built = get_year_built(detail_soup)
            elevator = get_elevator(detail_soup)
            construction_material = get_construction_material(detail_soup)
            nr_bathrooms = get_nr_bathrooms(detail_soup)
            latitude,longitude = get_latitude_longitude(detail_soup)
            listings.append({
                "url":        link,
                "surface_m2": surface,
                "rooms":      rooms,
                "floor":      floor,
                "price":      price,
                "city":       city,
                "year_built": year_built,
                "elevator": elevator,
                "construction_material": construction_material,
                "number_bathrooms":nr_bathrooms,
                "latitude" : latitude,
                "longitude" : longitude

            })
            print(
                f"  surface={surface}  |  rooms={rooms}  |  floor={floor}  |  price={price}  |  city={city}"
                f"  |  year_built={year_built}  |  elevator={elevator}  |  material={construction_material}"
                f"  | number of bathrooms={nr_bathrooms} | latitude = {latitude} | longitude = {longitude}"
            )

        except Exception as e:
            print(f"  ⚠ Error scraping {link}: {e}")

        time.sleep(.5)  # polite crawl delay

    return pd.DataFrame(listings)


if __name__ == "__main__":
    print("=" * 55)
    print("  storia.ro apartment scraper (HTTP)")
    print("=" * 55)
    df = scrape()

    if df.empty:
        print("\nNo data collected.")
    else:
        print("\n" + "=" * 55)
        print(df.to_string(index=False))
        df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
