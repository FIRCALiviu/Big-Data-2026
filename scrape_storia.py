import pandas as pd
import time
import re
import json
import requests
from bs4 import BeautifulSoup

# ── CONFIG ─────────────────────────────────────────
BASE_URL = "https://www.storia.ro/ro/rezultate/vanzare/apartament/toata-romania"
MAX_PROPERTIES = 5
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

    full = href if href.startswith("http") else "https://www.storia.ro" + href
    full = full.split("#", 1)[0].split("?", 1)[0]

    if re.match(r"^https://www\.storia\.ro/ro/oferta/[^/]+$", full):
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
    return soup.find('strong', attrs={'aria-label': 'Preț'}).get_text(strip=True)


def get_elevator(soup):
    property_qualities = soup.select("span.css-axw7ok.ei4dv7j1")
    for property in property_qualities: # check if one is elevator
        if property.get_text(strip=True).lower() == "lift":
            return 'da'
    container_items = soup.select("div[data-sentry-element=ItemGridContainer]")
    for item in container_items: 
        label = item.select_one("div[data-sentry-element=Item]")
        if label.get_text(strip=True).lower() == "lift:" :
            named_children = [c for c in item.children if c.name]
            return named_children[1].get_text(strip=True)
    return 'nu'
def get_rooms(soup):
    container_items = soup.select("div[data-sentry-element=ItemGridContainer]")
    for item in container_items: 
        label = item.select_one("div[data-sentry-element=Item]")
        if label.get_text(strip=True).lower() == "numărul de camere:" :
            named_children = [c for c in item.children if c.name]
            return named_children[1].get_text(strip=True)
    return "N/A"
def get_construction_material(soup):
    container_items = soup.select("div[data-sentry-element=ItemGridContainer]")
    for item in container_items: 
        label = item.select_one("div[data-sentry-element=Item]")
        if label.get_text(strip=True).lower() == "material de construcție:" :
            named_children = [c for c in item.children if c.name]
            return named_children[1].get_text(strip=True)
    return "N/A"
def get_floor_from_soup(soup):
    container_items = soup.select("div[data-sentry-element=ItemGridContainer]")
    for item in container_items: 
        label = item.select_one("div[data-sentry-element=Item]")
        if label.get_text(strip=True).lower() == "etaj:" :
            named_children = [c for c in item.children if c.name]
            return named_children[1].get_text(strip=True)
    return "N/A"

def parse_city_from_text(text):
    """Extract a city-like token from a location text line."""
    if not text:
        return "N/A"

    txt = clean(text)
    txt = txt.replace("📍", "").replace("🗺", "").replace("🗺️", "").strip(" -,")

    if not txt:
        return "N/A"

    parts = [p.strip() for p in txt.split(",") if p.strip()]
    if not parts:
        return "N/A"

    # Prefer the most specific city-like suffix (rightmost location part).
    for part in reversed(parts):
        if re.search(r"\d", part):
            continue
        if len(part) < 2:
            continue
        if any(word in part.lower() for word in ["românia", "romania", "sector", "str", "bulevard", "bd."]):
            continue
        return part

    # Last-resort fallback: rightmost raw token.
    return parts[-1]

def get_nr_bathrooms(soup:BeautifulSoup):
        
    pattern = re.compile(r"[2-9] baii", re.IGNORECASE)
    element = soup.find(string=pattern)
    if element:
        match = pattern.search(element)
        if match:
            return int(match.group(0)[0])

    return 1

def extract_address_locality(node):
    """Recursively search JSON-LD for addressLocality."""
    if isinstance(node, dict):
        if "addressLocality" in node and node["addressLocality"]:
            return clean(str(node["addressLocality"]))
        for value in node.values():
            found = extract_address_locality(value)
            if found:
                return found
    elif isinstance(node, list):
        for item in node:
            found = extract_address_locality(item)
            if found:
                return found
    return None


def get_city_from_soup(soup):
    """Extract city from detail page using structured data and UI fallbacks."""
    # Strategy 1: JSON-LD usually contains a clean addressLocality field.
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        payload = (script.string or script.get_text() or "").strip()
        if not payload:
            continue
        try:
            data = json.loads(payload)
            city = extract_address_locality(data)
            if city:
                return city
        except Exception:
            continue

    # Strategy 2: known address containers in ad header.
    for attr, value in [("data-cy", "adPageAdAddress"), ("data-sentry-element", "Address")]:
        el = soup.find(attrs={attr: value})
        if el:
            city = parse_city_from_text(el.get_text(" ", strip=True))
            if city != "N/A":
                return city

    # Strategy 3: any string containing map-pin emoji in visible text.
    for text_node in soup.find_all(string=re.compile(r"📍|🗺")):
        city = parse_city_from_text(str(text_node))
        if city != "N/A":
            return city

    return "N/A"

def get_surface(soup):
    container_items = soup.select("div[data-sentry-element=ItemGridContainer]")
    for item in container_items: 
        label = item.select_one("div[data-sentry-element=Item]")
        if label.get_text(strip=True).lower() == "suprafață utilă:" :
            named_children = [c for c in item.children if c.name]
            return named_children[1].get_text(strip=True)
    return "N/A"
def get_year_from_soup(soup):
    container_items = soup.select("div[data-sentry-element=ItemGridContainer]")
    for item in container_items: 
        label = item.select_one("div[data-sentry-element=Item]")
        if label.get_text(strip=True).lower() == "anul construcției:" :
            named_children = [c for c in item.children if c.name]
            return named_children[1].get_text(strip=True)
    return "N/A"

def get_latitude_longitude(soup:BeautifulSoup):
    lats = re.findall(r'"lat(?:itude)?"\s*:\s*([-\d.]+)', str(soup))
    lngs = re.findall(r'"lo(?:n|ng)(?:itude)?"\s*:\s*([-\d.]+)', str(soup))
    if lats and lngs:
        return lats[0],lngs[0]
    return "N/A","N/A"
from stations import distance_to_metro,distance_to_stb


def scrape():
    listings = []

    # ── Step 1: collect links with pagination ─────────────────────
    print("Collecting property links...")
    links = []
    seen_links = set()

    for page in range(4, MAX_SEARCH_PAGES + 1):
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

            surface = get_surface(detail_soup)
            rooms   = get_rooms(detail_soup)
            floor   = get_floor_from_soup(detail_soup)
            price   = get_price_from_soup(detail_soup)
            city    = get_city_from_soup(detail_soup)

            year_built = get_year_from_soup(detail_soup)
            elevator = get_elevator(detail_soup)
            construction_material = get_construction_material(detail_soup)
            nr_bathrooms = get_nr_bathrooms(detail_soup)
            latitude,longitude = get_latitude_longitude(detail_soup)
            metrou = distance_to_metro(latitude,longitude)
            stb =  distance_to_stb(latitude,longitude)
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
                "number_bathrooms" : nr_bathrooms,
                "latitude": latitude,
                "longitude" : longitude,
                "metro_proximity" : metrou,
                "stb_proximity": stb
            })
            print(
                f"  surface={surface}  |  rooms={rooms}  |  floor={floor}  |  price={price}  |  city={city}"
                f"  |  year_built={year_built}  |  elevator={elevator}  |  material={construction_material} | "
                f"bathrooms = {nr_bathrooms} | latitude = {latitude} | longitude = {longitude}"
                f"metrou_proximity = {metrou} | stb_proximity = {stb}"
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
