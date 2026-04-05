import pandas as pd
import time
import re
import json
import requests
from bs4 import BeautifulSoup

# ── CONFIG ─────────────────────────────────────────
BASE_URL = "https://www.storia.ro/ro/rezultate/vanzare/apartament/toata-romania"
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
    """Extract price using data-cy or data-sentry-element attributes."""
    for attr, value in [("data-cy", "adPageHeaderPrice"), ("data-sentry-element", "Price")]:
        el = soup.find(attrs={attr: value})
        if el:
            val = clean(el.get_text())
            if val and val != "N/A":
                return val
    return "N/A"


def is_valid_floor_value(value):
    """Return True when value looks like a floor and not like a posted date."""
    if not value or value == "N/A":
        return False

    v = clean(value).lower()

    if re.search(r"\b\d{1,2}[./-]\d{1,2}[./-]\d{2,4}\b", v):
        return False
    if re.search(r"\b\d{4}[./-]\d{1,2}[./-]\d{1,2}\b", v):
        return False

    bad_tokens = ["azi", "ieri", "publicat", "actualizat", "anun", "postat"]
    if any(token in v for token in bad_tokens):
        return False

    allowed_words = ["parter", "demisol", "mansarda", "mezanin", "subsol", "fără informații"]
    if any(word in v for word in allowed_words):
        return True

    if re.fullmatch(r"\s*>?\s*\d{1,2}(\s*/\s*\d{1,2})?\s*", v):
        return True

    return False


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


def get_feature_from_soup(soup, label, validator=None):
    """
    Try multiple strategies to extract a labelled value from a detail page.
    """
    # Strategy 1: <dt> / <dd> pattern
    for dt in soup.find_all("dt"):
        if clean(dt.get_text()) == label:
            dd = dt.find_next_sibling("dd")
            if dd:
                val = clean(dd.get_text())
                if val and val != label and (validator is None or validator(val)):
                    return val

    # Strategy 2: any tag whose text matches the label, value in next sibling
    for tag in soup.find_all(string=re.compile(rf"^\s*{re.escape(label)}\s*$")):
        parent = tag.parent
        sibling = parent.find_next_sibling()
        if sibling:
            val = clean(sibling.get_text())
            if val and val != label and (validator is None or validator(val)):
                return val

    # Strategy 3: parent of the label tag, look one level up for value sibling
    for tag in soup.find_all(string=re.compile(rf"^\s*{re.escape(label)}\s*$")):
        grandparent = tag.parent.parent if tag.parent else None
        if grandparent:
            sibling = grandparent.find_next_sibling()
            if sibling:
                val = clean(sibling.get_text())
                if val and val != label and (validator is None or validator(val)):
                    return val

    # Strategy 4: label anywhere in text, next <p> or <div> sibling
    for el in soup.find_all(["p", "div", "span", "li"]):
        if clean(el.get_text()) == label:
            for sibling in el.find_next_siblings(["p", "div", "span", "li"]):
                val = clean(sibling.get_text())
                if val and val != label and len(val) < 100 and (validator is None or validator(val)):
                    return val

    # Strategy 5: raw regex on page source — last resort
    try:
        src = str(soup)
        pattern = re.escape(label) + r"[\s\S]{0,60}?>([\w\s\.,/]+)<"
        match = re.search(pattern, src)
        if match:
            val = clean(match.group(1))
            if validator is None or validator(val):
                return val
    except Exception:
        pass

    return "N/A"


def get_feature_from_soup_any_label(soup, labels, validator=None):
    """Try multiple label variants (e.g., with/without trailing colon)."""
    for label in labels:
        val = get_feature_from_soup(soup, label, validator=validator)
        if val != "N/A":
            return val
    return "N/A"


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

    if not links:
        print("⚠ No links found. Saving debug_page.html ...")
        soup = fetch_page(BASE_URL)
        if soup:
            with open("debug_page.html", "w", encoding="utf-8") as f:
                f.write(str(soup))
        return pd.DataFrame()

    # ── Step 2: scrape each detail page ───────────────────────────
    for i, link in enumerate(links, 1):
        print(f"\n[{i}/{len(links)}] {link}")
        try:
            detail_soup = fetch_page(link)
            if not detail_soup:
                raise ValueError("Failed to fetch detail page")

            surface = get_feature_from_soup(detail_soup, "Suprafață utilă")
            rooms   = get_feature_from_soup(detail_soup, "Numărul de camere")
            floor   = get_feature_from_soup(detail_soup, "Etaj", validator=is_valid_floor_value)
            price   = get_price_from_soup(detail_soup)
            city    = get_city_from_soup(detail_soup)

            year_built = get_feature_from_soup_any_label(detail_soup, ["Anul construcției", "Anul construcției:"])
            elevator = get_feature_from_soup_any_label(detail_soup, ["Lift", "Lift:"])
            construction_material = get_feature_from_soup_any_label(
                detail_soup,
                ["Material de construcție", "Material de construcție:"],
            )

            # If all N/A, save debug html for that page
            if surface == rooms == floor == price == "N/A":
                fname = f"debug_property_{i}.html"
                with open(fname, "w", encoding="utf-8") as f:
                    f.write(str(detail_soup))
                print(f"  ⚠ All N/A — saved {fname} for inspection")

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
            })
            print(
                f"  surface={surface}  |  rooms={rooms}  |  floor={floor}  |  price={price}  |  city={city}"
                f"  |  year_built={year_built}  |  elevator={elevator}  |  material={construction_material}"
            )

        except Exception as e:
            print(f"  ⚠ Error scraping {link}: {e}")
            listings.append({
                "url": link, "surface_m2": "ERROR",
                "rooms": "ERROR", "floor": "ERROR", "price": "ERROR", "city": "ERROR",
                "year_built": "ERROR", "elevator": "ERROR", "construction_material": "ERROR",
            })

        time.sleep(1.5)  # polite crawl delay

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
        print(f"\n✓ Saved {len(df)} rows → {OUTPUT_FILE}")