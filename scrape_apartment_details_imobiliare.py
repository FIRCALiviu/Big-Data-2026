import time
import re
import pandas as pd
from curl_cffi import requests
from bs4 import BeautifulSoup
import os

from stations import distance_to_metro, distance_to_stb

INPUT_FILE = "imobiliare_listing_links.csv"
OUTPUT_FILE = "imobiliare_apartments.csv"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "ro-RO,ro;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://www.imobiliare.ro/",
}

session = requests.Session()


def clean(text):
    return re.sub(r"\s+", " ", text or "").strip()


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


def get_price_from_soup(soup):
    el = soup.select_one(
        "span.text-xl.font-semibold.leading-none.md\\:text-xxl.md\\:font-extrabold"
    )
    return el.get_text(strip=True) if el else "N/A"


def get_info_box_value(soup, label_text):
    boxes = soup.select(
        ".swiper-item.flex.items-center.gap-2.rounded-lg.border.border-grey-300.px-3.py-2"
    )

    for box in boxes:
        label = box.select_one("span.whitespace-nowrap.text-xs.text-grey-700.md\\:text-sm")

        if label and label.get_text(strip=True) == label_text:
            value = box.select_one("span.whitespace-nowrap.text-sm.font-semibold")
            return value.get_text(strip=True) if value else "N/A"

    return "N/A"


def get_rooms_from_soup(soup):
    return get_info_box_value(soup, "Nr. cam.:")


def get_surface_from_soup(soup):
    return get_info_box_value(soup, "Sup. utilă:")


def get_floor_from_soup(soup):
    return get_info_box_value(soup, "Etaj:")


def parse_year(year):
    try:
        if "(" in year:
            year = year[:year.index("(")]
        return int(year.strip())
    except Exception:
        return "N/A"


def get_year_built(soup):
    year = get_info_box_value(soup, "An constr.:")
    return parse_year(year) if year != "N/A" else "N/A"


def get_detail_label_value(soup, wanted_label):
    labels = soup.select(
        ".flex.w-full.justify-between.gap-x-2.border-b.border-gray-200.py-3.md\\:gap-x-8"
    )

    for label in labels:
        name_label = label.select_one(
            "span.flex.shrink-0.whitespace-nowrap.text-sm.font-normal.text-grey-550.md\\:text-base"
        )

        if not name_label:
            continue

        if name_label.get_text(strip=True).lower() == wanted_label.lower():
            value = label.select_one(
                "span.flex.flex-wrap.text-right.text-sm.font-bold.text-grey-550.md\\:justify-end.md\\:text-base"
            )
            return value.get_text(strip=True).lower() if value else "N/A"

    return "N/A"


def get_construction_material(soup):
    return get_detail_label_value(soup, "structură rezistență:")


def get_nr_bathrooms(soup):
    return get_detail_label_value(soup, "nr. băi:")


def get_elevator(soup):
    elements = soup.select(
        ".text-content.rounded-full.rounded-full.border.border-grey-300.bg-transparent.bg-white.px-2.py-1.text-base"
    )

    for element in elements:
        if element.get_text(strip=True).lower() == "lift":
            return "da"

    return "nu"


def get_city_from_soup(soup):
    addresses = soup.find_all("p", attrs={"data-cy": "listing-address"})

    for addr in addresses:
        span = addr.find("span")
        if span:
            span.decompose()

        text = addr.get_text(strip=True)
        if text:
            return text

    return "N/A"


def get_latitude_longitude(soup):
    link = soup.find("a", href=re.compile(r"www\.google\.com/maps\?\w+"))

    if not link:
        return "N/A", "N/A"

    href = link.get("href")

    try:
        coordinates = href[41:]
        latitude, longitude = coordinates.split(",")
        return float(latitude), float(longitude)
    except Exception:
        return "N/A", "N/A"


def scrape_details():
    links_df = pd.read_csv(INPUT_FILE)

    file_exists = os.path.isfile(OUTPUT_FILE)

    for i, row in links_df.iterrows():
        link = row["url"]
        print(f"\n[{i + 1}/{len(links_df)}] {link}")

        try:
            soup = fetch_page(link)

            if not soup:
                raise ValueError("Failed to fetch detail page")

            surface = get_surface_from_soup(soup)
            rooms = get_rooms_from_soup(soup)
            floor = get_floor_from_soup(soup)
            price = get_price_from_soup(soup)
            city = get_city_from_soup(soup)

            year_built = get_year_built(soup)
            elevator = get_elevator(soup)
            construction_material = get_construction_material(soup)
            nr_bathrooms = get_nr_bathrooms(soup)

            latitude, longitude = get_latitude_longitude(soup)

            if latitude != "N/A" and longitude != "N/A":
                metro = distance_to_metro(latitude, longitude)
                stb = distance_to_stb(latitude, longitude)
            else:
                metro = "N/A"
                stb = "N/A"

            row_data = {
                "url": link,
                "surface_m2": surface,
                "rooms": rooms,
                "floor": floor,
                "price": price,
                "city": city,
                "year_built": year_built,
                "elevator": elevator,
                "construction_material": construction_material,
                "number_bathrooms": nr_bathrooms,
                "latitude": latitude,
                "longitude": longitude,
                "metro_proximity": metro,
                "stb_proximity": stb,
            }

            df_row = pd.DataFrame([row_data])

            # Append to CSV
            df_row.to_csv(
                OUTPUT_FILE,
                mode="a",
                header=not file_exists,
                index=False,
                encoding="utf-8-sig"
            )

            file_exists = True  # after first write

            print(
                f"surface={surface} | rooms={rooms} | floor={floor} | "
                f"price={price} | city={city} | year_built={year_built}"
            )

        except Exception as e:
            print(f"⚠ Error scraping {link}: {e}")

        time.sleep(0.2)

    print(f"\nData continuously saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    scrape_details()