import pandas as pd

df = pd.read_csv("../imobiliare_apartments.csv", encoding="utf-8-sig")

df = df.drop(columns=["url"])
df = df[df["price"].notna()]
df = df[df["price"].str.contains("€", na=False)]
df = df.drop_duplicates().reset_index(drop=True)

df["surface_m2"] = df["surface_m2"].str.replace(" mp", "").str.replace(",", ".").astype(float)
df = df[(df["surface_m2"] >= 10) & (df["surface_m2"] <= 500)].reset_index(drop=True)

df["rooms"] = df["rooms"].astype(int)

materials = {
    "beton": 0,
    "cărămidă": 1,
    "altele": 2,
    "bca": 3,
    "data_properties.building_structure.bca": 3,
    "data_properties.building_structure.panels": 2,
    "lemn": 4,
    "metalică": 5,
}
df["construction_material"] = df["construction_material"].map(materials).fillna(-1).astype(int)

df["price"] = df["price"].str.replace(".", "").str.replace(" €", "").astype(int)
df = df[df["price"] >= 5000].reset_index(drop=True)

df[["neighborhood", "city"]] = df["city"].str.split(", ", n=1, expand=True)
df["city"] = (df["city"] == "Județul Ilfov").astype(int)

neighborhoods = sorted(df["neighborhood"].dropna().unique())
df["neighborhood"] = df["neighborhood"].map({n: i for i, n in enumerate(neighborhoods)})

df["year_built"] = pd.to_numeric(df["year_built"], errors="coerce")
df = df[df["year_built"].isna() | ((df["year_built"] >= 1850) & (df["year_built"] <= 2027))]
df["year_built"] = df["year_built"].fillna(int(df["year_built"].median())).astype(int)

df["elevator"] = df["elevator"].map({"da": 1, "nu": 0})

df["number_bathrooms"] = pd.to_numeric(df["number_bathrooms"], errors="coerce")
df["number_bathrooms"] = df["number_bathrooms"].fillna(1).clip(upper=6).astype(int)


def parse_floor(s):
    if pd.isna(s):
        return pd.NA, pd.NA
    parts = [p.strip() for p in str(s).split("/")]
    floor_str = parts[0]
    max_str = parts[1] if len(parts) > 1 else None

    max_floor = pd.NA
    if max_str:
        try:
            max_floor = int(max_str)
        except ValueError:
            pass

    fl = floor_str.lower()
    if fl.startswith("mansardă"):
        floor = max_floor
    elif fl.startswith("ultimele"):
        floor = pd.NA
    elif fl == "parter":
        floor = 0
    elif fl == "demisol" or floor_str == "-1":
        floor = -1
    else:
        try:
            floor = int(floor_str.strip())
        except ValueError:
            floor = pd.NA

    return floor, max_floor


parsed = df["floor"].apply(parse_floor)
df["floor"] = pd.array([f for f, _ in parsed], dtype="Int64")
df["max_floor"] = pd.array([m for _, m in parsed], dtype="Int64")

df = df[df["floor"].notna()].reset_index(drop=True)
df["floor"] = df["floor"].astype(int)
df["max_floor"] = df["max_floor"].fillna(df["max_floor"].median()).astype(int)
df.loc[df["floor"] > df["max_floor"], "max_floor"] = df.loc[df["floor"] > df["max_floor"], "floor"]

df = df.drop_duplicates(subset=["price", "surface_m2", "rooms", "latitude", "longitude"]).reset_index(drop=True)

price_per_m2 = df["price"] / df["surface_m2"]
df = df[(price_per_m2 >= 500) & (price_per_m2 <= 20000) & ~((df["rooms"] >= 4) & (df["surface_m2"] < 30))].reset_index(drop=True)

cols = [
    "surface_m2", "rooms", "floor", "max_floor", "price",
    "neighborhood", "city", "year_built", "elevator",
    "construction_material", "number_bathrooms",
    "latitude", "longitude", "metro_proximity", "stb_proximity",
]
df = df[cols]

df.to_csv("../imobiliare_cleaned.csv", index=False)
print(f"Saved imobiliare_cleaned.csv — {len(df)} rows, {len(df.columns)} cols")
print(df.dtypes)
print(df.isnull().sum())
