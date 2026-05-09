import pandas as pd

df = pd.read_csv("../storia_apartments.csv", encoding="utf-8-sig")

df = df.drop(columns=["url", "number_bathrooms"])
df = df[df["price"].str.contains("€", na=False)]
df = df.drop_duplicates().reset_index(drop=True)

df["surface_m2"] = df["surface_m2"].str.replace("m²", "").astype(float)
df = df[(df["surface_m2"] >= 10) & (df["surface_m2"] <= 500)].reset_index(drop=True)

df["price"] = df["price"].str.replace(" ", "").str.replace("€", "").astype(int)
df = df[df["price"] >= 5000].reset_index(drop=True)

df = df.rename(columns={"city": "neighborhood"})

neighborhoods = sorted(df["neighborhood"].dropna().unique())
df["neighborhood"] = df["neighborhood"].map({n: i for i, n in enumerate(neighborhoods)}).fillna(-1).astype(int)

df["year_built"] = pd.to_numeric(df["year_built"], errors="coerce")
df = df[df["year_built"].isna() | ((df["year_built"] >= 1850) & (df["year_built"] <= 2027))]
df["year_built"] = df["year_built"].fillna(int(df["year_built"].median())).astype(int)

df["elevator"] = df["elevator"].map({"da": 1, "nu": 0})

materials = {
    "beton": 0,
    "cărămidă": 1,
    "beton armat": 2,
    "placa de beton": 3,
    "beton celular": 4,
    "cărămidă cu goluri": 5,
    "altul": 6,
    "lemn": 7,
}
df["construction_material"] = df["construction_material"].map(materials).fillna(-1).astype(int)


def parse_floor(s):
    s = str(s).strip()
    if s.startswith(".css") or s == "fără informații" or s == "nan":
        return pd.NA, pd.NA

    parts = s.split("/")
    floor_str = parts[0].strip()
    max_str = parts[1].strip() if len(parts) > 1 else None

    max_floor = pd.NA
    if max_str:
        try:
            max_floor = int(max_str)
        except ValueError:
            pass

    fl = floor_str.lower()
    if fl == "parter":
        floor = 0
    elif fl == "demisol":
        floor = -1
    elif fl in ("tip mansardă", "mansardă"):
        floor = max_floor if pd.notna(max_floor) else pd.NA
    elif fl.startswith("> 10"):
        floor = 11
    else:
        try:
            floor = int(fl)
        except ValueError:
            floor = pd.NA

    return floor, max_floor


parsed = df["floor"].apply(parse_floor)
df["floor"] = pd.array([f for f, _ in parsed], dtype="Int64")
df["max_floor"] = pd.array([m for _, m in parsed], dtype="Int64")

mask = df["floor"].notna() & df["max_floor"].notna() & (df["floor"] > df["max_floor"])
df.loc[mask, "max_floor"] = df.loc[mask, "floor"]

df = df[df["floor"].notna()].reset_index(drop=True)
df["floor"] = df["floor"].astype(int)
df["max_floor"] = df["max_floor"].fillna(df["max_floor"].median()).astype(int)
df.loc[df["floor"] > df["max_floor"], "max_floor"] = df.loc[df["floor"] > df["max_floor"], "floor"]

df = df.drop_duplicates(subset=["price", "surface_m2", "rooms", "latitude", "longitude"]).reset_index(drop=True)

price_per_m2 = df["price"] / df["surface_m2"]
df = df[(price_per_m2 >= 500) & (price_per_m2 <= 20000)].reset_index(drop=True)

cols = [
    "surface_m2", "rooms", "floor", "max_floor", "price",
    "neighborhood", "year_built", "elevator",
    "construction_material",
    "latitude", "longitude", "metro_proximity", "stb_proximity",
]
df = df[cols]

df.to_csv("../storia_cleaned.csv", index=False)
print(f"Saved storia_cleaned.csv — {len(df)} rows, {len(df.columns)} cols")
print(df.dtypes)
print(df.isnull().sum())
