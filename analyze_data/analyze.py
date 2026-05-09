import pandas as pd
import numpy as np
from collections import Counter

imobiliare = pd.read_csv("../imobiliare_apartments.csv", encoding="utf-8-sig")
storia = pd.read_csv("../storia_apartments.csv", encoding="utf-8-sig")

for df, name in [(imobiliare, "imobiliare_apartments.csv"), (storia, "storia_apartments.csv")]:
    print(f"\n{'='*60}")
    print(f"DATASET: {name}")
    print(f"{'='*60}")
    print(f"Shape: {df.shape}")
    print(f"\nColumns: {list(df.columns)}")
    print(f"\nDtypes:\n{df.dtypes}")
    print(f"\nNull counts:\n{df.isnull().sum()}")

    for col in df.columns:
        series = df[col].dropna().astype(str)
        unique_vals = series.unique()
        n_unique = len(unique_vals)
        print(f"\n{'-'*50}")
        print(f"COLUMN: {col}")
        print(f"  dtype: {df[col].dtype}, nulls: {df[col].isnull().sum()}, unique: {n_unique}")
        if n_unique <= 30:
            counts = Counter(series)
            for val, cnt in sorted(counts.items(), key=lambda x: -x[1]):
                print(f"    {repr(val)}: {cnt}")
        else:
            print(f"  Sample values (first 30):")
            for v in unique_vals[:30]:
                print(f"    {repr(v)}")
