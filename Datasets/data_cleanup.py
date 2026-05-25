import pandas as pd

df = pd.read_csv('full_clean_input.csv')

# we remove outliar values
print((x:=df["price"]/df['surface_m2']).quantile(0.99),x.quantile(0.01),df["rooms"].quantile(0.99),df["surface_m2"].quantile(0.01),df['surface_m2'].quantile(0.99),df["stb_proximity"].quantile(0.99))


# based on the output, we decide to remove values that beat 99% of rows.
dataset = df.query("price/surface_m2 < 6000 "
"and rooms <= 5 and surface_m2 < 200 "
"and price/surface_m2 > 1100" 
"and surface_m2 > 25"
"and stb_proximity < 2")

dataset.to_csv("Datasets/dataset.csv",index=False)
