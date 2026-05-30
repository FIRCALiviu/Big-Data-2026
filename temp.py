import pandas as pd


df = pd.read_csv("storia_apartments.csv")


print(df["number_bathrooms"].max() )