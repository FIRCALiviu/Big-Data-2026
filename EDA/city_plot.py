import pandas as pd
import matplotlib.pyplot as plt
import contextily as cx
df = pd.read_csv('Datasets/dataset.csv')
df = df.iloc[::1]
cx.set_cache_dir('./contextily_cache')

# quantiles = [(df["price"] / df["surface_m2"]).quantile(x) for x in (0.1,0.5,0.80,0.9,0.95,0.975,0.99)]
# print(quantiles,(df["price"] / df["surface_m2"]).max())
latitudes,longitudes,price = df["latitude"].values,df["longitude"].values,df["price"].values / df["surface_m2"].values

plt.figure(figsize=(15,15))


plt.scatter(longitudes,latitudes,c=price,cmap='jet',                     
    alpha=0.7,                           
    s=40,
    edgecolor='white',
)
plt.xlabel("Longitude")

plt.ylabel("Latitude")

plt.title("Price per square meter in each zone") 
cbar = plt.colorbar(shrink=0.7)          # shrink makes the colorbar look cleaner
cbar.set_label('Price per square meter')
plt.gca().set_aspect('equal')
cx.add_basemap(
    plt.gca(), 
    crs="EPSG:4326", 
    source=cx.providers.CartoDB.VoyagerNoLabels
)

plt.savefig('City plot.png',bbox_inches='tight',dpi=400)