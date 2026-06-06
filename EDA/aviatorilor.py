import pandas as pd
import matplotlib.pyplot as plt
import contextily as cx
df = pd.read_csv('Datasets/dataset.csv')

cx.set_cache_dir('./contextily_cache')

df = df.query("latitude<44.5 and latitude>44.45 and longitude > 26.06 and longitude < 26.13")

latitudes,longitudes,price = df["latitude"].values,df["longitude"].values,df["price"].values / df["surface_m2"].values

plt.figure(figsize=(15,15))



plt.scatter(longitudes,latitudes,c=price,cmap='jet',                     
    alpha=0.7,                           
    s=40,
    edgecolor='white',
)
plt.xlabel("Longitude")

plt.ylabel("Latitude")

plt.title("Price per square meter in Aviatorilor") 
cbar = plt.colorbar(shrink=0.7)          
cbar.set_label('Price per square meter')
plt.gca().set_aspect('equal')
cx.add_basemap(
    plt.gca(), 
    crs="EPSG:4326", 
    source=cx.providers.CartoDB.VoyagerNoLabels
)

plt.savefig('Aviatorilor.png',bbox_inches='tight',dpi=400)