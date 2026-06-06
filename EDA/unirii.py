import pandas as pd
import matplotlib.pyplot as plt
import contextily as cx
df = pd.read_csv('Datasets/dataset.csv')

cx.set_cache_dir('./contextily_cache')

df = df.query("latitude>44.415 and latitude<44.44 and longitude > 26.08 and longitude < 26.13")

latitudes,longitudes,price = df["latitude"].values,df["longitude"].values,df["price"].values / df["surface_m2"].values
plt.figure(figsize=(15,15))


plt.scatter(longitudes, latitudes, c=price, cmap='jet',                     
    alpha=0.6,                           
    s=15,
    edgecolor='none', 
)

plt.xlabel("Longitude")
plt.ylabel("Latitude")
plt.title("Price per square meter in Unirii") 

cbar = plt.colorbar(shrink=0.7)          
cbar.set_label('Price per square meter')

plt.gca().set_aspect('equal')


cx.add_basemap(
    plt.gca(), 
    crs="EPSG:4326", 
    source=cx.providers.CartoDB.Voyager, 
    zoom=15 
)


plt.savefig('Unirii.png', bbox_inches='tight', dpi=400)
plt.show()