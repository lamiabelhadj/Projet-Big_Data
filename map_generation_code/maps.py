import json
import folium
from folium.plugins import MarkerCluster, HeatMap

# Load coordinates
with open("coordinates.json", "r") as f:
    data = json.load(f)

stations = data["stations"]

# If the dataset is large, limit zoom to center area
avg_lat = sum(s["latitude"] for s in stations) / len(stations)
avg_lon = sum(s["longitude"] for s in stations) / len(stations)

# 1. Simple Marker Map
simple_map = folium.Map(location=[avg_lat, avg_lon], zoom_start=8)
for station in stations:
    folium.Marker(
        location=[station["latitude"], station["longitude"]],
        popup=f"ID: {station['id']}"
    ).add_to(simple_map)
simple_map.save("simple_marker_map.html")


# 2. Cluster Map
cluster_map = folium.Map(location=[avg_lat, avg_lon], zoom_start=8)
marker_cluster = MarkerCluster().add_to(cluster_map)

for station in stations:
    folium.Marker(
        location=[station["latitude"], station["longitude"]],
        popup=f"ID: {station['id']}"
    ).add_to(marker_cluster)
cluster_map.save("cluster_map.html")


# 3. Heatmap
heatmap_map = folium.Map(location=[avg_lat, avg_lon], zoom_start=8)
heat_data = [[s["latitude"], s["longitude"]] for s in stations]
HeatMap(heat_data).add_to(heatmap_map)
heatmap_map.save("heatmap.html")

print("Maps generated: simple_marker_map.html, cluster_map.html, heatmap.html")
