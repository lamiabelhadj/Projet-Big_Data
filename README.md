# Projet-Big_Data
This project analyzes the availability and characteristics of alternative fuel stations in Chicago using a Big Data pipeline. The pipeline encompasses data cleaning, batch processing, real-time streaming, and visualization to provide insights into sustainable transportation infrastructure.
📁 Hbase-Spark
Contains the Maven-based Spark project that reads data from HBase, performs distributed computations, and outputs analysis results.

📁 map_generation_code
Contains Python scripts responsible for generating maps from coordinates using libraries like folium, including cluster maps, heatmaps, and marker maps.

📁 spark-analysis-outputs
Stores output files  produced by Spark analysis, such as fuel type summaries, state-based station data, and EV-specific statistics.

📁 visualization code
Contains scripts for generating visual charts and graphs (e.g. bar charts, pie charts) from the analysis JSON files.

📁 visuals
Holds the image files (e.g. PNG) produced by the visualization scripts, representing summary statistics and trends.

🌍 cluster_map.html
An interactive map using marker clustering to visualize station distribution.

🌍 heatmap.html
A heatmap visualization of station density across geographical coordinates.

🌍 simple_marker_map.html
A basic interactive map showing individual station markers.