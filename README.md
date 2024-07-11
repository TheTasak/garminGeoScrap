## Garmin Geo ScrapViz

A tool for scraping data regarding .gpx routes from personal Garmin watch through GarminAPI, and then visualizing them using a frequency chart.

Dependencies:
* Airflow
* Folium
* Geopandas
* garth
* gpxpy
* Postgres DB

Create a `osm` db in Postgres and `./run-dev.sh` to start the airflow instance

I have plans to also implement machine learning to the visualization to generate interesting paths to try to ride.
