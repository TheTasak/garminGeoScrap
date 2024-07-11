import json

import geopandas
import pandas as pd
import numpy as np
import folium
from folium.plugins import HeatMap
from geojson import Feature, FeatureCollection, LineString
import matplotlib.pyplot as plt
from dags.helpers import connect_to_db


def create_geo_data_frame(data: [], reverse: bool) -> geopandas.GeoDataFrame:
    features = []
    for line_id, data_line in enumerate(data):
        data = json.loads(data_line[0])['coordinates']
        data = [(point[1], point[0]) if reverse else (point[0], point[1]) for point in data]
        features.append(Feature(geometry=LineString(data), properties={"id": line_id}))

    collection = FeatureCollection(features)

    df = geopandas.GeoDataFrame.from_features(collection['features'])
    df.crs = "epsg:4326"
    return df


def plot(df: geopandas.GeoDataFrame) -> None:
    df.plot('id')
    plt.show()


def main() -> None:
    conn = connect_to_db()
    cursor = conn.cursor()

    sql = "SELECT ST_AsGeoJson(ST_Transform(way, 4326)) FROM roads_view LIMIT 10000"
    cursor.execute(sql)
    data = cursor.fetchall()
    df = create_geo_data_frame(data, reverse=False)
    df['id'] = '0'

    sql = "SELECT ST_AsGeoJSON(line) FROM gpx_paths"
    cursor.execute(sql)
    data = cursor.fetchall()
    df2 = create_geo_data_frame(data, reverse=True)

    # df = geopandas.GeoDataFrame(pd.concat([df, df2]))

    points = df2.extract_unique_points().explode(index_parts=False)
    exploded_df = geopandas.GeoDataFrame({"geometry": points})
    exploded_df = exploded_df.get_coordinates()

    geo_map = folium.Map(location=(51.7, 19.3), zoom_start=12)
    folium.GeoJson(df).add_to(geo_map)
    HeatMap(exploded_df[['y', 'x']].values.tolist(), radius=8, min_opacity=0.5, blur=8).add_to(
        folium.FeatureGroup(name='Heat Map').add_to(geo_map))
    folium.LayerControl().add_to(geo_map)
    geo_map.show_in_browser()


if __name__ == "__main__":
    main()
