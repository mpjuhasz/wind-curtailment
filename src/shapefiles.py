from pathlib import Path

import geopandas as gpd
import pandas as pd
import shapely
import typer

from src.utils import bng_xy_to_lat_long

BOUNDARY = Path("data/processed/gb-transmission-map/boundary.shp")


def shp_to_coords(
    df: gpd.GeoDataFrame, transform_coords: bool
) -> list[gpd.GeoDataFrame]:
    dfs = []
    multilines = df[df["geometry"].apply(lambda x: type(x) is shapely.MultiLineString)]
    lines = df[df["geometry"].apply(lambda x: type(x) is shapely.LineString)]
    polygons = df[df["geometry"].apply(lambda x: type(x) is shapely.Polygon)]
    multi_polygons = df[df["geometry"].apply(lambda x: type(x) is shapely.MultiPolygon)]

    if not lines.empty:
        lines["latlongs"] = lines.apply(
            lambda x: {
                "coords": [
                    bng_xy_to_lat_long(c[0], c[1]) if transform_coords else c
                    for c in x["geometry"].coords
                ]
            },
            axis=1,
        )
        dfs.append(lines)

    if not polygons.empty:
        polygons["latlongs"] = polygons.apply(
            lambda x: {
                "coords": [
                    bng_xy_to_lat_long(c[0], c[1]) if transform_coords else c
                    for c in x["geometry"].exterior.coords
                ]
            },
            axis=1,
        )
        dfs.append(polygons)

    if not multi_polygons.empty:
        multi_polygons["geometry"] = multi_polygons["geometry"].apply(lambda x: x.geoms)
        multi_polygons = multi_polygons.explode("geometry")
        multi_polygons["latlongs"] = multi_polygons.apply(
            lambda x: {
                "coords": [
                    bng_xy_to_lat_long(c[0], c[1]) if transform_coords else c
                    for c in x["geometry"].exterior.coords
                ]
            },
            axis=1,
        )
        dfs.append(multi_polygons)

    if not multilines.empty:
        multilines["geometry"] = multilines["geometry"].apply(lambda x: x.geoms)
        multilines = multilines.explode("geometry")
        multilines["latlongs"] = multilines.apply(
            lambda x: {
                "coords": [
                    bng_xy_to_lat_long(c[0], c[1]) if transform_coords else c
                    for c in x["geometry"].coords
                ]
            },
            axis=1,
        )
        dfs.append(multilines)
    return dfs


def main(input_path: str, output_path: str, transform_coords: bool):
    dfs = []
    for file in [input_path]:
        df = gpd.read_file(file)
        dfs.extend(shp_to_coords(df, transform_coords))

    df = pd.concat(dfs)
    df.reset_index(inplace=True, drop=True)
    df["latlongs"].to_json(output_path)


if __name__ == "__main__":
    typer.run(main)
