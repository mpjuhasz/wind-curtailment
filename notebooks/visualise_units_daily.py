import marimo

__generated_with = "0.17.5"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import json
    from pathlib import Path
    import numpy as np
    import pandas as pd
    import plotly.express as px
    import plotly.graph_objects as go
    import polars as pl
    import pyproj
    import shapely
    return Path, go, json, mo, pd, pl, px, shapely


@app.cell
def _(Path, json):
    uk = json.load(Path("./data/raw/uk_buc.geojson").open())
    list(range(len(uk['features'])))
    return (uk,)


@app.cell
def _(pd):
    boundaries = pd.read_json("./data/processed/boundary.json").T
    return (boundaries,)


@app.cell
def _(boundaries):
    boundaries
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    For each point I need to be able to say whether it's above or below this boundary. I'm going to use the fact, that the boundary is east-west oriented:
    """)
    return


@app.cell
def _(boundaries, shapely):
    b6 = shapely.LineString(boundaries["coords"].item())


    def point_below_boundary(long: float, lat: float, boundary: shapely.LineString) -> bool:
        point = shapely.Point(long, lat)
        north_south_line_across_point = shapely.LineString([
            (point.x, -90),
            (point.x, 90)
        ])
        intersection = boundary.intersection(north_south_line_across_point)
        if isinstance(intersection, shapely.geometry.Point):
            return point.y < intersection.y
        elif isinstance(intersection, shapely.geometry.MultiPoint):
            highest_intersection = max(intersection.geoms, key=lambda p: p.y)
            lowest_intersection = min(intersection.geoms, key=lambda p: p.y)
            if point.y < lowest_intersection.y:
                return True
            elif point.y > highest_intersection.y:
                return False
            else:
                raise ValueError("Point is between multiple intersections; cannot determine above/below.")
        else:
            raise ValueError("Unexpected intersection type; cannot determine above/below.")


    example_point = shapely.Point(-0.42102, 53.79827)

    point_below_boundary(example_point.x, example_point.y, b6)
    return b6, point_below_boundary


@app.cell
def _(pl):
    bm_units = pl.read_csv("data/processed/bm_unit_with_repd.csv")
    return (bm_units,)


@app.cell
def _(b6, bm_units, pl, point_below_boundary):
    units_with_boundary = bm_units.with_columns(
        below_b6 = pl.struct('repd_long', 'repd_lat').map_elements(lambda x: point_below_boundary(x['repd_long'], x['repd_lat'], b6))
    )
    return (units_with_boundary,)


@app.cell
def _(boundaries, go, pd, uk, units_with_boundary):
    fig = go.Figure()
    for feature in uk['features']:
        fig.add_trace(
            go.Scattergeo(
                lon=[],
                lat=[],
                mode='lines',
                line=dict(width=1, color='gray'),
                showlegend=False,
                hoverinfo='skip'
            )
        )

    fig.add_trace(
        go.Choropleth(
            geojson=uk,
            z=[1] * (len(uk['features']) + 1),  # Uniform values for white fill
            locations=list(range(len(uk['features']) + 1)),
            colorscale=[[0, 'white'], [1, 'white']],
            showscale=False,
            marker_line_color='gray',
            marker_line_width=1,
            hoverinfo='skip'
        )
    )



    for name, long, lat, capacity, below_b6 in units_with_boundary.select(['repd_site_name', 'repd_long', 'repd_lat', 'capacity', 'below_b6']).to_numpy().tolist():
        fig.add_trace(
            go.Scattergeo(
                lon=[long],
                lat=[lat],
                opacity=0.5,
                hovertemplate=f"{name} ({capacity}MW)<br>{long}, {lat}",
                mode='markers',
                marker=dict(
                    size=capacity ** 0.5 / 2 if not pd.isna(capacity) else 2,
                    color='blue' if not below_b6 else 'green',
                    line=dict(width=0.5, color='darkblue' if not below_b6 else 'darkgreen'),
                    sizemode='area'
                )

            )
        )

    for boundary in boundaries["coords"].tolist():
        print(boundary)
        fig.add_trace(
            go.Scattergeo(
                lon=[coord[0] for coord in boundary],
                lat=[coord[1] for coord in boundary],
                mode='lines',
                line=dict(width=2, color='red'),
                showlegend=False,
                hoverinfo='skip'
            )
        )


    fig.update_geos(
        fitbounds="geojson", 
        visible=False,
        projection_type="mercator"
    )

    fig.update_layout(
        margin=dict(l=0, r=0, t=50, b=0),
        title_text="Renewables in the UK",
        geo=dict(
            showframe=False,
            showcoastlines=False,
            projection_type="mercator"
        )
    )

    fig.show()
    return


@app.cell
def _(Path):
    bm_unit = "T_SGRWO-1"
    daily_folder = Path(f"./data/processed/daily/")
    return bm_unit, daily_folder


@app.cell
def _(bm_unit, daily_folder, pl):
    df = pl.read_csv(daily_folder / f"{bm_unit}.csv")
    return (df,)


@app.cell
def _(df):
    df
    return


@app.cell
def _(bm_unit, df, px):
    # create a stacked area plot with generated + curtailment:
    _fig = px.area(
        df.to_pandas(),
        x='time',
        y=['curtailment','generated'],
        labels={'value': 'MW', 'datetime': 'DateTime', 'variable': 'Type'},
        title=f"Daily Generation and Curtailment for {bm_unit}",

    )

    _fig.show()
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
