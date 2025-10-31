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


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Now that we've got the partitioning of the wind farms by above vs below the b6 boundary, we can start looking at the curtailment too:
    - how do curtailment figures differ on the two sides of this boundary?
    - how do curtailment figures look throughout the day?
    """)
    return


@app.cell
def _(Path):
    bm_unit = "T_SGRWO-1"
    daily_folder = Path(f"./data/processed/daily/")
    weekly_folder = Path(f"./data/processed/weekly/")
    quarter_hourly_folder = Path(f"./data/processed/15m/")
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
def _(mo):
    mo.md(r"""
    This is slightly bizarre, but Hare Hill seems to have two sites: on in Durham and another in Strathclyde. Ignoring this issue for now, as it's only one unit.
    """)
    return


@app.cell
def _(daily_folder, pl, units_with_boundary):
    below, above = [], []

    for unit in daily_folder.glob("*.csv"):
        _bm_unit = unit.stem

        if _bm_unit == "E_HRHLW-1":
            # taking the Strathclyde one as real
            _below_b6 = True
        else:
            _below_b6 = units_with_boundary.filter(pl.col("bm_unit") == _bm_unit).select("below_b6").unique().item()


        _df = pl.read_csv(daily_folder / f"{_bm_unit}.csv")

        if _below_b6:
            below.append(_df.select("time", "curtailment", "generated"))
        else:
            above.append(_df.select("time", "curtailment", "generated"))


    df_above = pl.concat(above).group_by("time").sum().sort("time")
    df_below = pl.concat(below).group_by("time").sum().sort("time")
    return df_above, df_below


@app.cell
def _(df_above, df_below, go, pl):

    fig2 = go.Figure()
    fig2.add_trace(
        go.Scatter(
            x=df_above.select("time").to_numpy().flatten(),
            y=df_above.select(pl.col("curtailment").mul(pl.col("generated").pow(-1))).to_numpy().flatten(),
            mode='lines',
            name='Above B6 Curtailment',
            line=dict(color='blue')
        )
    )
    fig2.add_trace(
        go.Scatter(
            x=df_below.select("time").to_numpy().flatten(),
            y=df_below.select(pl.col("curtailment").mul(pl.col("generated").pow(-1))).to_numpy().flatten(),
            mode='lines',
            name='Below B6 Curtailment',
            line=dict(color='green')
        )
    )
    fig2.update_layout(
        title="Total Curtailment in % of potenetial generation Above and Below B6 Boundary",
        xaxis_title="Time",
        yaxis_title="Curtailment (MW)",
        legend_title="Location"
    )
    fig2.show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    I mean, this is crystal clear. Above the B6 it's hitting 20 - 35% curtailment of the total potential generation regularly, whereas below it's pretty decent with the spikes only being around 5 - 10%.

    Let's look at a weekly aggregation which makes it more manageable:
    """)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
