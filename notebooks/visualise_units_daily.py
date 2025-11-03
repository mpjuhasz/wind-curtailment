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
    from typing import Optional
    return Optional, Path, go, json, mo, np, pd, pl, px, shapely


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
    return bm_unit, daily_folder, quarter_hourly_folder, weekly_folder


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
def _(Path, daily_folder, pl, units_with_boundary):
    def partition_and_aggregate_by_boundary(folder: Path):
        below, above = [], []

        for unit in folder.glob("*.csv"):
            _bm_unit = unit.stem

            if _bm_unit == "E_HRHLW-1":
                # taking the Strathclyde one as real
                _below_b6 = True
            else:
                _below_b6 = units_with_boundary.filter(pl.col("bm_unit") == _bm_unit).select("below_b6").unique().item()


            _df = pl.read_csv(folder / f"{_bm_unit}.csv")

            if _below_b6:
                below.append(_df.select("time", "curtailment", "generated"))
            else:
                above.append(_df.select("time", "curtailment", "generated"))


        df_above = pl.concat(above).group_by("time").sum().sort("time")
        df_below = pl.concat(below).group_by("time").sum().sort("time")
        return {
            "above": df_above,
            "below": df_below
        }

    daily_aggregates = partition_and_aggregate_by_boundary(daily_folder)
    return daily_aggregates, partition_and_aggregate_by_boundary


@app.cell
def _(daily_aggregates, go, pl):

    fig2 = go.Figure()
    fig2.add_trace(
        go.Scatter(
            x=daily_aggregates["above"].select("time").to_numpy().flatten(),
            y=daily_aggregates["above"].select(pl.col("curtailment").mul(pl.col("generated").pow(-1))).to_numpy().flatten(),
            mode='lines',
            name='Above B6 Curtailment',
            line=dict(color='blue')
        )
    )
    fig2.add_trace(
        go.Scatter(
            x=daily_aggregates["below"].select("time").to_numpy().flatten(),
            y=daily_aggregates["below"].select(pl.col("curtailment").mul(pl.col("generated").pow(-1))).to_numpy().flatten(),
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
def _(go, partition_and_aggregate_by_boundary, pl, weekly_folder):
    weekly_aggregates = partition_and_aggregate_by_boundary(weekly_folder)

    fig3 = go.Figure()
    fig3.add_trace(
        go.Scatter(
            x=weekly_aggregates["above"].select("time").to_numpy().flatten(),
            y=weekly_aggregates["above"].select(pl.col("curtailment").mul(pl.col("generated").pow(-1))).to_numpy().flatten(),
            mode='lines',
            name='Above B6 Curtailment',
            line=dict(color='blue')
        )
    )
    fig3.add_trace(
        go.Scatter(
            x=weekly_aggregates["below"].select("time").to_numpy().flatten(),
            y=weekly_aggregates["below"].select(pl.col("curtailment").mul(pl.col("generated").pow(-1))).to_numpy().flatten(),
            mode='lines',
            name='Below B6 Curtailment',
            line=dict(color='green')
        )
    )
    fig3.update_layout(
        title="Total Curtailment in % of potenetial generation Above and Below B6 Boundary",
        xaxis_title="Time",
        yaxis_title="Curtailment (MW)",
        legend_title="Location"
    )
    fig3.show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Now let's look at the peak generation vs the known maximum capacity of the B6 boundary (6.7GW)
    """)
    return


@app.cell
def _(go, partition_and_aggregate_by_boundary, pl, quarter_hourly_folder):
    quarter_hour_aggregates = partition_and_aggregate_by_boundary(quarter_hourly_folder)


    _aggregate = quarter_hour_aggregates
    _fig3 = go.Figure()

    # note, that our dataset is in GWh. Let's assume that generation is constant during the day, so that way we get the GW generation by dividing by 24. 

    x_ticks = _aggregate["above"].select("time").to_numpy().flatten()

    _fig3.add_trace(
        go.Scatter(
            x=x_ticks,
            y=_aggregate["above"].select(pl.col("generated").mul(4)).to_numpy().flatten(),
            mode='lines+markers',
            name='Above B6 Generated',
            line=dict(color='lightblue'),
            stackgroup='one'
        )
    )

    _fig3.add_trace(
        go.Scatter(
            x=x_ticks,
            y=_aggregate["above"].select(pl.col("curtailment").mul(4)).to_numpy().flatten(),
            mode='lines+markers',
            name='Above B6 Curtailment',
            line=dict(color='blue'),
            stackgroup='one'
        )
    )

    _fig3.add_trace(
        go.Scatter(
            x=x_ticks,
            y=[6.7] * len(x_ticks),
            mode='lines',
            name='B6 Capacity (6.7GW)',
            line=dict(color='red', dash='dash')
        )
    )
    _fig3.update_layout(
        title="Total Potential Generation Above B6 Boundary by Hour",
        xaxis_title="Hour of Day",
        yaxis_title="Generation (GW)",
        legend_title="Type"
    )

    _fig3.show()
    return (quarter_hour_aggregates,)


@app.cell
def _(mo):
    mo.md(r"""
    It seems like the curtailment isn't even kicking in at the 6.7GW level (of course there's internal demand + storage available). Let's take a look at how curtailment differes per generation level!

    Note, that this is assuming that generation is constant during the day (which it isn't).
    """)
    return


@app.cell
def _(np, pl, quarter_hour_aggregates):
    to_bin_above = quarter_hour_aggregates["above"].with_columns(
        pl.col("curtailment").mul(4).alias("curtailment_gw"),
        pl.col("generated").mul(4).alias("generated_gw")
    )

    quantiles = np.arange(0, to_bin_above.select(pl.col("generated_gw").max()).item() + 1, 0.5)

    bin_results_above = to_bin_above.with_columns(
        pl.col("generated_gw").cut(
        quantiles,
        labels=[f"{i}-{i+0.5} GW" for i in [float(i) for i in quantiles] + [float(quantiles.max() + 1)]]
    ).cast(pl.String).alias("bin")).group_by("bin").agg(
        pl.col("curtailment_gw").sum(),
        pl.col("generated_gw").sum()
    ).with_columns(
        (pl.col("curtailment_gw") / pl.col("generated_gw")).alias("curtailment_percent")
    ).sort(pl.col("bin").str.split(by="-").list.get(0).cast(pl.Float64))
    return (bin_results_above,)


@app.cell
def _(np, pl, quarter_hour_aggregates):
    to_bin_below = quarter_hour_aggregates["below"].with_columns(
        pl.col("curtailment").mul(4).alias("curtailment_gw"),
        pl.col("generated").mul(4).alias("generated_gw")
    )

    _quantiles = np.arange(0, to_bin_below.select(pl.col("generated_gw").max()).item() + 1, 0.5)

    bin_results_below = to_bin_below.with_columns(
        pl.col("generated_gw").cut(
        _quantiles,
        labels=[f"{i}-{i+0.5} GW" for i in [float(i) for i in _quantiles] + [float(_quantiles.max() + 1)]]
    ).cast(pl.String).alias("bin")).group_by("bin").agg(
        pl.col("curtailment_gw").sum(),
        pl.col("generated_gw").sum()
    ).with_columns(
        (pl.col("curtailment_gw") / pl.col("generated_gw")).alias("curtailment_percent")
    ).sort(pl.col("bin").str.split(by="-").list.get(0).cast(pl.Float64))
    return (bin_results_below,)


@app.cell
def _(bin_results_above, bin_results_below, go):
    fig6 = go.Figure()

    fig6.add_bar(
        x=bin_results_above.select("bin").to_numpy().flatten(),
        y=bin_results_above.select("curtailment_percent").to_numpy().flatten(),
        name='Above B6 Boundary',
        marker_color='blue',
        opacity=0.8,
    )

    fig6.add_bar(
        x=bin_results_below.select("bin").to_numpy().flatten(),
        y=bin_results_below.select("curtailment_percent").to_numpy().flatten(),
        name='Below B6 Boundary',
        marker_color='lightgreen',
        opacity=0.8,

    )

    fig6.update_layout(
        barmode='overlay'
    )

    fig6.show()
    return


@app.cell
def _():
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Time distribution
    """)
    return


@app.cell
def _(Path, pl, quarter_hourly_folder, units_with_boundary):
    def partition_and_aggregate_by_boundary_and_hour(folder: Path):
        below, above = [], []

        for unit in folder.glob("*.csv"):
            _bm_unit = unit.stem

            if _bm_unit == "E_HRHLW-1":
                # taking the Strathclyde one as real
                _below_b6 = True
            else:
                _below_b6 = units_with_boundary.filter(pl.col("bm_unit") == _bm_unit).select("below_b6").unique().item()


            _df = pl.read_csv(folder / f"{_bm_unit}.csv")

            if _below_b6:
                below.append(_df.with_columns(pl.col("time").str.to_datetime(format="%Y-%m-%dT%H:%M:%S%.f").dt.hour().alias("hour")).select("hour", "curtailment", "generated"))
            else:
                above.append(_df.with_columns(pl.col("time").str.to_datetime(format="%Y-%m-%dT%H:%M:%S%.f").dt.hour().alias("hour")).select("hour", "curtailment", "generated"))


        df_above = pl.concat(above).group_by("hour").sum().sort("hour")
        df_below = pl.concat(below).group_by("hour").sum().sort("hour")
        return {
            "above": df_above,
            "below": df_below
        }

    hourly_data = partition_and_aggregate_by_boundary_and_hour(quarter_hourly_folder)
    return (hourly_data,)


@app.cell
def _(go, hourly_data, pl):
    fig4 = go.Figure()

    fig4.add_trace(
        go.Scatter(
            x=hourly_data["above"].select("hour").to_numpy().flatten(),
            y=hourly_data["above"].select(pl.col("curtailment").mul(pl.col("generated").pow(-1))).to_numpy().flatten(),
            mode='lines+markers',
            name='Above B6 Curtailment',
            line=dict(color='blue')
        )
    )

    fig4.add_trace(
        go.Scatter(
            x=hourly_data["below"].select("hour").to_numpy().flatten(),
            y=hourly_data["below"].select(pl.col("curtailment").mul(pl.col("generated").pow(-1))).to_numpy().flatten(),
            mode='lines+markers',
            name='Below B6 Curtailment',
            line=dict(color='green')
        )
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Curtailment vs location

    Checking whether there's a link between the location and the curtailment percentage.
    """)
    return


@app.cell
def _(daily_folder, pl, units_with_boundary):
    def get_curtailment_for_unit(bm_unit: str) -> float:
        _file_path = daily_folder / f"{bm_unit}.csv"
        if _file_path.exists() is False:
            print(f"No data for {bm_unit}")
            return None
        _df = pl.read_csv(_file_path)
        total_generated = _df.select(pl.col("generated").sum()).item()
        total_curtailment = _df.select(pl.col("curtailment").sum()).item()
        if total_generated == 0:
            return 0.0
        return total_curtailment / total_generated

    units_with_curtailment = units_with_boundary.filter(pl.col("technology_type").str.contains("Wind")).with_columns(
        curtailment_ratio=pl.col("bm_unit").map_elements(get_curtailment_for_unit, return_dtype=pl.Float64)
    )
    return (units_with_curtailment,)


@app.cell
def _(px, units_with_curtailment):
    fig5 = px.scatter(
        units_with_curtailment.to_pandas(),
        x='repd_lat',
        y='curtailment_ratio',
        size='capacity',
        color='below_b6',
        labels={'repd_lat': 'Latitude', 'curtailment_ratio': 'Curtailment Ratio', 'below_b6': 'Below B6 Boundary'},
        title='Curtailment Ratio vs Latitude for Wind Units',
        hover_data=['repd_site_name', 'capacity']
    )

    fig5.show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Location granurality

    Looking at country - region - county level aggregation
    """)
    return


@app.cell
def _(daily_folder, pl, units_with_boundary):
    def get_gen_and_curtailment_for_unit(bm_unit: str) -> float:
        _file_path = daily_folder / f"{bm_unit}.csv"
        if _file_path.exists() is False:
            print(f"No data for {bm_unit}")
            return 0.0, 0.0
        _df = pl.read_csv(_file_path)
        total_generated = _df.select(pl.col("generated").sum()).item()
        total_curtailment = _df.select(pl.col("curtailment").sum()).item()
        if total_generated == 0:
            return 0.0, 0.0
        return total_generated, total_curtailment

    units_with_gen_and_curtailment = units_with_boundary.filter(pl.col("technology_type").str.contains("Wind")).with_columns(
        pl.col("bm_unit").map_elements(lambda x: get_gen_and_curtailment_for_unit(x)[0], return_dtype=pl.Float64).round(decimals=5).alias("total_generated"),
        pl.col("bm_unit").map_elements(lambda x: get_gen_and_curtailment_for_unit(x)[1], return_dtype=pl.Float64).round(decimals=5).alias("total_curtailment")
    )
    return (units_with_gen_and_curtailment,)


@app.cell
def _():
    return


@app.cell
def _(Path, json):
    counties = json.load(Path("./data/raw/uk_counties_buc.geojson").open())
    return (counties,)


@app.cell
def _(mo):
    mo.md(r"""
    After a few attempts at matching the ONS regions and county data with REPD, I've deemed it easier to use the polygons to map to the locations rather than attempts to convert the names at various levels and times to the ONS database... This means that offshore will have to be tagged by hand (but that would have been the case anyways, given it's its own category.)
    """)
    return


@app.cell
def _(Optional, counties, shapely):
    county_name_to_polygon = {}

    for f in counties['features']:
        county_name = f["properties"]["CTYUA24NM"]

        if f["geometry"]["type"] == "Polygon":
            p = shapely.geometry.Polygon(f["geometry"]["coordinates"][0])
        else:
            p = shapely.geometry.MultiPolygon([shapely.Polygon(_i[0]) for _i in f["geometry"]["coordinates"]])

        county_name_to_polygon[county_name] = p



    def _match_long_lat_to_county(long: float, lat: float, county_polygons: dict) -> Optional[str]:
        if long is None or lat is None:
            return None
        point = shapely.Point(long, lat)
        for county_name, polygon in county_polygons.items():
            if polygon.contains(point):
                return county_name
        return None

    def point_in_which_county(long: float, lat: float, connector_long: float, connector_lat: float, county_polygons: dict) -> Optional[str]:
        county_name = _match_long_lat_to_county(long, lat, county_polygons)
        if county_name is None:
            # try the connector location
            county_name = _match_long_lat_to_county(connector_long, connector_lat, county_polygons)

        return county_name or "Unknown"
    return county_name_to_polygon, point_in_which_county


@app.cell
def _(pl):
    # manually mapped data:

    offshore_wind_connectors = pl.read_csv("./data/processed/offshore_wind_locations.csv")



    return (offshore_wind_connectors,)


@app.cell
def _(
    county_name_to_polygon,
    offshore_wind_connectors,
    pl,
    point_in_which_county,
    units_with_gen_and_curtailment,
):
    units_with_uk_county = units_with_gen_and_curtailment.join(offshore_wind_connectors, on=["repd_site_name", "repd_long", "repd_lat"], how="left").with_columns(
        uk_county=pl.struct('repd_long', 'repd_lat', 'connection_long', 'connection_lat').map_elements(lambda x: point_in_which_county(x['repd_long'], x['repd_lat'], x['connection_long'], x['connection_lat'], county_name_to_polygon), return_dtype=pl.String).alias("uk_county")
    )
    return (units_with_uk_county,)


@app.cell
def _():
    # units_with_uk_county.filter(pl.col("uk_county") == "Offshore").unique(subset=["repd_site_name", "repd_lat", "repd_long"]).write_csv("./data/interim/offshore_wind_locations.csv")
    return


@app.cell
def _(pl, units_with_uk_county):
    county_to_curtailment = units_with_uk_county.group_by("uk_county").agg(
        pl.col("total_generated").sum().round(decimals=5),
        pl.col("total_curtailment").sum().round(decimals=5)
    ).with_columns(
        (pl.col("total_curtailment") / pl.col("total_generated")).alias("curtailment_ratio")
    ).sort(pl.col("curtailment_ratio"), descending=True).select("uk_county", "curtailment_ratio")
    return (county_to_curtailment,)


@app.cell
def _(counties, county_to_curtailment, pl):
    for _f in counties["features"]:
        _county_name = _f["properties"]["CTYUA24NM"]
        if _county_name in county_to_curtailment.select("uk_county").to_numpy().flatten().tolist():
            curtailment_ratio = county_to_curtailment.filter(pl.col("uk_county") == _county_name).select("curtailment_ratio").item()
            _f["curtailment_ratio"] = curtailment_ratio
        else:
            _f["curtailment_ratio"] = None
    return


@app.cell
def _(pl, units_with_uk_county):
    units_with_uk_county.filter(pl.col("repd_site_name") == "Seagreen")
    return


@app.cell
def _(units_with_uk_county):
    units_with_uk_county.select("bm_unit", "repd_site_name", "repd_lat", "repd_long", "connection_long", "connection_lat", "uk_county", "below_b6", "technology_type", "capacity", "total_generated", "total_curtailment").write_csv("./data/visual/units_with_county_and_curtailment.csv")
    return


@app.cell
def _(counties, go):

    fig7 = go.Figure()

    z_values = [
        i["curtailment_ratio"] if i["curtailment_ratio"] is not None else -0.001
        for i in counties['features']
    ]

    actual_min = min([v for v in z_values if v > 0])


    fig7.add_trace(
        go.Choropleth(
            geojson=counties,
            featureidkey="id",
            locations=[f["id"] for f in counties['features']],
            z=z_values,
            # z_min=actual_min,
            colorscale="YlOrRd",
            showscale=False,
            marker_line_color='gray',
            marker_line_width=1,
            hovertemplate="%{customdata[0]}<br>Curtailment Ratio: %{z:.2%}",
            customdata=[[i["properties"]["CTYUA24NM"]] for i in counties['features']],
            marker=dict(
                line=dict(width=0.5, color='darkgray'),
            )
        )
    )




    fig7.update_geos(
        fitbounds="locations", 
        visible=False,
        projection_type="mercator"
    )

    fig7.update_layout(
        margin=dict(l=0, r=0, t=50, b=0),
        title_text="Renewables in the UK",
        geo=dict(
            showframe=False,
            showcoastlines=False,
            projection_type="mercator"
        )
    )

    fig7.show()
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
