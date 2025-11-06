import marimo

__generated_with = "0.17.5"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import pandas as pd
    from pathlib import Path
    import plotly.express as px
    return mo, pd, pl, px


@app.cell
def _(mo):
    mo.md(r"""
    ## Timeline plots for the BM

    Below looking at the 15m October data for all the units from `./data/processed/15m-october-all/`. This data is aggregated in duckdb, and exports are created. For further info on the exports, check out `/src/db_scripts/`.
    """)
    return


@app.cell
def _(pl):
    october_by_fuel_and_time = pl.read_csv("./data/processed/analysis/october_by_fuel_and_time.csv")
    return (october_by_fuel_and_time,)


@app.cell
def _(october_by_fuel_and_time, pl):
    to_plot = october_by_fuel_and_time.group_by("time").agg(
        pl.col("total_curtailment").sum().alias("total_curtailment"),
        pl.col("total_pn").sum().alias("total_pn"),
        pl.col("total_extra").sum().alias("total_extra")
    ).with_columns(
        imbalance=pl.col("total_curtailment").add(pl.col("total_extra"))
    ).sort(by="time")
    return (to_plot,)


@app.cell
def _(to_plot):
    to_plot.write_csv("./data/visual/october_imbalance_over_time.csv")
    return


@app.cell
def _(px, to_plot):
    # plotly plot to show imbalance, total_curtailment and total_extra over time:


    fig = px.line(
        to_plot.to_pandas(),
        x="time",
        y=["imbalance", "total_curtailment", "total_extra"],
        # labels={"value": "GWh", "time": "Time", "variable": "Metric"},
        title="Imbalance, Total Curtailment and Total Extra over Time in October"
    )

    fig.update_layout(
        xaxis_title="Time",
        yaxis_title="GWh",
        legend_title="Metric",
        template="plotly_white"
    )
    fig.show()
    return


@app.cell
def _(to_plot):
    to_plot
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Completing metadata for the top extra generators

    First matching these elexonBmUnits to the Wikidata sites to get the locations. For the missing few, adding it manually. This can be found in `./data/processed/extra_generators_metadata.csv`
    """)
    return


@app.cell
def _(pl):
    top_extra_generators = pl.read_csv("./data/processed/analysis/extra_generators.csv")
    return (top_extra_generators,)


@app.cell
def _(top_extra_generators):
    top_extra_generators
    return


@app.cell
def _(pd, pl):
    wikidata = pl.read_json("./data/raw/station_to_bm_unit.json").to_pandas()
    bm_units = pd.read_json("./data/raw/bm_units.json")
    return bm_units, wikidata


@app.cell
def _(bm_units, top_extra_generators, wikidata):
    extra_generator_metadata = top_extra_generators.to_pandas().merge(
        bm_units, left_on="bm_unit", right_on="elexonBmUnit", how="left"
    ).merge(
        wikidata,
        left_on="nationalGridBmUnit",
        right_on="code",
        how="left"
    )[["extra", "fuel_type", "bm_unit", "coords", "itemLabel"]]
    return (extra_generator_metadata,)


@app.cell
def _(extra_generator_metadata):
    extra_generator_metadata
    return


@app.cell
def _(extra_generator_metadata, pd):
    def process_point_string(point: str) -> tuple[float, float]:
        point = point.replace("Point(", "").replace(")", "")
        long_str, lat_str = point.split(" ")
        return float(long_str), float(lat_str)

    extra_generator_metadata[["long", "lat"]] = extra_generator_metadata.apply(
        lambda x: process_point_string(x["coords"]) if not pd.isna(x["coords"]) else (None, None), axis=1, result_type="expand"
    )
    return


@app.cell
def _(extra_generator_metadata):
    extra_generator_metadata.rename(columns={"itemLabel": "site_name"}, inplace=True)

    extra_generator_metadata[["bm_unit", "site_name", "long", "lat"]].to_csv("./data/interim/extra_generator_metadata.csv", index=False)
    return


if __name__ == "__main__":
    app.run()
