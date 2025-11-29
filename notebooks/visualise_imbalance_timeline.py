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
    return Path, mo, pd, pl, px


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
    to_plot = october_by_fuel_and_time.with_columns(
            pl.col("time").str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S")
        ).sort("time").group_by_dynamic(
            index_column="time", every="1h"
        ).agg(
            pl.col("total_curtailment").sum().alias("total_curtailment"),
            pl.col("total_pn").sum().alias("total_pn"),
            pl.col("total_extra").sum().alias("total_extra")
        ).group_by(
            "time"
        ).sum().with_columns(
        imbalance=pl.col("total_curtailment").add(pl.col("total_extra"))
    ).sort(by="time")
    return (to_plot,)


@app.cell
def _(to_plot):
    to_plot
    return


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


@app.cell
def _(mo):
    mo.md(r"""
    ## Timeline data for extra generation and curtailment by fuel type
    """)
    return


@app.cell
def _(extra_generator_metadata, pd):
    _extra_generator_units = extra_generator_metadata["bm_unit"].tolist()
    _df = pd.read_csv("data/processed/bm_unit_with_repd.csv")
    _wind_units = _df[(_df["technology_type"].str.contains("Wind")) & (_df["development_status"] == "Operational")]["bm_unit"].tolist()

    print(len(set(_wind_units)), "wind units, ", len(set(_extra_generator_units)), " extra generator units")

    units_to_consider = set(_wind_units + _extra_generator_units)

    print("Considering", len(units_to_consider), "units in total")
    return (units_to_consider,)


@app.cell
def _(Path, pl, units_to_consider):
    counter_all, counter_to_save = 0, 0

    dfs = []
    for file in Path("./data/processed/15m-october-all/").glob("*.csv"):
        if file.stem in units_to_consider:
            counter_all += 1
            _df = pl.read_csv(file)
            # upsample to 6 hourly:
            _df = _df.with_columns(
                pl.col("time").str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%S%.f")
            ).sort("time").group_by_dynamic(
                index_column="time", every="6h"
            ).agg(
                pl.col("curtailment").sum().alias("total_curtailment"),
                pl.col("physical_level").sum().alias("total_pn"),
                pl.col("extra").sum().alias("total_extra"),
                pl.col("generated").sum().alias("total_generation"),
            ).with_columns(
                bm_unit=pl.lit(file.stem)
            )
            if _df.select(pl.col("total_extra").sum().add(pl.col("total_curtailment").sum())).item() != 0:
                dfs.append(_df)
                counter_to_save += 1

    print("Found", counter_all, "units, saved", counter_to_save, "with non-zero extra or curtailment")

    return (dfs,)


@app.cell
def _(dfs, pl, to_plot):
    print(f"Covering {round(pl.concat(dfs).select("total_extra").sum().item() / to_plot["total_extra"].sum() * 100, 2)}% of total extra generation in October")
    return


@app.cell
def _(bm_units, dfs, pl):
    october_timeline = pl.concat(dfs).to_pandas()

    october_timeline_with_fuel = october_timeline.merge(bm_units[["elexonBmUnit", "fuelType"]], left_on="bm_unit", right_on="elexonBmUnit", how="left")
    return october_timeline, october_timeline_with_fuel


@app.cell
def _(october_timeline):
    october_timeline
    return


@app.cell
def _(october_timeline_with_fuel):
    october_timeline_with_fuel.rename(columns={"fuelType": "fuel_type"}, inplace=True)
    return


@app.cell
def _(october_timeline_with_fuel):
    october_timeline_with_fuel["fuel_type"].value_counts()
    return


@app.cell
def _(pd):
    manual_generator_metadata = pd.read_csv("./data/processed/extra_generators_metadata.csv")
    bm_units_with_repd = pd.read_csv("./data/processed/bm_unit_with_repd.csv")
    return bm_units_with_repd, manual_generator_metadata


@app.cell
def _(bm_units_with_repd):
    bm_units_with_repd
    return


@app.cell
def _(bm_units_with_repd, manual_generator_metadata, pd, units_to_consider):
    units_with_location_and_name = pd.concat([
        bm_units_with_repd[bm_units_with_repd["bm_unit"].isin(units_to_consider)][["bm_unit", "repd_site_name", "repd_lat", "repd_long"]].rename(
            columns={"repd_lat": "lat", "repd_long": "long", "repd_site_name": "site_name"}
        ).groupby("bm_unit").first().reset_index(),
        manual_generator_metadata[manual_generator_metadata["bm_unit"].isin(units_to_consider)][["bm_unit", "site_name", "lat", "long"]]
    ]).drop_duplicates()

    units_with_location_and_name.to_csv("./data/visual/bm_unit_to_lat_long.csv", index=False)
    return (units_with_location_and_name,)


@app.cell
def _(october_by_fuel_and_time):
    october_by_fuel_and_time
    return


@app.cell
def _(october_timeline_with_fuel, units_with_location_and_name):
    october_timeline_with_fuel.merge(units_with_location_and_name, on="bm_unit", how="left").groupby(["time", "site_name", "lat", "long"]).agg(
        {
            "total_extra": sum,
            "total_curtailment": sum,
            "total_pn": sum,
            "total_generation": sum,
            "bm_unit": list,
            "fuel_type": "first",
        }
    ).reset_index()[["time", "site_name", "bm_unit", "fuel_type", "total_extra", "total_curtailment", "total_pn", "total_generation", "lat", "long"]].to_csv("./data/visual/october_timeline_extra_and_curtailment_by_fuel.csv", index=False)
    return


@app.cell
def _():
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
