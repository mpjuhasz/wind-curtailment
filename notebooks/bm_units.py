import marimo

__generated_with = "0.17.5"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import pandas as pd
    return mo, pd, pl


@app.cell
def _(pd):
    hackcollective_data = pd.read_excel("data/raw/bm_units_with_locations.xlsx")
    hackcollective_data_cleaned = hackcollective_data.rename(columns={
        "Match To Site Name": "repd_site_name",
        "lng": "repd_long",
        "lat": "repd_lat",
        "Unnamed: 0": "bm_unit",
        "BMU Name": "bm_unit_name",
        "Installed Capacity (Nominal Power) (MW)": "capacity"
    })[["bm_unit", "repd_site_name", "repd_lat", "repd_long"]]


    for _col in ["repd_lat", "repd_long"]:
        hackcollective_data_cleaned[_col] = hackcollective_data_cleaned[_col].round(5)

    print(hackcollective_data_cleaned.shape[0])
    _hackcollective_data_fixed = hackcollective_data_cleaned[
        ~hackcollective_data_cleaned["repd_site_name"].str.strip().isin(
            {"Clashindarroch 2", "East Anglia 1 (EA 1)", "Walney 1", "Walney 2"}
        )
    ]

    # Some of the hackcollective data was incorrect, so applying manual fixes here (mostly the lat-longs were slightly off compared to REPD)
    print(_hackcollective_data_fixed.shape[0])
    _hackcollective_data_fixed = pd.concat(
        [
            _hackcollective_data_fixed,
            pd.DataFrame.from_records(
                [
                    {
                        "bm_unit": "E_CLDRW-1",
                        "repd_site_name": "Clashindarroch 2",
                        "repd_lat": 57.38397,
                        "repd_long": -2.93309
                    },
                    {
                        "bm_unit": "T_EAAO-1",
                        "repd_site_name": "East Anglia 1 (EA 1)",
                        "repd_lat": 52.13696,
                        "repd_long": 2.1728
                    },
                    {
                        "bm_unit": "T_WLNYW-1",
                        "repd_site_name": "Walney 1",
                        "repd_lat": 54.03945,
                        "repd_long": -3.51579
                    },
                    {
                        "bm_unit": "T_WLNYO-2",
                        "repd_site_name": "Walney 2",
                        "repd_lat": 54.08074,
                        "repd_long": -3.60897
                    }
                ]
            )
        ]
    )

    print(_hackcollective_data_fixed.shape[0])

    _hackcollective_data_fixed.to_csv("data/interim/hackcollective_data.csv", index=False)
    hackcollective_data_fixed = _hackcollective_data_fixed
    return hackcollective_data, hackcollective_data_fixed


@app.cell
def _(pl):
    wikidata_to_repd = pl.read_csv("data/processed/wikidata_to_repd.csv")
    wikidata_with_bm_unit = pl.read_csv("data/interim/wikidata_to_repd/wikidata_name_to_bm_unit.csv")
    return wikidata_to_repd, wikidata_with_bm_unit


@app.cell
def _(wikidata_to_repd, wikidata_with_bm_unit):
    manual_data = wikidata_to_repd.join(wikidata_with_bm_unit, left_on="wikidata_site_name", right_on="name").select(
        "bm_unit", "repd_site_name", "repd_lat", "repd_long"
    ).unique(subset=["bm_unit", "repd_site_name", "repd_lat", "repd_long"]).to_pandas()

    for _col in ["repd_lat", "repd_long"]:
        manual_data[_col] = manual_data[_col].round(5)
    # NOTE: these units are national grid units, that need to be mapped into Elexon units. 
    return (manual_data,)


@app.cell
def _(pd):
    bm_units = pd.read_json("data/raw/bm_units.json")
    unit_mapping = bm_units.dropna(subset="elexonBmUnit")[["nationalGridBmUnit", "elexonBmUnit"]]
    return (unit_mapping,)


@app.cell
def _(manual_data, unit_mapping):
    mapped_manual = manual_data.merge(unit_mapping, left_on="bm_unit", right_on="nationalGridBmUnit", how="left").drop(columns=["bm_unit", "nationalGridBmUnit"]).rename(columns={"elexonBmUnit": "bm_unit"})

    # One ng unit can be mapped to multiple elexon units, so this is expected:
    mapped_manual.shape[0] - manual_data.shape[0]
    return (mapped_manual,)


@app.cell
def _(pd):
    small_manual_batch = pd.read_csv("./data/interim/manual_wind_mappings.csv")
    return (small_manual_batch,)


@app.cell
def _(hackcollective_data, mapped_manual, small_manual_batch):
    mapped_manual.shape[0], hackcollective_data.shape[0], small_manual_batch.shape[0]
    return


@app.cell
def _():
    return


@app.cell
def _(hackcollective_data_fixed, mapped_manual, pd, small_manual_batch):
    merged = pd.concat([mapped_manual, hackcollective_data_fixed, small_manual_batch]).drop_duplicates().sort_values("repd_site_name")
    merged["repd_site_name"] = merged["repd_site_name"].str.strip().str.replace(u'\xa0', " ")
    # rounding issues in the lat long prevent deduplicating on the full one
    return (merged,)


@app.cell
def _(merged):
    merged["bm_unit"].nunique()
    return


@app.cell
def _(pl):
    repd = pl.read_csv("data/processed/repd.csv").to_pandas()

    for _col in ["lat", "long"]:
        repd[_col] = repd[_col].round(5)

    repd["site_name"] = repd["site_name"].str.strip().str.replace(u'\xa0', " ")
    return (repd,)


@app.cell
def _():
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Validated below, that we have covered all of the hackcollective missing data points in the manual mapping, so these None values can be dropped.
    """)
    return


@app.cell
def _():
    # merged[merged["bm_unit"].isin(merged[merged["repd_lat"].isna()]["bm_unit"].tolist())].sort_values("bm_unit")
    return


@app.cell
def _(merged):
    merged.dropna(inplace=True)
    merged.shape[0]
    return


@app.cell
def _(merged, repd):
    final_merge = merged.merge(repd, left_on=["repd_site_name", "repd_lat", "repd_long"], right_on=["site_name", "lat", "long"], how="left")[["bm_unit", "repd_site_name", "repd_lat", "repd_long", "technology_type", "capacity", "county", "region", "development_status"]]
    return (final_merge,)


@app.cell
def _(final_merge):
    final_merge[final_merge["technology_type"].isna()].shape[0]
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    The merge is successful, all of the site names are correctly matched (this required rounding the lat-long, as well as some text processing)
    """)
    return


@app.cell
def _(final_merge):
    final_merge
    return


@app.cell
def _(final_merge):
    final_merge[final_merge["bm_unit"].apply(lambda x: "HEYM" in x)]
    return


@app.cell
def _(final_merge):
    final_merge.to_csv("data/processed/bm_unit_with_repd.csv")
    return


@app.cell
def _(final_merge):
    bm_units_to_query = final_merge[final_merge["technology_type"].str.contains("Wind")]["bm_unit"].unique().tolist()
    return (bm_units_to_query,)


@app.cell
def _(bm_units_to_query):
    for unit in bm_units_to_query:
        print(f'- {unit}')
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
