import marimo

__generated_with = "0.17.5"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import pandas as pd
    return mo, pd


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Completeness

    Now that REPD is joint with BM Units + Wikidata, we need to look at the completeness of this data. Wikidata was used as an intermediary, and it needs to be checked, how many of the wind farms are entered into Wikidata.
    """)
    return


@app.cell
def _(pd):
    repd = pd.read_csv("./data/processed/repd.csv")
    bm_units = pd.read_json("./data/raw/bm_units.json")

    mapping = pd.read_csv("./data/processed/bm_unit_with_repd.csv")

    wikidata_to_repd = pd.read_csv("./data/processed/wikidata_to_repd.csv")
    return bm_units, mapping, repd


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Elexon units

    One of our references is the Elexon data with all the BM units. Now the issue here, is that only a portion of the BM units are categorised by fuel type. Nonetheless, it's a useful sense-check for these 213 unique units.
    """)
    return


@app.cell
def _(bm_units):
    wind_units = bm_units[bm_units["fuelType"] == "WIND"]["elexonBmUnit"].unique().tolist()
    wind_units.remove(None)
    len(wind_units)
    return (wind_units,)


@app.cell
def _(mapping, wind_units):
    unmpapped_units = set(wind_units).difference(set(mapping["bm_unit"].tolist()))
    return (unmpapped_units,)


@app.cell
def _(bm_units, unmpapped_units):
    bm_units[bm_units["elexonBmUnit"].isin(unmpapped_units)]
    return


@app.cell
def _(pd):
    manual_mapping = [
        {
            "repd_site_name": "Pines Burn Wind Farm",
            "repd_long": -2.73027,
            "repd_lat": 55.35374,
            "bm_unit": "E_PIBUW-1"
        },
        {
            "repd_site_name": "Solwaybank Wind Farm",
            "repd_long": -3.10775,
            "repd_lat": 55.10017,
            "bm_unit": "E_SWBKW-1"
        },
        {
            "repd_site_name": "Limekiln Wind Farm",
            "repd_long": -3.74205,
            "repd_lat": 58.52848,
            "bm_unit": "T_LIMKW-1"
        },
        {
            "repd_site_name": "Limekiln Wind Farm (Extension)",
            "repd_long": -3.71381,
            "repd_lat": 58.52844,
            "bm_unit": "T_LIMKW-1"
        },
        {
            "repd_site_name": "Hagshaw Hill Wind Farm Extension",
            "repd_long": -3.92032,
            "repd_lat": 55.56660,
            "bm_unit": "T_HAHAW-1"
        },
        {
            "repd_site_name": "Hagshaw Hill Wind Farm",
            "repd_long": -3.92008,
            "repd_lat": 55.55528,
            "bm_unit": "T_HAHAW-1"
        },
        {
            "repd_site_name": "Hagshaw Hill Wind Farm",
            "repd_long": -3.92033,
            "repd_lat": 55.55537,
            "bm_unit": "T_HAHAW-1"
        }  
    ]

    pd.DataFrame(manual_mapping).to_csv("./data/interim/manual_wind_mappings.csv", index=False)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### REPD wind farms
    """)
    return


@app.cell
def _(mapping):
    wind_mapping = mapping[(mapping["technology_type"].str.contains("Wind")) & (mapping["development_status"].str.contains("Operational"))]
    return (wind_mapping,)


@app.cell
def _(repd):
    repd[repd["technology_type"].str.contains("Wind")]["development_status"].value_counts()
    return


@app.cell
def _(repd):
    repd_operational_windfarms = repd[(repd["technology_type"].str.contains("Wind")) & (repd["development_status"] == "Operational")]
    return (repd_operational_windfarms,)


@app.cell
def _(repd_operational_windfarms):
    for col in ["lat", "long"]:
        repd_operational_windfarms[col] = repd_operational_windfarms[col].apply(lambda x: round(x, 5))

    return


@app.cell
def _(repd_operational_windfarms, wind_mapping):
    print(f"All operational wind farms total capacity: {repd_operational_windfarms['capacity'].apply(float).sum()}\n\nMapped wind farms total capacity: {round(wind_mapping.drop_duplicates('repd_site_name')['capacity'].sum(), 3)}")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Okay, so we've got around 2/3 of the operational wind generation capacity mapped to BM unit from the REPD dataset. Not bad, but let's see what more can be done:
    """)
    return


@app.cell
def _(repd_operational_windfarms, wind_mapping):
    repd_operational_windfarms["site_name"] = repd_operational_windfarms["site_name"].str.strip().str.replace(u'\xa0', u' ')
    wind_mapping["repd_site_name"] = wind_mapping["repd_site_name"].str.strip().str.replace(u'\xa0', u' ')

    outer_merge = repd_operational_windfarms.merge(wind_mapping, left_on=["site_name", "long", "lat"], right_on=["repd_site_name", "repd_long", "repd_lat"], how="outer", suffixes=["", "_mapping"])

    unmapped = outer_merge[outer_merge["bm_unit"].isnull()]
    unmapped["capacity"] = unmapped["capacity"].apply(float)
    return (unmapped,)


@app.cell
def _(unmapped):
    unmapped.sort_values(by="capacity", ascending=False)
    return


@app.cell
def _(wind_mapping):
    wind_mapping[wind_mapping["repd_site_name"].str.contains("Whitelee")]
    return


@app.cell
def _(mo):
    mo.md(r"""
    Looking through some of the rest, and trying to match with the BM database, I _think_ what's happening is that the rest of these are not linked to the balancing mechanism.
    """)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
