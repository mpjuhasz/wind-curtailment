import marimo

__generated_with = "0.17.5"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _():
    import polars as pl
    import re
    from typing import Optional
    import pandas as pd
    from pathlib import Path
    return Optional, Path, pd, re


@app.cell
def _(pd):
    bm = pd.read_json("./data/raw/bm_units.json")
    return


@app.cell
def _(pd):
    wd = pd.read_json("./data/raw/station_to_bm_unit.json")
    wd
    return (wd,)


@app.cell
def _(re, wd):
    pattern = r"Point\((?P<lon>[-]?\d*\.?\d*) (?P<lat>[?-]?\d*\.?\d+)\).*"

    m = re.match(pattern, wd.iloc[0]["coords"])
    m.group("lat"), m.group("lon")
    return (pattern,)


@app.cell
def _(Optional, pattern, pd, re):
    def extract_lat_long(point_string: Optional[str]) -> Optional[tuple[float, float]]:
        if pd.isna(point_string):
            return None
        match = re.match(pattern, point_string)
        if match:
            lat = float(match.group("lat"))
            lon = float(match.group("lon"))
            return lat, lon
        return None
    return (extract_lat_long,)


@app.cell
def _(extract_lat_long, wd):
    wd[["lat", "long"]] = wd.apply(lambda x: extract_lat_long(x["coords"]), axis=1, result_type="expand")
    return


@app.cell
def _(wd):
    wd.rename(columns={"itemLabel": "site_name"}, inplace=True)
    wd
    return


@app.cell
def _(pd):
    repd = pd.read_csv("./data/processed/repd.csv")
    repd
    return (repd,)


@app.cell
def _(repd):
    repd.dropna(subset=["site_name"], inplace=True)
    return


@app.cell
def _(pd, re, repd, wd):
    default_sub_pattern = r"\b(A|B|C|STORAGE|ENERGY|GENERATION|PLANT|BESS|POWER|STATION|BATTERY|STORAGE|BOUNDARY|GSP|GROUP|FARM|HYDRO|ONSHORE|OFFSHORE|\d{2,3}KV|\/|ONE|TWO|\d{1,3})\b|\([^)]*\)"
    recall_sub_pattern = r"\b(A|B|C|STORAGE|ENERGY|GENERATION|PLANT|BESS|POWER|STATION|BATTERY|STORAGE|BOUNDARY|WIND|GSP|GROUP|FARM|HYDRO|ONSHORE|OFFSHORE|\d{2,3}KV|\/|ONE|TWO|\d{1,3})\b|\([^)]*\)"

    def clean_site_name(site_name: str, pattern: str) -> str:
        site_name = re.sub(r"WINDFARM", "WIND", site_name)
        site_name = re.sub(pattern, "", site_name)
        site_name = re.sub(r"\s+"," ", site_name, flags=re.IGNORECASE)
        site_name = site_name.upper().strip()
        return site_name


    def exact_match(site_1: str, site_2: str, clean_pattern: str, clean_second: bool = True) -> bool:
        a = (site_1 if clean_second else site_2).upper().strip()
        b = clean_site_name((site_2 if clean_second else site_1).upper().strip(), clean_pattern)
        return a == b


    def includes_match(site_1: str, site_2: str, clean_pattern: str, clean_second: bool = True) -> bool:
        """Checks if site_1 includes the cleaned version of site_2"""
        a = (site_1 if clean_second else site_2).upper().strip()
        b = clean_site_name((site_2 if clean_second else site_1).upper().strip(), clean_pattern)
        a_words = set(re.split(r"\b", a))
        b_words = set(re.split(r"\b", b))
        return b_words == a_words.intersection(b_words)

    def match_site(neso_site: str, df: pd.DataFrame, dataset: str, only_exact: bool = True) -> list[dict]:
        """Matches the site to a dataframe, returning all matches as a list"""
        matches = df[df["site_name"].apply(lambda x: x.lower().strip() == neso_site.lower().strip())].copy()
        if not matches.empty:
            matches.loc[:, "type"] = "exact"
            return [{"dataset": dataset, **d} for d in matches.to_dict(orient="records")]

        matches = df[df["site_name"].apply(lambda x: exact_match(x, neso_site, clean_pattern=default_sub_pattern))].copy()
        if not matches.empty:
            matches.loc[:, "type"] = "exact"
            return [{"dataset": dataset, **d} for d in matches.to_dict(orient="records")]

        matches = df[df["site_name"].apply(lambda x: exact_match(x, neso_site, clean_pattern=recall_sub_pattern))].copy()
        if not matches.empty:
            matches.loc[:, "type"] = "exact"
            return [{"dataset": dataset, **d} for d in matches.to_dict(orient="records")]

        matches = df[df["site_name"].apply(lambda x: includes_match(x, neso_site, clean_pattern=default_sub_pattern))].copy()
        if not matches.empty and not only_exact:
            matches.loc[:, "type"] = "includes"
            return [{"dataset": dataset, **d} for d in matches.to_dict(orient="records")]

        matches = df[df["site_name"].apply(lambda x: includes_match(x, neso_site, clean_pattern=recall_sub_pattern))].copy()
        if not matches.empty and not only_exact:
            matches.loc[:, "type"] = "includes"
            return [{"dataset": dataset, **d} for d in matches.to_dict(orient="records")]

        return []

    wd["matches"] = wd.apply(lambda x: match_site(x["site_name"], repd, "repd"), axis=1)
    return


@app.cell
def _(pd, wd):
    wd_matched = wd.explode("matches")
    wd_matched[["repd_site_name", "repd_lat", "repd_long", "repd_technology_type", "repd_capacity", "match_type"]] = wd_matched.apply(lambda x: pd.Series({
        "repd_site_name": x["matches"]["site_name"],
        "repd_lat": x["matches"]["lat"],
        "repd_long": x["matches"]["long"],
        "repd_technology_type": x["matches"]["technology_type"],
        "repd_capacity": x["matches"]["capacity"],
        "match_type": x["matches"]["type"],
    }) if not pd.isna(x["matches"]) else pd.Series({
        "repd_site_name": None,
        "repd_lat": None,
        "repd_long": None,
        "repd_technology_type": None,
        "repd_capacity": None,
        "match_type": None,
    }), axis=1)

    algorithmic_matches = wd_matched[["site_name", "code", "lat", "long", "repd_site_name", "repd_technology_type", "repd_lat", "repd_long", "repd_capacity", "match_type"]]
    return


@app.cell
def _():
    # algorithmic_matches[~algorithmic_matches["site_name"].isin(reviewed_matches[reviewed_matches["Accepted"].notna()]["site_name"].tolist())].to_csv("./data/processed/algorithmic_matches_wikidata_and_repd_2.csv")
    return


@app.cell
def _():
    # algorithmic_matches.to_csv("./data/processed/algorithmic_matches_wikidata_and_repd.csv")
    return


@app.cell
def _(pd):
    reviewed_matches = pd.read_csv("./data/processed/wikidata_to_repd/algorithmic_matches_wikidata_and_repd_reviewed.csv")
    reviewed_matches_2 = pd.read_csv("./data/processed/wikidata_to_repd/algorithmic_matches_wikidata_and_repd_reviewed_2.csv")
    return reviewed_matches, reviewed_matches_2


@app.cell
def _(reviewed_matches, reviewed_matches_2):
    reviewed_matches[reviewed_matches["Accepted"].str.lower() == "y"][["site_name", "repd_site_name", "repd_lat", "repd_long"]].rename(columns={"site_name": "wikidata_site_name"}).drop_duplicates().to_csv("./data/processed/wikidata_to_repd/wikidata_to_repd_part_1.csv", index=False)

    reviewed_matches_2[reviewed_matches_2["Accepted"].str.lower() == "y"][["site_name", "repd_site_name", "repd_lat", "repd_long"]].rename(columns={"site_name": "wikidata_site_name"}).drop_duplicates().to_csv("./data/processed/wikidata_to_repd/wikidata_to_repd_part_2.csv", index=False)
    return


@app.cell
def _(pd, reviewed_matches, reviewed_matches_2):
    names_reviewed = reviewed_matches[reviewed_matches["Accepted"].str.lower() == "y"]["site_name"].unique().tolist() + reviewed_matches_2[reviewed_matches_2["Accepted"].str.lower() == "y"]["site_name"].unique().tolist()
    wd_to_bm = pd.read_csv("./data/processed/wikidata_to_repd/wikidata_name_to_bm_unit.csv")
    filtered_wd_to_bm = wd_to_bm[wd_to_bm["name"].apply(lambda x: x not in names_reviewed)]

    filtered_wd_to_bm.to_csv("./data/processed/wikidata_to_repd/wikidata_name_to_bm_unit_unreviewed.csv", index=False)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Matching

    There are 2 files that contain the algorithmic matching:
    - `wikidata_to_repd/wikidata_to_repd_part_1.csv`
    - `wikidata_to_repd/wikidata_to_repd_part_2.csv`
    And there's a file that contains the manual matching carried out on the remaining wikidata entries:
    - `wikidata_to_repd/wikidata_to_repd_part_3.csv`

    These together allow to match wikidata to repd, and hence get the BM units for the REPD entries.

    Matching all the above together in a main file at the processed root (`wikidata_to_repd`).
    """)
    return


@app.cell
def _(Path, pd):
    dfs = []
    for f in Path("data/processed/wikidata_to_repd/").glob("wikidata_to_repd_part_*.csv"):
        dfs.append(
            pd.read_csv(f)
        )

    pd.concat(dfs).to_csv("./data/processed/wikidata_to_repd.csv", index=False)
    return


if __name__ == "__main__":
    app.run()
