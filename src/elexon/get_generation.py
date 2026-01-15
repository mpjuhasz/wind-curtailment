import json
import os
from datetime import datetime
from pathlib import Path
from typing import Literal, Optional

import pandas as pd
import polars as pl
import typer
import yaml
from rich.progress import track

from src.elexon.query import get_acceptances, get_physical
from src.elexon.utils import aggregate_acceptance_and_pn, aggregate_bm_unit_generation


def downsample_aggregate_for_bm_unit(
    bm_unit: str,
    from_time: str,
    to_time: str,
    downsample_frequency: str,
    energy_unit: Literal["MWh", "GWh"],
) -> tuple[Optional[pl.DataFrame], Optional[pl.DataFrame]]:
    """Daily aggregates for the bm unit generation and curtailment"""
    start_time = datetime.now()
    physical = get_physical(bm_unit, from_time, to_time)
    acceptances = get_acceptances(bm_unit, from_time, to_time)
    print(f"Queried data in {datetime.now() - start_time} for {bm_unit}")

    if physical is None and acceptances is None:
        print(f"No data for {bm_unit}")
        return None
    agg = aggregate_acceptance_and_pn(acceptances, physical, downsample_frequency, energy_unit)

    return agg, acceptances


def save_with_empty_default(df: Optional[pl.DataFrame], path: str) -> None:
    """Saves the dataframe if exists, otherwise an empty csv"""
    if df is not None:
        df.write_csv(path)
    else:
        # creating a file so that it's not re-queried next time
        pd.DataFrame().to_csv(path)

def safe_create_dir(path: Path) -> None:
    """Creates a dir if it doesn't already exist"""
    if not path.exists():
        os.mkdir(path)

def downsample_for_config(config_path: str, output_folder: str):
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    from_time = config["from_time"]
    to_time = config["to_time"]
    downsample_frequency = config["downsample_frequency"]
    energy_unit = config["energy_unit"]

    output_folder = Path(output_folder)

    safe_create_dir(output_folder / "generation")
    safe_create_dir(output_folder / "acceptance")

    for unit in track(config["units"], description="Getting generation data:"):
        output_path = Path(f"{output_folder}/generation/{unit}.csv")
        if output_path.exists():
            continue
        agg, acceptances = downsample_aggregate_for_bm_unit(
            unit, from_time, to_time, downsample_frequency, energy_unit
        )
        
        save_with_empty_default(agg, f"{output_folder}/generation/{unit}.csv")
        save_with_empty_default(acceptances, f"{output_folder}/acceptance/{unit}.csv")



def totals_for_bm_unit(bm_unit: str, from_time: str, to_time: str) -> dict:
    """Queries and aggregates the generation and curtailment figures for the time period."""
    start_time = datetime.now()
    physical = get_physical(bm_unit, from_time, to_time)
    acceptances = get_acceptances(bm_unit, from_time, to_time)
    print(f"Queried data in {datetime.now() - start_time} for {bm_unit}")

    return aggregate_bm_unit_generation(acceptances, physical)


def totals_for_config(config_path: str, output_path: str):
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    from_time = config["from_time"]
    to_time = config["to_time"]

    output = {"from_time": from_time, "to_time": to_time, "units": []}
    for unit in config["units"]:
        output["units"].append({unit: totals_for_bm_unit(unit, from_time, to_time)})

    with open(output_path, "w") as f:
        json.dump(output, f)


if __name__ == "__main__":
    typer.run(downsample_for_config)
