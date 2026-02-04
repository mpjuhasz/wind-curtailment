import asyncio
import json
from datetime import datetime
from pathlib import Path
from typing import Literal, Optional

import pandas as pd
import polars as pl
import typer
import yaml
from rich.progress import track

from src.elexon.query import get_acceptances, get_physical
from src.elexon.utils import (
    aggregate_acceptance_and_pn,
    aggregate_bm_unit_generation,
    safe_create_dir,
    smoothen_physical,
)


def downsample_aggregate_for_bm_unit(
    physical: Optional[pl.DataFrame],
    acceptances: Optional[pl.DataFrame],
    downsample_frequency: str,
    energy_unit: Literal["MWh", "GWh"],
) -> tuple[Optional[pl.DataFrame], Optional[pl.DataFrame]]:
    """Daily aggregates for the bm unit generation and curtailment"""
    if physical is None and acceptances is None:
        return None, None

    physical_smoothened = smoothen_physical(physical)
    agg_so_only = None
    if acceptances is not None:
        so_only_acceptances = acceptances.filter(pl.col("soFlag"))
        if so_only_acceptances.shape[0] != 0:
            agg_so_only = aggregate_acceptance_and_pn(
                so_only_acceptances,
                physical_smoothened,
                downsample_frequency,
                energy_unit,
            )

    agg = aggregate_acceptance_and_pn(
        acceptances, physical_smoothened, downsample_frequency, energy_unit
    )

    return agg, agg_so_only


def save_with_empty_default(df: Optional[pl.DataFrame], path: str) -> None:
    """Saves the dataframe if exists, otherwise an empty csv"""
    if df is not None:
        df.write_csv(path)
    else:
        # creating a file so that it's not re-queried next time
        pd.DataFrame().to_csv(path)


def downsample_for_config(config_path: str, output_folder: str):
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    from_time = config["from_time"]
    to_time = config["to_time"]
    downsample_frequency = config["downsample_frequency"]
    energy_unit = config["energy_unit"]

    output_folder = Path(output_folder)

    safe_create_dir(output_folder / "generation")
    safe_create_dir(output_folder / "generation/total")
    safe_create_dir(output_folder / "generation/so_only")
    safe_create_dir(output_folder / "acceptance")
    safe_create_dir(output_folder / "physical")

    for unit in track(config["units"], description="Getting generation data:"):
        output_path = Path(f"{output_folder}/generation/total/{unit}.csv")
        if output_path.exists():
            continue

        if Path(f"{output_folder}/acceptance/{unit}.csv").exists():
            acceptances = pl.read_csv(f"{output_folder}/acceptance/{unit}.csv")
        else:
            acceptances = asyncio.run(get_acceptances(unit, from_time, to_time))

        if Path(f"{output_folder}/physical/{unit}.csv").exists():
            physical = pl.read_csv(f"{output_folder}/physical/{unit}.csv")
        else:
            physical = asyncio.run(get_physical(unit, from_time, to_time))

        agg, agg_so_only = downsample_aggregate_for_bm_unit(
            physical, acceptances, downsample_frequency, energy_unit
        )

        save_with_empty_default(agg, f"{output_folder}/generation/total/{unit}.csv")
        save_with_empty_default(
            agg_so_only, f"{output_folder}/generation/so_only/{unit}.csv"
        )
        save_with_empty_default(acceptances, f"{output_folder}/acceptance/{unit}.csv")
        save_with_empty_default(physical, f"{output_folder}/physical/{unit}.csv")


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
