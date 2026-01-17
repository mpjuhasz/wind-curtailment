import asyncio
from pathlib import Path

import pandas as pd
import polars as pl
import typer
import yaml
from rich.progress import track

from src.elexon.query import fetch_unit_cashflows
from src.elexon.utils import safe_create_dir


def run_from_config(config_path: str, output_folder: str):
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    with open(Path(output_folder) / "config.yaml", "w") as f:
        yaml.safe_dump(config, f)

    from_time = config["from_time"]
    to_time = config["to_time"]

    for cashflow_type in ["bid", "offer"]:
        type_folder = Path(output_folder) / cashflow_type
        safe_create_dir(type_folder)
        
        for unit in track(config["units"], description=f"Getting indicative cashflow data ({cashflow_type}):"):
            output_path = Path(type_folder / f"{unit}.csv")
            if output_path.exists():
                continue

            dfs = asyncio.run(fetch_unit_cashflows(unit, from_time, to_time, cashflow_type))

            if dfs:
                agg = pl.concat(dfs)
                if agg is not None:
                    # NOTE: if more granular data is needed, then we need to unnest the `bidOfferPairCashflows`
                    agg.write_csv(output_path)
                    continue
                else:
                    print(f"Aggregate is None for {unit}")
            else:
                print(f"No valid days found for {unit}")

            pd.DataFrame().to_csv(output_path)


if __name__ == "__main__":
    typer.run(run_from_config)
