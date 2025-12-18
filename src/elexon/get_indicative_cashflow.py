import asyncio
from pathlib import Path

import pandas as pd
import polars as pl
import typer
import yaml
from rich.progress import track

from src.elexon.query import fetch_indicative_cashflows_batch


def run_from_config(config_path: str, output_folder: str):
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    with open(Path(output_folder) / "config.yaml", "w") as f:
        yaml.safe_dump(config, f)

    from_time = config["from_time"]
    to_time = config["to_time"]

    async def fetch_unit_cashflows(unit: str, from_time: str, to_time: str) -> list[pl.DataFrame]:
        """Fetch all cashflow data for a single unit using async requests."""
        tasks = [(str(_d).split(" ")[0], unit) for _d in pd.date_range(from_time, to_time)]
        results = await fetch_indicative_cashflows_batch(tasks, max_concurrent=20)

        dfs = []
        for result in results:
            if isinstance(result, Exception):
                print(f"Request failed: {result}")
                continue
            if result is not None and not result.is_empty():
                dfs.append(result.select("settlementDate", "settlementPeriod", "bmUnit", "totalCashflow"))
        return dfs

    for unit in track(config["units"], description="Getting indicative cashflow data:"):
        output_path = Path(f"{output_folder}/{unit}.csv")
        if output_path.exists():
            continue

        dfs = asyncio.run(fetch_unit_cashflows(unit, from_time, to_time))

        if dfs:
            agg = pl.concat(dfs)
            if agg is not None:
                # NOTE: if more granular data is needed, then we need to unnest the `bidOfferPairCashflows`
                agg.write_csv(f"{output_folder}/{unit}.csv")
                continue
            else:
                print(f"Aggregate is None for {unit}")
        else:
            print(f"No valid days found for {unit}")

        pd.DataFrame().to_csv(f"{output_folder}/{unit}.csv")


if __name__ == "__main__":
    typer.run(run_from_config)
