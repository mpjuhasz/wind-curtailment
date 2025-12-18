import concurrent.futures
from pathlib import Path

import pandas as pd
import polars as pl
import typer
import yaml
from rich.progress import track

from src.elexon.query import get_indicative_cashflow


def run_from_config(config_path: str, output_folder: str):
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    with open(Path(output_folder) / "config.yaml", "w") as f:
        yaml.safe_dump(config, f)

    from_time = config["from_time"]
    to_time = config["to_time"]

    for unit in track(config["units"], description="Getting indicative cashflow data:"):
        output_path = Path(f"{output_folder}/{unit}.csv")
        if output_path.exists():
            continue

        tasks = []
        for _d in pd.date_range(from_time, to_time):
            tasks.append((str(_d).split(" ")[0], unit))

        dfs = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=500) as executor:
            future_to_task = {
                executor.submit(get_indicative_cashflow, *task): task for task in tasks
            }
            for future in concurrent.futures.as_completed(future_to_task):
                result = future.result()
                if result is not None and not result.is_empty():
                    dfs.append(result.select("settlementDate", "settlementPeriod", "bmUnit", "totalCashflow"))

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
