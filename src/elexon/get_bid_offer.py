from pathlib import Path

import pandas as pd
import typer
import yaml
from rich.progress import track

from src.elexon.query import get_bid_offer


def run_from_config(config_path: str, output_folder: str):
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    from_time = config["from_time"]
    to_time = config["to_time"]

    for unit in track(config["units"], description="Getting bid-offer data:"):
        output_path = Path(f"{output_folder}/{unit}.csv")
        if output_path.exists():
            continue

        agg = get_bid_offer(unit, from_time, to_time)

        if agg is not None:
            agg.write_csv(f"{output_folder}/{unit}.csv")
        else:
            # creating a file so that it's not re-queried next time
            pd.DataFrame().to_csv(f"{output_folder}/{unit}.csv")


if __name__ == "__main__":
    typer.run(run_from_config)
