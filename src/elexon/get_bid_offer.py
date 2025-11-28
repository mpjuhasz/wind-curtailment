from src.elexon.query import get_bid_offer

from rich.progress import track

from pathlib import Path
import yaml
import typer

def run_from_config(config_path: str, output_folder: str):
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    
    from_time = config["from_time"]
    to_time = config["to_time"]

    for unit in track(config["units"]):
        output_path = Path(f"{output_folder}/{unit}.csv")
        if output_path.exists():
            continue

        agg = get_bid_offer(unit, from_time, to_time)

        if agg is not None:
            agg.write_csv(f"{output_folder}/{unit}.csv")

        
if __name__ == "__main__":
    typer.run(run_from_config)