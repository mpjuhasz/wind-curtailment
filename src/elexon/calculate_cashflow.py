from pathlib import Path

import polars as pl
import typer
from rich.progress import track

from src.elexon.utils import cashflow


def run_from_config(bid_offer_folder: str, generation_folder: str, output_folder: str):
    for p in track(Path(bid_offer_folder).glob("*.csv")):
        unit = p.stem    
        bo = pl.read_csv(p)
        gen = pl.read_csv(Path(generation_folder) / f"{unit}.csv")
        out = cashflow(bo, gen)
        out.write_csv(f"{output_folder}/{unit}.csv")


if __name__ == "__main__":
    typer.run(run_from_config)
