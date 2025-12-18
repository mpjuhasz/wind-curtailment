from pathlib import Path

import polars as pl
import typer
from rich.progress import track

from src.elexon.utils import cashflow


def run_from_config(bid_offer_folder: str, generation_folder: str, output_folder: str):
    paths_to_process = [
        p
        for p in Path(bid_offer_folder).glob("*.csv")
        if not Path(f"{output_folder}/{p.stem}.csv").exists()
    ]

    for p in track(
        paths_to_process,
        description="Calculating cashflow:",
    ):
        unit = p.stem
        bo = pl.read_csv(p)
        gen = pl.read_csv(Path(generation_folder) / f"{unit}.csv")

        if not bo.is_empty() and not gen.is_empty():
            out = cashflow(bo, gen)
            out.write_csv(f"{output_folder}/{unit}.csv")


if __name__ == "__main__":
    typer.run(run_from_config)
