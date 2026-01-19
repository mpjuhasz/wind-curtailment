import asyncio
from pathlib import Path

import polars as pl
import typer

from src.elexon.query import fetch_imbalance_settlement


def main(from_time: str, to_time: str, output_folder: str):
    df: pl.DataFrame = asyncio.run(fetch_imbalance_settlement(from_time=from_time, to_time=to_time))
    
    df.write_csv(Path(output_folder) / "imbalance_settlement.csv")
    
    
if __name__ == "__main__":
    typer.run(main)