import os
from pathlib import Path

import typer
import yaml

from src.elexon.calculate_cashflow import run_from_config as calc_cf
from src.elexon.get_bid_offer import run_from_config as run_bo
from src.elexon.get_generation import downsample_for_config as run_gen
from src.elexon.get_indicative_cashflow import run_from_config as run_ic


def run_from_config(config_path: str, output_folder: str):
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    with open(Path(output_folder) / "config.yaml", "w") as f:
        yaml.safe_dump(config, f)

    bo_folder = output_folder + "/bid_offer"
    gen_folder = output_folder  # run_gen creates its own folders for generation and acceptances
    ic_folder = output_folder + "/indicative_cashflow"
    cashflow_folder = output_folder + "/calculated_cashflow"
    
    for f in [bo_folder, gen_folder, ic_folder, cashflow_folder]:
        p = Path(f)
        if not p.exists():
            os.mkdir(p)
    
    run_bo(config_path, bo_folder)
    run_gen(config_path, gen_folder)
    run_ic(config_path, ic_folder)
    
    
    # turning off calc cf for now to speed things up. 
    calc_cf(bo_folder, gen_folder + "/generation/total", cashflow_folder)


if __name__ == "__main__":
    typer.run(run_from_config)
