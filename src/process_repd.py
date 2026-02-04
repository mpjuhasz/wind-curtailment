from pathlib import Path

import pandas as pd

from src.utils import bng_xy_to_lat_long

REPD_DATA = Path("./data/raw/repd-q2-jul-2025.csv")


def main():
    repd = pd.read_csv(REPD_DATA, encoding="ISO-8859-1")

    repd_column_map = {
        "Technology Type": "technology_type",
        "Development Status (short)": "development_status",
        "Site Name": "site_name",
        "X-coordinate": "x_coord",
        "Y-coordinate": "y_coord",
        "Installed Capacity (MWelec)": "capacity",
        "County": "county",
        "Region": "region",
        "Country": "country",
        "Development Status": "development_status",
    }

    repd = repd[repd_column_map.keys()]
    repd = repd.rename(columns=repd_column_map)

    # NOTE: keeping these for now, can be filtered later
    # repd = repd[repd["development_status"] == "Operational"]

    repd[["long", "lat"]] = repd.apply(
        lambda x: bng_xy_to_lat_long(x["x_coord"], x["y_coord"]),
        axis=1,
        result_type="expand",
    )
    repd = repd[
        [
            "site_name",
            "development_status",
            "long",
            "lat",
            "technology_type",
            "capacity",
            "county",
            "region",
            "country",
        ]
    ]
    repd.drop_duplicates(inplace=True)
    repd.reset_index(inplace=True, drop=True)

    print(repd)

    repd.to_csv("./data/processed/repd.csv")


if __name__ == "__main__":
    main()
