# Data

This folder contains the raw and processed data for the project. Raw datasets are taken as is, unchanged from their sources. Processed datasets are usually outputs of a script, notebook or manual updating of the data.

Please see the `catalogue.yaml` file for detail on every raw, interim and processed dataset in the folder.

There was a bunch of manual mapping and linking of messy datasets, so I'll highlight those that might be useful:
- offshore windfarm to onshore connection map (`/processed/offshore_wind_locations.csv`): contains a list of offshore sites and their corresponding onshore cable connection, all added by hand, so the points are approximate. This is useful if one wants to group by region, or understand on which side of a boundary these sites are linked to the grid.
- REPD to BM Unit matching (`/processed/bm_unit_with_repd.csv`): a more thorough matching of the REPD dataset to BM units. Covers almost all WIND tagged BM units (check `notebooks/completeness.py` for more on this). I've used Wikidata, the Hackcollective's manually matched data, plus my own manual matching and correction of data sources to get to this mapping.
- wind sites matched to regions and counties in the UK (`/visual/units_with_county_region_and_curtailment.csv`): the REPD dataset counties and regions are not aligned with ONS, and this contains the sites matched to the ONS counties and regions
- the B6 boundary (`/processed/boundary.json`): approximate (hand-drawn) coordinates for the transmission boundary B6
