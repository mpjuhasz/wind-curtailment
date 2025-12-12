query-gen output_folder:
    uv run src/elexon/get_generation.py src/elexon/config.yaml {{output_folder}}

query-bo output_folder:
    uv run src/elexon/get_bid_offer.py src/elexon/config.yaml {{output_folder}}
