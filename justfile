default_config := "src/elexon/config.yaml"

query-gen output_folder config=default_config:
    uv run src/elexon/get_generation.py src/elexon/config.yaml {{output_folder}}

query-bo output_folder:
    uv run src/elexon/get_bid_offer.py src/elexon/config.yaml {{output_folder}}

orchestrate output_folder config=default_config:
    uv run src/elexon/orchestrate.py {{config}} {{output_folder}}