import polars as pl
import pytest

from src.elexon.utils import format_bid_price_table


@pytest.mark.parametrize(
    ("input_table", "expected_output"),
    [
        (
            pl.DataFrame(
                {
                    "levelFrom": [-300, 33, 300],
                    "levelTo": [-300, 33, 300],
                    "bid": [-32.89, 0.0, 0.0,],
                    "offer": [15.93, 77.67, 999.0],
                    "curtailment": [-70] * 3,
                    "extra": [0] * 3,
                }
            ),
            pl.DataFrame(
                {
                    "levelFrom": [-300, 0, 33, 300],
                    "levelTo": [0, 33, 300, None],
                    "bid": [-32.89, 0.0, 0.0, None],
                    "offer": [15.93, 77.67, 999.0, None],
                    "curtailment": [-70] * 4,
                    "extra": [0] * 4,
                }
            ),
        )
    ],
)
def test_format_bid_price_table(
    input_table: pl.DataFrame, expected_output: pl.DataFrame
):
    assert format_bid_price_table(input_table).equals(expected_output)
