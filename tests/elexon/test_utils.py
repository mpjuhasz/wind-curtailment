import polars as pl
import pytest
from polars.testing import assert_frame_equal

from src.elexon.utils import aggregate_prices, cashflow, format_bid_price_table


@pytest.mark.parametrize(
    ("input_table", "expected_output"),
    [
        (
            pl.DataFrame(
                {
                    "levelFrom": [-300, 33, 300],
                    "levelTo": [-300, 33, 300],
                    "bid": [
                        -32.89,
                        0.0,
                        0.0,
                    ],
                    "offer": [15.93, 77.67, 999.0],
                    "curtailment": [-70] * 3,
                    "extra": [0] * 3,
                }
            ),
            pl.DataFrame(
                {
                    "levelFrom": [-300, 0, 33],
                    "levelTo": [0, 33, 300],
                    "bid": [-32.89, 0.0, 0.0],
                    "offer": [15.93, 77.67, 999.0],
                    "curtailment": [-70] * 3,
                    "extra": [0] * 3,
                }
            ),
        ),
        (
            pl.DataFrame(
                {
                    "levelFrom": [-587, 587, 0],
                    "levelTo": [-587, 587, 0],
                    "bid": [-150.0, -150.0, -150.0],
                    "offer": [2000.0, 2000.0, 2000.0],
                    "curtailment": [0] * 3,
                    "extra": [100] * 3,
                }
            ),
            pl.DataFrame(
                {
                    "levelFrom": [-587, 0],
                    "levelTo": [0, 587],
                    "bid": [-150.0, -150.0],
                    "offer": [2000.0, 2000.0],
                    "curtailment": [0] * 2,
                    "extra": [100] * 2,
                }
            ),
        ),
        (
            pl.DataFrame(
                {
                    "levelFrom": [-500, 500, 0],
                    "levelTo": [-500, 500, 0],
                    "bid": [-6.42, 100.0, -6.42],
                    "offer": [0.0, 500.0, 0.0],
                    "curtailment": [0] * 3,
                    "extra": [100] * 3,
                }
            ),
            pl.DataFrame(
                {
                    "levelFrom": [-500, 0],
                    "levelTo": [0, 500],
                    "bid": [-6.42, 100.0],
                    "offer": [0.0, 500.0],
                    "curtailment": [0] * 2,
                    "extra": [100] * 2,
                }
            ),
        ),
    ],
)
def test_format_bid_price_table(
    input_table: pl.DataFrame, expected_output: pl.DataFrame
):
    assert format_bid_price_table(input_table).equals(expected_output)


@pytest.mark.parametrize(
    ("bid_price_table", "prices"),
    [
        (
            pl.DataFrame(
                {
                    "levelFrom": [-300, 0, 33],
                    "levelTo": [0, 33, 300],
                    "bid": [-32.89, 0.0, 0.0],
                    "offer": [15.93, 77.67, 999.0],
                    "curtailment": [-70] * 3,
                    "extra": [0] * 3,
                }
            ),
            {"extra": 0, "curtailment": -70 * -32.89},
        ),
        (
            pl.DataFrame(
                {
                    "levelFrom": [-300, 0, 33],
                    "levelTo": [0, 33, 300],
                    "bid": [-32.89, 0.0, 0.0],
                    "offer": [15.93, 77.67, 999.0],
                    "curtailment": [0] * 3,
                    "extra": [100] * 3,
                }
            ),
            {"extra": 33 * 77.67 + 67 * 999.0, "curtailment": 0},
        ),
        (
            pl.DataFrame(
                {
                    "levelFrom": [-300, 0, 33],
                    "levelTo": [0, 33, 300],
                    "bid": [-32.89, 0.0, 0.0],
                    "offer": [15.93, 77.67, 999.0],
                    "curtailment": [0] * 3,
                    "extra": [0] * 3,
                }
            ),
            {"extra": 0, "curtailment": 0},
        ),
        (
            pl.DataFrame(
                {
                    "levelFrom": [-500, 0],
                    "levelTo": [0, 500],
                    "bid": [-6.42, 100.0],
                    "offer": [0.0, 500.0],
                    "curtailment": [0] * 2,
                    "extra": [100] * 2,
                }
            ),
            {"extra": 50_000, "curtailment": 0},
        ),
    ],
)
def test_aggregate_prices(bid_price_table: pl.DataFrame, prices: dict[str, float]):
    assert aggregate_prices(bid_price_table) == prices


@pytest.mark.parametrize(
    ("bo_df", "gen_df", "expected_result"),
    [
        (
            pl.DataFrame(
                {
                    "settlementDate": ["2024-12-03", "2024-12-03"],
                    "settlementPeriod": [43, 43],
                    "nationalGridBmUnit": ["VKNGW-3", "VKNGW-3"],
                    "bmUnit": ["T_VKNGW-3", "T_VKNGW-3"],
                    "timeFrom": ["2024-12-05T21:00:00Z", "2024-12-05T21:00:00Z"],
                    "timeTo": ["2024-12-05T21:30:00Z", "2024-12-05T21:30:00Z"],
                    "levelFrom": [-500, 500],
                    "levelTo": [-500, 500],
                    "bid": [-6.44, 100.0],
                    "offer": [500.0, 0.0],
                    "pairId": [-1, 1],
                }
            ),
            pl.DataFrame(
                {
                    "time": ["2024-12-05T21:00:00.000000"],
                    "physical_level": [19.5],
                    "extra": [0.0],
                    "curtailment": [-19.5],
                    "generated": [0.0],
                    "settlementDate": ["2024-12-03"],
                    "settlementPeriod": [43],
                }
            ),
            pl.DataFrame(
                {
                    "settlementDate": ["2024-12-03"],
                    "settlementPeriod": [43],
                    "calculated_cashflow_curtailment": [125.58],
                    "calculated_cashflow_extra": [0.0],
                }
            ),
        )
    ],
)
def test_calculate_cashflow(
    bo_df: pl.DataFrame, gen_df: pl.DataFrame, expected_result: pl.DataFrame
):
    cf = cashflow(bo_df, gen_df)
    assert_frame_equal(expected_result, cf)
