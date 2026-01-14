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
                    "levelFrom": [-300, -100, 33, 300],
                    "levelTo": [-300, -100, 33, 300],
                    "bid": [
                        -32.89,
                        -15.5,
                        0.0,
                        0.0,
                    ],
                    "offer": [15.93, 10.2, 77.67, 999.0],
                    "curtailment": [-70] * 4,
                    "extra": [0] * 4,
                    "pairId": [-2, -1, 1, 2],
                }
            ),
            pl.DataFrame(
                {
                    "levelFrom": [-300, -100, 0, 33],
                    "levelTo": [-100, 0, 33, 300],
                    "bid": [-32.89, -15.5, 0.0, 0.0],
                    "offer": [15.93, 10.2, 77.67, 999.0],
                    "curtailment": [-70] * 4,
                    "extra": [0] * 4,
                }
            ),
        ),
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
                    "pairId": [-1, 1, 2],
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
                    "pairId": [-1, 2, 1],
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
                    "pairId": [-1, 1, 1],
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
        (
            pl.DataFrame(
                {
                    "levelFrom": [37],
                    "levelTo": [37],
                    "bid": [125.0],
                    "offer": [130.0],
                    "curtailment": [0],
                    "extra": [10],
                    "pairId": [1],
                }
            ),
            pl.DataFrame(
                {
                    "levelFrom": [0],
                    "levelTo": [37],
                    "bid": [125.0],
                    "offer": [130.0],
                    "curtailment": [0],
                    "extra": [10],
                }
            ),
        ),
        (
            pl.DataFrame(
                {
                    "levelFrom": [-100, -100, 100, 100],
                    "levelTo": [-100, -100, 100, 100],
                    "bid": [-99999.0, 105.0, 105.0, 99999.0],
                    "offer": [-99999.0, 174.0, 174.0, 99999.0],
                    "curtailment": [0, 0, 0, 0],
                    "extra": [0, 0, 0, 0],
                    "pairId": [-2, -1, 1, 2],
                }
            ),
            pl.DataFrame(
                {
                    "levelFrom": [-100, 0],
                    "levelTo": [0, 100],
                    "bid": [105.0, 105.0],
                    "offer": [174.0, 174.0],
                    "curtailment": [0, 0],
                    "extra": [0, 0],
                }
            ),
        ),
    ],
)
def test_format_bid_price_table(
    input_table: pl.DataFrame, expected_output: pl.DataFrame
):
    output = format_bid_price_table(input_table)
    assert_frame_equal(output, expected_output)


@pytest.mark.parametrize(
    ("bid_price_table", "prices"),
    [
        (
            pl.DataFrame(
                {
                    "levelFrom": [-300, 0, 33],
                    "levelTo": [0, 33, 300],
                    "bid": [-32.89, 1.0, 1.0],
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


"""
bo_df:

|    | settlementDate      |   settlementPeriod | nationalGridBmUnit   | bmUnit    | timeFrom                  | timeTo                    |   levelFrom |   levelTo |    bid |   offer |   pairId |
|---:|:--------------------|-------------------:|:---------------------|:----------|:--------------------------|:--------------------------|------------:|----------:|-------:|--------:|---------:|
|  0 | 2024-12-10 00:00:00 |                 37 | LKSDB-1              | T_LKSDB-1 | 2024-12-10 18:00:00+00:00 | 2024-12-10 18:30:00+00:00 |        -100 |      -100 | -99999 |  -99999 |       -2 |
|  1 | 2024-12-10 00:00:00 |                 37 | LKSDB-1              | T_LKSDB-1 | 2024-12-10 18:00:00+00:00 | 2024-12-10 18:30:00+00:00 |        -100 |      -100 |    116 |     174 |       -1 |
|  2 | 2024-12-10 00:00:00 |                 37 | LKSDB-1              | T_LKSDB-1 | 2024-12-10 18:00:00+00:00 | 2024-12-10 18:30:00+00:00 |          55 |        55 |    116 |     174 |        1 |
|  3 | 2024-12-10 00:00:00 |                 37 | LKSDB-1              | T_LKSDB-1 | 2024-12-10 18:00:00+00:00 | 2024-12-10 18:30:00+00:00 |         145 |       145 |  99999 |   99999 |        2 |


gen_df: 

|    | time                |   physical_level |   extra |   curtailment |   generated |   settlementPeriod | settlementDate      |
|---:|:--------------------|-----------------:|--------:|--------------:|------------:|-------------------:|:--------------------|
|  0 | 2024-12-10 18:00:00 |            21.75 |       0 |        -18.75 |           3 |                 37 | 2024-12-10 00:00:00 |


expected output:
-18.75 * 116
"""


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
        ),
        (
            pl.DataFrame(
                {
                    "settlementDate": [
                        "2024-04-15",
                        "2024-04-15",
                        "2024-04-15",
                        "2024-04-15",
                        "2024-04-15",
                        "2024-04-15",
                        "2024-04-15",
                        "2024-04-15",
                    ],
                    "settlementPeriod": [2, 2, 2, 2, 3, 3, 3, 3],
                    "nationalGridBmUnit": [
                        "SGRWO-1",
                        "SGRWO-1",
                        "SGRWO-1",
                        "SGRWO-1",
                        "SGRWO-1",
                        "SGRWO-1",
                        "SGRWO-1",
                        "SGRWO-1",
                    ],
                    "bmUnit": [
                        "T_SGRWO-1",
                        "T_SGRWO-1",
                        "T_SGRWO-1",
                        "T_SGRWO-1",
                        "T_SGRWO-1",
                        "T_SGRWO-1",
                        "T_SGRWO-1",
                        "T_SGRWO-1",
                    ],
                    "timeFrom": [
                        "2024-04-14T23:30:00Z",
                        "2024-04-14T23:30:00Z",
                        "2024-04-14T23:30:00Z",
                        "2024-04-14T23:30:00Z",
                        "2024-04-15T00:00:00Z",
                        "2024-04-15T00:00:00Z",
                        "2024-04-15T00:00:00Z",
                        "2024-04-15T00:00:00Z",
                    ],
                    "timeTo": [
                        "2024-04-15T00:00:00Z",
                        "2024-04-15T00:00:00Z",
                        "2024-04-15T00:00:00Z",
                        "2024-04-15T00:00:00Z",
                        "2024-04-15T00:30:00Z",
                        "2024-04-15T00:30:00Z",
                        "2024-04-15T00:30:00Z",
                        "2024-04-15T00:30:00Z",
                    ],
                    "levelFrom": [-500, 500, -500, 500, -500, 500, -500, 500],
                    "levelTo": [-500, 500, -500, 500, -500, 500, -500, 500],
                    "bid": [-18.75, 100.0, -18.75, 100.0, -18.75, 100.0, -18.75, 100.0],
                    "offer": [0.0, 1500.0, 0.0, 1500.0, 0.0, 1500.0, 0.0, 1500.0],
                    "pairId": [-1, 1, -1, 1, -1, 1, -1, 1],
                }
            ),
            pl.DataFrame(
                {
                    "time": [
                        "2024-04-14T23:30:00.000000",
                        "2024-04-15T00:00:00.000000",
                    ],
                    "physical_level": [145.18333333333300, 145.70000000000000],
                    "extra": [0.0, 0.0],
                    "curtailment": [-145.18333333333300, -145.70000000000000],
                    "generated": [0.0, 0.0],
                    "settlementPeriod": [2, 3],
                    "settlementDate": ["2024-04-15", "2024-04-15"],
                }
            ),
            pl.DataFrame(
                {
                    "settlementDate": ["2024-04-15", "2024-04-15"],
                    "settlementPeriod": [3, 2],
                    "calculated_cashflow_curtailment": [2731.875, 2722.1875],
                    "calculated_cashflow_extra": [0.0, 0.0],
                }
            ),
        ),
        # (
        #     pl.DataFrame(
        #         {
        #             "settlementDate": ["2024-12-10"] * 4,
        #             "settlementPeriod": [34] * 4,
        #             "nationalGridBmUnit": ["LKSDB-1"] * 4,
        #             "bmUnit": ["T_LKSDB-1"] * 4,
        #             "timeFrom": ["2024-12-10T16:30:00Z"] * 4,
        #             "timeTo": ["2024-12-10T17:00:00Z"] * 4,
        #             "levelFrom": [-100, -100, 100, 100],
        #             "levelTo": [-100, -100, 100, 100],
        #             "bid": [-99999.0, 105.0, 105.0, 99999.0],
        #             "offer": [-99999.0, 174.0, 174.0, 99999.0],
        #             "pairId": [-2, -1, 1, 2],
        #         }
        #     ),
        #     pl.DataFrame(
        #         {
        #             "time": [
        #                 "2024-12-10T16:30:00.000000", "2024-12-10T16:31:00.000000", "2024-12-10T16:32:00.000000",
        #                 "2024-12-10T16:33:00.000000", "2024-12-10T16:34:00.000000", "2024-12-10T16:35:00.000000",
        #                 "2024-12-10T16:36:00.000000", "2024-12-10T16:37:00.000000", "2024-12-10T16:38:00.000000",
        #                 "2024-12-10T16:39:00.000000", "2024-12-10T16:40:00.000000", "2024-12-10T16:41:00.000000",
        #                 "2024-12-10T16:42:00.000000", "2024-12-10T16:43:00.000000", "2024-12-10T16:44:00.000000",
        #                 "2024-12-10T16:45:00.000000", "2024-12-10T16:46:00.000000", "2024-12-10T16:47:00.000000",
        #                 "2024-12-10T16:48:00.000000", "2024-12-10T16:49:00.000000", "2024-12-10T16:50:00.000000",
        #                 "2024-12-10T16:51:00.000000", "2024-12-10T16:52:00.000000", "2024-12-10T16:53:00.000000",
        #                 "2024-12-10T16:54:00.000000", "2024-12-10T16:55:00.000000", "2024-12-10T16:56:00.000000",
        #                 "2024-12-10T16:57:00.000000", "2024-12-10T16:58:00.000000", "2024-12-10T16:59:00.000000"
        #             ],
        #             "physical_level": [0.0] * 30,
        #             "extra": [0.0] * 30,
        #             "curtailment": [
        #                 -0.13333333333333300, -0.18333333333333300, -0.18333333333333300,
        #                 -0.18333333333333300, -0.1, -0.08333333333333330,
        #                 -0.08333333333333330, -0.11666666666666700, -0.11666666666666700,
        #                 -0.2833333333333330, -0.2833333333333330, -0.4, -0.4, -0.4,
        #                 -0.26666666666666700, -0.13333333333333300, 0.0, 0.0, 0.0, 0.0,
        #                 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
        #             ],
        #             "generated": [
        #                 -0.13333333333333300, -0.18333333333333300, -0.18333333333333300,
        #                 -0.18333333333333300, -0.1, -0.08333333333333330,
        #                 -0.08333333333333330, -0.11666666666666700, -0.11666666666666700,
        #                 -0.2833333333333330, -0.2833333333333330, -0.4, -0.4, -0.4,
        #                 -0.26666666666666700, -0.13333333333333300, 0.0, 0.0, 0.0, 0.0,
        #                 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
        #             ],
        #             "settlementPeriod": [34] * 30,
        #             "settlementDate": ["2024-12-10"] * 30,
        #         }
        #     ),
        #     pl.DataFrame(
        #         {
        #             "settlementDate": ["2024-12-10"],
        #             "settlementPeriod": [34],
        #             "calculated_cashflow_curtailment": [-351.75],
        #             "calculated_cashflow_extra": [0.0],
        #         }
        #     )
        # ),
        (
            pl.DataFrame(
                {
                    "settlementDate": ["2024-12-10"] * 4,
                    "settlementPeriod": [34] * 4,
                    "nationalGridBmUnit": ["LKSDB-1"] * 4,
                    "bmUnit": ["T_LKSDB-1"] * 4,
                    "timeFrom": ["2024-12-10T16:30:00Z"] * 4,
                    "timeTo": ["2024-12-10T17:00:00Z"] * 4,
                    "levelFrom": [-100, -100, 100, 100],
                    "levelTo": [-100, -100, 100, 100],
                    "bid": [-99999.0, 105.0, 105.0, 99999.0],
                    "offer": [-99999.0, 174.0, 174.0, 99999.0],
                    "pairId": [-2, -1, 1, 2],
                }
            ),
            pl.DataFrame(
                {
                    "time": ["2024-12-10T16:30:00.000000"],
                    "physical_level": [0.0],
                    "extra": [0.0],
                    "curtailment": [-3.35],
                    "generated": [-3.35],
                    "settlementPeriod": [34],
                    "settlementDate": ["2024-12-10"],
                }
            ),
            pl.DataFrame(
                {
                    "settlementDate": ["2024-12-10"],
                    "settlementPeriod": [34],
                    "calculated_cashflow_curtailment": [-351.75],
                    "calculated_cashflow_extra": [0.0],
                }
            ),
        ),
        (
            pl.DataFrame(
                {
                    "settlementDate": ["2024-12-10"] * 4,
                    "settlementPeriod": [37] * 4,
                    "nationalGridBmUnit": ["LKSDB-1"] * 4,
                    "bmUnit": ["T_LKSDB-1"] * 4,
                    "timeFrom": ["2024-12-10T18:00:00Z"] * 4,
                    "timeTo": ["2024-12-10T18:30:00Z"] * 4,
                    "levelFrom": [-100, -100, 55, 145],
                    "levelTo": [-100, -100, 55, 145],
                    "bid": [-99999, 116, 116, 99999],
                    "offer": [-99999, 174, 174, 99999],
                    "pairId": [-2, -1, 1, 2],
                }
            ),
            pl.DataFrame(
                {
                    "time": ["2024-12-10T18:00:00.000000"],
                    "physical_level": [21.75],
                    "extra": [0],
                    "curtailment": [-18.75],
                    "generated": [3],
                    "settlementPeriod": [37],
                    "settlementDate": ["2024-12-10"],
                }
            ),
            pl.DataFrame(
                {
                    "settlementDate": ["2024-12-10"],
                    "settlementPeriod": [37],
                    "calculated_cashflow_curtailment": [-18.75 * 116],
                    "calculated_cashflow_extra": [0.0],
                }
            ),
        ),
    ],
)
def test_calculate_cashflow(
    bo_df: pl.DataFrame, gen_df: pl.DataFrame, expected_result: pl.DataFrame
):
    cf = cashflow(bo_df, gen_df)
    assert_frame_equal(expected_result, cf, check_row_order=False)
