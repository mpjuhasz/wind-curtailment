import polars as pl
import pytest
from polars.testing import assert_frame_equal

from src.elexon.utils import (
    aggregate_prices,
    cashflow,
    format_bid_offer_table,
    smoothen_physical,
)


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
    output = format_bid_offer_table(input_table)
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


@pytest.mark.parametrize(
    ("raw_df", "expected_result"),
    [
        # NOTE: not sure about this, one, it currenlty fails with taking into
        # account the last row that overwrites the first
        # I don't actually know whether this ever happens, so will have to look
        # at the PNs first
        (
            pl.DataFrame(
                {
                    "dataset": ["PN"] * 4,
                    "settlementDate": ["2021-02-28"] * 4,
                    "settlementPeriod": [21, 21, 22, 22],
                    "timeFrom": [
                        "2021-02-28T10:00:00Z",
                        "2021-02-28T10:01:00Z",
                        "2021-02-28T10:30:00Z",
                        "2021-02-28T10:30:00Z",
                    ],
                    "timeTo": [
                        "2021-02-28T10:01:00Z",
                        "2021-02-28T10:30:00Z",
                        "2021-02-28T11:00:00Z",
                        "2021-02-28T10:40:00Z",
                    ],
                    "levelFrom": [421, 446, 446, 446],
                    "levelTo": [446, 446, 446, 410],
                    "nationalGridBmUnit": ["WBURB-2"] * 4,
                    "bmUnit": ["T_WBURB-2"] * 4,
                }
            ),
            pl.DataFrame(
                {
                    "time": [
                        f"2021-02-28T{10 + i // 60:02d}:{i % 60:02d}:00Z"
                        for i in range(60)
                    ],
                    "level": (
                        [421.0] * 1  # 10:00 (1 min at 446 after ramping)
                        + [446.0] * 29  # 10:01-10:29 (29 mins at 446)
                        # + [446.0 - 36.0 * i / 10 for i in range(10)] # coming down to 410
                        + [446.0] * 30  # 10:30-10:59 (30 mins at 446)
                    ),
                    "settlementPeriod": [21] * 30 + [22] * 30,
                    "settlementDate": ["2021-02-28"] * 60,
                }
            ).with_columns(
                pl.col("time").str.strptime(
                    format="%Y-%m-%dT%H:%M:%SZ", dtype=pl.Datetime
                ),
                pl.col("level").cast(pl.Float64),
            ),
        ),
        (
            pl.DataFrame(
                {
                    "dataset": ["PN", "PN", "PN", "PN", "PN", "PN"],
                    "settlementDate": ["2021-02-10"] * 6,
                    "settlementPeriod": [41, 42, 42, 42, 42, 43],
                    "timeFrom": [
                        "2021-02-10T20:00:00Z",
                        "2021-02-10T20:30:00Z",
                        "2021-02-10T20:36:00Z",
                        "2021-02-10T20:47:00Z",
                        "2021-02-10T20:59:00Z",
                        "2021-02-10T21:00:00Z",
                    ],
                    "timeTo": [
                        "2021-02-10T20:30:00Z",
                        "2021-02-10T20:36:00Z",
                        "2021-02-10T20:47:00Z",
                        "2021-02-10T20:59:00Z",
                        "2021-02-10T21:00:00Z",
                        "2021-02-10T21:30:00Z",
                    ],
                    "levelFrom": [421, 421, 421, 158, 30, 0],
                    "levelTo": [421, 421, 158, 30, 0, 0],
                    "nationalGridBmUnit": ["WBURB-2"] * 6,
                    "bmUnit": ["T_WBURB-2"] * 6,
                }
            ),
            pl.DataFrame(
                {
                    "time": [
                        f"2021-02-10T{20 + i // 60:02d}:{i % 60:02d}:00Z"
                        for i in range(90)
                    ],
                    "level": (
                        [421.0] * 30  # 20:00-20:29 (30 mins at 421)
                        + [421.0] * 6  # 20:30-20:35 (6 mins at 421)
                        + [
                            421.0 - (421.0 - 158.0) * i / 11 for i in range(11)
                        ]  # 20:36-20:46 (11 mins declining)
                        + [
                            158.0 - (158.0 - 30.0) * i / 12 for i in range(12)
                        ]  # 20:47-20:58 (12 mins declining)
                        + [
                            30.0 - 30.0 * i / 1 for i in range(1)
                        ]  # 20:59 (1 min declining)
                        + [0.0] * 30  # 21:00-21:29 (30 mins at 0)
                    ),
                    "settlementPeriod": [41] * 30 + [42] * 30 + [43] * 30,
                    "settlementDate": ["2021-02-10"] * 90,
                }
            ).with_columns(
                pl.col("time").str.strptime(
                    format="%Y-%m-%dT%H:%M:%SZ", dtype=pl.Datetime
                ),
                pl.col("level").cast(pl.Float64),
            ),
        ),
        (
            pl.DataFrame(
                {
                    "dataset": ["PN", "PN", "PN", "PN", "PN", "PN", "PN"],
                    "settlementDate": [
                        "2021-02-23",
                        "2021-02-24",
                        "2021-02-24",
                        "2021-02-24",
                        "2021-02-28",
                        "2021-02-28",
                        "2021-02-28",
                    ],
                    "settlementPeriod": [48, 1, 1, 2, 3, 4, 5],
                    "timeFrom": [
                        "2021-02-23T23:30:00Z",
                        "2021-02-24T00:00:00Z",
                        "2021-02-24T00:00:00Z",
                        "2021-02-24T00:30:00Z",
                        "2021-02-28T01:00:00Z",
                        "2021-02-28T01:30:00Z",
                        "2021-02-28T02:00:00Z",
                    ],
                    "timeTo": [
                        "2021-02-24T00:00:00Z",
                        "2021-02-24T00:30:00Z",
                        "2021-02-24T00:30:00Z",
                        "2021-02-24T01:00:00Z",
                        "2021-02-28T01:30:00Z",
                        "2021-02-28T02:00:00Z",
                        "2021-02-28T02:30:00Z",
                    ],
                    "levelFrom": [0, 0, 0, 0, 421, 421, 421],
                    "levelTo": [0, 0, 0, 0, 421, 421, 421],
                    "nationalGridBmUnit": ["WBURB-2"] * 7,
                    "bmUnit": ["T_WBURB-2"] * 7,
                }
            ),
            pl.DataFrame(
                {
                    "time": pl.datetime_range(
                        start=pl.datetime(2021, 2, 23, 23, 30),
                        end=pl.datetime(2021, 2, 28, 2, 30),
                        interval="1m",
                        eager=True,
                        closed="left",
                    ).to_list(),
                    "level": [0.0] * 5850
                    + [421.0]
                    * 90,  # Feb 23-28: zeros until Feb 28 01:00, then 421 until 02:30
                    "settlementPeriod": (
                        [48] * 30  # Feb 23
                        + [
                            period
                            for _ in range(4)
                            for period in range(1, 49)
                            for _ in range(30)
                        ]  # Feb 24-27
                        + [
                            period for period in range(1, 6) for _ in range(30)
                        ]  # Feb 28
                    ),
                    "settlementDate": (
                        ["2021-02-23"] * 30  # Feb 23
                        + [
                            f"2021-02-{24 + d:02d}"
                            for d in range(4)
                            for _ in range(24 * 60)
                        ]  # Feb 24-27
                        + ["2021-02-28"] * 150  # Feb 28
                    ),
                }
            ).with_columns(
                pl.col("level").cast(pl.Float64),
            ),
        ),
    ],
)
def test_smoothen_physical(raw_df: pl.DataFrame, expected_result: pl.DataFrame):
    with pl.Config(tbl_rows=100, tbl_cols=20, fmt_str_lengths=100):
        output = smoothen_physical(raw_df)

        assert_frame_equal(output, expected_result)
