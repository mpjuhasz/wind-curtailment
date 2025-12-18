from typing import Literal, Optional

import polars as pl

ENERGY_MULTIPLIERS = {"MWh": 1, "GWh": 1 / 1_000}


def resolve_acceptances(df: pl.DataFrame) -> pl.DataFrame:
    """
    Discarding overwritten accepted bids and offers

    Acceptance data comes back as a list of acceptances that might or might not
    be overwritten by a later acceptance covering the same time period. To handle
    this, the acceptance levels are interpolated to 1 minute time intervals,
    and for each time interval the level from the last acceptance is selected.
    """
    result = df.with_columns(
        pl.col("timeFrom")
        .str.strptime(format="%Y-%m-%dT%H:%M:%SZ", dtype=pl.Datetime)
        .alias("from"),
        pl.col("timeTo")
        .str.strptime(format="%Y-%m-%dT%H:%M:%SZ", dtype=pl.Datetime)
        .alias("to"),
    )
    dfs = []
    for d in result.group_by(pl.col("acceptanceNumber")):
        for row in d[1].iter_rows(named=True):
            time_series = pl.datetime_range(
                start=row["from"],
                end=row["to"],
                interval="1m",
                eager=True,
            ).alias("time")

            level_series = pl.Series(
                name="level",
                values=[
                    row["levelFrom"],
                    *(None for _ in range(time_series.shape[0] - 2)),
                    row["levelTo"],
                ],
            )
            new_df = pl.DataFrame(
                {
                    "time": time_series,
                    "level": level_series,
                    "acceptanceTime": pl.Series(
                        [row["acceptanceTime"]] * time_series.shape[0]
                    ),
                }
            ).interpolate()

            dfs.append(new_df)
    return (
        pl.concat(dfs)
        .sort(by=["time", "acceptanceTime"])
        .unique(subset=["time"], keep="last")
    )


def aggregate_acceptance_and_pn(
    accepted: Optional[pl.DataFrame],
    physical: pl.DataFrame,
    downsample_frequency: str,
    energy_unit: Literal["MWh", "GWh"],
) -> pl.DataFrame:
    """
    Aggregates and upsamples the accepted-level and physical notification dataframes

    It takes the difference as `accepted - physical`. This means, that curtailment will
    have negative sign, and extra generation will be positive. This makes more sense
    than the other way around (which is how I've initially set this up).
    """
    physical_smoothened = (
        physical.select(
            pl.col("timeFrom").alias("time"),
            pl.col("levelFrom").alias("level"),
            pl.col("settlementPeriod"),
            pl.col("settlementDate"),
        )
        .with_columns(
            pl.col("level"),
            pl.col("time").str.strptime(format="%Y-%m-%dT%H:%M:%SZ", dtype=pl.Datetime),
        )
        .sort(by="time")
        .upsample(time_column="time", every="1m")
        .fill_null(strategy="forward")
    )

    if accepted is not None:
        accepted_smoothened = resolve_acceptances(accepted)

        diffs = accepted_smoothened.join(
            physical_smoothened,
            left_on="time",
            right_on="time",
            how="right",
        ).with_columns(
            pl.col("level").alias("accepted_level"),
            pl.col("level_right").alias("physical_level"),
            pl.col("time"),
            # accepted - physical => curtailment (-), or extra (+)
            pl.when(pl.col("level").is_not_null())
            .then(pl.col("level").sub(pl.col("level_right")))
            .otherwise(0)
            .alias("diff"),
        )
    else:
        diffs = physical_smoothened.with_columns(
            physical_level=pl.col("level"),
            time=pl.col("time"),
            accepted_level=pl.lit(0),
            diff=pl.lit(0),
        )

    multiplier = ENERGY_MULTIPLIERS[energy_unit]
    output = (
        diffs.with_columns(
            # this roundabout way is required to account for no accepted level
            # cases (where diff = 0)
            pl.col("physical_level").add(pl.col("diff")).alias("generated"),
            pl.col("diff").clip(lower_bound=0).alias("extra"),
            pl.col("diff").clip(upper_bound=0).alias("curtailment"),
        )
        .select(
            "physical_level",
            "time",
            "diff",
            "generated",
            "curtailment",
            "extra",
            "settlementPeriod",
            "settlementDate",
        )
        .group_by_dynamic(index_column="time", every=downsample_frequency)
        .agg(
            # At this point we're aggregating power figures by the minute.
            # I'm turning this into energy here, assuming constant generation within the
            # minute, i.e. using: E = P x t
            # Keeping physical level for validation: G = E + C + PL
            pl.col("physical_level").mul(1 / 60).mul(multiplier).sum(),
            pl.col("extra").mul(1 / 60).mul(multiplier).sum(),
            pl.col("curtailment").mul(1 / 60).mul(multiplier).sum(),
            pl.col("generated").mul(1 / 60).mul(multiplier).sum(),
            pl.col("settlementPeriod").first(),
            pl.col("settlementDate").first(),
        )
    )
    return output


def aggregate_bm_unit_generation(
    accepted: pl.DataFrame, physical: pl.DataFrame
) -> dict:
    """Calculating curtailment, extra generation and total for acceptance and PN for a BM unit"""
    diffs = aggregate_acceptance_and_pn(accepted, physical)

    return {
        "curtailment": round(
            diffs.filter(pl.col("diff") > 0).select("diff").sum().item(), 2
        ),
        "extra_generation": -round(
            diffs.filter(pl.col("diff") < 0).select("diff").sum().item(), 2
        ),
        "total": round(diffs.select("generated").sum().item(), 2),
    }


def format_bid_price_table(df: pl.DataFrame) -> pl.DataFrame:
    """Formats the bids and prices adding a row with zero so that the intervals are complete"""
    sorted_negatives = df.filter(pl.col("levelTo").lt(pl.lit(0))).sort(by="levelTo")

    df = df.filter(
        ~(pl.col("levelFrom").eq(pl.lit(0)) & pl.col("levelTo").eq(pl.lit(0)))
    )
    
    if sorted_negatives.is_empty():
        sorted_negatives = df.sort(by="levelTo")

    zero_row = pl.DataFrame(
        {
            "levelFrom": 0,
            "levelTo": 0,
            "bid": sorted_negatives.select(pl.col("bid")).limit(1).item(),
            "offer": sorted_negatives.select(pl.col("offer")).limit(1).item(),
            "curtailment": df.select(pl.col("curtailment")).limit(1).item(),
            "extra": df.select(pl.col("extra")).limit(1).item(),
        }
    )

    bid_price_table = (
        df.extend(zero_row)
        .sort(by="levelFrom")
        .with_columns(
            pl.col("levelTo").shift(-1),
            pl.col("bid").shift(-1),
            pl.col("offer").shift(-1),
            pl.col("curtailment"),
            pl.col("extra"),
        )
        .select("*")
        .limit(df.shape[0] - 1)
    )

    return bid_price_table


expr_curtailment = (
    pl.when(pl.col("curtailment") < pl.col("levelFrom"))
    .then(
        pl.col("levelFrom")  # outside of this range (in the negative direction)
    )
    .otherwise(
        pl.when(pl.col("curtailment") > pl.col("levelTo"))
        .then(
            pl.lit(0)  # outside of this range (closer to 0)
        )
        .otherwise(
            pl.col("curtailment").sub(pl.col("levelTo"))  # in the range
        )
    )
    .alias("curtailment_in_range")
)

expr_extra = (
    pl.when(pl.col("extra") > pl.col("levelTo"))
    .then(pl.col("levelTo"))
    .otherwise(
        pl.when(pl.col("extra") < pl.col("levelFrom"))
        .then(pl.lit(0))
        .otherwise(pl.col("extra").sub(pl.col("levelFrom")))
    )
    .alias("extra_in_range")
)


c_or_e_map = {
    "curtailment": ("bid", expr_curtailment),
    "extra": ("offer", expr_extra),
}


def aggregate_prices(bid_price_table: pl.DataFrame) -> dict[str, float]:
    """Aggregates the extra generation and curtailment prices for the bid-price table"""
    prices = dict()
    for col, (price_col, expr) in c_or_e_map.items():
        if bid_price_table.select(pl.col(col)).limit(1).item() == 0:
            prices[col] = 0.0
            continue
        prices[col] = (
            bid_price_table.with_columns(expr)
            .with_columns(
                pl.col(f"{col}_in_range").mul(pl.col(price_col)).alias(f"{col}_price")
            )
            .select(pl.col(f"{col}_price").sum())
            .item()
        )
    return prices


def calculate_cashflow(df: pl.DataFrame) -> float:
    """Calculates the cashflow for a single settlement period"""
    # for debugging: https://github.com/pola-rs/polars/issues/7704
    try: 
        bid_price_table = format_bid_price_table(
            # TODO: I need to make sure this is fine here, and there aren't multiple for some other reason
            df.select("levelFrom", "levelTo", "bid", "offer", "curtailment", "extra").unique()
        )
        prices = aggregate_prices(bid_price_table)
        return df.select("settlementDate", "settlementPeriod").unique().with_columns(
            pl.lit(v).alias(f"calculated_cashflow_{k}") for k, v in prices.items()
        )
    except Exception as e:
        from traceback import print_exc
        print_exc()
        print(e)



def cashflow(bo_df: pl.DataFrame, gen_df: pl.DataFrame) -> pl.DataFrame:
    """Creates cashflow columns form the bid-offer and generation dataframe"""
    merged = (
        bo_df.join(gen_df, on=["settlementDate", "settlementPeriod"])
        .group_by("settlementDate", "settlementPeriod")
        .map_groups(calculate_cashflow)
    )

    return merged
