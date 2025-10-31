from typing import Optional

import polars as pl


def resolve_acceptances(df: pl.DataFrame) -> pl.DataFrame:
    """
    Discarding overwritten accepted bids and offers
    
    Acceptance data comes back as a list of acceptances that might or might not be overwritten by a later acceptance
    covering the same time period. To handle this, the acceptance levels are interpolated to 1 minute time intervals,
    and for each time interval the level from the last acceptance is selected. 
    """
    result = df.with_columns(
        pl.col("timeFrom").str.strptime(format="%Y-%m-%dT%H:%M:%SZ", dtype=pl.Datetime).alias("from"),
        pl.col("timeTo").str.strptime(format="%Y-%m-%dT%H:%M:%SZ", dtype=pl.Datetime).alias("to"),
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
                values=[row["levelFrom"], *(None for _ in range(time_series.shape[0] - 2)), row["levelTo"]],
            )
            new_df = pl.DataFrame({
                "time": time_series,
                "level": level_series,
                "acceptanceTime": pl.Series([row["acceptanceTime"]] * time_series.shape[0]),
            }).interpolate()

            dfs.append(new_df)
    return pl.concat(dfs).sort(by=["time", "acceptanceTime"]).unique(subset=["time"], keep="last")


def aggregate_acceptance_and_pn(accepted: Optional[pl.DataFrame], physical: pl.DataFrame, downsample_frequency: str = "1d") -> pl.DataFrame:
    physical_smoothened = physical.select(
        pl.col("timeFrom").alias("time"),
        pl.col("levelFrom").alias("level"),
    ).with_columns(
        pl.col("level"),
        pl.col("time").str.strptime(format="%Y-%m-%dT%H:%M:%SZ", dtype=pl.Datetime),
    ).upsample(time_column="time", every="1m").fill_null(strategy="forward")
    
    
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
            pl.when(pl.col("level").is_not_null()).then(pl.col("level_right").sub(pl.col("level"))).otherwise(0).alias("diff"),
        )
    else:
        diffs = physical_smoothened.with_columns(
            physical_level=pl.col("level"),
            time=pl.col("time"),
            accepted_level=pl.lit(0),
            diff=pl.lit(0)
        )

    output = diffs.with_columns(
        pl.col("physical_level").add(pl.col("diff")).alias("generated"),
        pl.col("diff").clip(lower_bound=0).alias("curtailment"),
        pl.col("diff").clip(upper_bound=0).alias("extra_generation"),
    ).select(
        "time", "diff", "generated", "curtailment", "extra_generation"
    ).group_by_dynamic(
        index_column="time", every=downsample_frequency
    ).agg(
        # turning everything into daily GWh figures
        pl.col("diff").mul(1 / 60).mul(1 / 1_000).sum(),
        pl.col("extra_generation").mul(1 / 60).mul(1 / 1_000).sum(),
        pl.col("curtailment").mul(1 / 60).mul(1 / 1_000).sum(),
        pl.col("generated").mul(1 / 60).mul(1 / 1_000).sum(),
    )
    return output


def aggregate_bm_unit_generation(accepted: pl.DataFrame, physical: pl.DataFrame) -> dict:
    """Calculating curtailmant, extra generation and total figures for the acceptance and PN datasets for a BM unit"""
    diffs = aggregate_acceptance_and_pn(accepted, physical)
    print(diffs)
    
    return {
        "curtailment": round(diffs.filter(pl.col("diff") > 0).select("diff").sum().item(), 2),
        "extra_generation": - round(diffs.filter(pl.col("diff") < 0).select("diff").sum().item(), 2),
        "total": round(diffs.select("generated").sum().item(), 2)
    }