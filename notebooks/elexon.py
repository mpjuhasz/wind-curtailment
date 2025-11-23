import marimo

__generated_with = "0.17.5"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _():
    import polars as pl
    import re
    from typing import Optional
    import pandas as pd
    from pathlib import Path
    return (pl,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Elexon API
    """)
    return


@app.cell
def _():
    import requests
    import datetime
    from typing import Callable
    return Callable, datetime, requests


@app.cell
def _(Callable, datetime, pl, requests):
    def date_range_wrapper(bm_unit: str, start_date: datetime.date, end_date: datetime.date, f: Callable):
        from_time = datetime.datetime.strptime(start_date, "%Y-%m-%dT00:00Z")
        to_time = datetime.datetime.strptime(end_date, "%Y-%m-%dT00:00Z")
        if to_time - from_time > datetime.timedelta(days=6):
            dfs = []
            current_start = from_time
            while current_start < to_time:
                current_end = min(current_start + datetime.timedelta(days=5), to_time)
                result = f(bm_unit, current_start, current_end)
                if result is not None and not result.is_empty():
                    dfs.append(result)
                current_start = current_end
            if dfs:
                return pl.concat(dfs).sort(by="timeFrom")


    def get_physical(bm_unit: str, from_time: str, to_time: str):
        url = f"https://data.elexon.co.uk/bmrs/api/v1/balancing/physical?bmUnit={bm_unit}&from={from_time}&to={to_time}&dataset=PN"
        response = requests.get(url)
        if response.status_code == 200:
            return pl.DataFrame(response.json().get("data")).sort(by="timeFrom")
        else:
            print(f"Error: {response.status_code}")
            return None

    def get_boal(bm_unit: str, from_time: str, to_time: str):
        url = f"https://data.elexon.co.uk/bmrs/api/v1/balancing/acceptances?bmUnit={bm_unit}&from={from_time}&to={to_time}&format=json"
        response = requests.get(url)
        if response.status_code == 200:
            return pl.DataFrame(response.json().get("data"))
        else:
            print(f"Error: {response.status_code}")
            return None


    def get_bid_offer(bm_unit: str, from_time: str, to_time: str):
        url = f"https://data.elexon.co.uk/bmrs/api/v1/balancing/bid-offer?bmUnit={bm_unit}&from={from_time}&to={to_time}"
        response = requests.get(url)
        if response.status_code == 200:
            return pl.DataFrame(response.json().get("data"))
        else:
            print(f"Error: {response.status_code}")
            return None
    return date_range_wrapper, get_bid_offer, get_boal, get_physical


@app.cell
def _():
    bm_unit = "SGRWO-2"
    from_time = "2025-01-01T00:00Z"
    to_time = "2025-01-10T00:00Z"
    return bm_unit, from_time, to_time


@app.cell
def _(bm_unit, date_range_wrapper, from_time, get_physical, to_time):
    physical = date_range_wrapper(bm_unit, from_time, to_time, get_physical)
    return (physical,)


@app.cell
def _(bm_unit, date_range_wrapper, from_time, get_boal, to_time):
    accepted = date_range_wrapper(bm_unit, from_time, to_time, get_boal)
    return (accepted,)


@app.cell
def _(accepted):
    accepted
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    There are multiple acceptances, and always the latest is to be taken into account (it overwrites the previous ones).
    """)
    return


@app.cell
def _(pl):
    def smoothen_accepted(accepted: pl.DataFrame) -> pl.DataFrame:
        result = accepted.with_columns(
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
    return (smoothen_accepted,)


@app.cell
def _(accepted, smoothen_accepted):
    if accepted is not None:
        print(smoothen_accepted(accepted))
    return


@app.cell
def _():
    # bo = date_range_wrapper(bm_unit, from_time, to_time, get_bid_offer)
    # if bo is not None and not bo.is_empty():
    #     print(bo.filter(pl.col("settlementPeriod") == 2))
    return


@app.cell
def _():
    return


@app.cell
def _(accepted, physical, pl, smoothen_accepted):
    import altair as alt

    result = pl.concat(
        [
        physical.select(
            pl.col("timeFrom").alias("time"),
            pl.col("levelFrom").alias("level"),
        ), 
    ])

    if accepted is not None:
        result_accepted = smoothen_accepted(accepted)
    else:
        result_accepted = None

    chart = alt.Chart(result.to_pandas()).mark_line().encode(
        x='time:T',
        y='level:Q',
        color=alt.value('blue'),
        tooltip=['time:T', 'level:Q']
    ).properties(
        title='Level over time'
    ) + alt.Chart(result_accepted.to_pandas() if result_accepted is not None else pl.DataFrame().to_pandas()).mark_line().encode(
        x='time:T',
        y='level:Q',
        color=alt.value('red'),
        tooltip=['time:T', 'level:Q']
    ).properties(
        title='Level over time'
    )

    chart.show()
    chart.interactive()
    return


@app.cell
def _(mo):
    mo.md(r"""
    Not very nicely plotted (unrelated intervals are linked in the accepted data by those long sloped red lines), but shows the idea: where the red line is below the blue, we've got curtailment.
    """)
    return


@app.cell
def _(pl, smoothen_accepted):
    def calculate_curtailment(accepted: pl.dataframe, physical: pl.dataframe) -> dict:
        accepted_smoothened = smoothen_accepted(accepted)
        physical_smoothened = physical.select(
            pl.col("timeFrom").alias("time"),
            pl.col("levelFrom").alias("level"),
        ).with_columns(
            pl.col("level"),
            pl.col("time").str.strptime(format="%Y-%m-%dT%H:%M:%SZ", dtype=pl.Datetime),
        ).upsample(time_column="time", every="1m").fill_null(strategy="forward")

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
        ).select("time", "diff", "accepted_level", "physical_level")

        total = diffs.select(pl.col("physical_level").add(pl.col("diff"))).sum().item()

        return {
            "curtailment": round(diffs.filter(pl.col("diff") > 0).select("diff").sum().item() / 60 / 1_000, 2),
            "extra_generation": round(diffs.filter(pl.col("diff") < 0).select("diff").sum().item() / -60 / 1_000, 2),
            "total": round(total / 60 / 1_000, 2)
        }
    return (calculate_curtailment,)


@app.cell
def _(accepted, calculate_curtailment, physical):
    print(calculate_curtailment(accepted, physical))
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Bid offer prices
    """)
    return


@app.cell
def _(bm_unit, date_range_wrapper, from_time, get_bid_offer, to_time):
    bid_offer = date_range_wrapper(bm_unit, from_time, to_time, get_bid_offer)
    return (bid_offer,)


@app.cell
def _(bid_offer):
    bid_offer
    return


@app.cell
def _(accepted, bid_offer):
    bid_offer.join(
        accepted,
        left_on=["settlementDate", "settlementPeriod", "levelFrom"],
        right_on=["settlementDate", "settlementPeriodFrom", "levelFrom"],
        how="inner",
    )
    return


@app.cell
def _(accepted):
    accepted
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
