import marimo

__generated_with = "0.18.0"
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
    return pd, pl


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
    bm_unit = "T_MOWEO-1"
    # bm_unit = "T_DINO-2"
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
    return alt, result_accepted


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
def _(mo):
    mo.md(r"""
    From [the docs](https://bscdocs.elexon.co.uk/bsc/bsc-section-x-2-technical-glossary?highlight=%5B%22Q4.1%22%2C%223%22%5D#block-69033c95ab14fcbafaec77c1):

    | Defined Term|Acronym|Units|Definition/Explanatory Text|
    |----|----|----|----|
    |Accepted Bid-Offer Volume|qABOknij(t)|MW|The quantity established in accordance with Section T3.6 The Accepted Bid-Offer Volume is the quantity of Bid or Offer from Bid-Offer Pair n accepted as a result of Bid-Offer Acceptance k, that is not flagged as relating to an RR Instruction, in Settlement Period j from BM Unit i, for any spot time t within Settlement Period j|
    |Accepted Offer Volume|qAOknij(t)|MW|The quantity established in accordance with . T3.7.1. The Accepted Offer Volume is the quantity of Offer n being the positive part of the Accepted Bid-Offer Volume accepted as a result of Bid-Offer Acceptance k from BM Unit i at spot times t within Settlement Period j.|
    |Bid Price |PBnij|£/MWh|The amount in £/MWh associated with a Bid and comprising part of a Bid-Offer Pair.|
    |Account Energy Imbalance Cashflow|CAEIaj|£|The amount determined in accordance with Section T4.7.1. The Account Energy Imbalance Cashflow is the total cashflow resulting from the Energy Imbalance of Energy Account a in Settlement Period j such that a negative quantity represents a payment to the Trading Party holding Energy Account a and a positive quantity represents a payment by the Trading Party holding Energy Account a.|
    """)
    return


@app.cell
def _(pl, requests):
    def get_indicative_cashflow(time: str, bm_unit: str):
        url = f"https://data.elexon.co.uk/bmrs/api/v1/balancing/settlement/indicative/cashflows/all/bid/{time}?bmUnit={bm_unit}&format=json"
        response = requests.get(url)
        if response.status_code == 200:
            return pl.DataFrame(response.json().get("data"))
        else:
            print(f"Error: {response.status_code}")
            return None
    return (get_indicative_cashflow,)


@app.cell
def _():
    from_time_date = "2025-01-02"
    return (from_time_date,)


@app.cell
def _(bm_unit, from_time_date, get_indicative_cashflow):
    indicative_cashflow = get_indicative_cashflow(from_time_date, bm_unit)
    return (indicative_cashflow,)


@app.cell
def _(indicative_cashflow):
    indicative_cashflow
    return


@app.cell
def _(mo):
    mo.md(r"""
    The cashflow calculation is:  if QAEIaj > 0 then CAEIaj = – QAEIaj * SSPj otherswise CAEIaj = – QAEIaj * SBPj

    Where:  Account Energy Imbalance Volume = QAEIaj and

    SSPj is the system sell price

        (the System Sell Price will be determined as follows:
        SSPj = {ΣiΣnΣk {QABknij * PBnij * TLMij} + Σm {QBSASmj * BSAPmj}} + ΣJ {VGBJ * QHRRAPJ} + {RRAUSSj * 0}}
        / {ΣiΣn Σk {QABknij * TLMij} + Σm {QBSASmj } + {SPAj} + ΣJ {VGBjJ} + RRAUSSj})


    TLMij = The Transmission Loss Multiplier is the factor applied to BM Unit i in Settlement Period j in order to adjust for Transmission Losses.

    For T_SGRWO_2 it is "transmissionLossFactor": "-0.0301340",


    SBPj is the system buy price.
    """)
    return


@app.cell
def _(alt, indicative_cashflow, pl, result_accepted):
    cashflows = indicative_cashflow.with_columns(
        pl.col("startTime"),
        pl.col("bidOfferPairCashflows").map_elements(lambda x: x.get("negative1", 0)).alias("negative"),
        pl.col("bidOfferPairCashflows").map_elements(lambda x: x.get("positive1", 0)).alias("positive"),
    )

    chart_2 = alt.Chart(cashflows.to_pandas()).mark_line().encode(
        x='startTime:T',
        y='negative:Q',
        color=alt.value('blue'),
        tooltip=['startTime:T', 'negative:Q']
    ).properties(
        title='Negative cashflows over time'
    ) + alt.Chart(cashflows.to_pandas() if result_accepted is not None else pl.DataFrame().to_pandas()).mark_line().encode(
        x='startTime:T',
        y='positive:Q',
        color=alt.value('red'),
        tooltip=['startTime:T', 'positive:Q']
    ).properties(
        title='Positive cashflows over time'
    )

    chart_2.show()
    chart_2.interactive()
    return


@app.cell
def _(bid_offer):
    bid_offer
    return


@app.cell
def _(mo):
    mo.md(r"""
    I think this is what's happening:
    - levelFrom and levelTo must be equal as per the docs - they outline a constant level for thiem period between timeFrom and timeTo
    - bid and offer has been perplexing me: I think what they mean is:
        - bid: each MWh decrease in this band (between FPN and levelFrom) costs this much
        - offer: each MWh increase in this band (between FPN and levelFrom) earns this much
        - example: levelFrom = -500, levelTo = -500, bid = -2.59, offer = 0. It's going to cost you £2.59 to decrease my output by 1 MWh within this level.
    - when multiple pairs exist for the same period, they offer different bands of pricing. E.g., the first 33 MWh extra will cost this much, then that much etc.
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    Okay, so below is the cashflow / diff calculation for the T_SGRWO-2 and T_SGRWO-2 sites respectively (for 2025-01-02 15:30). What we're seeing here is that the indicative cashflow is indicative, because it's calculated from the system price - which is constant within the period (i.e. unrelated to the bm-unit).
    """)
    return


@app.cell
def _():
    71.453543413 / (75 - 16)
    return


@app.cell
def _():
    99.308314574 / 82
    return


@app.cell
def _(mo):
    mo.md(r"""
    Indicative will still be useful though for sense-checking my calculation. The system price is pretty complex and calculates a bunch of things, but it might be a good pointer still.
    """)
    return


@app.cell
def _(physical):
    physical
    return


@app.cell
def _():
    from elexon.utils import aggregate_acceptance_and_pn
    return (aggregate_acceptance_and_pn,)


@app.cell
def _(accepted, aggregate_acceptance_and_pn, physical):
    diffs = aggregate_acceptance_and_pn(accepted, physical, downsample_frequency="30m", energy_unit="MWh")
    diffs
    return (diffs,)


@app.cell
def _(pd):
    def consolidate_settlement_period(df: pd.DataFrame) -> float:
        """Takes the bid-offer pairs for a settlement period, and the diff, to calculate the cashflow"""
        diff = df.iloc[0]["curtailment"]

        if diff:
            df = df[df["pairId"] < 0]
        else:
            return 0

        df.sort_values(by="pairId", ascending=False, inplace=True)

        cashflow = 0.0
        level = 0
        for i, row in df.iterrows():
            if diff < row["levelTo"]:
                cashflow += (row["levelTo"] - level) * row["bid"]
                diff -= row["levelTo"]
                level = row["levelTo"]
            else:
                cashflow += diff * row["bid"]
                diff = 0
                break

        return cashflow            
    return (consolidate_settlement_period,)


@app.cell
def _(bid_offer, diffs):
    diffs.join(bid_offer, on=["settlementDate", "settlementPeriod"], how="left")
    return


@app.cell
def _(bid_offer, pl):
    price = 11

    toy_example = bid_offer.filter(
        (pl.col("settlementPeriod") == 6) & (pl.col("settlementDate") == "2025-01-02")
    ).select("levelFrom", "levelTo", "bid", "offer")

    sorted_negatives = toy_example.filter(pl.col("levelTo").lt(pl.lit(0))).sort(by="levelTo")

    zero_row = pl.DataFrame(
        {
            "levelFrom": 0,
            "levelTo": 0,
            "bid": sorted_negatives.select(pl.col("bid")).limit(1).item(),
            "offer": sorted_negatives.select(pl.col("offer")).limit(1).item(),
        }
    )

    bid_price_table = bid_offer.filter(
        (pl.col("settlementPeriod") == 6) & (pl.col("settlementDate") == "2025-01-02")
    ).select("levelFrom", "levelTo", "bid", "offer")\
     .extend(zero_row)\
     .sort(by="levelFrom")\
     .with_columns(
         pl.col("levelTo").shift(-1),
         pl.col("bid").shift(-1),
         pl.col("offer").shift(-1),
         pl.lit(price).alias("diff")
     )

    # TODO swap for negative
    bid_price_table.filter(
        pl.col("levelFrom").add(pl.lit(0.5)).sign() == pl.col("diff").sign()
    ).with_columns(
        pl.when(
            pl.col("diff") > pl.col("levelTo")
        ).then(
            pl.col("levelTo")
        ).otherwise(
            pl.when(
                pl.col("diff") < pl.col("levelFrom")
            ).then(
                pl.lit(0)
            ).otherwise(
                pl.col("diff").sub(pl.col("levelFrom"))
            )
        ).alias("diff_in_range")
    ).with_columns(
        pl.col("diff_in_range").mul(
            pl.when(pl.col("diff") < 0).then(
                pl.col("bid")
            ).otherwise(
                pl.col("offer")
            )
        )
    )
    return


@app.cell
def _(bid_offer, consolidate_settlement_period, diffs):
    diffs.join(bid_offer, on=["settlementDate", "settlementPeriod"], how="left").to_pandas().groupby(
        ["settlementDate", "settlementPeriod"]
    ).apply(consolidate_settlement_period)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
