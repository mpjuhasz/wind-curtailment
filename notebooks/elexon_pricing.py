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
    import requests
    from typing import Optional, Callable
    import pandas as pd
    from pathlib import Path
    import datetime
    import altair as alt
    from elexon.utils import aggregate_acceptance_and_pn
    return (
        Callable,
        Path,
        aggregate_acceptance_and_pn,
        alt,
        datetime,
        pd,
        pl,
        requests,
    )


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
    # bm_unit = "T_MOWEO-1"
    # bm_unit = "T_DINO-2"
    bm_unit = "T_VKNGW-2"
    from_time = "2025-01-01T00:00Z"
    to_time = "2025-01-10T00:00Z"
    return bm_unit, from_time, to_time


@app.cell
def _(bm_unit, date_range_wrapper, from_time, get_bid_offer, to_time):
    bid_offer = date_range_wrapper(bm_unit, from_time, to_time, get_bid_offer)
    return (bid_offer,)


@app.cell(hide_code=True)
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
def _(bm_unit, get_indicative_cashflow):
    from_time_date = "2025-01-02"

    indicative_cashflow = get_indicative_cashflow(from_time_date, bm_unit)

    indicative_cashflow
    return (indicative_cashflow,)


@app.cell(hide_code=True)
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
def _(
    bm_unit,
    date_range_wrapper,
    from_time,
    get_boal,
    get_physical,
    smoothen_accepted,
    to_time,
):
    physical = date_range_wrapper(bm_unit, from_time, to_time, get_physical)
    accepted = date_range_wrapper(bm_unit, from_time, to_time, get_boal)
    result_accepted = smoothen_accepted(accepted)
    return accepted, physical, result_accepted


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


@app.cell(hide_code=True)
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
    71.453543413 / (75 - 16), 99.308314574 / 82
    return


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
    return


@app.cell
def _(bid_offer, diffs):
    diffs.join(bid_offer, on=["settlementDate", "settlementPeriod"], how="left")
    return


@app.cell
def _(bid_offer, pl):
    price = 20

    toy_example = bid_offer.filter(
        (pl.col("settlementPeriod") == 29) & (pl.col("settlementDate") == "2025-01-02")
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

    bid_price_table = toy_example.extend(zero_row)\
     .sort(by="levelFrom")\
     .with_columns(
         pl.col("levelTo").shift(-1),
         pl.col("bid").shift(-1),
         pl.col("offer").shift(-1),
         pl.lit(price).alias("diff")
     )

    expr_positive_diff = pl.when(
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


    expr_negative_diff = pl.when(
        pl.col("diff") < pl.col("levelFrom")
    ).then(
        pl.col("levelFrom")  # outside of this range (in the negative direction)
    ).otherwise(
        pl.when(
            pl.col("diff") > pl.col("levelTo")
        ).then(
            pl.lit(0)  # outside of this range (closer to 0)
        ).otherwise(
            pl.col("diff").sub(pl.col("levelTo"))  # in the range
        )
    ).alias("diff_in_range")


    bid_price_table.with_columns(
        pl.when(pl.col("diff") > 0).then(
            expr_positive_diff
        ).otherwise(
            expr_negative_diff
        )
    ).with_columns(
        pl.col("diff_in_range").mul(
            pl.when(pl.col("diff") < 0).then(
                pl.col("bid")
            ).otherwise(
                pl.col("offer")
            )
        ).alias("diff_price")
    ).to_dict(as_series=False)
    return (toy_example,)


@app.cell
def _(toy_example):
    toy_example.to_dict(as_series=False)
    return


@app.cell
def _(pl):
    from typing import Literal

    def format_bid_price_table(df: pl.DataFrame) -> pl.DataFrame:
        """Formats the bids and prices adding a row with zero so that the intervals are complete"""
        sorted_negatives = df.filter(pl.col("levelTo").lt(pl.lit(0))).sort(by="levelTo")

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

        bid_price_table = df.extend(zero_row)\
         .sort(by="levelFrom")\
         .with_columns(
             pl.col("levelTo").shift(-1),
             pl.col("bid").shift(-1),
             pl.col("offer").shift(-1),
             pl.col("curtailment"),
             pl.col("extra"),
         )

        return bid_price_table



    expr_curtailment = pl.when(
        pl.col("curtailment") < pl.col("levelFrom")
    ).then(
        pl.col("levelFrom")  # outside of this range (in the negative direction)
    ).otherwise(
        pl.when(
            pl.col("curtailment") > pl.col("levelTo")
        ).then(
            pl.lit(0)  # outside of this range (closer to 0)
        ).otherwise(
            pl.col("curtailment").sub(pl.col("levelTo"))  # in the range
        )
    ).alias("curtailment_in_range")

    expr_extra = pl.when(
        pl.col("extra") > pl.col("levelTo")
    ).then(
        pl.col("levelTo")
    ).otherwise(
        pl.when(
            pl.col("extra") < pl.col("levelFrom")
        ).then(
            pl.lit(0)
        ).otherwise(
            pl.col("extra").sub(pl.col("levelFrom"))
        )
    ).alias("extra_in_range")


    c_or_e_map = {
        "curtailment": ("bid", expr_curtailment),
        "extra": ("offer", expr_extra),
    }

    def calculate_cashflow_nb(df: pl.DataFrame) -> float:
        """Calculates the cashflow for a single settlement period"""
        bid_price_table = format_bid_price_table(df.select("levelFrom", "levelTo", "bid", "offer", "curtailment", "extra"))

        prices = dict()
        for col, (price_col, expr) in c_or_e_map.items():
            if df.select(pl.col(col)).limit(1).item() == 0:
                prices[col] = 0.0
                continue
            prices[col] = bid_price_table.with_columns(expr).with_columns(
                pl.col(f"{col}_in_range").mul(pl.col(price_col)).alias(f"{col}_price")
            ).select(pl.col(f"{col}_price").sum()).item()

        return df.with_columns(
            pl.lit(v).alias(f"calculated_cashflow_{k}") for k, v in prices.items()
        )
    return (calculate_cashflow_nb,)


@app.cell
def _(bid_offer, calculate_cashflow_nb, diffs):
    diffs.join(bid_offer, on=["settlementDate", "settlementPeriod"], how="left").group_by(
        "settlementDate", "settlementPeriod"
    ).map_groups(calculate_cashflow_nb)
    # .select(
    #     "settlementDate", "settlementPeriod", "time", "calculated_cashflow_curtailment", "calculated_cashflow_extra",
    #     "curtailment", "extra", "physical_level", "generated", "bmUnit"
    # )
    return


@app.cell
def _(diffs):
    diffs
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## On live data
    """)
    return


@app.cell
def _():
    from elexon.utils import cashflow
    return (cashflow,)


@app.cell
def _(Path):
    generation_folder = Path("./data/processed/gen-viking-2024-mwh/")
    bid_offer_folder = Path("./data/processed/bo-viking-2024-mwh/")
    return bid_offer_folder, generation_folder


@app.cell
def _():
    # _g = pl.read_csv(generation_folder / f"{bm_unit}.csv")
    # _g = _g.with_columns(
    #     pl.col("time").str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%S%.f").alias("time"),
    # )
    # # _g.group_by_dynamic("time", every="30m").agg(
    # #     pl.col("generated").sum().alias("generated"),
    # #     pl.col("physical_level").sum().alias("physical_level"),
    # #     pl.col("curtailment").sum().alias("curtailment"),
    # #     pl.col("extra").sum().alias("extra"),
    # # ).sort(by="time")

    # # MWh
    # _g = _g.with_columns(
    #     pl.col("generated").mul(1_000).alias("generated"),
    #     pl.col("physical_level").mul(1_000).alias("physical_level"),
    #     pl.col("curtailment").mul(1_000).alias("curtailment"),
    #     pl.col("extra").mul(1_000).alias("extra"),
    # )

    # _bo = pl.read_csv(bid_offer_folder / f"{bm_unit}.csv")

    # cashflow(_bo, _g).select("calculated_cashflow_curtailment", "calculated_cashflow_extra").sum()
    return


@app.cell
def _(mo):
    mo.md(r"""
    This number is pretty shocking, I'll have to double check if this is correct.
    """)
    return


@app.cell
def _(bid_offer_folder, cashflow, generation_folder, pl):
    total_cashflows = []
    total_gens = []

    for i in generation_folder.glob("*.csv"):
        unit = i.stem

        _g = pl.read_csv(generation_folder / f"{unit}.csv")
        _g = _g.with_columns(
            pl.col("time").str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%S%.f").alias("time"),
        )

        # MWh
        _g = _g.with_columns(
            pl.col("generated").alias("generated"),
            pl.col("physical_level").alias("physical_level"),
            pl.col("curtailment").alias("curtailment"),
            pl.col("extra").alias("extra"),
        )
        total_gens.append(_g)

        try:
            _bo = pl.read_csv(bid_offer_folder / f"{unit}.csv")
        except:
            print(f"Not found: {unit}")
            continue
    
        total_cashflows.append(
            cashflow(_bo, _g).select(
                pl.col("bmUnit").first(),
                pl.col("calculated_cashflow_curtailment").sum(),
                pl.col("calculated_cashflow_extra").sum()
            )
        )
    return total_cashflows, total_gens


@app.cell
def _(pl, total_gens):
    vals = pl.concat(
        total_gens
    ).group_by("settlementPeriod", "settlementDate", "time").agg(
        pl.col("generated").sum(),
        pl.col("curtailment").sum(),
        pl.col("extra").sum(),
        pl.col("physical_level").sum()
    ).sort(by=["settlementDate", "settlementPeriod"]).sum()

    abs(vals.select("curtailment").item() / vals.select("physical_level").item()), vals.select("generated").item() / 1_000_000
    return (vals,)


@app.cell
def _(vals):
    vals
    return


@app.cell
def _(pl, total_cashflows):
    pl.concat(total_cashflows).sum()
    return


@app.cell
def _(mo):
    mo.md(r"""
    There already are conflicting figures for Seagreen 2024:
    - https://www.ref.org.uk/ref-blog/384-discarded-wind-energy-increases-by-91-in-2024 says:
      "in 2024 the consumer paid Seagreen £104 million for actually generating electricity, plus £198 million for the constrained volumes, and £64 million for the premium charged to reduce output"
    - https://www.bbc.co.uk/news/articles/cdedjnw8e85o refers to Octopus, £65 million figure with 71% curtailment
    - octopus figure with the 71%: https://octopus.energy/press/as-wasted-wind-is-set-to-hit-650m-so-far-this-year-octopus-introduces-wasted-wind-ticker-on-homepage-to-highlight-colossal-costs-of-broken-energy-system/
    - https://www.telegraph.co.uk/business/2025/02/21/wind-farm-was-paid-65m-cut-power-output-three-quarters/ :
      The Seagreen offshore wind farm in the North Sea – the largest of its kind in Scotland – had its output curtailed for 71pc of the      time it was due to operate in 2024, grid data show.
      This meant that of 4.7 terawatt hours of power its turbines generated, 3.3 terawatt hours were effectively discarded – with owner SSE paid by grid operators each time this happened.
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    The same for viking:
    - https://www.telegraph.co.uk/business/2025/02/21/wind-farm-was-paid-65m-cut-power-output-three-quarters/ :
      SSE also owns the Viking wind farm in the Shetlands, which had 57pc of its output curtailed last year at a cost of £10m. It was only switched on in August.
    The two sites have been paid another £1.5m so far this year for cutting output.
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    This isn't in line with: https://wastedwind.energy/2025-11-29.
    """)
    return


@app.cell
def _():
    from elexon.query import get_physical as gp
    return (gp,)


@app.cell
def _(gp, pl):
    gp("T_SGRWO-1", "2024-01-01T00:00:00Z", "2025-01-01T00:00:00Z").filter(
        pl.col("settlementDate").eq("2024-01-01") & pl.col("settlementPeriod").eq(6)
    )
    return


@app.cell
def _(get_indicative_cashflow, pd):
    vik_vals = []

    for _unit in ["T_VKNGW-1", "T_VKNGW-2", "T_VKNGW-3", "T_VKNGW-4"]:
        for _d in pd.date_range("2024-01-01", "2025-01-01"):
            if not get_indicative_cashflow(str(_d).split(" ")[0], _unit).is_empty():
                vik_vals.append(get_indicative_cashflow(str(_d).split(" ")[0], _unit).select("totalCashflow").sum())
    return (vik_vals,)


@app.cell
def _(vik_vals):
    sum(vik_vals)
    return


@app.cell
def _(get_indicative_cashflow, pd):
    sea_vals = []

    for _unit in ["T_SGRWO-1", "T_SGRWO-3", "T_SGRWO-6", "T_SGRWO-5", "T_SGRWO-4", "T_SGRWO-2"]:
        for _d in pd.date_range("2024-01-01", "2025-01-01"):
            if not get_indicative_cashflow(str(_d).split(" ")[0], _unit).is_empty():
                sea_vals.append(get_indicative_cashflow(str(_d).split(" ")[0], _unit).select("totalCashflow").sum())

    sum(sea_vals)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
