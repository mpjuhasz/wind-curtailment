import marimo

__generated_with = "0.18.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import polars as pl
    import altair as alt
    return alt, pl


@app.cell
def _(pl):
    df = pl.read_csv("./data/processed/all/analysis/all_offers.csv")
    return (df,)


@app.cell
def _(alt, df, pl):
    _data = df.filter(pl.col("settlementDate").eq("2024-05-01"))

    _chart = alt.Chart(_data.to_pandas()).mark_circle(size=60).encode(
        x="settlementPeriod",
        y="offer",
        color="accepted",
        tooltip=["offer", "bm_unit"]
    ).interactive()

    _chart.show()
    return


@app.cell
def _(alt, df, pl):
    _data =  df.filter(pl.col("settlementDate").eq("2024-05-01") & pl.col("settlementPeriod").eq(1))

    _chart = alt.Chart(_data.to_pandas()).transform_density(
        density='offer',
        as_=['offer', 'density'],
        groupby=['accepted'],
    ).mark_area(opacity=0.5, orient="horizontal").encode(
        x=alt.X(
            'density:Q',
            stack="center",
        ),
        y='offer:Q',
        column=alt.Column("accepted:N"),
        color='accepted:N'
    ).interactive()

    _chart.show()
    return


@app.cell
def _(alt, pl):
    bids = pl.read_csv("./data/processed/all/analysis/all_bids.csv")

    _data =  bids.filter(pl.col("bm_unit").str.starts_with("T_BEATO"))
    _d = _data.group_by(["settlementDate"]).agg(
        pl.col("bid").mean().alias("avg_bid"),
        pl.col("bid").max().alias("max_bid"),
        pl.col("bid").min().alias("min_bid"),
    ).sort(["settlementDate"])


    _chart = alt.Chart(_d.to_pandas()).mark_line().encode(
        x="settlementDate:T",
        y="avg_bid:Q",
        tooltip=["settlementDate", "avg_bid"]
    ) + alt.Chart(_d.to_pandas()).mark_area(opacity=0.3).encode(
        x="settlementDate:T",
        y="max_bid:Q",
        y2="min_bid:Q",
        tooltip=["settlementDate:T", "max_bid", "min_bid"]
    ).interactive()


    _chart.show()
    return (bids,)


@app.cell
def _(alt, bids, pl):
    _data =  bids.filter(pl.col("bm_unit").str.starts_with("T_MOWEO"))
    _d = _data.group_by(["settlementDate"]).agg(
        pl.col("bid").mean().alias("avg_bid"),
        pl.col("bid").max().alias("max_bid"),
        pl.col("bid").min().alias("min_bid"),
    ).sort(["settlementDate"])


    _chart = alt.Chart(_d.to_pandas()).mark_line().encode(
        x="settlementDate:T",
        y="avg_bid:Q",
        tooltip=["settlementDate:T", "avg_bid"]
    ) + alt.Chart(_d.to_pandas()).mark_area(opacity=0.3).encode(
        x="settlementDate:T",
        y="max_bid:Q",
        y2="min_bid:Q",
        tooltip=["settlementDate:T", "max_bid", "min_bid"]
    ).interactive()


    _chart.show()
    return


@app.cell
def _(bids, pl):
    bids.filter(pl.col("bm_unit").str.starts_with("T_MOWEO"))
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
