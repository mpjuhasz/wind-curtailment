import marimo

__generated_with = "0.18.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import polars as pl
    return (pl,)


@app.cell
def _():
    bad_unit = "T_LKSDB-1"

    data_folder = "./data/processed/test-bess-orchestration/"
    return bad_unit, data_folder


@app.cell
def _(bad_unit, data_folder, pl):
    print(pl.read_csv(f"{data_folder}/bid_offer/{bad_unit}.csv", try_parse_dates=True).filter(
        pl.col("settlementDate").is_between(pl.datetime(2024, 12, 10), pl.datetime(2024, 12, 10)) &
        pl.col("settlementPeriod").eq(pl.lit(39))
    ).to_pandas().to_markdown())
    return


@app.cell
def _(bad_unit, data_folder, pl):
    print(pl.read_csv(f"{data_folder}/generation/{bad_unit}.csv", try_parse_dates=True).filter(
        pl.col("settlementDate").is_between(pl.datetime(2024, 12, 10), pl.datetime(2024, 12, 10)) &
        pl.col("settlementPeriod").eq(pl.lit(39))
    ).to_pandas().to_markdown())
    return


@app.cell
def _(bad_unit, data_folder, pl):
    pl.read_csv(f"{data_folder}/calculated_cashflow/{bad_unit}.csv", try_parse_dates=True).filter(
        pl.col("settlementDate").is_between(pl.datetime(2024, 12, 10), pl.datetime(2024, 12, 11))
    )
    return


@app.cell
def _(bad_unit, data_folder, pl):
    pl.read_csv(f"{data_folder}/indicative_cashflow/{bad_unit}.csv", try_parse_dates=True).filter(
        pl.col("settlementDate").is_between(pl.datetime(2024, 12, 10), pl.datetime(2024, 12, 11))
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
