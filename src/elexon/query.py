import asyncio
import datetime
from typing import Callable, Literal, Optional

import aiohttp
import pandas as pd
import polars as pl


def long_date_range_handler(
    func: Callable,
    max_concurrent: int = 10,
    timeout_seconds: int = 30,
):
    """Wraps request functions and ensures that the max date-range error is worked around"""

    async def wrapper(bm_unit: str, from_time: str, to_time: str):
        start_dt = datetime.datetime.strptime(from_time, "%Y-%m-%dT%H:%M:%SZ")
        end_dt = datetime.datetime.strptime(to_time, "%Y-%m-%dT%H:%M:%SZ")

        if end_dt - start_dt > datetime.timedelta(days=6):
            dfs = []
            tasks = []
            current_start = start_dt

            while current_start < end_dt:
                current_end = min(current_start + datetime.timedelta(days=7), end_dt)
                current_start_str = current_start.strftime("%Y-%m-%dT%H:%M:%SZ")
                current_end_str = current_end.strftime("%Y-%m-%dT%H:%M:%SZ")
                tasks.append((bm_unit, current_start_str, current_end_str))
                current_start = current_end

            semaphore = asyncio.Semaphore(max_concurrent)
            timeout = aiohttp.ClientTimeout(total=timeout_seconds)

            async def bounded_fetch(
                session: aiohttp.ClientSession, *args
            ) -> Optional[pl.DataFrame]:
                async with semaphore:
                    return await func(session, *args)

            async with aiohttp.ClientSession(timeout=timeout) as session:
                dfs = await asyncio.gather(
                    *[bounded_fetch(session, *args) for args in tasks],
                    return_exceptions=True,
                )

            dfs = [d for d in dfs if d is not None and d.shape[0] > 0]
            if dfs and pl.concat(dfs).shape[0] > 0:
                return pl.concat(dfs).sort(by="timeFrom")
            return None
        else:
            timeout = aiohttp.ClientTimeout(total=timeout_seconds)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                return await func(session, bm_unit, from_time, to_time)

    return wrapper


async def _elexon_get_request_async(
    session: aiohttp.ClientSession,
    url: str,
    max_retries: int = 7,
) -> Optional[pl.DataFrame]:
    """Async version of _elexon_get_request for use with aiohttp.

    Includes retry logic with exponential backoff for rate limiting (429).
    """
    delays = [
        50,
        10,
        20,
        30,
        30,
        30,
        30,
    ]  # https://bmrs.elexon.co.uk/api-documentation/guidance
    for attempt in range(max_retries):
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                return pl.DataFrame(data.get("data"))
            elif response.status == 429:
                delay = delays[attempt]
                print(
                    f"Rate limited (429), retrying in {delay:.1f}s (attempt {attempt + 1}/{max_retries})"
                )
                await asyncio.sleep(delay)
            else:
                print(f"Error: {response.status}")
                return None
    print(f"Max retries exceeded for {url}")
    return None


async def get_indicative_imbalance_settlement(
    session: aiohttp.ClientSession, settlementDate: str, settlementPeriod: int
):
    """Gets the indicative imbalance settlement for a particular settlementPeriod."""
    url = f"https://data.elexon.co.uk/bmrs/api/v1/balancing/settlement/system-prices/{settlementDate}/{settlementPeriod}"
    return await _elexon_get_request_async(session, url)


async def get_indicative_cashflow(
    session: aiohttp.ClientSession,
    time: str,
    bm_unit: str,
    cashflow_type: Literal["bid", "offer"],
) -> Optional[pl.DataFrame]:
    """Async version of get_indicative_cashflow for use with aiohttp."""
    url = (
        f"https://data.elexon.co.uk/bmrs/api/v1/balancing/settlement/indicative/cashflows/all"
        f"/{cashflow_type}/{time}?bmUnit={bm_unit}&format=json"
    )
    return await _elexon_get_request_async(session, url)


@long_date_range_handler
async def get_physical(
    session: aiohttp.ClientSession, bm_unit: str, from_time: str, to_time: str
):
    """Gets the physical notification data per BM unit"""
    url = (
        f"https://data.elexon.co.uk/bmrs/api/v1/balancing/physical?"
        f"bmUnit={bm_unit}&from={from_time}&to={to_time}&dataset=PN"
    )
    return await _elexon_get_request_async(session, url)


@long_date_range_handler
async def get_acceptances(
    session: aiohttp.ClientSession, bm_unit: str, from_time: str, to_time: str
):
    """Gets the bid-offer acceptances per BM unit"""
    url = (
        f"https://data.elexon.co.uk/bmrs/api/v1/balancing/acceptances?"
        f"bmUnit={bm_unit}&from={from_time}&to={to_time}&format=json"
    )
    return await _elexon_get_request_async(session, url)


@long_date_range_handler
async def get_bid_offer(
    session: aiohttp.ClientSession, bm_unit: str, from_time: str, to_time: str
):
    """Gets the bid-offer pairs per BM unit"""
    url = (
        f"https://data.elexon.co.uk/bmrs/api/v1/balancing/bid-offer?"
        f"bmUnit={bm_unit}&from={from_time}&to={to_time}"
    )
    return await _elexon_get_request_async(session, url)


async def fetch_indicative_cashflows_batch(
    tasks: list[tuple[str, str, str]],
    max_concurrent: int = 10,
    timeout_seconds: int = 30,
) -> list[pl.DataFrame | Exception]:
    """Fetch multiple indicative cashflows concurrently with rate limiting.

    Args:
        tasks: List of (time, bm_unit, flow_type) tuples to fetch.
        max_concurrent: Maximum number of concurrent requests.
        timeout_seconds: Timeout for each request in seconds.

    Returns:
        List of DataFrames or Exceptions for failed requests.
    """
    semaphore = asyncio.Semaphore(max_concurrent)
    timeout = aiohttp.ClientTimeout(total=timeout_seconds)

    async def bounded_fetch(
        session: aiohttp.ClientSession,
        time: str,
        bm_unit: str,
        cashflow_type: Literal["bid", "offer"],
    ) -> Optional[pl.DataFrame]:
        async with semaphore:
            return await get_indicative_cashflow(session, time, bm_unit, cashflow_type)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        results = await asyncio.gather(
            *[
                bounded_fetch(session, time, bm_unit, cashflow_type)
                for time, bm_unit, cashflow_type in tasks
            ],
            return_exceptions=True,
        )

    return results


async def fetch_unit_cashflows(
    unit: str, from_time: str, to_time: str, cashflow_type: Literal["bid", "offer"]
) -> list[pl.DataFrame]:
    """Fetch all cashflow data for a single unit using async requests."""
    tasks = [
        (str(_d).split(" ")[0], unit, cashflow_type)
        for _d in pd.date_range(from_time, to_time)
    ]
    results = await fetch_indicative_cashflows_batch(tasks, max_concurrent=20)

    dfs = []
    for result in results:
        if isinstance(result, Exception):
            print(f"Request failed: {result}")
            continue
        if result is not None and not result.is_empty():
            dfs.append(
                result.select(
                    "settlementDate", "settlementPeriod", "bmUnit", "totalCashflow"
                )
            )
    return dfs


async def _fetch_imbalance_settlement_batch(
    tasks: list[tuple[str, str]],
    max_concurrent: int = 10,
    timeout_seconds: int = 30,
) -> list[pl.DataFrame | Exception]:
    semaphore = asyncio.Semaphore(max_concurrent)
    timeout = aiohttp.ClientTimeout(total=timeout_seconds)

    async def bounded_fetch(
        session: aiohttp.ClientSession, settlementDate: str, settlementPeriod: str
    ) -> Optional[pl.DataFrame]:
        async with semaphore:
            return await get_indicative_imbalance_settlement(
                session, settlementDate, settlementPeriod
            )

    async with aiohttp.ClientSession(timeout=timeout) as session:
        results = await asyncio.gather(
            *[
                bounded_fetch(session, settlementDate, settlementPeriod)
                for settlementDate, settlementPeriod in tasks
            ],
            return_exceptions=True,
        )

    return results


async def fetch_imbalance_settlement(
    from_time: str, to_time: str
) -> list[pl.DataFrame]:
    """Fetch all cashflow data for a single unit using async requests."""
    tasks = [
        (str(_d).split(" ")[0], i)
        for _d in pd.date_range(from_time, to_time)
        for i in range(1, 49)
    ]
    results = await _fetch_imbalance_settlement_batch(tasks, max_concurrent=20)

    dfs = []
    for result in results:
        if isinstance(result, Exception):
            print(f"Request failed: {result}")
            continue
        if result is not None and not result.is_empty():
            dfs.append(
                result.select(
                    "settlementDate",
                    "settlementPeriod",
                    "systemSellPrice",
                    "systemBuyPrice",
                    "netImbalanceVolume",
                    "totalAcceptedOfferVolume",
                    "totalAcceptedBidVolume",
                    "totalAdjustmentSellVolume",
                    "totalAdjustmentBuyVolume",
                )
            )
    return pl.concat(dfs)
