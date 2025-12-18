import asyncio
import concurrent.futures
import datetime
from functools import lru_cache
from typing import Callable, Optional

import aiohttp
import polars as pl
import requests


def long_date_range_handler(func: Callable):
    """Wraps request functions and ensures that the max date-range error is worked around"""

    def wrapper(bm_unit: str, from_time: str, to_time: str):
        start_dt = datetime.datetime.strptime(from_time, "%Y-%m-%dT%H:%M:%SZ")
        end_dt = datetime.datetime.strptime(to_time, "%Y-%m-%dT%H:%M:%SZ")

        if end_dt - start_dt > datetime.timedelta(days=6):
            dfs = []
            tasks = []
            current_start = start_dt

            while current_start < end_dt:
                current_end = min(current_start + datetime.timedelta(days=5), end_dt)
                current_start_str = current_start.strftime("%Y-%m-%dT%H:%M:%SZ")
                current_end_str = current_end.strftime("%Y-%m-%dT%H:%M:%SZ")
                tasks.append((bm_unit, current_start_str, current_end_str))
                current_start = current_end

            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                future_to_task = {executor.submit(func, *task): task for task in tasks}
                for future in concurrent.futures.as_completed(future_to_task):
                    result = future.result()
                    if result is not None and not result.is_empty():
                        dfs.append(result)
            if dfs:
                return pl.concat(dfs).sort(by="timeFrom")
            return None
        else:
            return func(bm_unit, from_time, to_time)

    return wrapper


def _elexon_get_request(url: str) -> Optional[pl.DataFrame]:
    response = requests.get(url)
    if response.status_code == 200:
        return pl.DataFrame(response.json().get("data"))
    else:
        print(f"Error: {response.status_code}")
        return None


async def _elexon_get_request_async(
    session: aiohttp.ClientSession,
    url: str,
    max_retries: int = 5,
    base_delay: float = 1.0,
) -> Optional[pl.DataFrame]:
    """Async version of _elexon_get_request for use with aiohttp.
    
    Includes retry logic with exponential backoff for rate limiting (429).
    """
    for attempt in range(max_retries):
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                return pl.DataFrame(data.get("data"))
            elif response.status == 429:
                delay = base_delay * (4 ** (attempt + 1))
                print(f"Rate limited (429), retrying in {delay:.1f}s (attempt {attempt + 1}/{max_retries})")
                await asyncio.sleep(delay)
            else:
                print(f"Error: {response.status}")
                return None
    print(f"Max retries exceeded for {url}")
    return None


@lru_cache(maxsize=1024)
@long_date_range_handler
def get_physical(bm_unit: str, from_time: str, to_time: str):
    """Gets the physical notification data per BM unit"""
    url = (
        f"https://data.elexon.co.uk/bmrs/api/v1/balancing/physical?"
        f"bmUnit={bm_unit}&from={from_time}&to={to_time}&dataset=PN"
    )
    return _elexon_get_request(url)


@lru_cache(maxsize=1024)
@long_date_range_handler
def get_acceptances(bm_unit: str, from_time: str, to_time: str):
    """Gets the bid-offer acceptances per BM unit"""
    url = (
        f"https://data.elexon.co.uk/bmrs/api/v1/balancing/acceptances?"
        f"bmUnit={bm_unit}&from={from_time}&to={to_time}&format=json"
    )
    return _elexon_get_request(url)


@lru_cache(maxsize=1024)
@long_date_range_handler
def get_bid_offer(bm_unit: str, from_time: str, to_time: str):
    """Gets the bid-offer pairs per BM unit"""
    url = (
        f"https://data.elexon.co.uk/bmrs/api/v1/balancing/bid-offer?"
        f"bmUnit={bm_unit}&from={from_time}&to={to_time}"
    )
    return _elexon_get_request(url)


@lru_cache(maxsize=1024)
def get_indicative_cashflow(time: str, bm_unit: str):
    url = (
        f"https://data.elexon.co.uk/bmrs/api/v1/balancing/settlement/indicative/cashflows/all"
        f"/bid/{time}?bmUnit={bm_unit}&format=json"
    )
    return _elexon_get_request(url)


async def get_indicative_cashflow_async(
    session: aiohttp.ClientSession, time: str, bm_unit: str
) -> Optional[pl.DataFrame]:
    """Async version of get_indicative_cashflow for use with aiohttp."""
    url = (
        f"https://data.elexon.co.uk/bmrs/api/v1/balancing/settlement/indicative/cashflows/all"
        f"/bid/{time}?bmUnit={bm_unit}&format=json"
    )
    return await _elexon_get_request_async(session, url)


async def fetch_indicative_cashflows_batch(
    tasks: list[tuple[str, str]],
    max_concurrent: int = 20,
    timeout_seconds: int = 30,
) -> list[pl.DataFrame | Exception]:
    """Fetch multiple indicative cashflows concurrently with rate limiting.

    Args:
        tasks: List of (time, bm_unit) tuples to fetch.
        max_concurrent: Maximum number of concurrent requests.
        timeout_seconds: Timeout for each request in seconds.

    Returns:
        List of DataFrames or Exceptions for failed requests.
    """
    semaphore = asyncio.Semaphore(max_concurrent)
    timeout = aiohttp.ClientTimeout(total=timeout_seconds)

    async def bounded_fetch(
        session: aiohttp.ClientSession, time: str, bm_unit: str
    ) -> Optional[pl.DataFrame]:
        async with semaphore:
            return await get_indicative_cashflow_async(session, time, bm_unit)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        results = await asyncio.gather(
            *[bounded_fetch(session, time, bm_unit) for time, bm_unit in tasks],
            return_exceptions=True,
        )

    return results
