#!/usr/bin/env python3
"""
Utilities for parsing Binance order book recorder output into analysis-ready structures.

This module works with the gzipped NDJSON files produced by `binance_orderbook_recorder.py`.
It provides helpers to iterate the raw events, convert them into pandas DataFrames, derive
top-of-book metrics, and export the results in several convenient formats for downstream
visualization or research.
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, MutableMapping, Optional, Sequence, Tuple, Union

import numpy as np
import pandas as pd

RawEvent = MutableMapping[str, object]
NormalizedLevels = List[Tuple[float, float]]
NormalizedEvent = Dict[str, object]
PathLike = Union[str, Path]


def iter_snapshot_records(
    path: PathLike,
    *,
    limit: Optional[int] = None,
    normalize: bool = True,
) -> Iterator[NormalizedEvent]:
    """
    Stream order book snapshot entries saved by the recorder's snapshot mode.

    Each record contains full bid/ask ladders fetched via REST along with `ts_utc`
    and `lastUpdateId`.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(path)

    with gzip.open(path, mode="rt", encoding="utf-8") as fh:
        for idx, line in enumerate(fh):
            if limit is not None and idx >= limit:
                break
            raw: RawEvent = json.loads(line)
            if normalize:
                bids = _normalize_levels(raw.get("bids", []), is_bid=True)
                asks = _normalize_levels(raw.get("asks", []), is_bid=False)
                yield {
                    "ts_utc": raw.get("ts_utc"),
                    "symbol": raw.get("symbol"),
                    "last_update_id": raw.get("lastUpdateId"),
                    "bids": bids,
                    "asks": asks,
                }
            else:
                yield raw


def load_snapshot_dataframe(
    path: PathLike,
    *,
    limit: Optional[int] = None,
) -> pd.DataFrame:
    """
    Load snapshot records into a DataFrame for downstream analysis or plotting.

    Adds a timezone-aware datetime column parsed from `ts_utc`.
    """
    records = list(iter_snapshot_records(path, limit=limit, normalize=True))
    if not records:
        return pd.DataFrame(columns=[
            "ts_utc",
            "symbol",
            "last_update_id",
            "bids",
            "asks",
            "ts_dt",
        ])

    df = pd.DataFrame(records)
    df["ts_dt"] = pd.to_datetime(df["ts_utc"], utc=True)
    return df


def iter_raw_events(
    path: PathLike,
    *,
    limit: Optional[int] = None,
    normalize: bool = True,
) -> Iterator[NormalizedEvent]:
    """
    Stream depth update events from a gzipped NDJSON file.

    Args:
        path: Path to a `.ndjson.gz` file produced by the recorder.
        limit: Optional cap on the number of events to yield (useful for quick tests).
        normalize: If True, price/quantity strings are converted into floats and a few
            helper fields are appended. If False, the original event dictionary is returned.

    Yields:
        Dict representing a single Binance `depthUpdate` message.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(path)

    with gzip.open(path, mode="rt", encoding="utf-8") as fh:
        for idx, line in enumerate(fh):
            if limit is not None and idx >= limit:
                break
            raw: RawEvent = json.loads(line)
            yield _normalize_event(raw) if normalize else raw


def load_events_dataframe(
    path: PathLike,
    *,
    limit: Optional[int] = None,
) -> pd.DataFrame:
    """
    Load raw depth updates into a pandas DataFrame.

    Columns include:
        - event_time: Binance event timestamp (ms since epoch)
        - event_type: Event name (should be "depthUpdate")
        - symbol: Trading symbol (e.g., BTCUSDT)
        - first_update_id / last_update_id / prev_update_id: Depth diff metadata
        - bids / asks: Lists of (price, quantity) tuples as floats
        - event_dt: tz-aware pandas datetime for easy plotting/resampling
    """
    events = list(iter_raw_events(path, limit=limit, normalize=True))
    if not events:
        return pd.DataFrame(columns=_dataframe_columns())

    df = pd.DataFrame(events)
    df["event_dt"] = pd.to_datetime(df["event_time"], unit="ms", utc=True)
    return df


def compute_top_of_book(df: pd.DataFrame) -> pd.DataFrame:
    """
    Derive best bid/ask, mid-price, and spread from a depth updates DataFrame.

    Args:
        df: DataFrame returned by `load_events_dataframe`.

    Returns:
        New DataFrame with top-of-book metrics aligned with the original index.
    """
    if df.empty:
        return pd.DataFrame(columns=[
            "best_bid_px",
            "best_bid_qty",
            "best_ask_px",
            "best_ask_qty",
            "mid_price",
            "spread",
        ])

    best_bid_px: List[float] = []
    best_bid_qty: List[float] = []
    best_ask_px: List[float] = []
    best_ask_qty: List[float] = []

    for bids, asks in zip(df["bids"], df["asks"]):
        bid_px, bid_qty = _extract_level(bids, is_bid=True)
        ask_px, ask_qty = _extract_level(asks, is_bid=False)
        best_bid_px.append(bid_px)
        best_bid_qty.append(bid_qty)
        best_ask_px.append(ask_px)
        best_ask_qty.append(ask_qty)

    best_df = pd.DataFrame(
        {
            "best_bid_px": best_bid_px,
            "best_bid_qty": best_bid_qty,
            "best_ask_px": best_ask_px,
            "best_ask_qty": best_ask_qty,
        },
        index=df.index,
    )
    best_df["mid_price"] = (best_df["best_bid_px"] + best_df["best_ask_px"]) / 2.0
    best_df["spread"] = best_df["best_ask_px"] - best_df["best_bid_px"]
    best_df["event_dt"] = df.get("event_dt", pd.NaT)
    best_df["event_time"] = df.get("event_time")
    return best_df


def explode_levels(
    df: pd.DataFrame,
    *,
    side: str,
    depth: Optional[int] = None,
) -> pd.DataFrame:
    """
    Convert bids or asks column into a long-form DataFrame for visualization.

    Args:
        df: DataFrame returned by `load_events_dataframe`.
        side: 'bids' or 'asks'.
        depth: Optional maximum number of price levels to retain per event.

    Returns:
        DataFrame with columns: event_time, event_dt, side, level, price, quantity.
    """
    if side not in {"bids", "asks"}:
        raise ValueError("side must be 'bids' or 'asks'")

    records: List[Dict[str, object]] = []
    levels_series = df[side]
    for idx, levels in levels_series.items():
        selected = levels if depth is None else levels[:depth]
        for rank, (price, qty) in enumerate(selected, start=1):
            records.append(
                {
                    "event_time": df.at[idx, "event_time"],
                    "event_dt": df.at[idx, "event_dt"],
                    "side": side,
                    "level": rank,
                    "price": price,
                    "quantity": qty,
                }
            )

    return pd.DataFrame.from_records(records, columns=[
        "event_time",
        "event_dt",
        "side",
        "level",
        "price",
        "quantity",
    ])


def export_dataframe(
    df: pd.DataFrame,
    out_path: PathLike,
    *,
    fmt: str,
    **kwargs,
) -> Path:
    """
    Persist a DataFrame to disk for later analysis.

    Supported formats:
        - csv
        - pickle
        - npz  (NumPy archive with one array per column)

    Args:
        df: DataFrame to persist.
        out_path: Target file path (extension is not enforced).
        fmt: Desired format name (case-insensitive).
        **kwargs: Passed through to the underlying pandas or numpy writer.

    Returns:
        The resolved Path where the data was saved.
    """
    path = Path(out_path).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)

    fmt_normalized = fmt.lower()
    if fmt_normalized == "csv":
        df.to_csv(path, index=False, **kwargs)
    elif fmt_normalized in {"pickle", "pkl"}:
        df.to_pickle(path, **kwargs)
    elif fmt_normalized in {"npz", "numpy"}:
        arrays = {col: df[col].to_numpy() for col in df.columns}
        np.savez(path, **arrays)
    else:
        raise ValueError(f"Unsupported export format: {fmt}")

    return path


# ----- Internal helpers ----------------------------------------------------

def _normalize_event(raw: RawEvent) -> NormalizedEvent:
    bids = _normalize_levels(raw.get("b", []), is_bid=True)
    asks = _normalize_levels(raw.get("a", []), is_bid=False)
    return {
        "event_type": raw.get("e"),
        "event_time": raw.get("E"),
        "symbol": raw.get("s"),
        "first_update_id": raw.get("U"),
        "last_update_id": raw.get("u"),
        "prev_update_id": raw.get("pu"),
        "bids": bids,
        "asks": asks,
    }


def _normalize_levels(
    raw_levels: Sequence[Sequence[Union[str, float]]],
    *,
    is_bid: bool,
) -> NormalizedLevels:
    levels: NormalizedLevels = []
    for entry in raw_levels:
        if len(entry) != 2:
            continue
        price = float(entry[0])
        quantity = float(entry[1])
        if quantity < 0:
            # Binance should not send negative quantities, but we guard just in case.
            continue
        levels.append((price, quantity))

    # Ensure deterministic ordering for downstream calculations.
    levels.sort(key=lambda kv: kv[0], reverse=is_bid)
    return levels


def _extract_level(levels: NormalizedLevels, *, is_bid: bool) -> Tuple[float, float]:
    if not levels:
        return (float("nan"), 0.0)

    # Lists are pre-sorted so the first entry is the top-of-book.
    price, qty = levels[0]
    return price, qty


def _dataframe_columns() -> List[str]:
    return [
        "event_type",
        "event_time",
        "symbol",
        "first_update_id",
        "last_update_id",
        "prev_update_id",
        "bids",
        "asks",
        "event_dt",
    ]


__all__ = [
    "iter_raw_events",
    "iter_snapshot_records",
    "load_events_dataframe",
    "load_snapshot_dataframe",
    "compute_top_of_book",
    "explode_levels",
    "export_dataframe",
]
