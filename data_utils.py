import os
import time
import json
import hmac
import hashlib
import base64
import requests
import pandas as pd
from datetime import datetime, timezone
from typing import Optional, List, Dict, Union, Any

def parse_okx_orderbook_file(
    file_path: Union[str, os.PathLike],
    as_dataframe: bool = True,
    convert_timestamp: bool = True,
    nrows: float | None = None
) -> Union[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Parse a local OKX Level 2 orderbook dump (newline-delimited JSON) into structured data.

    Args:
        file_path: Path to the `.data` file downloaded from OKX.
        as_dataframe: Return a `pandas.DataFrame` when True, otherwise return a list of dictionaries.
        convert_timestamp: When True, append a UTC `timestamp` derived from the millisecond `ts`.

    Returns:
        pandas.DataFrame or list: Orderbook rows with columns/keys:
            ['instrument', 'action', 'side', 'price', 'size', 'count', 'ts', 'timestamp' (optional)]

    Raises:
        FileNotFoundError: If `file_path` does not point to an existing file.
        ValueError: If any line in the file cannot be decoded as JSON.
    """
    path_str = os.fspath(file_path)
    if not os.path.exists(path_str):
        raise FileNotFoundError(f"File not found: {path_str}")

    records: List[Dict[str, Any]] = []

    with open(path_str, "r", encoding="utf-8") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            if nrows is not None and line_number > nrows:
                break

            raw_line = raw_line.strip()
            if not raw_line:
                continue

            try:
                payload = json.loads(raw_line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Failed to parse JSON on line {line_number}: {exc}") from exc

            ts_raw = payload.get("ts")
            try:
                ts_value = int(ts_raw) if ts_raw is not None else None
            except (TypeError, ValueError):
                ts_value = None

            instrument = payload.get("instId")
            action = payload.get("action")

            for side_key in ("bids", "asks"):
                levels = payload.get(side_key) or []
                side = "bid" if side_key == "bids" else "ask"

                for level in levels:
                    if not isinstance(level, (list, tuple)) or len(level) < 2:
                        continue

                    price_raw = level[0]
                    size_raw = level[1]
                    count_raw = level[2] if len(level) > 2 else None

                    try:
                        price_val = float(price_raw)
                    except (TypeError, ValueError):
                        price_val = None

                    try:
                        size_val = float(size_raw)
                    except (TypeError, ValueError):
                        size_val = None

                    try:
                        count_val = int(count_raw) if count_raw is not None else None
                    except (TypeError, ValueError):
                        count_val = None

                    records.append(
                        {
                            "instrument": instrument,
                            "action": action,
                            "side": side,
                            "price": price_val,
                            "size": size_val,
                            "count": count_val,
                            "ts": ts_value,
                        }
                    )

            # If no bids/asks were present, still keep a placeholder record so nothing is lost.
            if not payload.get("bids") and not payload.get("asks"):
                records.append(
                    {
                        "instrument": instrument,
                        "action": action,
                        "side": None,
                        "price": None,
                        "size": None,
                        "count": None,
                        "ts": ts_value,
                    }
                )

    def _attach_timestamp(target_records: List[Dict[str, Any]]) -> None:
        for record in target_records:
            ts_entry = record.get("ts")
            try:
                record["timestamp"] = (
                    pd.to_datetime(ts_entry, unit="ms", utc=True) if ts_entry is not None else pd.NaT
                )
            except (TypeError, ValueError, OverflowError):
                record["timestamp"] = pd.NaT

    if not as_dataframe:
        if convert_timestamp:
            _attach_timestamp(records)
        return records

    df = pd.DataFrame.from_records(records, columns=["instrument", "action", "side", "price", "size", "count", "ts"])

    if df.empty:
        return df

    if convert_timestamp:
        df["timestamp"] = pd.to_datetime(df["ts"], unit="ms", utc=True, errors="coerce")

    return df

def rebuild_snapshots_from_updates(df: pd.DataFrame, depth: int | None = None) -> pd.DataFrame:
    df = df.sort_values(['instrument', 'ts', 'side', 'price']).reset_index(drop=True)
    rebuilt_rows = []

    for instrument, inst_df in df.groupby('instrument'):
        book = {'bid': {}, 'ask': {}}
        have_snapshot = False

        for ts, ts_chunk in inst_df.groupby('ts'):
            action = ts_chunk['action'].iloc[0]

            if action == 'snapshot':
                book = {'bid': {}, 'ask': {}}
                have_snapshot = True
                for side in ('bid', 'ask'):
                    side_rows = ts_chunk[ts_chunk['side'] == side]
                    for _, row in side_rows.iterrows():
                        if pd.notna(row['price']) and pd.notna(row['size']):
                            book[side][row['price']] = row['size']

            elif action == 'update' and have_snapshot:
                for _, row in ts_chunk.iterrows():
                    side, price, size = row['side'], row['price'], row['size']
                    if side not in ('bid', 'ask') or pd.isna(price):
                        continue
                    if pd.isna(size) or size == 0:
                        book[side].pop(price, None)
                    else:
                        book[side][price] = size
            else:
                continue  # ignore updates that arrive before the first snapshot

            ts_timestamp = (
                ts_chunk['timestamp'].iloc[0]
                if 'timestamp' in ts_chunk.columns
                else pd.to_datetime(ts, unit='ms', utc=True)
            )

            for side in ('bid', 'ask'):
                levels = sorted(
                    book[side].items(),
                    key=(lambda kv: -kv[0]) if side == 'bid' else (lambda kv: kv[0])
                )
                if depth is not None:
                    levels = levels[:depth]
                for price, size in levels:
                    rebuilt_rows.append(
                        {
                            'instrument': instrument,
                            'action': 'snapshot',
                            'side': side,
                            'price': price,
                            'size': size,
                            'ts': ts,
                            'timestamp': ts_timestamp,
                        }
                    )

    return pd.DataFrame(rebuilt_rows).sort_values(['instrument', 'ts', 'side', 'price']).reset_index(drop=True)

def rebuild_snapshots_every_100ms(df: pd.DataFrame, freq_ms: int = 100, depth: int | None = None) -> pd.DataFrame:
    """
    Replay OKX order-book snapshots + updates but only emit a synthetic snapshot every `freq_ms`.
    """
    df = df.sort_values(["instrument", "ts", "side", "price"]).reset_index(drop=True)

    output_rows: list[dict] = []

    for instrument, inst_df in df.groupby("instrument"):
        book = {"bid": {}, "ask": {}}
        have_snapshot = False
        next_emit_ts: int | None = None

        for ts, ts_chunk in inst_df.groupby("ts"):
            action = ts_chunk["action"].iloc[0]

            if action == "snapshot":
                book = {"bid": {}, "ask": {}}
                have_snapshot = True
                for side in ("bid", "ask"):
                    side_rows = ts_chunk[ts_chunk["side"] == side]
                    for _, row in side_rows.iterrows():
                        if pd.notna(row["price"]) and pd.notna(row["size"]):
                            book[side][row["price"]] = row["size"]
                next_emit_ts = ts if next_emit_ts is None else max(next_emit_ts, ts)

            elif action == "update" and have_snapshot:
                for _, row in ts_chunk.iterrows():
                    side, price, size = row["side"], row["price"], row["size"]
                    if side not in ("bid", "ask") or pd.isna(price):
                        continue
                    if pd.isna(size) or size == 0:
                        book[side].pop(price, None)
                    else:
                        book[side][price] = size

            else:
                continue  # ignore updates before the first snapshot

            if not have_snapshot:
                continue

            if next_emit_ts is None:
                next_emit_ts = ts

            while next_emit_ts <= ts:
                ts_dt = pd.to_datetime(next_emit_ts, unit="ms", utc=True)
                for side in ("bid", "ask"):
                    levels = sorted(
                        book[side].items(),
                        key=(lambda kv: -kv[0]) if side == "bid" else (lambda kv: kv[0]),
                    )
                    if depth is not None:
                        levels = levels[:depth]
                    for price, size in levels:
                        output_rows.append(
                            {
                                "instrument": instrument,
                                "action": "snapshot",
                                "side": side,
                                "price": price,
                                "size": size,
                                "ts": next_emit_ts,
                                "timestamp": ts_dt,
                            }
                        )
                next_emit_ts += freq_ms

    return (
        pd.DataFrame(output_rows)
        .sort_values(["instrument", "ts", "side", "price"])
        .reset_index(drop=True)
    )