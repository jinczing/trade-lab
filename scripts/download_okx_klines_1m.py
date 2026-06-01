#!/usr/bin/env python
from __future__ import annotations

import argparse
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd
import requests

OKX_ENDPOINT = "https://www.okx.com/api/v5/market/history-candles"
INTERVAL = "1m"
INTERVAL_MS = 60_000
MAX_LIMIT = 300

RAW_COLUMNS = [
    "open_time_ms",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "vol_ccy",
    "quote_asset_volume",
    "confirm",
]

OUTPUT_COLUMNS = [
    "open_time",
    "close_time",
    "open_time_ms",
    "close_time_ms",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_asset_volume",
    "number_of_trades",
    "taker_buy_base_asset_volume",
    "taker_buy_quote_asset_volume",
]


@dataclass(frozen=True)
class DateRange:
    start_ms: int
    end_ms: int
    start_label: str
    end_label: str


def parse_ymd(text: str) -> datetime:
    parts = text.strip().split("-")
    if len(parts) != 3:
        raise ValueError(f"Invalid date '{text}', expected YYYY-MM-DD")
    year, month, day = (int(p) for p in parts)
    return datetime(year, month, day, tzinfo=timezone.utc)


def build_date_range(start_date: str, end_date: str) -> DateRange:
    start_dt = parse_ymd(start_date)
    end_dt = parse_ymd(end_date).replace(hour=23, minute=59, second=0, microsecond=0)
    if end_dt < start_dt:
        raise ValueError("end_date must be on or after start_date")
    return DateRange(
        start_ms=int(start_dt.timestamp() * 1000),
        end_ms=int(end_dt.timestamp() * 1000),
        start_label=start_dt.strftime("%Y-%m-%d"),
        end_label=parse_ymd(end_date).strftime("%Y-%m-%d"),
    )


def normalize_okx_inst_id(symbol: str) -> str:
    s = symbol.strip().upper()
    if "-" in s:
        return s

    m = re.fullmatch(r"([A-Z0-9]+)USDT", s)
    if m:
        return f"{m.group(1)}-USDT"

    raise ValueError(
        f"Unsupported symbol format '{symbol}'. Use OKX form like BTC-USDT "
        "or compact form like BTCUSDT."
    )


def compact_symbol(inst_id: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", inst_id.upper())


def request_candles(
    session: requests.Session,
    inst_id: str,
    after_ms: int,
    limit: int,
    max_retries: int,
) -> list[list]:
    params = {
        "instId": inst_id,
        "bar": INTERVAL,
        "limit": str(limit),
        # "after" returns records with ts strictly earlier than this value.
        "after": str(after_ms),
    }

    last_error: Exception | None = None
    for attempt in range(max_retries):
        try:
            response = session.get(OKX_ENDPOINT, params=params, timeout=30)
            if response.status_code == 429:
                wait_seconds = min(2 ** attempt, 30)
                print(f"[{inst_id}] rate-limited (429), waiting {wait_seconds}s")
                time.sleep(wait_seconds)
                continue
            if response.status_code >= 500:
                wait_seconds = min(2 ** attempt, 30)
                print(f"[{inst_id}] server error {response.status_code}, waiting {wait_seconds}s")
                time.sleep(wait_seconds)
                continue

            response.raise_for_status()
            payload = response.json()
            if payload.get("code") != "0":
                raise RuntimeError(
                    f"[{inst_id}] OKX API error code={payload.get('code')} msg={payload.get('msg')}"
                )
            return payload.get("data", [])
        except (requests.RequestException, RuntimeError) as exc:
            last_error = exc
            wait_seconds = min(2 ** attempt, 30)
            print(f"[{inst_id}] request error: {exc} | retry in {wait_seconds}s")
            time.sleep(wait_seconds)

    if last_error is not None:
        raise last_error
    raise RuntimeError(f"[{inst_id}] request failed for unknown reasons")


def fetch_symbol_candles(
    inst_id: str,
    start_ms: int,
    end_ms: int,
    limit: int = MAX_LIMIT,
    sleep_seconds: float = 0.08,
    max_retries: int = 8,
) -> list[list]:
    if limit < 1 or limit > MAX_LIMIT:
        raise ValueError(f"limit must be in [1, {MAX_LIMIT}]")

    session = requests.Session()
    # Add 1ms so the candle at end_ms is included.
    cursor = end_ms + 1
    all_rows: list[list] = []
    request_count = 0

    while True:
        rows = request_candles(
            session=session,
            inst_id=inst_id,
            after_ms=cursor,
            limit=limit,
            max_retries=max_retries,
        )
        if not rows:
            break

        all_rows.extend(rows)
        request_count += 1

        oldest_open_ms = int(rows[-1][0])
        newest_open_ms = int(rows[0][0])
        if oldest_open_ms >= cursor:
            raise RuntimeError(f"[{inst_id}] cursor stalled at {cursor}, oldest_open_ms={oldest_open_ms}")
        cursor = oldest_open_ms

        if request_count % 100 == 0:
            newest_dt = datetime.fromtimestamp(newest_open_ms / 1000, tz=timezone.utc).isoformat()
            oldest_dt = datetime.fromtimestamp(oldest_open_ms / 1000, tz=timezone.utc).isoformat()
            print(
                f"[{inst_id}] requests={request_count} rows={len(all_rows)} "
                f"page_range=[{oldest_dt} .. {newest_dt}]"
            )

        if oldest_open_ms <= start_ms:
            break

        time.sleep(sleep_seconds)

    print(f"[{inst_id}] completed with {request_count} requests and {len(all_rows)} raw rows")
    return all_rows


def rows_to_dataframe(rows: Iterable[list], start_ms: int, end_ms: int) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=RAW_COLUMNS)
    df["open_time_ms"] = pd.to_numeric(df["open_time_ms"], errors="coerce").astype("Int64")

    numeric_cols = ["open", "high", "low", "close", "volume", "quote_asset_volume"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["open_time_ms"])
    df["open_time_ms"] = df["open_time_ms"].astype("int64")

    df = df[(df["open_time_ms"] >= start_ms) & (df["open_time_ms"] <= end_ms)]
    df = df.sort_values("open_time_ms").drop_duplicates(subset=["open_time_ms"], keep="first")

    df["close_time_ms"] = df["open_time_ms"] + (INTERVAL_MS - 1)
    df["open_time"] = pd.to_datetime(df["open_time_ms"], unit="ms", utc=True)
    df["close_time"] = pd.to_datetime(df["close_time_ms"], unit="ms", utc=True)

    # OKX candles API does not provide these fields. Keep schema parity with Binance output.
    df["number_of_trades"] = 0
    df["taker_buy_base_asset_volume"] = 0.0
    df["taker_buy_quote_asset_volume"] = 0.0

    return df[OUTPUT_COLUMNS].reset_index(drop=True)


def verify_continuity(inst_id: str, df: pd.DataFrame, start_ms: int, end_ms: int) -> None:
    expected = ((end_ms - start_ms) // INTERVAL_MS) + 1
    actual = len(df)
    if actual != expected:
        print(f"[{inst_id}] row-count mismatch: expected={expected}, actual={actual}")
    else:
        print(f"[{inst_id}] row-count check passed: {actual} rows")

    diffs = df["open_time_ms"].diff().dropna()
    gap_count = int((diffs != INTERVAL_MS).sum())
    if gap_count == 0:
        print(f"[{inst_id}] continuity check passed (no missing-minute gaps detected)")
    else:
        print(f"[{inst_id}] continuity check warning: {gap_count} non-1-minute gaps detected")


def save_outputs(
    df: pd.DataFrame,
    output_dir: Path,
    output_symbol: str,
    start_label: str,
    end_label: str,
) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    stem = f"{output_symbol}_1m_{start_label}_to_{end_label}"

    csv_path = output_dir / f"{stem}.csv.gz"
    parquet_path = output_dir / f"{stem}.parquet"

    df.to_csv(csv_path, index=False, compression="gzip")
    df.to_parquet(parquet_path, index=False, compression="snappy")
    return csv_path, parquet_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Download OKX 1-minute candles with Binance-compatible schema.")
    parser.add_argument("--symbols", nargs="+", default=["BTCUSDT"], help="Symbols, e.g. BTCUSDT or BTC-USDT")
    parser.add_argument("--start-date", default="2025-01-01", help="Inclusive UTC date, YYYY-MM-DD")
    parser.add_argument("--end-date", default="2026-05-31", help="Inclusive UTC date, YYYY-MM-DD")
    parser.add_argument("--output-dir", default="data/okx_1m", help="Directory for output files")
    parser.add_argument("--sleep-seconds", type=float, default=0.08, help="Pause between requests")
    parser.add_argument("--limit", type=int, default=MAX_LIMIT, help=f"Rows per API call (max {MAX_LIMIT})")
    args = parser.parse_args()

    date_range = build_date_range(args.start_date, args.end_date)
    output_dir = Path(args.output_dir)

    print(
        "Downloading OKX 1-minute candles (UTC) "
        f"from {date_range.start_label} to {date_range.end_label} for symbols: {', '.join(args.symbols)}"
    )

    for symbol in args.symbols:
        inst_id = normalize_okx_inst_id(symbol)
        out_symbol = compact_symbol(inst_id)

        raw_rows = fetch_symbol_candles(
            inst_id=inst_id,
            start_ms=date_range.start_ms,
            end_ms=date_range.end_ms,
            limit=args.limit,
            sleep_seconds=args.sleep_seconds,
        )
        if not raw_rows:
            print(f"[{inst_id}] no rows returned")
            continue

        df = rows_to_dataframe(
            rows=raw_rows,
            start_ms=date_range.start_ms,
            end_ms=date_range.end_ms,
        )
        verify_continuity(inst_id, df, date_range.start_ms, date_range.end_ms)
        csv_path, parquet_path = save_outputs(
            df=df,
            output_dir=output_dir,
            output_symbol=out_symbol,
            start_label=date_range.start_label,
            end_label=date_range.end_label,
        )
        print(f"[{inst_id}] wrote: {csv_path}")
        print(f"[{inst_id}] wrote: {parquet_path}")


if __name__ == "__main__":
    main()
