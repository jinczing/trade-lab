#!/usr/bin/env python
from __future__ import annotations

import argparse
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd
import requests

BINANCE_ENDPOINTS = (
    "https://data-api.binance.vision/api/v3/klines",
    "https://api.binance.com/api/v3/klines",
)
INTERVAL = "1m"
INTERVAL_MS = 60_000
MAX_LIMIT = 1_000

RAW_COLUMNS = [
    "open_time_ms",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time_ms",
    "quote_asset_volume",
    "number_of_trades",
    "taker_buy_base_asset_volume",
    "taker_buy_quote_asset_volume",
    "ignore",
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
    end_dt = parse_ymd(end_date).replace(hour=23, minute=59, second=59, microsecond=999000)
    if end_dt < start_dt:
        raise ValueError("end_date must be on or after start_date")
    return DateRange(
        start_ms=int(start_dt.timestamp() * 1000),
        end_ms=int(end_dt.timestamp() * 1000),
        start_label=start_dt.strftime("%Y-%m-%d"),
        end_label=parse_ymd(end_date).strftime("%Y-%m-%d"),
    )


def request_klines(
    session: requests.Session,
    symbol: str,
    start_ms: int,
    end_ms: int,
    limit: int,
    max_retries: int,
) -> list[list]:
    params = {
        "symbol": symbol,
        "interval": INTERVAL,
        "startTime": start_ms,
        "endTime": end_ms,
        "limit": limit,
    }
    last_error: Exception | None = None

    for attempt in range(max_retries):
        for endpoint in BINANCE_ENDPOINTS:
            try:
                response = session.get(endpoint, params=params, timeout=30)
                if response.status_code == 429:
                    wait_seconds = max(float(response.headers.get("Retry-After", "1")), 1.0) * (attempt + 1)
                    print(f"[{symbol}] rate-limited (429), waiting {wait_seconds:.1f}s")
                    time.sleep(wait_seconds)
                    continue
                if response.status_code >= 500:
                    wait_seconds = min(2 ** attempt, 30)
                    print(f"[{symbol}] server error {response.status_code}, waiting {wait_seconds}s")
                    time.sleep(wait_seconds)
                    continue
                response.raise_for_status()
                return response.json()
            except requests.RequestException as exc:
                last_error = exc
                wait_seconds = min(2 ** attempt, 30)
                print(f"[{symbol}] request error via {endpoint}: {exc} | retry in {wait_seconds}s")
                time.sleep(wait_seconds)

    if last_error is not None:
        raise last_error
    raise RuntimeError(f"[{symbol}] request failed for unknown reasons")


def fetch_symbol_klines(
    symbol: str,
    start_ms: int,
    end_ms: int,
    limit: int = MAX_LIMIT,
    sleep_seconds: float = 0.12,
    max_retries: int = 8,
) -> list[list]:
    if limit < 1 or limit > MAX_LIMIT:
        raise ValueError(f"limit must be in [1, {MAX_LIMIT}]")

    session = requests.Session()
    cursor = start_ms
    all_rows: list[list] = []
    request_count = 0

    while cursor <= end_ms:
        rows = request_klines(
            session=session,
            symbol=symbol,
            start_ms=cursor,
            end_ms=end_ms,
            limit=limit,
            max_retries=max_retries,
        )
        if not rows:
            break

        all_rows.extend(rows)
        request_count += 1

        last_open_ms = int(rows[-1][0])
        if last_open_ms < cursor:
            raise RuntimeError(f"[{symbol}] cursor stalled at {cursor}, last_open_ms={last_open_ms}")
        cursor = last_open_ms + INTERVAL_MS

        if request_count % 50 == 0:
            last_dt = datetime.fromtimestamp(last_open_ms / 1000, tz=timezone.utc).isoformat()
            print(f"[{symbol}] requests={request_count} rows={len(all_rows)} last_open={last_dt}")

        time.sleep(sleep_seconds)

    print(f"[{symbol}] completed with {request_count} requests and {len(all_rows)} raw rows")
    return all_rows


def rows_to_dataframe(rows: Iterable[list]) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=RAW_COLUMNS)

    numeric_cols = [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quote_asset_volume",
        "taker_buy_base_asset_volume",
        "taker_buy_quote_asset_volume",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["open_time_ms"] = pd.to_numeric(df["open_time_ms"], downcast="integer")
    df["close_time_ms"] = pd.to_numeric(df["close_time_ms"], downcast="integer")
    df["number_of_trades"] = pd.to_numeric(df["number_of_trades"], downcast="integer")
    df["open_time"] = pd.to_datetime(df["open_time_ms"], unit="ms", utc=True)
    df["close_time"] = pd.to_datetime(df["close_time_ms"], unit="ms", utc=True)

    df = df.sort_values("open_time_ms").drop_duplicates(subset=["open_time_ms"], keep="first")
    return df[OUTPUT_COLUMNS].reset_index(drop=True)


def verify_continuity(symbol: str, df: pd.DataFrame, start_ms: int, end_ms: int) -> None:
    expected = ((end_ms - start_ms) // INTERVAL_MS) + 1
    actual = len(df)
    if actual != expected:
        print(f"[{symbol}] row-count mismatch: expected={expected}, actual={actual}")

    diffs = df["open_time_ms"].diff().dropna()
    gap_count = int((diffs != INTERVAL_MS).sum())
    if gap_count == 0:
        print(f"[{symbol}] continuity check passed (no missing-minute gaps detected)")
    else:
        print(f"[{symbol}] continuity check warning: {gap_count} non-1-minute gaps detected")


def save_outputs(df: pd.DataFrame, output_dir: Path, symbol: str, start_label: str, end_label: str) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    stem = f"{symbol}_1m_{start_label}_to_{end_label}"

    csv_path = output_dir / f"{stem}.csv.gz"
    parquet_path = output_dir / f"{stem}.parquet"

    df.to_csv(csv_path, index=False, compression="gzip")
    df.to_parquet(parquet_path, index=False, compression="snappy")
    return csv_path, parquet_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Download Binance 1-minute klines for spot symbols.")
    parser.add_argument("--symbols", nargs="+", default=["BTCUSDT", "ETHUSDT"], help="Spot symbols, e.g. BTCUSDT")
    parser.add_argument("--start-date", default="2025-01-01", help="Inclusive UTC date, YYYY-MM-DD")
    parser.add_argument("--end-date", default="2026-05-31", help="Inclusive UTC date, YYYY-MM-DD")
    parser.add_argument("--output-dir", default="data/binance_1m", help="Directory for output files")
    parser.add_argument("--sleep-seconds", type=float, default=0.12, help="Pause between requests")
    parser.add_argument("--limit", type=int, default=MAX_LIMIT, help=f"Rows per API call (max {MAX_LIMIT})")
    args = parser.parse_args()

    date_range = build_date_range(args.start_date, args.end_date)
    output_dir = Path(args.output_dir)

    print(
        "Downloading 1-minute klines (UTC) "
        f"from {date_range.start_label} to {date_range.end_label} for symbols: {', '.join(args.symbols)}"
    )

    for symbol in args.symbols:
        raw_rows = fetch_symbol_klines(
            symbol=symbol,
            start_ms=date_range.start_ms,
            end_ms=date_range.end_ms,
            limit=args.limit,
            sleep_seconds=args.sleep_seconds,
        )
        if not raw_rows:
            print(f"[{symbol}] no rows returned")
            continue

        df = rows_to_dataframe(raw_rows)
        verify_continuity(symbol, df, date_range.start_ms, date_range.end_ms)
        csv_path, parquet_path = save_outputs(
            df=df,
            output_dir=output_dir,
            symbol=symbol,
            start_label=date_range.start_label,
            end_label=date_range.end_label,
        )
        print(f"[{symbol}] wrote: {csv_path}")
        print(f"[{symbol}] wrote: {parquet_path}")


if __name__ == "__main__":
    main()
