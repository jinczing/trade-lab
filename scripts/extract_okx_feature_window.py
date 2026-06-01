#!/usr/bin/env python
"""Extract a timestamp window from okx-features parquet into CSV."""

from __future__ import annotations

import argparse
from pathlib import Path

import polars as pl


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract a timestamp window from okx-features parquet into CSV."
    )
    parser.add_argument("--input", required=True, help="Input parquet file path")
    parser.add_argument("--output", required=True, help="Output CSV file path")
    parser.add_argument(
        "--start-ms",
        required=True,
        type=int,
        help="Inclusive window start timestamp in Unix milliseconds",
    )
    parser.add_argument(
        "--end-ms",
        required=True,
        type=int,
        help="Inclusive window end timestamp in Unix milliseconds",
    )
    parser.add_argument(
        "--columns",
        default="",
        help="Optional comma-separated columns to keep. Empty keeps all columns.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    output_path = Path(args.output)

    if not input_path.exists():
        raise FileNotFoundError(f"Input parquet not found: {input_path}")
    if args.end_ms < args.start_ms:
        raise ValueError("end-ms must be greater than or equal to start-ms")

    query = (
        pl.scan_parquet(str(input_path))
        .filter(
            (pl.col("timestamp") >= args.start_ms) & (pl.col("timestamp") <= args.end_ms)
        )
        .sort("timestamp")
    )

    if args.columns.strip():
        keep_cols = [c.strip() for c in args.columns.split(",") if c.strip()]
        if keep_cols:
            query = query.select(keep_cols)

    out_df = query.collect()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    out_df.write_csv(str(output_path))

    print(
        f"rows={out_df.height} cols={out_df.width} "
        f"start_ms={args.start_ms} end_ms={args.end_ms} "
        f"output={output_path}"
    )


if __name__ == "__main__":
    main()
