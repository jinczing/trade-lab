# Trade Lab OKX Orderbook Workflow

Utilities, Rust tooling, and notebooks to turn historical OKX Level 2 dumps into analysis-ready CSV files and fixed-interval snapshots.

## Prerequisites

- Rust toolchain (https://rustup.rs) for the `okx-orderbook-csv` binary.
- Python environment with the notebook dependencies from `requirements.txt` (used in `lab-okx-profile.ipynb` and `lab-okx-bbo.ipynb`).
- Downloaded `.data` archives from OKX Historical Data: https://www.okx.com/zh-hant/historical-data .

## 1. Download & Uncompress

1. Visit the OKX historical data portal and request the Level 2 order book dataset (e.g., `BTC-USDT-L2orderbook-400lv-2025-11-08.zip`).
2. Unzip the archive so the raw NDJSON file is available locally: `BTC-USDT-L2orderbook-400lv-2025-11-08.data`.
3. Place the `.data` file anywhere in your workspace (examples below assume `data/raw/`).

## 2. Convert `.data` → flat CSV (Rust)

The `rust/okx-orderbook-csv` crate includes a `convert` subcommand that mirrors the Python `parse_okx_orderbook_file` logic but runs entirely in Rust for speed and repeatability.

```powershell
cd rust/okx-orderbook-csv
cargo run --release -- convert `
  --input ..\..\data\raw\BTC-USDT-L2orderbook-400lv-2025-11-08.data `
  --output ..\..\data\processed\BTC-USDT-2025-11-08.csv
```

- Reads newline-delimited JSON rows, emits one CSV row per bid/ask level, preserves the raw millisecond `ts`, and adds an RFC3339 `timestamp` column by default.
- Optional flags:
  - `--nrows 100000` to stop early for sampling.
  - `--no-timestamp` to drop the human-readable timestamp column.

## 3. Rebuild fixed-interval snapshots (Rust)

Use the `rebuild` subcommand to replay the CSV and emit Level 2 snapshots at a consistent cadence (default 100 ms).

```powershell
cd rust/okx-orderbook-csv
cargo run --release -- rebuild `
  --input ..\..\data\processed\BTC-USDT-2025-11-08.csv `
  --output ..\..\data\snapshots\BTC-USDT-2025-11-08.snapshots.csv `
  --freq-ms 100 `
  --depth 40 `
  --max-duration-minutes 10
```

- Maintains an in-memory book per instrument while streaming the CSV.
- Emits two rows per price level (`side=bid/ask`) with `action=snapshot` and the UTC timestamp.
- `--depth` trims each side to the top *n* levels; omit it for full depth.
- `--max-duration-minutes` / `--max-duration-ms` limit how far from the first timestamp to simulate (useful when profiling just the first 10 minutes).

## 4. Notebook workflows

- `lab-okx-profile.ipynb` – end-to-end Python workflow: call the Rust utilities (or Python equivalents), convert `.data` files, rebuild snapshots, and profile throughput/latency/volume characteristics.
- `lab-okx-bbo.ipynb` – focuses solely on the fixed-interval snapshot outputs: loads the rebuilt CSV, computes best-bid/ask series, and produces visualizations/metrics on spread, depth, and BBO dynamics.

Both notebooks assume the processed CSVs live under `data/processed/` and snapshots under `data/snapshots/`; update the paths in the first cell if you store files elsewhere.

## Tips & Notes

- Keep raw `.data` dumps compressed once converted; the CSV + snapshot files are sufficient for most subsequent analyses.
- When batching multiple instruments or days, automate the two Rust commands above (for example via PowerShell or a Makefile) before launching the notebooks.
- For ultra-long sessions, prefer `cargo run --release` (or build `cargo build --release` once and call the binaries) to keep processing times predictable.

