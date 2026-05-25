# okx-highfrequency

Convert `okx-features` outputs into a `highfrequency`-compatible table.

The converter supports:

- `PRICE` source selection: `vwap` or `midquote`
- coarser target sampling frequency than the source data
- time period filtering
- CSV or Parquet output

## Example

```powershell
cargo run -p okx-highfrequency -- ^
  --input target\okx_btcusdt_features_0.1_10_31_+p ^
  --output target\hf_15m.csv ^
  --freq 15m ^
  --price-source midquote ^
  --start 2025-10-01 ^
  --end 2025-10-31 ^
  --symbol BTC-USDT ^
  --exchange OKX ^
  --output-format csv
```

## Output columns

Always present:

- `DT` (RFC3339 UTC timestamp, interval close)
- `TIMESTAMP` (Unix milliseconds)
- `EX`
- `SYMBOL`
- `PRICE`
- `SIZE`

If the input has top-of-book prices (`ask_price_1`, `bid_price_1`), the output also includes:

- `BID`
- `OFR`
- `BIDSIZ`
- `OFRSIZ`
- `MIDQUOTE`
