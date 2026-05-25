use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use arrow::array::{Array, ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, NaiveDate, Utc};
use clap::{Parser, ValueEnum};
use humantime::parse_duration;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;

fn main() -> Result<()> {
    let cli = Cli::parse();
    run(cli)
}

fn run(cli: Cli) -> Result<()> {
    if cli.freq_ms == 0 {
        bail!("sampling frequency must be positive");
    }

    let start_ms = cli
        .start
        .as_deref()
        .map(parse_start_bound_ms)
        .transpose()?;
    let end_ms = cli.end.as_deref().map(parse_end_bound_ms).transpose()?;
    if let (Some(start), Some(end)) = (start_ms, end_ms) {
        if end < start {
            bail!("end must not be earlier than start");
        }
    }

    let input_files = discover_input_files(&cli.input)?;
    let quote_columns = detect_quote_columns(&input_files)?;
    if matches!(cli.price_source, PriceSource::Midquote) && !quote_columns.all {
        bail!(
            "price_source=midquote requires ask_price_1 and bid_price_1 columns. Re-run okx-features with --include-price."
        );
    }

    let mut converter = Converter::new(ConverterConfig {
        target_freq_ms: i64::try_from(cli.freq_ms).context("freq_ms does not fit in i64")?,
        price_source: cli.price_source,
        has_quotes_in_input: quote_columns.all,
        start_ms,
        end_ms,
        symbol: cli.symbol,
        exchange: cli.exchange,
    })?;

    for path in &input_files {
        match detect_file_kind(path) {
            FileKind::Csv => process_csv(path, &mut converter)?,
            FileKind::Parquet => process_parquet(path, &mut converter)?,
        }
    }
    converter.finish()?;

    if converter.rows.is_empty() {
        bail!("no output rows produced for the selected options/time range");
    }

    let source_freq = converter
        .source_min_diff_ms
        .context("unable to infer source frequency from input timestamps")?;
    if cli.freq_ms <= source_freq {
        bail!(
            "target frequency ({}) must be strictly coarser than source frequency ({}ms)",
            format_duration_ms(cli.freq_ms),
            source_freq
        );
    }

    if let Some(parent) = cli.output.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)
                .with_context(|| format!("unable to create {}", parent.display()))?;
        }
    }

    match cli.output_format {
        OutputFormat::Csv => write_csv(&cli.output, &converter.rows)?,
        OutputFormat::Parquet => write_parquet(&cli.output, &converter.rows)?,
    }

    println!(
        "wrote {} rows to {} (source_freq={}ms, target_freq={})",
        converter.rows.len(),
        cli.output.display(),
        source_freq,
        format_duration_ms(cli.freq_ms),
    );

    Ok(())
}

#[derive(Parser, Debug)]
#[command(
    name = "okx-highfrequency",
    about = "Convert okx-features outputs into highfrequency-compatible data"
)]
struct Cli {
    #[arg(
        long,
        value_name = "PATH",
        help = "okx-features input: a single .csv/.parquet file or a directory containing them"
    )]
    input: PathBuf,
    #[arg(
        long,
        value_name = "PATH",
        help = "Output file path for converted highfrequency-compatible data"
    )]
    output: PathBuf,
    #[arg(
        long = "freq",
        value_name = "DURATION",
        value_parser = parse_frequency_ms,
        help = "Target sampling frequency (must be coarser than source), e.g. 15m"
    )]
    freq_ms: u64,
    #[arg(
        long,
        value_enum,
        default_value_t = PriceSource::Vwap,
        help = "PRICE column source: vwap or midquote"
    )]
    price_source: PriceSource,
    #[arg(
        long,
        value_name = "TIME",
        help = "Inclusive start bound. Accepts RFC3339 or YYYY-MM-DD (UTC)."
    )]
    start: Option<String>,
    #[arg(
        long,
        value_name = "TIME",
        help = "Inclusive end bound. Accepts RFC3339 or YYYY-MM-DD (UTC)."
    )]
    end: Option<String>,
    #[arg(
        long,
        default_value = "BTC-USDT",
        value_name = "SYMBOL",
        help = "SYMBOL value in output"
    )]
    symbol: String,
    #[arg(
        long,
        default_value = "OKX",
        value_name = "EXCHANGE",
        help = "EX value in output"
    )]
    exchange: String,
    #[arg(
        long,
        value_enum,
        default_value_t = OutputFormat::Csv,
        help = "Output format: csv or parquet"
    )]
    output_format: OutputFormat,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum PriceSource {
    Vwap,
    Midquote,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum OutputFormat {
    Csv,
    Parquet,
}

#[derive(Copy, Clone, Debug)]
enum FileKind {
    Csv,
    Parquet,
}

#[derive(Clone, Debug)]
struct InputRow {
    ts_ms: i64,
    vwap: f64,
    buy_volume: f64,
    sell_volume: f64,
    ask_size_1: f64,
    bid_size_1: f64,
    ask_price_1: Option<f64>,
    bid_price_1: Option<f64>,
}

#[derive(Clone, Debug)]
struct OutputRow {
    dt: String,
    timestamp: i64,
    ex: String,
    symbol: String,
    price: f64,
    size: f64,
    bid: Option<f64>,
    ofr: Option<f64>,
    bidsiz: Option<f64>,
    ofrsiz: Option<f64>,
    midquote: Option<f64>,
}

#[derive(Clone, Debug)]
struct ConverterConfig {
    target_freq_ms: i64,
    price_source: PriceSource,
    has_quotes_in_input: bool,
    start_ms: Option<i64>,
    end_ms: Option<i64>,
    symbol: String,
    exchange: String,
}

#[derive(Clone, Debug)]
struct Bucket {
    end_ms: i64,
    last_price: Option<f64>,
    size_sum: f64,
    last_bid: Option<f64>,
    last_ofr: Option<f64>,
    last_bidsiz: Option<f64>,
    last_ofrsiz: Option<f64>,
}

#[derive(Clone, Debug)]
struct Converter {
    cfg: ConverterConfig,
    bucket: Option<Bucket>,
    prev_ts: Option<i64>,
    source_min_diff_ms: Option<u64>,
    prev_price: Option<f64>,
    prev_bid: Option<f64>,
    prev_ofr: Option<f64>,
    prev_bidsiz: Option<f64>,
    prev_ofrsiz: Option<f64>,
    rows: Vec<OutputRow>,
}

impl Converter {
    fn new(cfg: ConverterConfig) -> Result<Self> {
        if cfg.target_freq_ms <= 0 {
            bail!("target frequency must be positive");
        }
        Ok(Self {
            cfg,
            bucket: None,
            prev_ts: None,
            source_min_diff_ms: None,
            prev_price: None,
            prev_bid: None,
            prev_ofr: None,
            prev_bidsiz: None,
            prev_ofrsiz: None,
            rows: Vec::new(),
        })
    }

    fn push(&mut self, row: InputRow) -> Result<()> {
        self.update_source_frequency(row.ts_ms)?;
        let bucket_end = align_bucket_end(row.ts_ms, self.cfg.target_freq_ms)?;

        match self.bucket.as_ref() {
            Some(current) if current.end_ms != bucket_end => self.flush_bucket()?,
            _ => {}
        }

        if self.bucket.is_none() {
            self.bucket = Some(Bucket {
                end_ms: bucket_end,
                last_price: None,
                size_sum: 0.0,
                last_bid: None,
                last_ofr: None,
                last_bidsiz: None,
                last_ofrsiz: None,
            });
        }

        let row_price = self.row_price(&row);
        let bucket = self.bucket.as_mut().expect("bucket must be initialized");
        bucket.size_sum += row.buy_volume + row.sell_volume;

        if let Some(price) = row_price {
            bucket.last_price = Some(price);
        }

        if self.cfg.has_quotes_in_input {
            if let Some(bid) = row.bid_price_1 {
                bucket.last_bid = Some(bid);
                bucket.last_bidsiz = Some(row.bid_size_1);
            }
            if let Some(ofr) = row.ask_price_1 {
                bucket.last_ofr = Some(ofr);
                bucket.last_ofrsiz = Some(row.ask_size_1);
            }
        }

        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        self.flush_bucket()
    }

    fn flush_bucket(&mut self) -> Result<()> {
        let Some(bucket) = self.bucket.take() else {
            return Ok(());
        };

        if let Some(price) = bucket.last_price {
            self.prev_price = Some(price);
        }
        if let Some(bid) = bucket.last_bid {
            self.prev_bid = Some(bid);
        }
        if let Some(ofr) = bucket.last_ofr {
            self.prev_ofr = Some(ofr);
        }
        if let Some(bidsiz) = bucket.last_bidsiz {
            self.prev_bidsiz = Some(bidsiz);
        }
        if let Some(ofrsiz) = bucket.last_ofrsiz {
            self.prev_ofrsiz = Some(ofrsiz);
        }

        if !self.in_time_range(bucket.end_ms) {
            return Ok(());
        }

        let Some(price) = self.prev_price else {
            return Ok(());
        };

        let (bid, ofr, bidsiz, ofrsiz, midquote) = if self.cfg.has_quotes_in_input {
            let (Some(bid), Some(ofr), Some(bidsiz), Some(ofrsiz)) =
                (self.prev_bid, self.prev_ofr, self.prev_bidsiz, self.prev_ofrsiz)
            else {
                return Ok(());
            };
            (Some(bid), Some(ofr), Some(bidsiz), Some(ofrsiz), Some((bid + ofr) / 2.0))
        } else {
            (None, None, None, None, None)
        };

        self.rows.push(OutputRow {
            dt: format_timestamp(bucket.end_ms)?,
            timestamp: bucket.end_ms,
            ex: self.cfg.exchange.clone(),
            symbol: self.cfg.symbol.clone(),
            price,
            size: bucket.size_sum.max(0.0),
            bid,
            ofr,
            bidsiz,
            ofrsiz,
            midquote,
        });
        Ok(())
    }

    fn in_time_range(&self, ts_ms: i64) -> bool {
        if let Some(start_ms) = self.cfg.start_ms {
            if ts_ms < start_ms {
                return false;
            }
        }
        if let Some(end_ms) = self.cfg.end_ms {
            if ts_ms > end_ms {
                return false;
            }
        }
        true
    }

    fn row_price(&self, row: &InputRow) -> Option<f64> {
        match self.cfg.price_source {
            PriceSource::Vwap => {
                if row.vwap > 0.0 {
                    Some(row.vwap)
                } else {
                    None
                }
            }
            PriceSource::Midquote => match (row.bid_price_1, row.ask_price_1) {
                (Some(bid), Some(ofr)) => Some((bid + ofr) / 2.0),
                _ => None,
            },
        }
    }

    fn update_source_frequency(&mut self, ts_ms: i64) -> Result<()> {
        if let Some(prev) = self.prev_ts {
            if ts_ms < prev {
                bail!(
                    "input timestamps must be non-decreasing, found {} then {}",
                    prev,
                    ts_ms
                );
            }
            let diff = ts_ms - prev;
            if diff > 0 {
                let diff_u64 = u64::try_from(diff).context("timestamp diff overflow")?;
                self.source_min_diff_ms = Some(match self.source_min_diff_ms {
                    Some(current) => current.min(diff_u64),
                    None => diff_u64,
                });
            }
        }
        self.prev_ts = Some(ts_ms);
        Ok(())
    }
}

fn process_csv(path: &Path, converter: &mut Converter) -> Result<()> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(path)
        .with_context(|| format!("unable to open {}", path.display()))?;
    let columns = CsvColumns::from_headers(reader.headers()?)?;

    for record in reader.records() {
        let record = record.with_context(|| format!("invalid csv row in {}", path.display()))?;
        let row = InputRow {
            ts_ms: parse_i64_field(&record, columns.timestamp, "timestamp")?,
            vwap: parse_f64_field(&record, columns.vwap, "vwap")?,
            buy_volume: parse_f64_field(&record, columns.buy_volume, "buy_volume")?,
            sell_volume: parse_f64_field(&record, columns.sell_volume, "sell_volume")?,
            ask_size_1: parse_f64_field(&record, columns.ask_size_1, "ask_size_1")?,
            bid_size_1: parse_f64_field(&record, columns.bid_size_1, "bid_size_1")?,
            ask_price_1: parse_optional_f64_field(&record, columns.ask_price_1, "ask_price_1")?,
            bid_price_1: parse_optional_f64_field(&record, columns.bid_price_1, "bid_price_1")?,
        };
        converter.push(row)?;
    }
    Ok(())
}

fn process_parquet(path: &Path, converter: &mut Converter) -> Result<()> {
    let file = File::open(path).with_context(|| format!("unable to open {}", path.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .with_context(|| format!("unable to read parquet metadata from {}", path.display()))?;
    let mut reader = builder
        .with_batch_size(8_192)
        .build()
        .with_context(|| format!("unable to build parquet reader for {}", path.display()))?;

    for maybe_batch in &mut reader {
        let batch = maybe_batch.with_context(|| format!("error while reading {}", path.display()))?;
        process_parquet_batch(&batch, converter)?;
    }
    Ok(())
}

fn process_parquet_batch(batch: &RecordBatch, converter: &mut Converter) -> Result<()> {
    let schema = batch.schema();
    let ts_idx = schema.index_of("timestamp").context("missing timestamp column")?;
    let vwap_idx = schema.index_of("vwap").context("missing vwap column")?;
    let buy_idx = schema
        .index_of("buy_volume")
        .context("missing buy_volume column")?;
    let sell_idx = schema
        .index_of("sell_volume")
        .context("missing sell_volume column")?;
    let ask_size_idx = schema
        .index_of("ask_size_1")
        .context("missing ask_size_1 column")?;
    let bid_size_idx = schema
        .index_of("bid_size_1")
        .context("missing bid_size_1 column")?;

    let ask_price_idx = schema.index_of("ask_price_1").ok();
    let bid_price_idx = schema.index_of("bid_price_1").ok();

    let ts = as_int64_array(batch.column(ts_idx), "timestamp")?;
    let vwap = as_float64_array(batch.column(vwap_idx), "vwap")?;
    let buy = as_float64_array(batch.column(buy_idx), "buy_volume")?;
    let sell = as_float64_array(batch.column(sell_idx), "sell_volume")?;
    let ask_size = as_float64_array(batch.column(ask_size_idx), "ask_size_1")?;
    let bid_size = as_float64_array(batch.column(bid_size_idx), "bid_size_1")?;
    let ask_price = ask_price_idx
        .map(|idx| as_float64_array(batch.column(idx), "ask_price_1"))
        .transpose()?;
    let bid_price = bid_price_idx
        .map(|idx| as_float64_array(batch.column(idx), "bid_price_1"))
        .transpose()?;

    for i in 0..batch.num_rows() {
        let row = InputRow {
            ts_ms: ts.value(i),
            vwap: vwap.value(i),
            buy_volume: buy.value(i),
            sell_volume: sell.value(i),
            ask_size_1: ask_size.value(i),
            bid_size_1: bid_size.value(i),
            ask_price_1: ask_price
                .and_then(|arr| if arr.is_null(i) { None } else { Some(arr.value(i)) }),
            bid_price_1: bid_price
                .and_then(|arr| if arr.is_null(i) { None } else { Some(arr.value(i)) }),
        };
        converter.push(row)?;
    }
    Ok(())
}

fn write_csv(path: &Path, rows: &[OutputRow]) -> Result<()> {
    let mut writer = csv::WriterBuilder::new()
        .has_headers(true)
        .from_path(path)
        .with_context(|| format!("unable to create {}", path.display()))?;
    let has_quotes = rows.iter().any(|row| row.bid.is_some());

    if has_quotes {
        writer.write_record([
            "DT",
            "TIMESTAMP",
            "EX",
            "SYMBOL",
            "PRICE",
            "SIZE",
            "BID",
            "OFR",
            "BIDSIZ",
            "OFRSIZ",
            "MIDQUOTE",
        ])?;
    } else {
        writer.write_record(["DT", "TIMESTAMP", "EX", "SYMBOL", "PRICE", "SIZE"])?;
    }

    for row in rows {
        if has_quotes {
            writer.write_record([
                row.dt.as_str(),
                &row.timestamp.to_string(),
                row.ex.as_str(),
                row.symbol.as_str(),
                &format_float(row.price),
                &format_float(row.size),
                &format_optional_float(row.bid),
                &format_optional_float(row.ofr),
                &format_optional_float(row.bidsiz),
                &format_optional_float(row.ofrsiz),
                &format_optional_float(row.midquote),
            ])?;
        } else {
            writer.write_record([
                row.dt.as_str(),
                &row.timestamp.to_string(),
                row.ex.as_str(),
                row.symbol.as_str(),
                &format_float(row.price),
                &format_float(row.size),
            ])?;
        }
    }
    writer.flush()?;
    Ok(())
}

fn write_parquet(path: &Path, rows: &[OutputRow]) -> Result<()> {
    let has_quotes = rows.iter().any(|row| row.bid.is_some());
    let schema = build_output_schema(has_quotes);
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(if has_quotes { 11 } else { 6 });

    let dt: Vec<&str> = rows.iter().map(|row| row.dt.as_str()).collect();
    let ts: Vec<i64> = rows.iter().map(|row| row.timestamp).collect();
    let ex: Vec<&str> = rows.iter().map(|row| row.ex.as_str()).collect();
    let symbol: Vec<&str> = rows.iter().map(|row| row.symbol.as_str()).collect();
    let price: Vec<f64> = rows.iter().map(|row| row.price).collect();
    let size: Vec<f64> = rows.iter().map(|row| row.size).collect();

    columns.push(Arc::new(StringArray::from(dt)) as ArrayRef);
    columns.push(Arc::new(Int64Array::from(ts)) as ArrayRef);
    columns.push(Arc::new(StringArray::from(ex)) as ArrayRef);
    columns.push(Arc::new(StringArray::from(symbol)) as ArrayRef);
    columns.push(Arc::new(Float64Array::from(price)) as ArrayRef);
    columns.push(Arc::new(Float64Array::from(size)) as ArrayRef);

    if has_quotes {
        columns.push(Arc::new(Float64Array::from(
            rows.iter().map(|row| row.bid).collect::<Vec<_>>(),
        )) as ArrayRef);
        columns.push(Arc::new(Float64Array::from(
            rows.iter().map(|row| row.ofr).collect::<Vec<_>>(),
        )) as ArrayRef);
        columns.push(Arc::new(Float64Array::from(
            rows.iter().map(|row| row.bidsiz).collect::<Vec<_>>(),
        )) as ArrayRef);
        columns.push(Arc::new(Float64Array::from(
            rows.iter().map(|row| row.ofrsiz).collect::<Vec<_>>(),
        )) as ArrayRef);
        columns.push(Arc::new(Float64Array::from(
            rows.iter().map(|row| row.midquote).collect::<Vec<_>>(),
        )) as ArrayRef);
    }

    let batch = RecordBatch::try_new(schema.clone(), columns)?;
    let file = File::create(path).with_context(|| format!("unable to create {}", path.display()))?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close().map(|_| ()).map_err(|e| e.into())
}

fn build_output_schema(has_quotes: bool) -> Arc<Schema> {
    let mut fields = vec![
        Field::new("DT", DataType::Utf8, false),
        Field::new("TIMESTAMP", DataType::Int64, false),
        Field::new("EX", DataType::Utf8, false),
        Field::new("SYMBOL", DataType::Utf8, false),
        Field::new("PRICE", DataType::Float64, false),
        Field::new("SIZE", DataType::Float64, false),
    ];
    if has_quotes {
        fields.push(Field::new("BID", DataType::Float64, true));
        fields.push(Field::new("OFR", DataType::Float64, true));
        fields.push(Field::new("BIDSIZ", DataType::Float64, true));
        fields.push(Field::new("OFRSIZ", DataType::Float64, true));
        fields.push(Field::new("MIDQUOTE", DataType::Float64, true));
    }
    Arc::new(Schema::new(fields))
}

fn discover_input_files(input: &Path) -> Result<Vec<PathBuf>> {
    if input.is_file() {
        let kind = detect_file_kind(input);
        match kind {
            FileKind::Csv | FileKind::Parquet => return Ok(vec![input.to_path_buf()]),
        }
    }

    if !input.is_dir() {
        bail!("input path {} is neither a file nor directory", input.display());
    }

    let mut files = Vec::new();
    for entry in fs::read_dir(input)
        .with_context(|| format!("unable to read directory {}", input.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        match detect_file_kind(&path) {
            FileKind::Csv | FileKind::Parquet => files.push(path),
        }
    }
    files.sort();
    if files.is_empty() {
        bail!(
            "no .csv or .parquet files found under input directory {}",
            input.display()
        );
    }
    Ok(files)
}

#[derive(Clone, Copy, Debug)]
struct QuoteColumns {
    all: bool,
}

fn detect_quote_columns(files: &[PathBuf]) -> Result<QuoteColumns> {
    let mut all = true;
    for path in files {
        let has_quotes = match detect_file_kind(path) {
            FileKind::Csv => {
                let mut reader = csv::ReaderBuilder::new()
                    .has_headers(true)
                    .from_path(path)
                    .with_context(|| format!("unable to open {}", path.display()))?;
                let headers = reader.headers()?;
                let has_ask = headers.iter().any(|h| h == "ask_price_1");
                let has_bid = headers.iter().any(|h| h == "bid_price_1");
                has_ask && has_bid
            }
            FileKind::Parquet => {
                let file = File::open(path)
                    .with_context(|| format!("unable to open {}", path.display()))?;
                let builder = ParquetRecordBatchReaderBuilder::try_new(file).with_context(|| {
                    format!("unable to read parquet metadata from {}", path.display())
                })?;
                let schema = builder.schema();
                let has_ask = schema.field_with_name("ask_price_1").is_ok();
                let has_bid = schema.field_with_name("bid_price_1").is_ok();
                has_ask && has_bid
            }
        };
        all &= has_quotes;
    }
    Ok(QuoteColumns { all })
}

fn detect_file_kind(path: &Path) -> FileKind {
    match path
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.to_ascii_lowercase())
        .as_deref()
    {
        Some("parquet") => FileKind::Parquet,
        _ => FileKind::Csv,
    }
}

fn parse_frequency_ms(src: &str) -> std::result::Result<u64, String> {
    let duration = parse_duration(src).map_err(|err| err.to_string())?;
    let millis = duration.as_millis();
    if millis == 0 {
        return Err("duration must be positive".to_string());
    }
    u64::try_from(millis).map_err(|_| "duration is too large".to_string())
}

fn parse_start_bound_ms(src: &str) -> Result<i64> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(src) {
        return Ok(dt.timestamp_millis());
    }
    let date = NaiveDate::parse_from_str(src, "%Y-%m-%d")
        .with_context(|| format!("invalid start bound '{}'", src))?;
    let dt = date
        .and_hms_milli_opt(0, 0, 0, 0)
        .context("invalid midnight for start bound")?;
    Ok(dt.and_utc().timestamp_millis())
}

fn parse_end_bound_ms(src: &str) -> Result<i64> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(src) {
        return Ok(dt.timestamp_millis());
    }
    let date = NaiveDate::parse_from_str(src, "%Y-%m-%d")
        .with_context(|| format!("invalid end bound '{}'", src))?;
    let dt = date
        .and_hms_milli_opt(23, 59, 59, 999)
        .context("invalid end-of-day for end bound")?;
    Ok(dt.and_utc().timestamp_millis())
}

fn align_bucket_end(ts_ms: i64, freq_ms: i64) -> Result<i64> {
    if freq_ms <= 0 {
        bail!("frequency must be positive");
    }
    let remainder = ts_ms.rem_euclid(freq_ms);
    if remainder == 0 {
        Ok(ts_ms)
    } else {
        ts_ms
            .checked_add(freq_ms - remainder)
            .context("bucket timestamp overflow")
    }
}

fn format_timestamp(ts_ms: i64) -> Result<String> {
    let dt = DateTime::<Utc>::from_timestamp_millis(ts_ms)
        .with_context(|| format!("invalid timestamp {}", ts_ms))?;
    Ok(dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
}

fn format_duration_ms(ms: u64) -> String {
    if ms % 3_600_000 == 0 {
        format!("{}h", ms / 3_600_000)
    } else if ms % 60_000 == 0 {
        format!("{}m", ms / 60_000)
    } else if ms % 1_000 == 0 {
        format!("{}s", ms / 1_000)
    } else {
        format!("{ms}ms")
    }
}

fn format_float(value: f64) -> String {
    if value.abs() < 1e-12 {
        "0".to_string()
    } else {
        format!("{value:.10}")
    }
}

fn format_optional_float(value: Option<f64>) -> String {
    match value {
        Some(v) => format_float(v),
        None => String::new(),
    }
}

fn as_int64_array<'a>(array: &'a ArrayRef, name: &str) -> Result<&'a Int64Array> {
    array
        .as_any()
        .downcast_ref::<Int64Array>()
        .with_context(|| format!("column {} is not Int64", name))
}

fn as_float64_array<'a>(array: &'a ArrayRef, name: &str) -> Result<&'a Float64Array> {
    array
        .as_any()
        .downcast_ref::<Float64Array>()
        .with_context(|| format!("column {} is not Float64", name))
}

#[derive(Clone, Debug)]
struct CsvColumns {
    timestamp: usize,
    vwap: usize,
    buy_volume: usize,
    sell_volume: usize,
    ask_size_1: usize,
    bid_size_1: usize,
    ask_price_1: Option<usize>,
    bid_price_1: Option<usize>,
}

impl CsvColumns {
    fn from_headers(headers: &csv::StringRecord) -> Result<Self> {
        let timestamp = find_header(headers, "timestamp")?;
        let vwap = find_header(headers, "vwap")?;
        let buy_volume = find_header(headers, "buy_volume")?;
        let sell_volume = find_header(headers, "sell_volume")?;
        let ask_size_1 = find_header(headers, "ask_size_1")?;
        let bid_size_1 = find_header(headers, "bid_size_1")?;
        let ask_price_1 = find_optional_header(headers, "ask_price_1");
        let bid_price_1 = find_optional_header(headers, "bid_price_1");
        Ok(Self {
            timestamp,
            vwap,
            buy_volume,
            sell_volume,
            ask_size_1,
            bid_size_1,
            ask_price_1,
            bid_price_1,
        })
    }
}

fn find_header(headers: &csv::StringRecord, name: &str) -> Result<usize> {
    find_optional_header(headers, name).with_context(|| format!("missing {} column", name))
}

fn find_optional_header(headers: &csv::StringRecord, name: &str) -> Option<usize> {
    headers.iter().position(|header| header == name)
}

fn parse_i64_field(record: &csv::StringRecord, index: usize, name: &str) -> Result<i64> {
    let raw = record
        .get(index)
        .with_context(|| format!("missing {} at index {}", name, index))?;
    raw.parse::<i64>()
        .with_context(|| format!("invalid {} value '{}'", name, raw))
}

fn parse_f64_field(record: &csv::StringRecord, index: usize, name: &str) -> Result<f64> {
    let raw = record
        .get(index)
        .with_context(|| format!("missing {} at index {}", name, index))?;
    raw.parse::<f64>()
        .with_context(|| format!("invalid {} value '{}'", name, raw))
}

fn parse_optional_f64_field(
    record: &csv::StringRecord,
    index: Option<usize>,
    name: &str,
) -> Result<Option<f64>> {
    let Some(index) = index else {
        return Ok(None);
    };
    let Some(raw) = record.get(index) else {
        return Ok(None);
    };
    if raw.trim().is_empty() {
        return Ok(None);
    }
    raw.parse::<f64>()
        .map(Some)
        .with_context(|| format!("invalid {} value '{}'", name, raw))
}
