use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use chrono::{DateTime, TimeZone, Utc};
use clap::{ArgAction, Parser};
use serde_json::Value;

#[derive(Parser, Debug)]
#[command(name = "okx-orderbook-csv", version, about = "Convert OKX L2 orderbook .data (NDJSON) to CSV")] 
struct Args {
    /// Input .data file path
    #[arg(short = 'i', long = "input")]
    input: PathBuf,

    /// Output .csv file path (defaults to input with .csv extension)
    #[arg(short = 'o', long = "output")]
    output: Option<PathBuf>,

    /// Max number of lines (NDJSON rows) to read from input
    #[arg(long = "nrows")]
    nrows: Option<usize>,

    /// Do not append a human-readable UTC timestamp column
    #[arg(long = "no-timestamp", action = ArgAction::SetTrue)]
    no_timestamp: bool,
}


fn parse_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Null => None,
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

fn parse_i64(v: &Value) -> Option<i64> {
    match v {
        Value::Null => None,
        Value::Number(n) => n.as_i64().or_else(|| n.as_f64().map(|f| f as i64)),
        Value::String(s) => s.parse::<i64>().ok(),
        _ => None,
    }
}

fn ms_to_rfc3339_utc(ms: i64) -> String {
    // Clamp to avoid panic on extreme ms values
    let secs = ms / 1000;
    let sub_ms = (ms % 1000) as u32;
    let nanos = sub_ms * 1_000_000;
    // Fallback to 0 on out-of-range
    let dt: DateTime<Utc> = Utc
        .timestamp_opt(secs, nanos)
        .single()
        .unwrap_or_else(|| Utc.timestamp_opt(0, 0).single().unwrap());
    dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

fn derive_output_path(input: &Path) -> PathBuf {
    let mut out = input.to_path_buf();
    out.set_extension("csv");
    out
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let input = args.input;
    if !input.exists() {
        anyhow::bail!("Input file not found: {}", input.display());
    }
    let output = args.output.unwrap_or_else(|| derive_output_path(&input));

    let infile = File::open(&input)?;
    let reader = BufReader::new(infile);

    let mut wtr = csv::Writer::from_path(&output)?;

    let write_timestamp = !args.no_timestamp;
    // Header to mirror Python function
    if write_timestamp {
        wtr.write_record(["instrument", "action", "side", "price", "size", "count", "ts", "timestamp"])?;
    } else {
        wtr.write_record(["instrument", "action", "side", "price", "size", "count", "ts"])?;
    }

    let mut line_no: usize = 0;
    for line_res in reader.lines() {
        line_no += 1;
        if let Some(n) = args.nrows { if line_no > n { break; } }

        let line = match line_res {
            Ok(s) => s.trim().to_string(),
            Err(e) => {
                eprintln!("Failed to read line {}: {}", line_no, e);
                continue;
            }
        };
        if line.is_empty() { continue; }

        let v: Value = match serde_json::from_str(&line) {
            Ok(val) => val,
            Err(e) => {
                return Err(anyhow::anyhow!("Failed to parse JSON on line {}: {}", line_no, e));
            }
        };

        let ts_opt = v.get("ts").and_then(parse_i64);
        let inst = v.get("instId").and_then(|x| x.as_str()).map(|s| s.to_string());
        let action = v.get("action").and_then(|x| x.as_str()).map(|s| s.to_string());

        let mut wrote_any = false;
        for (side_key, side_label) in [("bids", "bid"), ("asks", "ask")] {
            if let Some(levels) = v.get(side_key) {
                if let Some(arr) = levels.as_array() {
                    for level in arr {
                        if let Some(list) = level.as_array() {
                            if list.len() < 2 { continue; }
                            let price = parse_f64(&list[0]);
                            let size = parse_f64(&list[1]);
                            let count = if list.len() > 2 { parse_i64(&list[2]) } else { None };

                            let timestamp_str = if write_timestamp {
                                ts_opt.map(|ms| ms_to_rfc3339_utc(ms))
                            } else { None };

                            if write_timestamp {
                                wtr.write_record(&[
                                    inst.as_deref().unwrap_or(""),
                                    action.as_deref().unwrap_or("") ,
                                    side_label,
                                    &price.map(|x| x.to_string()).unwrap_or_default(),
                                    &size.map(|x| x.to_string()).unwrap_or_default(),
                                    &count.map(|x| x.to_string()).unwrap_or_default(),
                                    &ts_opt.map(|x| x.to_string()).unwrap_or_default(),
                                    timestamp_str.as_deref().unwrap_or(""),
                                ])?;
                            } else {
                                wtr.write_record(&[
                                    inst.as_deref().unwrap_or(""),
                                    action.as_deref().unwrap_or("") ,
                                    side_label,
                                    &price.map(|x| x.to_string()).unwrap_or_default(),
                                    &size.map(|x| x.to_string()).unwrap_or_default(),
                                    &count.map(|x| x.to_string()).unwrap_or_default(),
                                    &ts_opt.map(|x| x.to_string()).unwrap_or_default(),
                                ])?;
                            }
                            wrote_any = true;
                        }
                    }
                }
            }
        }

        if !wrote_any {
            // Mirror Python: emit placeholder when no bids/asks present
            let timestamp_str = if write_timestamp { ts_opt.map(|ms| ms_to_rfc3339_utc(ms)) } else { None };
            if write_timestamp {
                wtr.write_record(&[
                    inst.as_deref().unwrap_or(""),
                    action.as_deref().unwrap_or(""),
                    "",
                    "",
                    "",
                    "",
                    &ts_opt.map(|x| x.to_string()).unwrap_or_default(),
                    timestamp_str.as_deref().unwrap_or(""),
                ])?;
            } else {
                wtr.write_record(&[
                    inst.as_deref().unwrap_or(""),
                    action.as_deref().unwrap_or(""),
                    "",
                    "",
                    "",
                    "",
                    &ts_opt.map(|x| x.to_string()).unwrap_or_default(),
                ])?;
            }
        }
    }

    wtr.flush()?;
    // Provide a small success note to stderr for visibility in scripts
    eprintln!("Wrote CSV: {}", output.display());
    Ok(())
}
