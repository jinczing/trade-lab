use std::env;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Read};

fn print_usage() {
    eprintln!(
        "Usage: data-runner [--input <path>] [--delim <char>] [--sum-col <n>] [--skip-header]\n\n  - Reads CSV-like text from a file or stdin.\n  - Default operation: count lines.\n  - When --sum-col is provided: sum numeric values in the given 0-based column.\n\nOptions:\n  --input, -i <path>    Input file path. If omitted, reads from stdin.\n  --delim, -d <char>    Column delimiter (default ','). Use \\t for TAB.\n  --sum-col, -c <n>     Sum numeric values in column n (0-based).\n  --skip-header         Skip the first line (header).\n  --help, -h            Show this help."
    );
}

fn parse_delim(s: &str) -> char {
    match s {
        "\\t" | "tab" | "TAB" => '\t',
        _ => s.chars().next().unwrap_or(','),
    }
}

fn main() -> io::Result<()> {
    let mut args = env::args().skip(1);

    let mut input_path: Option<String> = None;
    let mut delim: char = ',';
    let mut sum_col: Option<usize> = None;
    let mut skip_header = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => {
                print_usage();
                return Ok(());
            }
            "--input" | "-i" => {
                input_path = args.next();
            }
            "--delim" | "-d" => {
                if let Some(d) = args.next() {
                    delim = parse_delim(&d);
                }
            }
            "--sum-col" | "-c" => {
                if let Some(c) = args.next() {
                    match c.parse::<usize>() {
                        Ok(n) => sum_col = Some(n),
                        Err(_) => {
                            eprintln!("Invalid --sum-col value: {c}");
                            std::process::exit(2);
                        }
                    }
                }
            }
            "--skip-header" => {
                skip_header = true;
            }
            other if other.starts_with('-') => {
                eprintln!("Unknown option: {other}\n");
                print_usage();
                std::process::exit(2);
            }
            // Positional: treat as input path if not already set
            pos => {
                if input_path.is_none() {
                    input_path = Some(pos.to_string());
                }
            }
        }
    }

    // Prepare input reader
    let reader: Box<dyn BufRead> = match input_path {
        Some(p) => Box::new(BufReader::new(File::open(p)?)),
        None => {
            // If stdin is piped, read it; if attached to TTY with no input, still proceed
            let mut stdin = io::stdin();
            // For performance, if large, wrap in BufReader
            let handle = stdin.lock();
            Box::new(BufReader::new(handle))
        }
    };

    if let Some(col_idx) = sum_col {
        // Summation mode
        let mut lines = reader.lines();
        if skip_header {
            let _ = lines.next();
        }
        let mut count = 0usize;
        let mut sum: f64 = 0.0;
        let mut skipped = 0usize;

        for line in lines {
            let line = match line {
                Ok(l) => l,
                Err(e) => {
                    eprintln!("Failed to read line: {e}");
                    continue;
                }
            };
            if line.is_empty() { continue; }
            let mut parts = line.split(delim);
            let value = parts.nth(col_idx).unwrap_or("").trim();
            if value.is_empty() {
                skipped += 1;
                continue;
            }
            match value.parse::<f64>() {
                Ok(v) => { sum += v; count += 1; }
                Err(_) => { skipped += 1; }
            }
        }

        println!("mode=sum-col, column={col_idx}, count={count}, skipped={skipped}, sum={sum}");
    } else {
        // Count-lines mode
        let mut buf_reader = reader;
        let mut buf = String::new();
        let mut count = 0usize;

        if skip_header {
            let _ = buf_reader.read_line(&mut buf)?; // discard first line
            buf.clear();
        }

        loop {
            buf.clear();
            let n = buf_reader.read_line(&mut buf)?;
            if n == 0 { break; }
            // If the last chunk didn't end with newline, still count it as a line
            count += 1;
        }
        println!("mode=count-lines, lines={count}");
    }

    Ok(())
}

