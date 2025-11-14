# data-runner

Minimal Rust CLI (no external crates) to process text/CSV-like data from a file or stdin.

## Build & Run

Prerequisite: Rust toolchain (rustup) installed.

- Windows (PowerShell):
  - winget: `winget install Rustlang.Rustup`
  - or Chocolatey: `choco install rustup` then `rustup-init -y`
- macOS/Linux: https://rustup.rs

From the repo root:

- Run with Cargo (dev):
  - `cd rust/data-runner`
  - `cargo run -- --help`
  - `cargo run -- --input ..\\..\\path\\to\\file.csv` (Windows)
  - `cargo run -- --input ../../path/to/file.csv` (macOS/Linux)

- Build a release binary:
  - `cargo build --release`
  - Binary at `rust/data-runner/target/release/data-runner`

- Use `rustc` directly on the main file (no Cargo):
  - `rustc src/main.rs -O -o data-runner.exe` (Windows)
  - `rustc src/main.rs -O -o data-runner` (macOS/Linux)

## Usage

```
Usage: data-runner [--input <path>] [--delim <char>] [--sum-col <n>] [--skip-header]

  - Reads CSV-like text from a file or stdin.
  - Default operation: count lines.
  - When --sum-col is provided: sum numeric values in the given 0-based column.

Options:
  --input, -i <path>    Input file path. If omitted, reads from stdin.
  --delim, -d <char>    Column delimiter (default ','). Use \\t for TAB.
  --sum-col, -c <n>     Sum numeric values in column n (0-based).
  --skip-header         Skip the first line (header).
  --help, -h            Show this help.
```

### Examples

- Count lines in a CSV:
  - `cargo run -- --input data.csv`
  - `type data.csv | cargo run --` (Windows, PowerShell)
  - `cat data.csv | cargo run --` (macOS/Linux)

- Sum a numeric column (0-based), comma-delimited:
  - `cargo run -- --input data.csv --sum-col 2`

- Tab-delimited with header row:
  - `cargo run -- --input data.tsv --delim \\t --sum-col 0 --skip-header`

## Notes

- This crate avoids external dependencies so it works offline. For richer parsing (CSV/JSON), you can later add crates like `csv` and `serde` when network access is available.
- The program prints simple summary lines to stdout; adjust as needed for your pipeline.
