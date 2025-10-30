import gzip
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from orderbook_data_tools import (
    compute_top_of_book,
    explode_levels,
    export_dataframe,
    iter_raw_events,
    iter_snapshot_records,
    load_events_dataframe,
    load_snapshot_dataframe,
)


def _write_sample_file(tmp_path: Path) -> Path:
    events = [
        {
            "e": "depthUpdate",
            "E": 1700000000000,
            "s": "BTCUSDT",
            "U": 100,
            "u": 101,
            "pu": 99,
            "b": [["35000.0", "1.5"], ["34999.5", "0.2"]],
            "a": [["35001.0", "1.0"], ["35002.0", "0.7"]],
        },
        {
            "e": "depthUpdate",
            "E": 1700000000100,
            "s": "BTCUSDT",
            "U": 102,
            "u": 103,
            "pu": 101,
            "b": [["35000.5", "1.0"]],
            "a": [["35001.5", "2.0"]],
        },
    ]
    out_file = tmp_path / "sample.ndjson.gz"
    with gzip.open(out_file, "wt", encoding="utf-8") as fh:
        for evt in events:
            fh.write(json.dumps(evt))
            fh.write("\n")
    return out_file


def _write_sample_snapshot_file(tmp_path: Path) -> Path:
    events = [
        {
            "ts_utc": "2024-01-01T00:00:00Z",
            "symbol": "BTCUSDT",
            "lastUpdateId": 200,
            "bids": [["35000.0", "1.5"], ["34999.5", "0.2"]],
            "asks": [["35001.0", "1.0"], ["35002.0", "0.7"]],
        },
        {
            "ts_utc": "2024-01-01T00:00:05Z",
            "symbol": "BTCUSDT",
            "lastUpdateId": 210,
            "bids": [["35000.5", "1.0"]],
            "asks": [["35001.5", "2.0"]],
        },
    ]
    out_file = tmp_path / "snapshot.ndjson.gz"
    with gzip.open(out_file, "wt", encoding="utf-8") as fh:
        for evt in events:
            fh.write(json.dumps(evt))
            fh.write("\n")
    return out_file


def test_iter_raw_events_normalizes(tmp_path: Path):
    file_path = _write_sample_file(tmp_path)

    events = list(iter_raw_events(file_path))
    assert len(events) == 2
    first = events[0]
    assert first["event_type"] == "depthUpdate"
    assert first["symbol"] == "BTCUSDT"
    assert first["bids"][0] == (35000.0, 1.5)
    assert first["asks"][0] == (35001.0, 1.0)


def test_load_events_dataframe(tmp_path: Path):
    file_path = _write_sample_file(tmp_path)

    df = load_events_dataframe(file_path)
    assert list(df.columns) == [
        "event_type",
        "event_time",
        "symbol",
        "first_update_id",
        "last_update_id",
        "prev_update_id",
        "bids",
        "asks",
        "event_dt",
    ]
    assert len(df) == 2
    assert pd.api.types.is_datetime64tz_dtype(df["event_dt"])


def test_compute_top_of_book(tmp_path: Path):
    file_path = _write_sample_file(tmp_path)
    df = load_events_dataframe(file_path)

    top_df = compute_top_of_book(df)
    assert list(top_df.columns) == [
        "best_bid_px",
        "best_bid_qty",
        "best_ask_px",
        "best_ask_qty",
        "mid_price",
        "spread",
        "event_dt",
        "event_time",
    ]
    assert np.isclose(top_df.iloc[0]["mid_price"], 35000.5)
    assert np.isclose(top_df.iloc[1]["spread"], 1.0)


def test_explode_levels(tmp_path: Path):
    file_path = _write_sample_file(tmp_path)
    df = load_events_dataframe(file_path)

    bid_levels = explode_levels(df, side="bids", depth=1)
    assert len(bid_levels) == 2
    assert set(bid_levels.columns) == {
        "event_time",
        "event_dt",
        "side",
        "level",
        "price",
        "quantity",
    }
    assert (bid_levels["side"] == "bids").all()
    assert bid_levels.iloc[0]["level"] == 1


@pytest.mark.parametrize("fmt", ["csv", "pickle", "npz"])
def test_export_dataframe(tmp_path: Path, fmt: str):
    file_path = _write_sample_file(tmp_path)
    df = load_events_dataframe(file_path)
    top_df = compute_top_of_book(df)

    output = tmp_path / f"top_of_book.{fmt}"
    saved_path = export_dataframe(top_df, output, fmt=fmt)
    assert saved_path.exists()

    if fmt == "csv":
        loaded = pd.read_csv(saved_path)
        assert len(loaded) == len(top_df)
    elif fmt == "pickle":
        loaded = pd.read_pickle(saved_path)
        assert len(loaded) == len(top_df)
    else:  # npz
        data = np.load(saved_path)
        keys = sorted(data.files)
        expected_keys = sorted(top_df.columns)
        assert keys == expected_keys


def test_iter_snapshot_records(tmp_path: Path):
    path = _write_sample_snapshot_file(tmp_path)
    records = list(iter_snapshot_records(path))
    assert len(records) == 2
    assert records[0]["symbol"] == "BTCUSDT"
    assert records[0]["bids"][0] == (35000.0, 1.5)


def test_load_snapshot_dataframe(tmp_path: Path):
    path = _write_sample_snapshot_file(tmp_path)
    df = load_snapshot_dataframe(path)
    assert list(df.columns) == [
        "ts_utc",
        "symbol",
        "last_update_id",
        "bids",
        "asks",
        "ts_dt",
    ]
    assert len(df) == 2
    assert pd.api.types.is_datetime64tz_dtype(df["ts_dt"])
