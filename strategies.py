"""
Minimal `hftbacktest` strategy scaffolding.

This module wires together three small building blocks:

1. Transform OKX CSV data into `hftbacktest`'s event dtype (see ``data_utils.demo_convert_okx_csv_for_hftbacktest``).
2. Configure a :class:`hftbacktest.BacktestAsset` with latency, queue, and fee models.
3. Run a tiny passive spread-capture strategy that works entirely through the official
   :class:`hftbacktest.HashMapMarketDepthBacktest` API so it can be executed inside
   `@njit`-compiled notebooks.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Sequence, Union

import numpy as np
import pandas as pd
from numba import njit

from hftbacktest import (
    ALL_ASSETS,
    BacktestAsset,
    GTC,
    LIMIT,
    MARKET,
    HashMapMarketDepthBacktest,
    Recorder,
)

# ---------------------------------------------------------------------------
# Asset construction
# ---------------------------------------------------------------------------

FeedSource = Union[str, np.ndarray]
FeedInput = Union[FeedSource, Sequence[FeedSource]]
SnapshotInput = Union[str, np.ndarray, None]

_QUEUE_METHODS = {
    "risk_adverse": "risk_adverse_queue_model",
    "log_prob": "log_prob_queue_model",
    "log_prob_v2": "log_prob_queue_model2",
    "power": "power_prob_queue_model",
    "power_v2": "power_prob_queue_model2",
    "power_v3": "power_prob_queue_model3",
    "l3_fifo": "l3_fifo_queue_model",
}

_EXCHANGE_METHODS = {
    "partial_fill": "partial_fill_exchange",
    "no_partial_fill": "no_partial_fill_exchange",
}


@dataclass
class AssetBuildConfig:
    """
    Parameters required to construct a :class:`BacktestAsset`.

    Args:
        feed: Path(s) to `.npz` feed(s) or in-memory event arrays.
        tick_size: Smallest permissible price increment.
        lot_size: Minimum tradable quantity.
        contract_size: Multiplier for linear contracts (1.0 for spot).
        maker_fee: Maker fee in decimal form (positive values represent a cost).
        taker_fee: Taker fee in decimal form.
        order_latency_ns: Order entry latency in nanoseconds.
        response_latency_ns: Response latency in nanoseconds.
        queue_model: Queue model key (see ``_QUEUE_METHODS``).
        exchange_model: Exchange fill model key (see ``_EXCHANGE_METHODS``).
        last_trades_capacity: How many recent market trades to keep (0 disables storage).
        initial_snapshot: Optional L2 snapshot to seed the book.
    """

    feed: FeedInput
    tick_size: float = 0.1
    lot_size: float = 0.001
    contract_size: float = 1.0
    maker_fee: float = 0.0002
    taker_fee: float = 0.0007
    order_latency_ns: int = 10_000_000
    response_latency_ns: int = 10_000_000
    queue_model: str = "risk_adverse"
    exchange_model: str = "no_partial_fill"
    last_trades_capacity: int = 0
    initial_snapshot: SnapshotInput = None

    def _apply_method(self, asset: BacktestAsset, model: str, namespace: Dict[str, str], label: str) -> None:
        method_name = namespace.get(model)
        if method_name is None:
            raise ValueError(f"Unknown {label} '{model}'. Valid options: {sorted(namespace)}")
        getattr(asset, method_name)()

    def to_asset(self) -> BacktestAsset:
        asset = BacktestAsset()
        asset.data(self.feed)
        asset.linear_asset(self.contract_size)
        asset.tick_size(self.tick_size)
        asset.lot_size(self.lot_size)
        asset.constant_latency(self.order_latency_ns, self.response_latency_ns)
        self._apply_method(asset, self.queue_model, _QUEUE_METHODS, "queue_model")
        self._apply_method(asset, self.exchange_model, _EXCHANGE_METHODS, "exchange_model")
        asset.trading_value_fee_model(self.maker_fee, self.taker_fee)
        asset.last_trades_capacity(self.last_trades_capacity)
        if self.initial_snapshot is not None:
            asset.initial_snapshot(self.initial_snapshot)
        return asset


# ---------------------------------------------------------------------------
# Strategy configuration + result containers
# ---------------------------------------------------------------------------

@dataclass
class SpreadCaptureConfig:
    """
    Strategy knobs for the passive spread capture demo.

    Args:
        step_ns: Time advanced via ``hbt.elapse`` at each iteration.
        min_spread_ticks: Minimum spread (in ticks) required before quoting both sides.
        quote_offset_ticks: Price offset (in ticks) from the current best bid/ask.
        quote_ttl_ns: Cancel + refresh latency for outstanding quotes.
        order_size_lots: Order quantity in multiples of ``lot_size``.
        max_inventory_lots: Hard inventory cap expressed in lots.
        record_every_n_steps: Recorder cadence; 1 records each iteration.
    """

    step_ns: int = 50_000_000  # 50 ms
    min_spread_ticks: float = 2.0
    quote_offset_ticks: float = 0.0
    quote_ttl_ns: int = 300_000_000  # 300 ms
    order_size_lots: float = 1.0
    max_inventory_lots: float = 2.0
    record_every_n_steps: int = 1

    def validate(self) -> None:
        if self.step_ns <= 0:
            raise ValueError("step_ns must be > 0.")
        if self.min_spread_ticks <= 0:
            raise ValueError("min_spread_ticks must be > 0.")
        if self.quote_ttl_ns <= 0:
            raise ValueError("quote_ttl_ns must be > 0.")
        if self.order_size_lots <= 0:
            raise ValueError("order_size_lots must be > 0.")
        if self.max_inventory_lots < self.order_size_lots:
            raise ValueError("max_inventory_lots must be >= order_size_lots.")
        if self.record_every_n_steps <= 0:
            raise ValueError("record_every_n_steps must be >= 1.")


@dataclass
class BookPressureConfig:
    """
    Adaptive maker strategy tuned via order-book pressure and short-term momentum.
    """

    step_ns: int = 20_000_000
    maker_order_ttl_ns: int = 250_000_000
    max_quote_levels: int = 2
    quote_spacing_ticks: float = 1.0
    quote_offset_ticks: float = 0.5
    min_spread_ticks: float = 1.0
    imbalance_entry: float = 0.60
    imbalance_exit: float = 0.52
    momentum_window: int = 20
    momentum_threshold_ticks: float = 0.8
    order_size_lots: float = 1.5
    max_inventory_lots: float = 5.0
    taker_size_lots: float = 2.0
    taker_cooldown_ns: int = 200_000_000
    record_every_n_steps: int = 5

    def validate(self) -> None:
        if self.step_ns <= 0:
            raise ValueError("step_ns must be > 0.")
        if self.maker_order_ttl_ns <= 0:
            raise ValueError("maker_order_ttl_ns must be > 0.")
        if self.max_quote_levels <= 0:
            raise ValueError("max_quote_levels must be >= 1.")
        if self.quote_spacing_ticks <= 0:
            raise ValueError("quote_spacing_ticks must be > 0.")
        if self.quote_offset_ticks < 0:
            raise ValueError("quote_offset_ticks must be >= 0.")
        if self.min_spread_ticks <= 0:
            raise ValueError("min_spread_ticks must be > 0.")
        if not (0.5 < self.imbalance_entry < 1.0):
            raise ValueError("imbalance_entry must be between 0.5 and 1.0.")
        if not (0.5 < self.imbalance_exit <= self.imbalance_entry):
            raise ValueError("imbalance_exit must be in (0.5, imbalance_entry].")
        if self.momentum_window < 3:
            raise ValueError("momentum_window must be >= 3.")
        if self.momentum_threshold_ticks <= 0:
            raise ValueError("momentum_threshold_ticks must be > 0.")
        if self.order_size_lots <= 0:
            raise ValueError("order_size_lots must be > 0.")
        if self.max_inventory_lots < self.order_size_lots:
            raise ValueError("max_inventory_lots must be >= order_size_lots.")
        if self.taker_size_lots <= 0:
            raise ValueError("taker_size_lots must be > 0.")
        if self.taker_cooldown_ns <= 0:
            raise ValueError("taker_cooldown_ns must be > 0.")
        if self.record_every_n_steps <= 0:
            raise ValueError("record_every_n_steps must be >= 1.")


@dataclass
class StrategyRunResult:
    """Simple wrapper for backtest outputs."""

    records: np.ndarray
    frame: pd.DataFrame
    summary: Dict[str, float]
    strategy_config: SpreadCaptureConfig | BookPressureConfig


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def run_spread_capture_backtest(
    asset_config: AssetBuildConfig,
    strategy_config: SpreadCaptureConfig | None = None,
    record_capacity: int = 200_000,
) -> StrategyRunResult:
    """
    Execute the passive spread capture loop inside `hftbacktest`.

    Args:
        asset_config: How to construct the :class:`BacktestAsset`.
        strategy_config: Parameters for the quoting logic.
        record_capacity: Number of rows allocated inside :class:`Recorder`.

    Returns:
        :class:`StrategyRunResult` containing both the structured record array and a pandas view.
    """

    if record_capacity <= 0:
        raise ValueError("record_capacity must be >= 1.")

    strat_cfg = strategy_config or SpreadCaptureConfig()
    strat_cfg.validate()

    asset = asset_config.to_asset()
    hbt = HashMapMarketDepthBacktest([asset])
    recorder = Recorder(num_assets=1, record_size=record_capacity)

    try:
        _passive_spread_capture(
            hbt,
            recorder.recorder,
            step_ns=strat_cfg.step_ns,
            min_spread_ticks=strat_cfg.min_spread_ticks,
            quote_offset_ticks=strat_cfg.quote_offset_ticks,
            quote_ttl_ns=strat_cfg.quote_ttl_ns,
            order_size_lots=strat_cfg.order_size_lots,
            max_inventory_lots=strat_cfg.max_inventory_lots,
            record_every_n_steps=strat_cfg.record_every_n_steps,
        )
    except IndexError as exc:
        raise RuntimeError(
            "Recorder capacity exhausted. Increase `record_capacity` or `record_every_n_steps`."
        ) from exc
    finally:
        # Cleanup ensures subsequent runs do not leak handles.
        hbt.clear_inactive_orders(ALL_ASSETS)
        hbt.close()

    raw_records = recorder.get(asset_no=0)
    frame = _records_to_frame(raw_records)
    summary = _build_summary(frame)
    return StrategyRunResult(records=raw_records, frame=frame, summary=summary, strategy_config=strat_cfg)


def run_book_pressure_strategy(
    asset_config: AssetBuildConfig,
    strategy_config: BookPressureConfig | None = None,
    record_capacity: int = 300_000,
) -> StrategyRunResult:
    """
    Execute the adaptive maker strategy driven by imbalance + short-term momentum.
    """

    if record_capacity <= 0:
        raise ValueError("record_capacity must be >= 1.")

    strat_cfg = strategy_config or BookPressureConfig()
    strat_cfg.validate()

    asset = asset_config.to_asset()
    hbt = HashMapMarketDepthBacktest([asset])
    recorder = Recorder(num_assets=1, record_size=record_capacity)

    try:
        _book_pressure_strategy(
            hbt,
            recorder.recorder,
            step_ns=strat_cfg.step_ns,
            maker_order_ttl_ns=strat_cfg.maker_order_ttl_ns,
            max_quote_levels=strat_cfg.max_quote_levels,
            quote_spacing_ticks=strat_cfg.quote_spacing_ticks,
            quote_offset_ticks=strat_cfg.quote_offset_ticks,
            min_spread_ticks=strat_cfg.min_spread_ticks,
            imbalance_entry=strat_cfg.imbalance_entry,
            imbalance_exit=strat_cfg.imbalance_exit,
            momentum_window=strat_cfg.momentum_window,
            momentum_threshold_ticks=strat_cfg.momentum_threshold_ticks,
            order_size_lots=strat_cfg.order_size_lots,
            max_inventory_lots=strat_cfg.max_inventory_lots,
            taker_size_lots=strat_cfg.taker_size_lots,
            taker_cooldown_ns=strat_cfg.taker_cooldown_ns,
            record_every_n_steps=strat_cfg.record_every_n_steps,
        )
    except IndexError as exc:
        raise RuntimeError(
            "Recorder capacity exhausted. Increase `record_capacity` or `record_every_n_steps`."
        ) from exc
    finally:
        hbt.clear_inactive_orders(ALL_ASSETS)
        hbt.close()

    raw_records = recorder.get(asset_no=0)
    frame = _records_to_frame(raw_records)
    summary = _build_summary(frame)
    return StrategyRunResult(records=raw_records, frame=frame, summary=summary, strategy_config=strat_cfg)


# ---------------------------------------------------------------------------
# NumPy/pandas helpers
# ---------------------------------------------------------------------------

def _records_to_frame(records: np.ndarray) -> pd.DataFrame:
    if records.size == 0:
        cols = ["timestamp", "price", "position", "balance", "fee", "num_trades", "trading_volume", "trading_value"]
        return pd.DataFrame(columns=cols + ["equity"])

    frame = pd.DataFrame.from_records(records)
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], unit="ns", utc=True)
    frame["equity"] = frame["balance"] + frame["position"] * frame["price"]
    return frame


def _build_summary(frame: pd.DataFrame) -> Dict[str, float]:
    if frame.empty:
        return {
            "total_pnl": 0.0,
            "max_drawdown": 0.0,
            "num_trades": 0.0,
            "turnover": 0.0,
            "final_inventory": 0.0,
        }

    equity = frame["equity"]
    pnl = float(equity.iloc[-1] - equity.iloc[0])
    drawdown = float((equity.cummax() - equity).max())
    num_trades = float(frame["num_trades"].iloc[-1])
    turnover = float(frame["trading_value"].iloc[-1])
    final_inventory = float(frame["position"].iloc[-1])
    return {
        "total_pnl": pnl,
        "max_drawdown": drawdown,
        "num_trades": num_trades,
        "turnover": turnover,
        "final_inventory": final_inventory,
    }


# ---------------------------------------------------------------------------
# Numba-compiled strategy loop
# ---------------------------------------------------------------------------

@njit
def _snap_to_tick(price: float, tick_size: float) -> float:
    return np.round(price / tick_size) * tick_size


@njit
def _passive_spread_capture(
    hbt,
    recorder,
    step_ns: int,
    min_spread_ticks: float,
    quote_offset_ticks: float,
    quote_ttl_ns: int,
    order_size_lots: float,
    max_inventory_lots: float,
    record_every_n_steps: int,
) -> None:
    asset_no = 0
    buy_order_id = 0
    sell_order_id = 0
    buy_order_price = 0.0
    sell_order_price = 0.0
    buy_order_ts = 0
    sell_order_ts = 0
    next_order_id = 1
    step_counter = 0

    recorder.record(hbt)

    while True:
        status = hbt.elapse(step_ns)
        if status != 0:
            break

        step_counter += 1
        if step_counter % record_every_n_steps == 0:
            recorder.record(hbt)

        depth = hbt.depth(asset_no)
        tick_size = depth.tick_size
        lot_size = depth.lot_size
        qty = max(order_size_lots * lot_size, lot_size)
        spread_ticks = (depth.best_ask - depth.best_bid) / tick_size
        now = hbt.current_timestamp

        orders = hbt.orders(asset_no)
        if buy_order_id != 0 and not orders.__contains__(buy_order_id):
            buy_order_id = 0
        if sell_order_id != 0 and not orders.__contains__(sell_order_id):
            sell_order_id = 0

        if buy_order_id != 0 and now - buy_order_ts >= quote_ttl_ns:
            hbt.cancel(asset_no, buy_order_id, True)
            buy_order_id = 0
        if sell_order_id != 0 and now - sell_order_ts >= quote_ttl_ns:
            hbt.cancel(asset_no, sell_order_id, True)
            sell_order_id = 0

        target_bid = _snap_to_tick(depth.best_bid - quote_offset_ticks * tick_size, tick_size)
        target_ask = _snap_to_tick(depth.best_ask + quote_offset_ticks * tick_size, tick_size)

        if buy_order_id != 0 and np.abs(target_bid - buy_order_price) >= tick_size:
            hbt.cancel(asset_no, buy_order_id, True)
            buy_order_id = 0
        if sell_order_id != 0 and np.abs(target_ask - sell_order_price) >= tick_size:
            hbt.cancel(asset_no, sell_order_id, True)
            sell_order_id = 0

        if spread_ticks >= min_spread_ticks:
            state = hbt.state_values(asset_no)
            max_position = max_inventory_lots * lot_size
            inventory = state.position

            if buy_order_id == 0 and inventory + qty <= max_position:
                order_id = next_order_id
                next_order_id += 1
                result = hbt.submit_buy_order(asset_no, order_id, target_bid, qty, GTC, LIMIT, True)
                if result == 0:
                    buy_order_id = order_id
                    buy_order_price = target_bid
                    buy_order_ts = now

            if sell_order_id == 0 and inventory - qty >= -max_position:
                order_id = next_order_id
                next_order_id += 1
                result = hbt.submit_sell_order(asset_no, order_id, target_ask, qty, GTC, LIMIT, True)
                if result == 0:
                    sell_order_id = order_id
                    sell_order_price = target_ask
                    sell_order_ts = now
        else:
            if buy_order_id != 0:
                hbt.cancel(asset_no, buy_order_id, True)
                buy_order_id = 0
            if sell_order_id != 0:
                hbt.cancel(asset_no, sell_order_id, True)
                sell_order_id = 0


@njit
def _book_pressure_strategy(
    hbt,
    recorder,
    step_ns: int,
    maker_order_ttl_ns: int,
    max_quote_levels: int,
    quote_spacing_ticks: float,
    quote_offset_ticks: float,
    min_spread_ticks: float,
    imbalance_entry: float,
    imbalance_exit: float,
    momentum_window: int,
    momentum_threshold_ticks: float,
    order_size_lots: float,
    max_inventory_lots: float,
    taker_size_lots: float,
    taker_cooldown_ns: int,
    record_every_n_steps: int,
) -> None:
    asset_no = 0
    buy_ids = np.zeros(max_quote_levels, dtype=np.int64)
    buy_prices = np.zeros(max_quote_levels, dtype=np.float64)
    buy_ts = np.zeros(max_quote_levels, dtype=np.int64)
    sell_ids = np.zeros(max_quote_levels, dtype=np.int64)
    sell_prices = np.zeros(max_quote_levels, dtype=np.float64)
    sell_ts = np.zeros(max_quote_levels, dtype=np.int64)
    target_bids = np.zeros(max_quote_levels, dtype=np.float64)
    target_asks = np.zeros(max_quote_levels, dtype=np.float64)

    momentum_buf = np.zeros(momentum_window, dtype=np.float64)
    momentum_idx = 0
    momentum_count = 0
    last_taker_ts = -taker_cooldown_ns
    step_counter = 0
    next_order_id = 1

    recorder.record(hbt)

    while True:
        status = hbt.elapse(step_ns)
        if status != 0:
            break

        step_counter += 1
        if step_counter % record_every_n_steps == 0:
            recorder.record(hbt)

        depth = hbt.depth(asset_no)
        tick_size = depth.tick_size
        if tick_size <= 0.0:
            tick_size = 1e-6
        lot_size = depth.lot_size
        qty = max(order_size_lots * lot_size, lot_size)
        taker_qty = max(taker_size_lots * lot_size, lot_size)
        spread_ticks = (depth.best_ask - depth.best_bid) / tick_size
        mid = (depth.best_bid + depth.best_ask) / 2.0
        total_qty = depth.best_bid_qty + depth.best_ask_qty
        imbalance = 0.5
        if total_qty > 0:
            imbalance = depth.best_bid_qty / total_qty

        if momentum_count < momentum_window:
            momentum_buf[momentum_idx] = mid
            momentum_count += 1
            momentum_ticks = 0.0
        else:
            prev_mid = momentum_buf[momentum_idx]
            momentum_buf[momentum_idx] = mid
            momentum_ticks = (mid - prev_mid) / tick_size
        momentum_idx += 1
        if momentum_idx == momentum_window:
            momentum_idx = 0
        momentum_ready = momentum_count >= momentum_window
        if not momentum_ready:
            momentum_ticks = 0.0

        buy_bias = 0.0
        sell_bias = 0.0
        if imbalance >= imbalance_entry:
            buy_bias -= 0.5 * quote_spacing_ticks
            sell_bias += 0.3 * quote_spacing_ticks
        elif imbalance <= (1.0 - imbalance_entry):
            sell_bias -= 0.5 * quote_spacing_ticks
            buy_bias += 0.3 * quote_spacing_ticks
        else:
            if imbalance >= imbalance_exit:
                buy_bias -= 0.3 * quote_spacing_ticks
            if imbalance <= (1.0 - imbalance_exit):
                sell_bias -= 0.3 * quote_spacing_ticks

        if momentum_ready:
            if momentum_ticks >= momentum_threshold_ticks:
                buy_bias -= 0.3 * quote_spacing_ticks
                sell_bias += 0.3 * quote_spacing_ticks
            elif momentum_ticks <= -momentum_threshold_ticks:
                sell_bias -= 0.3 * quote_spacing_ticks
                buy_bias += 0.3 * quote_spacing_ticks

        for level in range(max_quote_levels):
            level_offset = quote_offset_ticks + level * quote_spacing_ticks
            bid_offset = level_offset + buy_bias
            if bid_offset < 0.0:
                bid_offset = 0.0
            ask_offset = level_offset + sell_bias
            if ask_offset < 0.0:
                ask_offset = 0.0

            target_bid = _snap_to_tick(depth.best_bid - bid_offset * tick_size, tick_size)
            if target_bid >= depth.best_ask:
                target_bid = depth.best_ask - tick_size
            if target_bid > depth.best_bid:
                target_bid = depth.best_bid
            target_bids[level] = target_bid

            target_ask = _snap_to_tick(depth.best_ask + ask_offset * tick_size, tick_size)
            if target_ask <= depth.best_bid:
                target_ask = depth.best_bid + tick_size
            if target_ask < depth.best_ask:
                target_ask = depth.best_ask
            target_asks[level] = target_ask

        orders = hbt.orders(asset_no)
        now = hbt.current_timestamp
        for i in range(max_quote_levels):
            oid = buy_ids[i]
            if oid != 0:
                if not orders.__contains__(oid):
                    buy_ids[i] = 0
                    continue
                if now - buy_ts[i] >= maker_order_ttl_ns:
                    hbt.cancel(asset_no, oid, True)
                    buy_ids[i] = 0
                    continue
                if np.abs(target_bids[i] - buy_prices[i]) >= tick_size:
                    hbt.cancel(asset_no, oid, True)
                    buy_ids[i] = 0
        for i in range(max_quote_levels):
            oid = sell_ids[i]
            if oid != 0:
                if not orders.__contains__(oid):
                    sell_ids[i] = 0
                    continue
                if now - sell_ts[i] >= maker_order_ttl_ns:
                    hbt.cancel(asset_no, oid, True)
                    sell_ids[i] = 0
                    continue
                if np.abs(target_asks[i] - sell_prices[i]) >= tick_size:
                    hbt.cancel(asset_no, oid, True)
                    sell_ids[i] = 0

        if spread_ticks < min_spread_ticks:
            for i in range(max_quote_levels):
                if buy_ids[i] != 0:
                    hbt.cancel(asset_no, buy_ids[i], True)
                    buy_ids[i] = 0
                if sell_ids[i] != 0:
                    hbt.cancel(asset_no, sell_ids[i], True)
                    sell_ids[i] = 0
            continue

        state = hbt.state_values(asset_no)
        inventory = state.position
        max_position = max_inventory_lots * lot_size

        active_buy_orders = 0
        active_sell_orders = 0
        for i in range(max_quote_levels):
            if buy_ids[i] != 0:
                active_buy_orders += 1
            if sell_ids[i] != 0:
                active_sell_orders += 1

        buy_exposure = active_buy_orders * qty
        sell_exposure = active_sell_orders * qty

        for i in range(max_quote_levels):
            if buy_ids[i] == 0:
                potential = inventory + buy_exposure + qty
                if potential <= max_position:
                    order_id = next_order_id
                    next_order_id += 1
                    res = hbt.submit_buy_order(asset_no, order_id, target_bids[i], qty, GTC, LIMIT, True)
                    if res == 0:
                        buy_ids[i] = order_id
                        buy_prices[i] = target_bids[i]
                        buy_ts[i] = now
                        buy_exposure += qty
            if sell_ids[i] == 0:
                potential = inventory - sell_exposure - qty
                if potential >= -max_position:
                    order_id = next_order_id
                    next_order_id += 1
                    res = hbt.submit_sell_order(asset_no, order_id, target_asks[i], qty, GTC, LIMIT, True)
                    if res == 0:
                        sell_ids[i] = order_id
                        sell_prices[i] = target_asks[i]
                        sell_ts[i] = now
                        sell_exposure += qty

        taker_side = 0
        taker_amount = 0.0
        if now - last_taker_ts >= taker_cooldown_ns:
            if inventory >= max_position and taker_qty > 0:
                taker_side = -1
                taker_amount = min(taker_qty, inventory)
            elif inventory <= -max_position and taker_qty > 0:
                taker_side = 1
                taker_amount = min(taker_qty, -inventory)
            elif momentum_ready and momentum_ticks >= momentum_threshold_ticks and inventory < 0:
                taker_side = 1
                taker_amount = min(taker_qty, -inventory)
            elif momentum_ready and momentum_ticks <= -momentum_threshold_ticks and inventory > 0:
                taker_side = -1
                taker_amount = min(taker_qty, inventory)

        min_trade = lot_size * 0.5
        if taker_side != 0 and taker_amount >= min_trade:
            if taker_side > 0:
                res = hbt.submit_buy_order(asset_no, next_order_id, depth.best_ask, taker_amount, GTC, MARKET, True)
                next_order_id += 1
                if res == 0:
                    last_taker_ts = now
                    for i in range(max_quote_levels):
                        if sell_ids[i] != 0:
                            hbt.cancel(asset_no, sell_ids[i], True)
                            sell_ids[i] = 0
            else:
                res = hbt.submit_sell_order(asset_no, next_order_id, depth.best_bid, taker_amount, GTC, MARKET, True)
                next_order_id += 1
                if res == 0:
                    last_taker_ts = now
                    for i in range(max_quote_levels):
                        if buy_ids[i] != 0:
                            hbt.cancel(asset_no, buy_ids[i], True)
                            buy_ids[i] = 0
