"""
================================================================================
ORDERBOOK FOOTPRINT SIGNAL ANALYZER
================================================================================
Description:
    Real-time cryptocurrency trading signal generator that analyzes Binance 
    orderbook depth data and trade volume to identify directional trading 
    opportunities using orderbook imbalance and trend confirmation.

Main Features:
    • Real-time WebSocket streaming of orderbook depth (20-level) and trades
    • Orderbook imbalance detection (bid/ask volume ratio analysis)
    • Trend confirmation using price action and moving averages
    • Trade volume pressure analysis from actual executed trades
    • Automated entry/exit signal generation with stop-loss and target levels
    • Real-time P&L tracking and trade statistics
    • Color-coded console output for easy monitoring
    • Configurable thresholds for imbalance, trend lookback, and holding periods
    • Multi-tick confirmation to reduce false signals

Key Parameters:
    - SYMBOL: Trading pair (default: BTC/USDT)
    - IMBALANCE_THRESHOLD: Minimum bid/ask dominance to trigger signal (65%)
    - CONFIRM_TICKS: Minimum consecutive ticks to confirm signal (5 ticks)
    - TREND_LOOKBACK: Historical ticks for trend analysis (30 ticks)
    - MIN_MOVE_PCT: Minimum price movement before entry (0.03%)
    - TARGET_PCT / STOP_PCT: Profit target and loss limit (0.1% each)

Signal Logic:
    1. Calculate orderbook imbalance and moving average
    2. Detect trend direction from price action
    3. Confirm signal persists for minimum ticks
    4. Wait for minimum price movement in signal direction
    5. Generate buy/sell signal based on combined analysis
    6. Execute with defined stop-loss and profit target

Target Users:
    Traders using orderbook microstructure analysis for high-frequency or 
    intraday trading strategies on cryptocurrency markets.
================================================================================
"""

import asyncio
import json
import websockets
import numpy as np
from colorama import Fore, Style, init
from collections import deque
from datetime import datetime
import csv
import os

init(autoreset=True)

SYMBOL = "btcusdt"
DEPTH_URL = f"wss://stream.binance.com:9443/ws/{SYMBOL}@depth20@100ms"
TRADE_URL = f"wss://stream.binance.com:9443/ws/{SYMBOL}@trade"

# --- Config ---
WINDOW               = 20     # ticks to keep in memory (~2 seconds)
IMBALANCE_THRESHOLD  = 0.65   # 65% bid or ask dominance to trigger
CONFIRM_TICKS        = 5      # signal must persist this many ticks before entry
TARGET_PCT           = 0.001  # 0.1% target
STOP_PCT             = 0.001  # 0.1% stop
TREND_LOOKBACK       = 30     # ticks (~3 seconds)
MIN_MOVE_TICKS       = 3      # price must have moved in signal direction
MIN_MOVE_PCT         = 0.0003 # at least 0.03% move before entry
TRADE_WINDOW_SEC     = 2.0    # seconds of real trades to look back at

# --- State ---
imbalance_history = deque(maxlen=WINDOW)
mid_history       = deque(maxlen=100)
recent_trades     = deque()    # (timestamp, side, qty) — 'buy' or 'sell'
trade             = None
stats             = {"trades": 0, "wins": 0, "losses": 0, "pnl": 0.0}


def compute_imbalance(bids, asks):
    bid_vol = sum(float(b[1]) for b in bids[:10])
    ask_vol = sum(float(a[1]) for a in asks[:10])
    total = bid_vol + ask_vol
    return bid_vol / total if total > 0 else 0.5


def mid_price(bids, asks):
    return (float(bids[0][0]) + float(asks[0][0])) / 2


def real_trade_pressure():
    """Returns (buy_vol, sell_vol) from actual executed trades in last TRADE_WINDOW_SEC."""
    now = datetime.now().timestamp()
    cutoff = now - TRADE_WINDOW_SEC
    buy_vol = sell_vol = 0.0
    for (ts, side, qty) in recent_trades:
        if ts < cutoff:
            continue
        if side == "buy":
            buy_vol += qty
        else:
            sell_vol += qty
    return buy_vol, sell_vol


def print_scoreboard():
    t = stats
    wr = (t["wins"] / t["trades"] * 100) if t["trades"] > 0 else 0
    pnl_color = Fore.GREEN if t["pnl"] >= 0 else Fore.RED
    print(f"\n{Fore.WHITE}{'━'*68}")
    print(
        f" Trades: {t['trades']}  |  "
        f"{Fore.GREEN}W: {t['wins']}{Style.RESET_ALL}  "
        f"{Fore.RED}L: {t['losses']}{Style.RESET_ALL}  |  "
        f"Winrate: {wr:.1f}%  |  "
        f"PnL: {pnl_color}${t['pnl']:+.2f}{Style.RESET_ALL}"
    )
    print(f"{Fore.WHITE}{'━'*68}\n")


def write_trade_to_csv(direction, entry, exit_price, target, stop, entry_time, exit_time, outcome, pnl):
    """Write detailed trade record to trades.csv"""
    filename = "trades.csv"
    file_exists = os.path.isfile(filename)
    
    with open(filename, "a", newline="") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow([
                "Timestamp", "Direction", "Entry Price", "Exit Price", 
                "Target", "Stop", "Duration (s)", "Outcome", "PnL"
            ])
        
        duration = (exit_time - entry_time).seconds
        writer.writerow([
            entry_time.strftime("%Y-%m-%d %H:%M:%S"),
            direction,
            f"{entry:.2f}",
            f"{exit_price:.2f}",
            f"{target:.2f}",
            f"{stop:.2f}",
            duration,
            outcome,
            f"{pnl:+.4f}"
        ])


def write_summary_to_csv():
    """Write/update summary statistics to summary.csv"""
    filename = "summary.csv"
    t = stats
    wr = (t["wins"] / t["trades"] * 100) if t["trades"] > 0 else 0
    
    with open(filename, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Metric", "Value"])
        writer.writerow(["Total Trades", t["trades"]])
        writer.writerow(["Wins", t["wins"]])
        writer.writerow(["Losses", t["losses"]])
        writer.writerow(["Win Rate (%)", f"{wr:.2f}"])
        writer.writerow(["Total PnL ($)", f"{t['pnl']:+.2f}"])
        writer.writerow(["Timestamp", datetime.now().strftime("%Y-%m-%d %H:%M:%S")])


def open_trade(direction, entry, ts):
    global trade
    if direction == "LONG":
        target = entry * (1 + TARGET_PCT)
        stop   = entry * (1 - STOP_PCT)
    else:
        target = entry * (1 - TARGET_PCT)
        stop   = entry * (1 + STOP_PCT)

    trade = {
        "direction": direction,
        "entry":     entry,
        "target":    target,
        "stop":      stop,
        "open_time": ts,
    }

    arrow = Fore.GREEN + "▲ LONG " if direction == "LONG" else Fore.RED + "▼ SHORT"
    print(f"\n  📥 ENTER  {arrow}{Style.RESET_ALL}  @ ${entry:,.2f}")
    print(f"  🎯 Target @ ${target:,.2f}  ({'+' if direction=='LONG' else '-'}{TARGET_PCT*100:.1f}%)")
    print(f"  🛑 Stop   @ ${stop:,.2f}  ({'-' if direction=='LONG' else '+'}{STOP_PCT*100:.1f}%)")


def check_trade(current_price, ts):
    global trade
    if trade is None:
        return

    d = trade["direction"]
    hit_target = (d == "LONG"  and current_price >= trade["target"]) or \
                 (d == "SHORT" and current_price <= trade["target"])
    hit_stop   = (d == "LONG"  and current_price <= trade["stop"])   or \
                 (d == "SHORT" and current_price >= trade["stop"])

    if not hit_target and not hit_stop:
        return

    elapsed = (ts - trade["open_time"]).seconds
    pnl = abs(trade["target"] - trade["entry"]) if hit_target else -abs(trade["stop"] - trade["entry"])

    stats["trades"] += 1
    stats["pnl"]    += pnl

    if hit_target:
        stats["wins"] += 1
        outcome = "WIN"
        print(f"  ✅ {Fore.GREEN}WIN {Style.RESET_ALL} @ ${current_price:,.2f}  |  "
              f"{Fore.GREEN}+${pnl:.2f}{Style.RESET_ALL}  |  {elapsed}s")
    else:
        stats["losses"] += 1
        outcome = "LOSS"
        print(f"  ❌ {Fore.RED}LOSS{Style.RESET_ALL} @ ${current_price:,.2f}  |  "
              f"{Fore.RED}-${abs(pnl):.2f}{Style.RESET_ALL}  |  {elapsed}s")

    # Write to CSV files
    write_trade_to_csv(
        direction=d,
        entry=trade["entry"],
        exit_price=current_price,
        target=trade["target"],
        stop=trade["stop"],
        entry_time=trade["open_time"],
        exit_time=ts,
        outcome=outcome,
        pnl=pnl
    )
    write_summary_to_csv()

    trade = None
    print_scoreboard()


async def trade_stream():
    """Listens to real executed trades and stores them in recent_trades."""
    async with websockets.connect(TRADE_URL) as ws:
        while True:
            msg  = await ws.recv()
            data = json.loads(msg)
            # m=True means seller is market maker → buyer is aggressor → buy trade
            # m=False means buyer is market maker → seller is aggressor → sell trade
            side = "buy" if not data["m"] else "sell"
            qty  = float(data["q"])
            ts   = datetime.now().timestamp()
            recent_trades.append((ts, side, qty))
            # prune old entries beyond window
            cutoff = ts - TRADE_WINDOW_SEC - 1
            while recent_trades and recent_trades[0][0] < cutoff:
                recent_trades.popleft()


async def depth_stream():
    """Main loop — processes order book and fires paper trades."""
    async with websockets.connect(DEPTH_URL) as ws:
        print(Fore.CYAN + "Connected to Binance — watching BTC/USDT orderbook\n")

        while True:
            msg  = await ws.recv()
            data = json.loads(msg)

            bids = sorted(data["bids"][:10], key=lambda x: float(x[0]), reverse=True)
            asks = sorted(data["asks"][:10], key=lambda x: float(x[0]))

            imb = compute_imbalance(bids, asks)
            mid = mid_price(bids, asks)
            ts  = datetime.now()

            imbalance_history.append(imb)
            mid_history.append(mid)

            check_trade(mid, ts)

            if trade is None and len(imbalance_history) == WINDOW:
                recent = list(imbalance_history)[-CONFIRM_TICKS:]

                buy_signal  = all(x >= IMBALANCE_THRESHOLD for x in recent)
                sell_signal = all(x <= (1 - IMBALANCE_THRESHOLD) for x in recent)

                # Filter 1 — trend
                trending_up   = len(mid_history) >= TREND_LOOKBACK and mid > mid_history[-TREND_LOOKBACK]
                trending_down = len(mid_history) >= TREND_LOOKBACK and mid < mid_history[-TREND_LOOKBACK]

                # Filter 2 — price actually moved
                price_moved_up   = len(mid_history) >= MIN_MOVE_TICKS and mid > mid_history[-MIN_MOVE_TICKS] * (1 + MIN_MOVE_PCT)
                price_moved_down = len(mid_history) >= MIN_MOVE_TICKS and mid < mid_history[-MIN_MOVE_TICKS] * (1 - MIN_MOVE_PCT)

                # Filter 3 — real executed trades confirm the direction
                buy_vol, sell_vol = real_trade_pressure()
                total_vol = buy_vol + sell_vol
                real_buy_pressure  = (buy_vol / total_vol > 0.55) if total_vol > 0 else False
                real_sell_pressure = (sell_vol / total_vol > 0.55) if total_vol > 0 else False

                buy_confirmed  = buy_signal  and trending_up   and price_moved_up   and real_buy_pressure
                sell_confirmed = sell_signal and trending_down and price_moved_down and real_sell_pressure

                if buy_confirmed or sell_confirmed:
                    direction    = "LONG" if buy_confirmed else "SHORT"
                    imb_pct      = np.mean(recent) * 100
                    signal_color = Fore.GREEN if buy_confirmed else Fore.RED
                    flow_pct     = (buy_vol / total_vol * 100) if buy_confirmed else (sell_vol / total_vol * 100)

                    print(
                        f"{signal_color}⚡ SIGNAL [{ts.strftime('%H:%M:%S')}] "
                        f"{'BUY' if buy_confirmed else 'SELL'} pressure  |  "
                        f"Book: {imb_pct:.1f}%  |  "
                        f"Real flow: {flow_pct:.1f}%  |  "
                        f"Mid: ${mid:,.2f}{Style.RESET_ALL}"
                    )
                    open_trade(direction, mid, ts)

            await asyncio.sleep(0.1)

async def run_with_retry(coro_func):
    while True:
        try:
            await coro_func()
        except Exception as e:
            print(f"{Fore.YELLOW}⚠ Connection dropped: {e} — reconnecting in 3s...{Style.RESET_ALL}")
            await asyncio.sleep(3)

async def main():
    await asyncio.gather(
        run_with_retry(depth_stream),
        run_with_retry(trade_stream),
    )


if __name__ == "__main__":
    asyncio.run(main())

