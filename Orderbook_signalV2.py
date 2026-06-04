"""
================================================================================
ORDERBOOK FOOTPRINT SIGNAL ANALYZER — V2
================================================================================
Changes from V1:
    [1] Real Flow threshold raised to 75% (was 55%)
    [2] ADX > 25 regime filter added — skips choppy/sideways entries
    [3] 90s directional check — only exits if price is AGAINST you;
        winning trades are left completely untouched
    [4] 10s early exit — if trade is negative at 10s, exit immediately
        (cuts fast reversals cheap before they hit full stop)

Exit tier summary:
    • Fast wrong  → exit at 10s  (tiny loss)
    • Slow wrong  → exit at 90s  (medium loss, before full stop)
    • Right       → run to full target (full win)
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

SYMBOL    = "btcusdt"
DEPTH_URL = f"wss://stream.binance.com:9443/ws/{SYMBOL}@depth20@100ms"
TRADE_URL = f"wss://stream.binance.com:9443/ws/{SYMBOL}@trade"

# --- Config ---
WINDOW               = 20      # ticks to keep in memory (~2 seconds)
IMBALANCE_THRESHOLD  = 0.65    # 65% bid or ask dominance to trigger
CONFIRM_TICKS        = 5       # signal must persist this many ticks before entry
TARGET_PCT           = 0.001   # 0.1% target
STOP_PCT             = 0.001   # 0.1% stop
TREND_LOOKBACK       = 30      # ticks (~3 seconds)
MIN_MOVE_TICKS       = 3       # price must have moved in signal direction
MIN_MOVE_PCT         = 0.0003  # at least 0.03% move before entry
TRADE_WINDOW_SEC     = 2.0     # seconds of real trades to look back at

# --- V2 Config ---
REAL_FLOW_THRESHOLD  = 0.75    # [CHANGE 1] raised from 0.55 → 0.75
ADX_PERIOD           = 14      # [CHANGE 2] ADX period (needs ~14 ticks of highs/lows)
ADX_MIN              = 25      # [CHANGE 2] minimum ADX to allow entry
EARLY_EXIT_SEC       = 10      # [CHANGE 4] exit if still negative at 10s
DIRECTION_EXIT_SEC   = 90      # [CHANGE 3] directional check at 90s

# --- State ---
imbalance_history = deque(maxlen=WINDOW)
mid_history       = deque(maxlen=100)
high_history      = deque(maxlen=50)   # for ADX calc
low_history       = deque(maxlen=50)   # for ADX calc
recent_trades     = deque()
trade             = None
stats             = {"trades": 0, "wins": 0, "losses": 0, "pnl": 0.0}


# ─────────────────────────────────────────────
#  CORE CALCULATIONS
# ─────────────────────────────────────────────

def compute_imbalance(bids, asks):
    bid_vol = sum(float(b[1]) for b in bids[:10])
    ask_vol = sum(float(a[1]) for a in asks[:10])
    total = bid_vol + ask_vol
    return bid_vol / total if total > 0 else 0.5


def mid_price(bids, asks):
    return (float(bids[0][0]) + float(asks[0][0])) / 2


def real_trade_pressure():
    now    = datetime.now().timestamp()
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


# ─────────────────────────────────────────────
#  [CHANGE 2] ADX CALCULATION
# ─────────────────────────────────────────────

def compute_adx():
    """
    Lightweight ADX from mid_history treated as close, high_history, low_history.
    Returns float ADX value or None if not enough data.
    """
    n = ADX_PERIOD
    highs  = list(high_history)
    lows   = list(low_history)
    closes = list(mid_history)

    if len(highs) < n + 1 or len(lows) < n + 1 or len(closes) < n + 1:
        return None

    highs  = highs[-(n + 1):]
    lows   = lows[-(n + 1):]
    closes = closes[-(n + 1):]

    tr_list, pdm_list, ndm_list = [], [], []

    for i in range(1, len(highs)):
        high_diff = highs[i]  - highs[i - 1]
        low_diff  = lows[i - 1] - lows[i]
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i]  - closes[i - 1])
        )
        pdm = high_diff if high_diff > low_diff and high_diff > 0 else 0
        ndm = low_diff  if low_diff  > high_diff and low_diff  > 0 else 0
        tr_list.append(tr)
        pdm_list.append(pdm)
        ndm_list.append(ndm)

    atr  = np.mean(tr_list)
    if atr == 0:
        return None

    pdi = (np.mean(pdm_list) / atr) * 100
    ndi = (np.mean(ndm_list) / atr) * 100
    dx  = (abs(pdi - ndi) / (pdi + ndi)) * 100 if (pdi + ndi) > 0 else 0
    return dx   # simplified single-period DX as ADX proxy


# ─────────────────────────────────────────────
#  OUTPUT HELPERS
# ─────────────────────────────────────────────

def print_scoreboard():
    t  = stats
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


def write_trade_to_csv(direction, entry, exit_price, target, stop, entry_time, exit_time, outcome, pnl, reason, adx):
    """Write detailed trade record to trades.csv"""
    filename = "trades.csv"
    file_exists = os.path.isfile(filename)
    
    with open(filename, "a", newline="") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow([
                "Timestamp", "Direction", "Entry Price", "Exit Price", 
                "Target", "Stop", "Duration (s)", "Outcome", "PnL", "Exit Reason", "ADX"
            ])
        
        duration = (exit_time - entry_time).seconds
        adx_str = f"{adx:.1f}" if adx is not None else "N/A"
        writer.writerow([
            entry_time.strftime("%Y-%m-%d %H:%M:%S"),
            direction,
            f"{entry:.2f}",
            f"{exit_price:.2f}",
            f"{target:.2f}",
            f"{stop:.2f}",
            duration,
            outcome,
            f"{pnl:+.4f}",
            reason,
            adx_str
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


def close_trade(current_price, ts, reason, hit_target=False):
    """Unified trade closer — records result and resets state."""
    global trade
    if trade is None:
        return

    elapsed = (ts - trade["open_time"]).seconds
    won     = hit_target

    if won:
        pnl = abs(trade["target"] - trade["entry"])
        stats["wins"] += 1
        outcome = "WIN"
    else:
        # actual loss based on current price at exit, capped at full stop distance
        raw_loss = abs(current_price - trade["entry"])
        pnl      = -min(raw_loss, abs(trade["stop"] - trade["entry"]))
        stats["losses"] += 1
        outcome = "LOSS"

    stats["trades"] += 1
    stats["pnl"]    += pnl

    color  = Fore.GREEN if won else Fore.RED
    symbol = "✅ WIN " if won else "❌ LOSS"
    print(
        f"  {symbol} {color}@ ${current_price:,.2f}{Style.RESET_ALL}  |  "
        f"{color}{'+'if won else ''}{pnl:.2f}${Style.RESET_ALL}  |  "
        f"{elapsed}s  |  [{reason}]"
    )
    
    # Get current ADX for CSV logging
    adx = compute_adx()
    
    # Write to CSV files
    write_trade_to_csv(
        direction=trade["direction"],
        entry=trade["entry"],
        exit_price=current_price,
        target=trade["target"],
        stop=trade["stop"],
        entry_time=trade["open_time"],
        exit_time=ts,
        outcome=outcome,
        pnl=pnl,
        reason=reason,
        adx=adx
    )
    write_summary_to_csv()
    
    trade = None
    print_scoreboard()


# ─────────────────────────────────────────────
#  TRADE MANAGEMENT
# ─────────────────────────────────────────────

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
    """
    3-tier exit system (V2):
      Tier 0 — target / stop hit               (always fires)
      Tier 1 — [CHANGE 4] 10s negative check   (fast wrong → tiny loss)
      Tier 2 — [CHANGE 3] 90s directional check (slow wrong → medium loss)
    """
    global trade
    if trade is None:
        return

    d       = trade["direction"]
    elapsed = (ts - trade["open_time"]).seconds

    # ── Tier 0: target or stop ──────────────────────────────────────────────
    hit_target = (d == "LONG"  and current_price >= trade["target"]) or \
                 (d == "SHORT" and current_price <= trade["target"])
    hit_stop   = (d == "LONG"  and current_price <= trade["stop"])   or \
                 (d == "SHORT" and current_price >= trade["stop"])

    if hit_target:
        close_trade(current_price, ts, "TARGET", hit_target=True)
        return
    if hit_stop:
        close_trade(current_price, ts, "STOP", hit_target=False)
        return

    # ── Tier 1: [CHANGE 4] early exit at 10s if already negative ───────────
    if elapsed >= EARLY_EXIT_SEC:
        in_profit = (d == "LONG"  and current_price > trade["entry"]) or \
                    (d == "SHORT" and current_price < trade["entry"])
        # Only fire once, in the 10s window (before the 90s check takes over)
        if not in_profit and elapsed < DIRECTION_EXIT_SEC:
            print(f"  ⏱  {Fore.YELLOW}10s check{Style.RESET_ALL} — price against us, exiting early")
            close_trade(current_price, ts, "10s EXIT", hit_target=False)
            return

    # ── Tier 2: [CHANGE 3] 90s directional check ───────────────────────────
    if elapsed >= DIRECTION_EXIT_SEC:
        in_profit = (d == "LONG"  and current_price > trade["entry"]) or \
                    (d == "SHORT" and current_price < trade["entry"])

        if in_profit:
            # Trade is still working — leave it alone, let it run to target
            pass
        else:
            # Price is drifting against us — exit now, before full stop
            print(f"  ⏱  {Fore.YELLOW}90s check{Style.RESET_ALL} — price against us, exiting to limit loss")
            close_trade(current_price, ts, "90s EXIT", hit_target=False)


# ─────────────────────────────────────────────
#  WEBSOCKET STREAMS
# ─────────────────────────────────────────────

async def trade_stream():
    async with websockets.connect(TRADE_URL) as ws:
        while True:
            msg  = await ws.recv()
            data = json.loads(msg)
            side = "buy" if not data["m"] else "sell"
            qty  = float(data["q"])
            ts   = datetime.now().timestamp()
            recent_trades.append((ts, side, qty))
            cutoff = ts - TRADE_WINDOW_SEC - 1
            while recent_trades and recent_trades[0][0] < cutoff:
                recent_trades.popleft()


async def depth_stream():
    async with websockets.connect(DEPTH_URL) as ws:
        print(Fore.CYAN + "Connected to Binance — watching BTC/USDT orderbook  [V2]\n")

        while True:
            msg  = await ws.recv()
            data = json.loads(msg)

            bids = sorted(data["bids"][:10], key=lambda x: float(x[0]), reverse=True)
            asks = sorted(data["asks"][:10], key=lambda x: float(x[0]))

            imb  = compute_imbalance(bids, asks)
            mid  = mid_price(bids, asks)
            ts   = datetime.now()

            # track high/low from best bid/ask spread for ADX
            tick_high = float(asks[0][0])
            tick_low  = float(bids[0][0])

            imbalance_history.append(imb)
            mid_history.append(mid)
            high_history.append(tick_high)
            low_history.append(tick_low)

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

                # Filter 3 — real trade flow  [CHANGE 1: threshold 0.55 → 0.75]
                buy_vol, sell_vol = real_trade_pressure()
                total_vol = buy_vol + sell_vol
                real_buy_pressure  = (buy_vol  / total_vol > REAL_FLOW_THRESHOLD) if total_vol > 0 else False
                real_sell_pressure = (sell_vol / total_vol > REAL_FLOW_THRESHOLD) if total_vol > 0 else False

                # Filter 4 — [CHANGE 2] ADX regime filter (skip choppy markets)
                adx = compute_adx()
                trending_regime = (adx is not None and adx > ADX_MIN)

                buy_confirmed  = buy_signal  and trending_up   and price_moved_up   and real_buy_pressure  and trending_regime
                sell_confirmed = sell_signal and trending_down and price_moved_down and real_sell_pressure and trending_regime

                if buy_confirmed or sell_confirmed:
                    direction    = "LONG" if buy_confirmed else "SHORT"
                    imb_pct      = np.mean(recent) * 100
                    signal_color = Fore.GREEN if buy_confirmed else Fore.RED
                    flow_pct     = (buy_vol / total_vol * 100) if buy_confirmed else (sell_vol / total_vol * 100)
                    adx_str      = f"{adx:.1f}" if adx is not None else "n/a"

                    print(
                        f"{signal_color}⚡ SIGNAL [{ts.strftime('%H:%M:%S')}] "
                        f"{'BUY' if buy_confirmed else 'SELL'} pressure  |  "
                        f"Book: {imb_pct:.1f}%  |  "
                        f"Real flow: {flow_pct:.1f}%  |  "
                        f"ADX: {adx_str}  |  "
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