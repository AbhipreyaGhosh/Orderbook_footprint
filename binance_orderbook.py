import asyncio
import json
import websockets
import numpy as np
from colorama import Fore, Style

SYMBOL = "btcusdt"
URL = f"wss://stream.binance.com:9443/ws/{SYMBOL}@depth20@100ms"

def compute_metrics(bids, asks):
    prices = []
    delta = []
    volume = []

    for b, a in zip(bids, asks):
        bid_price = float(b[0])
        ask_price = float(a[0])
        bid_vol = float(b[1])
        ask_vol = float(a[1])

        prices.append(bid_price)  # You can choose bid or mid-price
        d = bid_vol - ask_vol
        v = bid_vol + ask_vol

        delta.append(d)
        volume.append(v)

    return np.array(prices), np.array(delta), np.array(volume)

def volume_bar(value, max_val, width=20, color=Fore.WHITE):
    bar_len = int((value / max_val) * width) if max_val > 0 else 0
    bar = "█" * bar_len + " " * (width - bar_len)
    return f"{color}{bar}{Style.RESET_ALL}"

async def update_orderbook():
    async with websockets.connect(URL) as ws:
        while True:
            msg = await ws.recv()
            data = json.loads(msg)

            bids = sorted(data["bids"][:10], key=lambda x: float(x[0]), reverse=True)
            asks = sorted(data["asks"][:10], key=lambda x: float(x[0]))

            prices, delta, volume = compute_metrics(bids, asks)
            total = np.sum(volume)
            profile = volume / total if total > 0 else volume
            max_profile = np.max(profile)

            print("\033[H\033[J", end="")  # Clear terminal
            print(f"{'Price':>10} | {'Delta':>7} | {'Volume':>7} | Volume Profile")
            print("-" * 60)
            for i in range(10):
                color = Fore.GREEN if delta[i] >= 0 else Fore.RED
                bar = volume_bar(profile[i], max_profile, color=color)
                print(f"{prices[i]:>10.2f} | {color}{delta[i]:>7.0f}{Style.RESET_ALL} | {volume[i]:>7.0f} | {bar}")

            await asyncio.sleep(0.1)

if __name__ == "__main__":
    asyncio.run(update_orderbook())
