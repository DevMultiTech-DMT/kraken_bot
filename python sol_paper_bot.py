import asyncio
import websockets
import json
from datetime import datetime
import pandas as pd
import os

# === CONFIG ===
PAIR = "SOLUSD"  # Kraken's pair for ETH/USD, update if needed (SOL is sometimes SOLUSD or XXRPZUSD format)
VIRTUAL_BALANCE = 100.0
LOG_PATH = r"D:\\Trading bots\\Bots\\Backtest\\SOL\\paper_trades\\sol_paper_log.csv"
DIP_TRIGGER = 0.06
TAKE_PROFIT = 0.10
STOP_LOSS = 0.06

# State
peak_price = None
in_position = False
buy_price = None
balance = VIRTUAL_BALANCE
trade_log = []

async def subscribe(ws):
    payload = {
        "event": "subscribe",
        "pair": ["SOL/USD"],
        "subscription": {"name": "ticker"}
    }
    await ws.send(json.dumps(payload))

async def handle_price(price):
    global peak_price, in_position, buy_price, balance, trade_log
    now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    if peak_price is None:
        peak_price = price
        return

    if not in_position:
        if price > peak_price:
            peak_price = price
        elif (peak_price - price) / peak_price >= DIP_TRIGGER:
            # Simulate buy
            buy_price = price
            in_position = True
            print(f"[BUY] {now} @ ${price:.2f} | Balance: ${balance:.2f}")
            trade_log.append([now, "BUY", price, balance])
    else:
        # Check take profit
        if (price - buy_price) / buy_price >= TAKE_PROFIT:
            profit = (price - buy_price) / buy_price * balance
            balance += profit
            in_position = False
            peak_price = price
            print(f"[SELL - TP] {now} @ ${price:.2f} | Balance: ${balance:.2f}")
            trade_log.append([now, "SELL_TP", price, balance])

        # Check stop loss
        elif (buy_price - price) / buy_price >= STOP_LOSS:
            loss = (buy_price - price) / buy_price * balance
            balance -= loss
            in_position = False
            peak_price = price
            print(f"[SELL - SL] {now} @ ${price:.2f} | Balance: ${balance:.2f}")
            trade_log.append([now, "SELL_SL", price, balance])

async def run():
    uri = "wss://ws.kraken.com"
    try:
        async with websockets.connect(uri) as ws:
            await subscribe(ws)
            print("✅ Subscribed to SOL/USD ticker feed (paper mode)...")

            async for message in ws:
                data = json.loads(message)
                if isinstance(data, list) and "c" in data[1]:
                    price = float(data[1]['c'][0])  # 'c' is the last trade price
                    await handle_price(price)

    except Exception as e:
        print(f"❌ Disconnected or error occurred: {e}")
        save_log()


def save_log():
    if trade_log:
        df = pd.DataFrame(trade_log, columns=["Time", "Action", "Price", "Balance"])
        os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
        df.to_csv(LOG_PATH, index=False)
        print(f"📁 Log saved to: {LOG_PATH}")
    else:
        print("No trades to save.")

if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("🛑 Interrupted. Saving log...")
        save_log()
