import asyncio
import ccxt.pro as ccxtpro
import os
import json
import argparse
from dotenv import load_dotenv
import pandas as pd
import logging
from datetime import datetime, timezone
import aiohttp
import time

# ===== Múi giờ UTC =====
UTC_TZ = timezone.utc

# ===== Load ENV =====
load_dotenv()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = ""
TELEGRAM_TOPIC_ID = None

# ===== Logging =====
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)
logger.info("📂 Logging mode: console only (UTC time)")

# ===== Global data =====
all_trades = []
trade_buffer = []
logged_timestamps = set()
BATCH_INTERVAL = 1  # giây


# ====== Argument ======
def parse_arguments():
    parser = argparse.ArgumentParser(description="Trade alert monitor")
    parser.add_argument("--config", "-c", type=str, default="config/trade_alert.json")
    return parser.parse_args()


# ====== Load Config ======
def load_config(config_path):
    try:
        with open(config_path, "r") as f:
            config = json.load(f)
        logger.info(f"✅ Configuration loaded from {config_path}")
        return config
    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        raise


# ====== Telegram ======
async def send_telegram_message(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message}
    if TELEGRAM_TOPIC_ID:
        payload["message_thread_id"] = TELEGRAM_TOPIC_ID

    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload) as resp:
            if resp.status != 200:
                err = await resp.text()
                logger.error(f"Telegram API error: {err}")
            else:
                logger.info(
                    f"✅ Message sent at {datetime.now(UTC_TZ).strftime('%H:%M:%S')} (UTC)"
                )


# ====== Exchange Setup ======
def setup_exchange(exchange_id, symbol=None):
    if exchange_id.lower() == "binance":
        return None
    exchange_class = getattr(ccxtpro, exchange_id)
    is_perp = symbol and ":USDT" in symbol
    exchange_config = {"options": {"defaultType": "swap" if is_perp else "spot"}}
    return exchange_class(exchange_config)


# ====== Lọc giao dịch lớn ======
async def filter_large_trades(
    df, symbol, min_trade_amount, exchange_name, market_type="", config=None
):
    global all_trades
    if df.empty:
        return

    df["notional"] = df["amount"] * df["price"]
    grouped = (
        df.groupby(["side"])
        .agg({"amount": "sum", "price": "mean", "notional": "sum"})
        .reset_index()
    )

    for _, row in grouped.iterrows():
        total_amount = row["amount"]
        avg_price = row["price"]
        total_notional = row["notional"]
        side = row["side"]

        if total_notional < min_trade_amount:
            continue

        icon = "🟢" if side.lower() == "buy" else "🔴"
        mean_large_notional = 0
        if len(all_trades) > 0:
            trades_df = pd.DataFrame(all_trades)
            trades_df["notional"] = trades_df["amount"] * trades_df["price"]
            trades_df["timestamp_sec"] = trades_df["timestamp"] // 1000
            recent = trades_df[trades_df["timestamp_sec"] >= (int(time.time()) - 3600)]
            large_trades = recent[recent["notional"] >= min_trade_amount]
            if not large_trades.empty:
                mean_large_notional = large_trades["notional"].mean()

        now_utc = datetime.now(UTC_TZ).strftime("%Y-%m-%d %H:%M:%S")
        message = (
            f"⚠️ LARGE TRADE ALERT!\n"
            f"📊 {exchange_name.upper()} {market_type}\n"
            f"🕒 {now_utc} (UTC)\n"
            f"{icon} {side.upper()} {total_amount/1000:.2f}k @ {avg_price:.{config.get('dp',4)}f} "
            f"~ ${total_notional/1000:.2f}k [mean 60m: ${mean_large_notional/1000:.2f}k]\n"
            f"------------"
        )
        logger.info(message)
        await send_telegram_message(message)


# ====== Gom batch trade ======
async def process_trade_batch(symbol, config, exchange_name, market_type):
    global trade_buffer, all_trades
    min_trade_amount = float(config.get("min_trade_amount", 10000))

    while True:
        await asyncio.sleep(BATCH_INTERVAL)
        if not trade_buffer:
            continue

        df = pd.DataFrame(trade_buffer)
        all_trades.extend(trade_buffer)
        trade_buffer = []

        await filter_large_trades(
            df, symbol, min_trade_amount, exchange_name, market_type, config
        )


# ====== Binance WebSocket ======
async def watch_binance_trades(symbol, config):
    global trade_buffer
    exchange_name = "binance"
    is_perp = ":USDT" in symbol
    market_type = "PERP" if is_perp else ""
    symbol_ws = symbol.replace("/", "").replace(":USDT", "").lower()
    ws_url = (
        f"wss://fstream.binance.com/ws/{symbol_ws}@trade"
        if is_perp
        else f"wss://stream.binance.com:9443/ws/{symbol_ws}@trade"
    )
    logger.info(f"📡 Connecting to Binance {market_type or 'SPOT'} stream: {ws_url}")

    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(ws_url) as ws:
            logger.info("✅ Connected to Binance WebSocket.")
            async for msg in ws:
                try:
                    trade = json.loads(msg.data)
                    if "p" not in trade:
                        continue
                    trade_buffer.append(
                        {
                            "timestamp": int(trade["T"]),
                            "side": "buy" if trade["m"] == False else "sell",
                            "amount": float(trade["q"]),
                            "price": float(trade["p"]),
                        }
                    )
                except Exception as e:
                    logger.error(f"Error processing Binance message: {e}")
                    await asyncio.sleep(2)


# ====== CCXT PRO (Bybit, OKX...) ======
async def watch_ccxt_trades(exchange, symbol, config):
    global trade_buffer
    exchange_name = exchange.id
    market_type = "PERP" if ":USDT" in symbol else ""
    logger.info(f"🚀 Starting CCXT stream for {exchange_name.upper()} {symbol}")

    while True:
        try:
            trades = await exchange.watch_trades(symbol)
            if not trades:
                continue
            for trade in trades:
                trade_buffer.append(
                    {
                        "timestamp": trade["timestamp"],
                        "side": trade["side"],
                        "amount": trade["amount"],
                        "price": trade["price"],
                    }
                )
        except Exception as e:
            logger.error(f"Error in {exchange_name} stream: {e}")
            await asyncio.sleep(10)


# ====== MAIN ======
async def main(config_path):
    global TELEGRAM_CHAT_ID, TELEGRAM_TOPIC_ID
    config = load_config(config_path)
    exchange_id = config.get("exchange", "binance")
    symbol = config.get("symbol", "ENSO/USDT:USDT")
    TELEGRAM_CHAT_ID = str(config.get("group_id", ""))
    TELEGRAM_TOPIC_ID = str(config.get("topic_id", "")) or None

    exchange = setup_exchange(exchange_id, symbol)
    logger.info(f"🔧 Running {exchange_id.upper()} monitor for {symbol}")

    try:
        if exchange_id.lower() == "binance":
            await asyncio.gather(
                watch_binance_trades(symbol, config),
                process_trade_batch(
                    symbol, config, "binance", "PERP" if ":USDT" in symbol else "SPOT"
                ),
            )
        else:
            await asyncio.gather(
                watch_ccxt_trades(exchange, symbol, config),
                process_trade_batch(
                    symbol, config, exchange.id, "PERP" if ":USDT" in symbol else "SPOT"
                ),
            )
    except Exception as e:
        logger.error(f"Unhandled error: {e}")


# ====== AUTO RETRY ======
async def run_with_retry(config_path):
    retry = 0
    while True:
        try:
            await main(config_path)
        except Exception as e:
            retry += 1
            delay = min(2**retry, 60)
            logger.error(f"Error: {e}, retrying in {delay}s...")
            await asyncio.sleep(delay)
        else:
            retry = 0


# ====== START ======
if __name__ == "__main__":
    args = parse_arguments()
    try:
        asyncio.run(run_with_retry(args.config))
    except KeyboardInterrupt:
        logger.info("🛑 Program terminated by user.")
