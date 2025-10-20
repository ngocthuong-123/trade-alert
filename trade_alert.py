import asyncio
import ccxt.pro as ccxtpro
import os
import json
import argparse
from dotenv import load_dotenv
import pandas as pd
import logging
from datetime import datetime, timezone, timedelta
import aiohttp
import time

# ===== Múi giờ Việt Nam =====
VN_TZ = timezone(timedelta(hours=7))

# ===== Load ENV =====
load_dotenv()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = ""
TELEGRAM_TOPIC_ID = None

# ===== Logging chỉ in ra terminal =====
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)
logger.info("📂 Logging mode: console only (no file output)")

# ===== Global data =====
logged_timestamps = set()
all_trades = []
trade_buffer = []
BATCH_INTERVAL = 2  # giây — gom trade mỗi 2s


# ====== Argument ======
def parse_arguments():
    parser = argparse.ArgumentParser(description='Trade alert monitor')
    parser.add_argument('--config', '-c', type=str, default="config/trade_alert.json",
                        help='Path to configuration file (default: config/trade_alert.json)')
    return parser.parse_args()


# ====== Load Config ======
def load_config(config_path):
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        logger.info(f"Configuration loaded from {config_path}")
        return config
    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        raise


# ====== Gửi Telegram ======
async def send_telegram_message(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {'chat_id': TELEGRAM_CHAT_ID, 'text': message}
    if TELEGRAM_TOPIC_ID:
        payload['message_thread_id'] = TELEGRAM_TOPIC_ID

    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload) as response:
            result = await response.json()
            if response.status != 200:
                logger.error(f"Telegram API error: {result}")
            else:
                now_vn = datetime.now(VN_TZ).strftime('%Y-%m-%d %H:%M:%S')
                logger.info(f"✅ Message sent successfully at {now_vn} (UTC+7)")


# ====== Exchange setup (CCXT) ======
def setup_exchange(exchange_id, symbol=None):
    if exchange_id.lower() == "binance":
        return None
    exchange_class = getattr(ccxtpro, exchange_id)
    is_perpetual = symbol and ':USDT' in symbol
    market_type = 'swap' if is_perpetual else 'spot'
    exchange_config = {'options': {'defaultType': market_type}}
    return exchange_class(exchange_config)


# ====== Lọc giao dịch lớn ======
async def filter_large_trades(df, symbol, min_trade_amount, exchange_name, market_type_indicator="", config=None):
    global logged_timestamps
    if df.empty:
        return

    df['notional'] = df['amount'] * df['price']
    grouped = df.groupby(['side']).agg({'amount': 'sum', 'price': 'mean', 'notional': 'sum'}).reset_index()

    for _, row in grouped.iterrows():
        total_amount = row['amount']
        avg_price = row['price']
        total_notional = row['notional']
        side = row['side']

        if total_notional >= min_trade_amount:
            icon = '🟢' if side.lower() == 'buy' else '🔴'
            current_time = datetime.now(VN_TZ).strftime("%Y-%m-%d %H:%M:%S")

            mean_large_notional = 0
            if len(all_trades) > 0:
                trades_df = pd.DataFrame(all_trades)
                trades_df['notional'] = trades_df['amount'] * trades_df['price']
                cutoff = int(time.time()) - 3600
                trades_df['timestamp_sec'] = trades_df['timestamp'] // 1000
                recent = trades_df[trades_df['timestamp_sec'] >= cutoff]
                large_trades = recent[recent['notional'] >= min_trade_amount]
                if not large_trades.empty:
                    mean_large_notional = large_trades['notional'].mean()

            warning = "⚠️ LARGE TRADE ALERT! ⚠️" if total_notional >= 90000 else "LARGE TRADE ALERT!"
            message = (
                f"{warning}\n"
                f"📊 {exchange_name.upper()} {market_type_indicator}\n"
                f"🕒 {current_time} (UTC+7)\n"
                f"{icon} {side.upper()} {total_amount/1000:.2f}k @ {avg_price:.{config.get('dp', 4)}f} "
                f"~ ${total_notional/1000:.2f}k [mean 60m: ${mean_large_notional/1000:.2f}k]\n"
                f"-----------------------------------------------------"
            )
            logger.info(message)
            await send_telegram_message(message)


# ====== Gom batch trade mỗi 2s ======
async def process_trade_batch(symbol, config):
    global trade_buffer, all_trades
    exchange_name = "binance"
    min_trade_amount = float(config.get("min_trade_amount", 10000))
    is_perpetual = ':USDT' in symbol
    market_type_indicator = "PERP" if is_perpetual else ""

    while True:
        await asyncio.sleep(BATCH_INTERVAL)
        if not trade_buffer:
            continue

        df = pd.DataFrame(trade_buffer)
        all_trades.extend(trade_buffer)
        trade_buffer = []

        await filter_large_trades(df, symbol, min_trade_amount, exchange_name, market_type_indicator, config)


# ====== Binance Socket (Spot + Futures) ======
async def watch_binance_trades(symbol, config):
    global trade_buffer
    exchange_name = "binance"
    is_perpetual = ':USDT' in symbol
    symbol_ws = symbol.replace("/", "").replace(":USDT", "").lower()

    # ✅ URL Binance WebSocket
    if is_perpetual:
        ws_url = f"wss://fstream.binance.com/ws/{symbol_ws}@trade"
        logger.info(f"📡 Connecting to Binance Futures stream: {ws_url}")
    else:
        ws_url = f"wss://stream.binance.com:9443/ws/{symbol_ws}@trade"
        logger.info(f"📡 Connecting to Binance Spot stream: {ws_url}")

    # ✅ Kết nối WebSocket
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(ws_url) as ws:
            logger.info("✅ Connected to Binance WebSocket successfully.")
            async for msg in ws:
                try:
                    trade = json.loads(msg.data)
                    if 'p' not in trade:
                        continue
                    trade_buffer.append({
                        'timestamp': int(trade['T']),
                        'side': 'buy' if trade['m'] == False else 'sell',
                        'amount': float(trade['q']),
                        'price': float(trade['p'])
                    })
                except Exception as e:
                    logger.error(f"Error processing trade message: {e}")
                    await asyncio.sleep(2)


# ====== MAIN ======
async def main(config_path):
    global TELEGRAM_CHAT_ID, TELEGRAM_TOPIC_ID
    config = load_config(config_path)
    exchange_id = config.get("exchange", "binance")
    symbol = config.get("symbol", "ENSO/USDT:USDT")

    TELEGRAM_CHAT_ID = str(config.get("group_id", ""))
    TELEGRAM_TOPIC_ID = str(config.get("topic_id", "")) or None

    exchange = setup_exchange(exchange_id, symbol)

    try:
        if exchange_id.lower() == "binance":
            await asyncio.gather(
                watch_binance_trades(symbol, config),
                process_trade_batch(symbol, config)
            )
        else:
            logger.warning("⚠️ CCXT mode not implemented for batch version.")
    except Exception as e:
        logger.error(f"Unhandled exception in main: {e}")


# ====== AUTO RETRY ======
async def run_with_retry(config_path):
    retry = 0
    while True:
        try:
            await main(config_path)
        except Exception as e:
            retry += 1
            backoff = min(2 ** retry, 60)
            logger.error(f"Error: {e}, retrying in {backoff}s...")
            await asyncio.sleep(backoff)
        else:
            retry = 0


# ====== START ======
if __name__ == "__main__":
    args = parse_arguments()
    try:
        asyncio.run(run_with_retry(args.config))
    except KeyboardInterrupt:
        logger.info("Program terminated by user.")
