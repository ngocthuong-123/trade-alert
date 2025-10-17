import asyncio
import ccxt.pro as ccxtpro
import os
import json
import argparse
from dotenv import load_dotenv
import pandas as pd
import numpy as np
import logging
from datetime import datetime
import aiohttp
import time
import math

# Load environment variables
load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = ""
TELEGRAM_TOPIC_ID = None  # Global variable for topic ID

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Track logged timestamps to avoid duplicate alerts
logged_timestamps = set()
all_trades = []  # Store all trades
time_windows = []  # For EMA
buy_volumes = []
sell_volumes = []
ratio_values = []
ema_values = []
last_alert_ema = None
current_ema_range = None  # Track the current EMA range

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Trade alert monitor')
    parser.add_argument('--config', '-c', type=str, default="config/trade_alert.json",
                        help='Path to configuration file (default: config/trade_alert.json)')
    return parser.parse_args()

def load_config(config_path):
    """Load configuration from a JSON file."""
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        logger.info(f"Configuration loaded from {config_path}")
        return config
    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        raise

async def send_telegram_message(message):
    """Send a message to Telegram, optionally to a specific topic."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        'chat_id': TELEGRAM_CHAT_ID,
        'text': message
    }
    if TELEGRAM_TOPIC_ID:
        payload['message_thread_id'] = TELEGRAM_TOPIC_ID

    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload) as response:
            result = await response.json()
            if response.status != 200:
                logger.error(f"Telegram API error: {result}")
            else:
                logger.info(f"Message sent successfully: {result}")

def setup_exchange(exchange_id, symbol=None):
    """Set up exchange based on exchange ID from config."""
    exchange_class = getattr(ccxtpro, exchange_id)

    is_perpetual = symbol and ':USDT' in symbol
    market_type = 'swap' if is_perpetual else 'spot'

    exchange_config = {
        'options': {
            'defaultType': market_type,
        }
    }

    return exchange_class(exchange_config)

def calculate_ema(series, window):
    """Calculate Exponential Moving Average."""
    return series.ewm(span=window, adjust=False).mean()

async def filter_large_trades(grouped_trades, symbol, min_trade_amount, exchange_name, market_type_indicator="", config=None):
    """Filter and alert for large trades based on notional value (price * quantity)."""
    global all_trades

    for (timestamp, side), group in grouped_trades:
        total_amount = group['amount'].sum()
        avg_price = group['price'].mean()
        total_notional = total_amount * avg_price  # Calculate notional value
        
        if total_notional >= min_trade_amount:  # Check notional value instead of quantity
            if (timestamp, side) not in logged_timestamps:
                icon = '🟢' if side.lower() == 'buy' else '🔴'

                current_time = int(time.time())
                lookback_period = 3600

                trades_df = pd.DataFrame(all_trades)
                mean_large_notional = 0
                if not trades_df.empty:
                    trades_df['timestamp_sec'] = trades_df['timestamp'] // 1000
                    recent_trades = trades_df[trades_df['timestamp_sec'] >= (current_time - lookback_period)]
                    
                    # Calculate notional value for each trade
                    recent_trades['notional'] = recent_trades['amount'] * recent_trades['price']
                    large_trades = recent_trades[recent_trades['notional'] >= min_trade_amount]
                    if not large_trades.empty:
                        mean_large_notional = large_trades['notional'].mean()

                warning = "⚠️ LARGE TRADE ALERT! ⚠️" if total_notional >= 90000 else "LARGE TRADE ALERT!"
                message = (
                    f"{warning}\n"
                    f"📊 {exchange_name.upper()} {market_type_indicator}\n"
                    f"{icon} {side.upper()} {total_amount/1000:.2f}k@{avg_price:.{config.get('dp', 4)}f} ~ ${total_notional/1000:.2f}k [mean large trades 60m: ${mean_large_notional/1000:.2f}k]\n"
                    f"-----------------------------------------------------"
                )
                logger.info(message)
                await send_telegram_message(message)
                logged_timestamps.add((timestamp, side))

async def process_ema_ratio(symbol, exchange_name, market_type_indicator="", timeframe=300, ema_window=15):
    """Calculate EMA ratio every 5 minutes based on the previous 5-minute trades, including 60-minute mean amounts."""
    global time_windows, buy_volumes, sell_volumes, ratio_values, ema_values, last_alert_ema, all_trades, current_ema_range
    retention_period = 7200
    lookback_period = 3600

    logger.info(f"Starting EMA ratio calculation for {symbol} with window {ema_window}")

    while True:
        current_time = int(time.time())
        next_snapshot = (current_time // timeframe + 1) * timeframe
        await asyncio.sleep(next_snapshot - current_time)

        trades_df_all = pd.DataFrame(all_trades)
        mean_amounts = 0
        if not trades_df_all.empty:
            trades_df_all['timestamp_sec'] = trades_df_all['timestamp'] // 1000
            recent_trades = trades_df_all[trades_df_all['timestamp_sec'] >= (next_snapshot - lookback_period)]
            mean_amounts = recent_trades['amount'].mean()

        snapshot_start = next_snapshot - timeframe
        trades_df = pd.DataFrame(all_trades)
        if not trades_df.empty:
            trades_df['timestamp_sec'] = trades_df['timestamp'] // 1000
            snapshot_trades = trades_df[
                (trades_df['timestamp_sec'] >= snapshot_start) &
                (trades_df['timestamp_sec'] < next_snapshot)
            ]

            window_data = snapshot_trades.groupby('side')['amount'].sum().to_dict()
            buy_vol = window_data.get('buy', 0)
            sell_vol = window_data.get('sell', 0)

            if sell_vol > 0:
                ratio = buy_vol / sell_vol
            elif buy_vol > 0:
                ratio = 2.0
            else:
                ratio = 1.0

            time_windows.append(next_snapshot)
            buy_volumes.append(buy_vol)
            sell_volumes.append(sell_vol)
            ratio_values.append(ratio)

            if len(time_windows) <= ema_window:
                window_time = datetime.fromtimestamp(next_snapshot)
                logger.info(
                    f"Building EMA data: Window {len(time_windows)}/{ema_window} - "
                    f"Time: {window_time.strftime('%H:%M:%S')}, "
                    f"Buy: {buy_vol/1000:.2f}k, Sell: {sell_vol/1000:.2f}k, Ratio: {ratio:.4f}"
                )

            if len(time_windows) >= ema_window:
                ratios = pd.Series(ratio_values[-ema_window:])
                current_ema = calculate_ema(ratios, ema_window).iloc[-1]
                ema_values.append(current_ema)

                if len(ema_values) == 1:
                    logger.info(f"First EMA{ema_window} value calculated: {current_ema:.4f}")

                new_ema_range = None
                if current_ema <= 0.8:
                    new_ema_range = 1
                elif 0.8 < current_ema < 1.2:
                    new_ema_range = 2
                else:
                    new_ema_range = 3

                if current_ema_range is not None and new_ema_range != current_ema_range:
                    range_desc = {
                        1: "Bearish (≤0.8)",
                        2: "Neutral (0.8-1.2)",
                        3: "huge bullish (≥1.2)"
                    }
                    window_time = datetime.fromtimestamp(next_snapshot)
                    alert_msg = (
                        f"⚠️ EMA RANGE CHANGE ⚠️ | {exchange_name.upper()} {market_type_indicator}\n"
                        f"EMA{ema_window}: {current_ema:.4f}\n"
                        f"Window Buy: {buy_vol/1000:.2f}k, Sell: {sell_vol/1000:.2f}k | Ratio: {ratio:.4f}\n"
                        f"Mean (60min): {mean_amounts/1000:.2f}k\n"
                        f"-----------------------------------------------------"
                    )
                    logger.info(alert_msg)
                    await send_telegram_message(alert_msg)

                current_ema_range = new_ema_range
                last_alert_ema = current_ema

                window_time = datetime.fromtimestamp(next_snapshot)
                logger.info(
                    f"Snapshot: {window_time.strftime('%H:%M:%S')}, "
                    f"Buy: {buy_vol/1000:.2f}k, Sell: {sell_vol/1000:.2f}k, Ratio: {ratio:.4f} | "
                    f"EMA{ema_window}: {current_ema:.4f} (Range {new_ema_range})"
                )

            if len(time_windows) > ema_window * 2:
                time_windows[:] = time_windows[-ema_window*2:]
                buy_volumes[:] = buy_volumes[-ema_window*2:]
                sell_volumes[:] = sell_volumes[-ema_window*2:]
                ratio_values[:] = ratio_values[-ema_window*2:]
                ema_values[:] = ema_values[-ema_window*2:]

        all_trades[:] = [t for t in all_trades if (t['timestamp'] // 1000) >= (next_snapshot - retention_period)]

async def watch_trades(exchange, symbol, config):
    """Watch trades in real-time and collect them."""
    exchange_name = exchange.id
    min_trade_amount = float(config.get("min_trade_amount", 10000))
    enable_large_trades = config.get("enable_large_trades", True)

    is_perpetual = ':USDT' in symbol
    market_type_indicator = "PERP" if is_perpetual else ""

    logger.info(f"Starting to watch trades for {symbol}...")

    while True:
        try:
            trades = await exchange.watch_trades(symbol)
            if trades:
                trades_df = pd.DataFrame(trades)
                trades_df['amount'] = trades_df['amount'].astype(float)
                trades_df['price'] = trades_df['price'].astype(float)

                if enable_large_trades:
                    grouped_trades = trades_df.groupby(['timestamp', 'side'])
                    await filter_large_trades(grouped_trades, symbol, min_trade_amount, exchange_name, market_type_indicator, config)

                all_trades.extend(trades_df.to_dict('records'))

        except Exception as e:
            logger.error(f"Error watching trades: {str(e)}")
            await asyncio.sleep(10)

async def monitor_depth(exchange, symbol, config):
    """Monitor order book depth and send alerts when conditions are met."""
    exchange_name = exchange.id.upper()
    alert_interval = config.get("depth_alert_interval", 15 * 60)
    depth_threshold = config.get("min_depth_notional", 25000)
    dp = config.get("dp", 4)
    
    # Get BPS thresholds array - if not specified, default to 50bps
    bps_thresholds = config.get("depth_bps_thresholds", [{"bps": 50, "notional": depth_threshold}])

    is_perpetual = ':USDT' in symbol
    market_type_indicator = "PERP" if is_perpetual else ""

    logger.info(f"Starting depth monitoring for {symbol} with {alert_interval/60} minute intervals")

    while True:
        try:
            order_book = await exchange.watch_order_book(symbol)
            mid_price = (order_book['bids'][0][0] + order_book['asks'][0][0]) / 2
            
            # Calculate depth for each BPS threshold
            depth_info = []
            liquidity_warnings = []
            
            for threshold in bps_thresholds:
                bps = threshold['bps']
                notional_threshold = threshold['notional']
                bps_range = mid_price * (bps / 10000)  # Convert bps to decimal
                
                minus_bps_px = math.floor((mid_price - bps_range) * 10**dp) / 10**dp
                plus_bps_px = math.ceil((mid_price + bps_range) * 10**dp) / 10**dp
                
                total_bid_notional = sum(p * q for p, q in order_book['bids'] if p >= minus_bps_px)
                total_ask_notional = sum(p * q for p, q in order_book['asks'] if p <= plus_bps_px)
                total_notional = total_bid_notional + total_ask_notional
                
                bid_qty_k = int(total_bid_notional / 1000)
                ask_qty_k = int(total_ask_notional / 1000)
                
                depth_info.append({
                    'bps': bps,
                    'minus_px': minus_bps_px,
                    'plus_px': plus_bps_px,
                    'bid_qty_k': bid_qty_k,
                    'ask_qty_k': ask_qty_k,
                    'total_notional': total_notional,
                    'notional_threshold': notional_threshold
                })
                
                if total_notional < notional_threshold * 2:
                    liquidity_warnings.append(True)
                else:
                    liquidity_warnings.append(False)
            
            cum_qty_bid_top5 = sum(q for _, q in order_book['bids'][:5]) / 1000
            cum_qty_ask_top5 = sum(q for _, q in order_book['asks'][:5]) / 1000
            best_bid = order_book['bids'][0][0]
            best_ask = order_book['asks'][0][0]
            spread_bps = (best_ask - best_bid) / mid_price * 10000
            sell_vs_buy_warning = "❌" if cum_qty_ask_top5 > 2 * cum_qty_bid_top5 else ""
            ohlcv_15m = await exchange.fetch_ohlcv(symbol, timeframe='15m', limit=60)
            df_15m = pd.DataFrame(ohlcv_15m, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            current_price = df_15m['close'].iloc[-1]
            ema_13 = df_15m['close'].ewm(span=13, adjust=False).mean().iloc[-1]
            ema_25 = df_15m['close'].ewm(span=25, adjust=False).mean().iloc[-1]
            ema_99 = df_15m['close'].ewm(span=99, adjust=False).mean().iloc[-1]
            sma_20 = df_15m['close'].rolling(window=20).mean().iloc[-1]
            std_20 = df_15m['close'].rolling(window=20).std().iloc[-1]
            lower_band = sma_20 - 2 * std_20

            # Build message with all BPS levels
            message_lines = [f"📊 {exchange_name} {market_type_indicator}"]
            
            has_warning = any(liquidity_warnings)
            
            for i, info in enumerate(depth_info):
                warning = "❌" if liquidity_warnings[i] else ""
                line = (
                    f"{warning} Total depth within {info['bps']}bps from mid {mid_price:.{dp}f} "
                    f"[{info['minus_px']:.{dp}f}@{info['bid_qty_k']}k - {info['plus_px']:.{dp}f}@{info['ask_qty_k']}k] = "
                    f"${info['total_notional']/1000:.2f}k"
                )
                message_lines.append(line)
            
            message_lines.extend([
                f"Spread: {spread_bps:.2f} bps",
                f"{sell_vs_buy_warning} Cumulative Quantity (Top 5 Levels) - Buy: {cum_qty_bid_top5:.2f}k, Sell: {cum_qty_ask_top5:.2f}k"
            ])
            
            message = "\n".join(message_lines)
            logger.info(message)
            
            if has_warning:
                await send_telegram_message(message)
            await asyncio.sleep(alert_interval)
        except Exception as e:
            logger.error(f"Error in depth monitoring: {e}")
            await asyncio.sleep(60)

async def main(config_path):
    """Main function to set up exchange and watch trades."""
    global TELEGRAM_CHAT_ID, TELEGRAM_TOPIC_ID
    config = load_config(config_path)
    exchange_id = config.get("exchange", "bybit")
    symbol = config.get("symbol", "A8/USDT")
    dp = config.get("dp", 4)
    TELEGRAM_CHAT_ID = str(config.get("group_id", ""))
    TELEGRAM_TOPIC_ID = str(config.get("topic_id", "")) or None

    enable_depth_monitor = config.get("enable_depth_monitor", False)
    enable_ema_ratio = config.get("enable_ema_ratio", True)
    ema_window = config.get("ema_window", 15)
    ema_timeframe = config.get("ema_timeframe", 300)

    logger.info(f"Setting up {exchange_id} exchange for {symbol}")
    logger.info(f"Telegram group ID: {TELEGRAM_CHAT_ID}, Topic ID: {TELEGRAM_TOPIC_ID}")
    exchange = setup_exchange(exchange_id, symbol)

    is_perpetual = ':USDT' in symbol
    market_type_indicator = "PERP" if is_perpetual else ""

    try:
        tasks = []
        trade_task = asyncio.create_task(watch_trades(exchange, symbol, config))
        tasks.append(trade_task)
        if enable_ema_ratio:
            ema_task = asyncio.create_task(
                process_ema_ratio(symbol, exchange.id, market_type_indicator, ema_timeframe, ema_window)
            )
            tasks.append(ema_task)
        if enable_depth_monitor:
            depth_task = asyncio.create_task(monitor_depth(exchange, symbol, config))
            tasks.append(depth_task)
        await asyncio.gather(*tasks)
    except Exception as e:
        logger.error(f"Unhandled exception in main: {e}")
    finally:
        await exchange.close()

async def run_with_retry(config_path):
    """Run main function with retry logic."""
    max_retries = 5
    retry_count = 0
    while True:
        try:
            await main(config_path)
        except Exception as e:
            retry_count += 1
            backoff_time = min(2 ** retry_count, 60)
            logger.error(f"Error in main function: {e}. Retrying in {backoff_time} seconds (Retry {retry_count}/{max_retries})...")
            await asyncio.sleep(backoff_time)
            if retry_count >= max_retries:
                logger.error("Max retries reached. Exiting.")
                break
        else:
            retry_count = 0

if __name__ == "__main__":
    args = parse_arguments()
    try:
        asyncio.run(run_with_retry(args.config))
    except KeyboardInterrupt:
        logger.info("Program terminated by user.")