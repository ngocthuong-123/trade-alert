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

# ===== Logging =====
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)
logger.info("📂 A8 Volume Monitor - Logging mode: console only")

# ===== Global data =====
volume_threshold = 50000  # 50k USD
notification_sent_today = False


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="A8 Volume Monitor")
    parser.add_argument(
        "--config",
        "-c",
        type=str,
        default="config/trade_alert.json",
        help="Path to configuration file (default: config/trade_alert.json)",
    )
    return parser.parse_args()


def load_config(config_path):
    """Load configuration from a JSON file."""
    try:
        with open(config_path, "r") as f:
            config = json.load(f)
        logger.info(f"✅ Configuration loaded from {config_path}")
        return config
    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        raise


async def send_telegram_message(message):
    """Send a message to Telegram, optionally to a specific topic."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message}
    if TELEGRAM_TOPIC_ID:
        payload["message_thread_id"] = TELEGRAM_TOPIC_ID

    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload) as response:
            result = await response.json()
            if response.status != 200:
                logger.error(f"Telegram API error: {result}")
            else:
                now_vn = datetime.now(VN_TZ).strftime("%Y-%m-%d %H:%M:%S")
                logger.info(f"✅ Message sent successfully at {now_vn} (UTC+7)")


def setup_exchange():
    """Set up Bybit exchange."""
    exchange = ccxtpro.bybit(
        {
            "options": {
                "defaultType": "spot",
            }
        }
    )
    return exchange


async def check_volume_and_notify(config):
    """Check A8 volume and send daily report at configured time + alert if volume < 50k."""
    global notification_sent_today

    try:
        exchange = setup_exchange()
        symbol = "A8/USDT"

        # Lấy thống kê 24h cho A8
        ticker = await exchange.fetch_ticker(symbol)
        volume_24h = ticker["quoteVolume"]  # Volume tính bằng USDT

        current_price = ticker["last"]
        volume_base = ticker["baseVolume"]

        now_vn = datetime.now(VN_TZ)
        current_time = now_vn.strftime("%H:%M")
        notification_time = config.get("notification_time", "13:00")

        logger.info(f"📊 A8 Volume 24h: ${volume_24h:,.2f}")
        logger.info(f"💰 A8 Price: ${current_price:.{config.get('dp', 4)}f}")
        logger.info(f"📈 A8 Volume (Base): {volume_base:,.2f}")
        logger.info(
            f"⏰ Current time: {current_time} VN, Notification time: {notification_time} VN"
        )

        # Kiểm tra nếu đã gửi thông báo hôm nay chưa
        if notification_sent_today:
            logger.info("📤 Daily notification already sent today, skipping...")
            return

        # Gửi báo cáo volume hàng ngày đúng giờ trong config
        if current_time == notification_time:
            # Tạo message chính
            if volume_24h < volume_threshold:
                # Volume thấp - thêm cảnh báo
                message = (
                    f"⚠️ A8 DAILY VOLUME REPORT ⚠️ - 🕒 {now_vn.strftime('%Y-%m-%d %H:%M:%S')} (UTC+7)\n"
                    f"📉 Volume 24h: ${volume_24h:,.2f}\n"
                    f"🚨 Volume Status: LOW (Below ${volume_threshold:,.2f})\n"
                    f"💰 Price: ${current_price:.{config.get('dp', 4)}f}\n"
                    f"--------------------------"
                )
            else:
                # Volume bình thường
                message = (
                    f"📊 A8 DAILY VOLUME REPORT - 🕒 {now_vn.strftime('%Y-%m-%d %H:%M:%S')} (UTC+7)\n"
                    f"📈 Volume 24h: ${volume_24h:,.2f}\n"
                    f"✅ Volume Status: Normal (Above ${volume_threshold:,.2f})\n"
                    f"--------------------------"
                )

            await send_telegram_message(message)
            notification_sent_today = True

            if volume_24h < volume_threshold:
                logger.info("📤 Daily volume report sent with LOW VOLUME ALERT!")
            else:
                logger.info("📤 Daily volume report sent!")

        else:
            logger.info(
                f"⏰ Too early for daily report. Current: {current_time}, Target: {notification_time} VN"
            )

    except Exception as e:
        logger.error(f"Error checking A8 volume: {e}")


async def monitor_volume_continuously(config):
    """Monitor A8 volume continuously and send reports at configured time."""
    while True:
        await check_volume_and_notify(config)
        # Check every minute
        await asyncio.sleep(60)


async def reset_daily_flag():
    """Reset daily notification flag at midnight VN time."""
    global notification_sent_today

    while True:
        now_vn = datetime.now(VN_TZ)

        # Reset flag at midnight (00:00 VN time)
        if now_vn.hour == 0 and now_vn.minute == 0:
            notification_sent_today = False
            logger.info("🔄 Daily notification flag reset at midnight VN")

        # Check every minute
        await asyncio.sleep(60)


async def main(config_path):
    """Main function to monitor A8 volume."""
    global TELEGRAM_CHAT_ID, TELEGRAM_TOPIC_ID

    config = load_config(config_path)
    TELEGRAM_CHAT_ID = str(config.get("group_id", ""))
    TELEGRAM_TOPIC_ID = str(config.get("topic_id", "")) or None

    logger.info(f"🚀 Starting A8 Volume Monitor")
    logger.info(f"📱 Telegram Group ID: {TELEGRAM_CHAT_ID}")
    logger.info(f"📋 Telegram Topic ID: {TELEGRAM_TOPIC_ID}")
    logger.info(f"🎯 Volume Threshold: ${volume_threshold:,}")

    try:
        # Chạy 2 tasks song song
        await asyncio.gather(
            monitor_volume_continuously(config),  # Monitor volume liên tục
            reset_daily_flag(),  # Reset flag hàng ngày
        )
    except Exception as e:
        logger.error(f"Unhandled exception in main: {e}")


async def run_with_retry(config_path):
    """Run main function with retry logic."""
    retry_count = 0
    max_retries = 5

    while True:
        try:
            await main(config_path)
        except Exception as e:
            retry_count += 1
            backoff_time = min(2**retry_count, 60)
            logger.error(
                f"Error in main function: {e}. Retrying in {backoff_time} seconds (Retry {retry_count}/{max_retries})..."
            )
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
        logger.info("🛑 A8 Volume Monitor terminated by user.")
