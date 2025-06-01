import yfinance as yf
import time
from datetime import datetime
from config import settings
from utils.kafka_utils import create_kafka_producer, send_to_kafka

def fetch_price(ticker):
    data = yf.download(tickers=ticker, period="1d", interval="1m")
    if not data.empty:
        return float(data['Close'].iloc[-1].item())
    return None

def start_producer():
    producer = create_kafka_producer(settings.KAFKA_BOOTSTRAP_SERVERS)

    while True:
        for ticker in settings.STOCK_TICKERS:
            price = fetch_price(ticker)
            if price is not None:
                message = {
                    "timestamp": datetime.utcnow().isoformat(),
                    "stock": ticker,
                    "price": round(price, 2)
                }
                print(f"Sending: {message}")
                send_to_kafka(producer, settings.KAFKA_TOPIC, message)
        time.sleep(settings.FETCH_INTERVAL_SECONDS)
