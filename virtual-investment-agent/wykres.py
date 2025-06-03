import streamlit as st
from kafka import KafkaConsumer
import json
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import matplotlib.dates as mdates

st.title("📊 Wykres wartości portfela (ostatnie 2 dni)")

# Ustawienia Kafka
KAFKA_TOPIC = 'portfolio'
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']

# Pomocnicza funkcja do parsowania dat
def parse_timestamp(ts):
    try:
        return pd.to_datetime(ts)
    except:
        return None

# Funkcja do pobierania danych z Kafka
def get_data_from_kafka():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=10000
    )

    data = []
    yesterday = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    tomorrow = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)

    for message in consumer:
        value = message.value
        ts = parse_timestamp(value.get("timestamp", ""))
        if ts and yesterday <= ts < tomorrow:
            cash = value.get("cash", 0)
            stocks = value.get("stocks", {})
            history = value.get("history", [])
            latest_prices = {h['stock']: h['price'] for h in history if 'stock' in h and 'price' in h}
            stocks_value = sum(stocks.get(s, 0) * latest_prices.get(s, 0) for s in stocks)
            total_value = cash + stocks_value

            data.append({
                "timestamp": ts,
                "portfolio_value": total_value
            })

    consumer.close()
    return pd.DataFrame(data)

# Pobierz dane
df = get_data_from_kafka()

# Sprawdź i narysuj wykres
if not df.empty:
    df = df.sort_values("timestamp")

    fig, ax = plt.subplots(figsize=(14, 6))
    ax.plot(df['timestamp'], df['portfolio_value'], marker='o', linestyle='-', markersize=3, label='Wartość portfela')

    ax.set_xlabel("Czas")
    ax.set_ylabel("Wartość portfela (PLN)")
    ax.set_title("Zmiana wartości portfela (co 30s)")

    ax.xaxis.set_major_formatter(mdates.DateFormatter('%d.%m %H:%M'))
    ax.xaxis.set_major_locator(mdates.HourLocator(interval=2))  # co 2h
    ax.xaxis.set_minor_locator(mdates.MinuteLocator(interval=30))  # co 30 min
    ax.tick_params(axis='x', labelrotation=45)

    ax.grid(True)
    ax.legend()
    st.pyplot(fig)
else:
    st.warning("Brak danych z ostatnich 2 dni do wygenerowania wykresu.")
