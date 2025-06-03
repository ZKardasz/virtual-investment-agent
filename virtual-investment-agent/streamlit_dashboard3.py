import streamlit as st 
from kafka import KafkaConsumer
import json
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import time

st.set_page_config(layout="wide")
st.title("📊 Dashboard Portfela Inwestycyjnego")

# Kafka settings
KAFKA_TOPIC = 'portfolio'
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']

def consume_portfolio_messages():
    try:
        group_id = f'portfolio_dashboard_group_{int(time.time())}'
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            group_id=group_id,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=10000,
            fetch_max_wait_ms=2000,
            max_poll_records=1000
        )
        
        messages = []
        start_time = time.time()
        empty_polls = 0
        max_empty_polls = 3
        st.info("🔍 Pobieranie wszystkich danych z Kafka topiku...")

        while True:
            message_batch = consumer.poll(timeout_ms=3000)
            if message_batch:
                empty_polls = 0
                for records in message_batch.values():
                    for record in records:
                        messages.append(record.value)
                st.info(f"📥 Pobrano dotychczas: {len(messages)} wiadomości...")
            else:
                empty_polls += 1
                if empty_polls >= max_empty_polls:
                    break
            if time.time() - start_time > 15:
                break
        consumer.close()

        unique_messages = []
        seen_timestamps = set()
        for msg in messages:
            timestamp = msg.get('timestamp')
            if timestamp and timestamp not in seen_timestamps:
                seen_timestamps.add(timestamp)
                unique_messages.append(msg)
            elif not timestamp:
                unique_messages.append(msg)

        # Filtrowanie tylko z ostatnich 2 dni
        cutoff = datetime.now() - timedelta(days=2)
        filtered_messages = [
            msg for msg in unique_messages
            if 'timestamp' in msg and pd.to_datetime(msg['timestamp']) >= cutoff
        ]
        st.success(f"✅ Załadowano {len(filtered_messages)} wiadomości z ostatnich 2 dni")
        return filtered_messages
    except Exception as e:
        st.error(f"❌ Błąd połączenia z Kafka: {e}")
        return []

def parse_timestamp(timestamp_str):
    try:
        return pd.to_datetime(timestamp_str)
    except:
        return pd.Timestamp.now()

def get_latest_prices_from_history(history):
    prices = {}
    sorted_history = sorted(history, key=lambda x: x['timestamp'], reverse=True)
    for transaction in sorted_history:
        stock = transaction['stock']
        if stock not in prices:
            prices[stock] = transaction['price']
    return prices

def calculate_portfolio_value(cash, stocks, history):
    prices = get_latest_prices_from_history(history)
    stocks_value = sum(stocks.get(stock, 0) * prices.get(stock, 0) for stock in stocks.keys())
    return cash + stocks_value

# Manual refresh button
if st.button("🔄 Odśwież dane"):
    st.experimental_set_query_params(refresh=int(time.time()))

# Get portfolio data
with st.spinner("Pobieranie danych..."):
    portfolio_data = consume_portfolio_messages()

if not portfolio_data:
    st.warning("Brak danych z Kafka.")
    st.stop()

latest_data = portfolio_data[-1]
cash = latest_data.get('cash', 0)
stocks = latest_data.get('stocks', {})
history = latest_data.get('history', [])

portfolio_value = calculate_portfolio_value(cash, stocks, history)

left, right = st.columns([1, 2])
with left:
    st.subheader("💰 Aktualny stan portfela")
    st.metric("Gotówka", f"{cash:.2f} PLN")
    st.metric("Wartość portfela", f"{portfolio_value:.2f} PLN")

    st.subheader("📈 Posiadane akcje")
    prices = get_latest_prices_from_history(history)
    for stock, amount in stocks.items():
        if amount > 0:
            price = prices.get(stock, 0)
            st.write(f"**{stock}**: {amount} × {price:.2f} PLN = {amount * price:.2f} PLN")

with right:
    st.subheader("📋 Historia transakcji")
    if history:
        hist_df = pd.DataFrame(history)
        hist_df['timestamp'] = hist_df['timestamp'].apply(parse_timestamp)
        hist_df = hist_df.sort_values('timestamp', ascending=False)
        st.dataframe(hist_df[['timestamp', 'action', 'stock', 'price']].head(10))
    else:
        st.write("Brak historii.")

# Timeline chart
timeline_data = []
for snapshot in portfolio_data:
    ts = parse_timestamp(snapshot.get('timestamp'))
    cash = snapshot.get('cash', 0)
    stocks = snapshot.get('stocks', {})
    history = snapshot.get('history', [])
    value = calculate_portfolio_value(cash, stocks, history)
    timeline_data.append({
        'timestamp': ts,
        'portfolio_value': value,
        'cash': cash,
        'stocks_value': value - cash
    })

if timeline_data:
    df = pd.DataFrame(timeline_data)
    df = df.sort_values('timestamp')

    st.subheader("📊 Zmiana wartości portfela (ostatnie 2 dni)")
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(df['timestamp'], df['portfolio_value'], label="Portfel", marker='o')
    ax.plot(df['timestamp'], df['cash'], label="Gotówka", linestyle='--')
    ax.plot(df['timestamp'], df['stocks_value'], label="Akcje", linestyle='--')
    ax.set_xlabel("Czas")
    ax.set_ylabel("PLN")
    ax.legend()
    ax.grid(True)
    plt.xticks(rotation=45)
    st.pyplot(fig)

    st.metric("Zmiana wartości", f"{df['portfolio_value'].iloc[-1] - df['portfolio_value'].iloc[0]:+.2f} PLN")
else:
    st.info("Brak danych do wykresu.")
