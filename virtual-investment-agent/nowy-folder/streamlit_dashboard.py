from kafka import KafkaConsumer
import json
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

# Ustawienia Strumienia
st.set_page_config(page_title="Dashboard Portfela", layout="wide")

st.title("📈 Virtual Investment Dashboard")
st.subheader("📊 Aktualny stan portfela")

# Konfiguracja Kafka Consumer
consumer = KafkaConsumer(
    'portfolio',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',
    group_id='portfolio_consumer_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Przechowywanie historii
portfolio_history = []
transactions = []

# Pobieramy tylko 1 wiadomość żeby odświeżyć widok (streamlit działa w pętli)
message = next(consumer)
data = message.value
portfolio_history.append(data)

# Transakcje
if 'last_action' in data:
    transactions.append({
        "timestamp": data["timestamp"],
        "stock": data["stock"],
        "action": data["last_action"],
        "price": data["last_price"]
    })

# Pokaż dane portfela
st.write(f"💵 Gotówka: **{data['cash']:.2f} USD**")
st.write(f"📦 Akcje: {data['stocks']}")

# Wartość całkowita
total_value = data['cash'] + sum([
    quantity * data['stock_prices'][stock]
    for stock, quantity in data['stocks'].items()
])
st.write(f"💰 **Wartość całkowita portfela: {total_value:.2f} USD**")

# Wykres zmian wartości portfela
df = pd.DataFrame(portfolio_history)
df['datetime'] = pd.to_datetime(df['timestamp'])
df['portfolio_value'] = df['cash'] + df['stocks'].apply(
    lambda s: sum(v * data['stock_prices'][k] for k, v in s.items())
)

st.subheader("📉 Wartość portfela w czasie")
fig, ax = plt.subplots()
ax.plot(df['datetime'], df['portfolio_value'], marker='o')
ax.set_xlabel("Czas")
ax.set_ylabel("Wartość [USD]")
ax.set_title("Zmiana wartości portfela")
st.pyplot(fig)

# Lista transakcji
st.subheader("📋 Historia transakcji")
if transactions:
    df_tx = pd.DataFrame(transactions)
    df_tx['timestamp'] = pd.to_datetime(df_tx['timestamp'])
    st.dataframe(df_tx.sort_values("timestamp", ascending=False))
else:
    st.info("Brak transakcji.")
