import streamlit as st
from kafka import KafkaConsumer
import json
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt

st.title("Dashboard Portfela Inwestycyjnego")

# Ustawienia Kafka
KAFKA_TOPIC = 'portfolio'
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']

@st.cache_data(ttl=10)
def consume_portfolio_messages():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='portfolio_consumer_group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=1000  # po 1 sekundzie kończy czekać na nowe wiadomości
    )
    messages = []
    for msg in consumer:
        messages.append(msg.value)
        if len(messages) >= 100:
            break
    consumer.close()
    return messages

# Pobierz dane
portfolio_data = consume_portfolio_messages()

if portfolio_data:
    df = pd.DataFrame(portfolio_data)
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    latest = df.iloc[-1]

    st.subheader("Aktualny stan portfela")
    st.write(f"Gotówka: {latest['cash']} PLN")
    st.write("Akcje:")
    for stock, amount in latest['stocks'].items():
        st.write(f"- {stock}: {amount}")

    st.subheader("Zmiana wartości gotówki w czasie")
    plt.figure(figsize=(10, 5))
    plt.plot(df['timestamp'], df['cash'], label='Gotówka')
    plt.xlabel("Czas")
    plt.ylabel("Wartość (PLN)")
    plt.legend()
    st.pyplot(plt)

    if 'history' in latest and latest['history']:
        st.subheader("Historia transakcji")
        transactions_df = pd.DataFrame(latest['history'])
        transactions_df['timestamp'] = pd.to_datetime(transactions_df['timestamp'])
        st.dataframe(transactions_df)
else:
    st.write("Brak danych z topicu portfolio.")
