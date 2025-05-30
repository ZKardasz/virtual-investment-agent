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
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    messages = []
    # Pobierz ostatnie 100 wiadomości (lub mniej)
    for _ in range(100):
        msg = next(consumer)
        messages.append(msg.value)
    consumer.close()
    return messages

# Pobierz dane
portfolio_data = consume_portfolio_messages()

if portfolio_data:
    # Zamień na DataFrame
    df = pd.DataFrame(portfolio_data)
    # Załóżmy, że są kolumny: timestamp, cash, stocks (dictionary z ilościami)
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Wyświetl aktualny stan portfela (ostatni wpis)
    latest = df.iloc[-1]
    st.subheader("Aktualny stan portfela")
    st.write(f"Gotówka: {latest['cash']} PLN")
    st.write("Akcje:")
    for stock, amount in latest['stocks'].items():
        st.write(f"- {stock}: {amount}")

    # Wartość portfela - jeśli jest w danych, albo wylicz (tu przykładowo jako suma gotówki i liczby akcji)
    # Tu możesz dopasować według danych, które masz w topicu

    # Wykres wartości portfela w czasie (przykładowo gotówka)
    st.subheader("Zmiana wartości portfela w czasie")
    plt.figure(figsize=(10, 5))
    plt.plot(df['timestamp'], df['cash'], label='Gotówka')
    plt.xlabel("Czas")
    plt.ylabel("Wartość (PLN)")
    plt.legend()
    st.pyplot(plt)

    # Historia transakcji (jeśli jest w danych, np. lista dictów z akcjami i akcjami)
    if 'transactions' in latest:
        st.subheader("Historia transakcji")
        transactions_df = pd.DataFrame(latest['transactions'])
        st.dataframe(transactions_df)
else:
    st.write("Brak danych z topicu portfolio.")
