import streamlit as st
from kafka import KafkaConsumer
import json
import pandas as pd
from datetime import datetime

st.title("Dashboard Portfela Inwestycyjnego")

# Kafka
KAFKA_TOPIC = 'portfolio'
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']

@st.cache_data(ttl=10)
def consume_latest_portfolio():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='portfolio_dashboard_group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    # Pobieramy tylko najnowszą wiadomość (najświeższy stan portfela)
    msg = None
    for message in consumer:
        msg = message.value
        break
    consumer.close()
    return msg

portfolio = consume_latest_portfolio()

if portfolio:
    # Wyświetl gotówkę
    st.subheader("Gotówka")
    st.write(f"{portfolio['cash']:.2f} PLN")
    
    # Wyświetl akcje i ich ilość
    st.subheader("Akcje w portfelu")
    stocks = portfolio.get('stocks', {})
    if stocks:
        for stock, amount in stocks.items():
            st.write(f"- {stock}: {amount}")
    else:
        st.write("Brak akcji w portfelu.")
    
    # Wyświetl historię transakcji
    st.subheader("Historia transakcji")
    history = portfolio.get('history', [])
    if history:
        # Zamiana na DataFrame i formatowanie timestampów
        df_history = pd.DataFrame(history)
        df_history['timestamp'] = pd.to_datetime(df_history['timestamp'])
        # Formatowanie kolumn dla czytelności
        df_history['timestamp'] = df_history['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df_history['price'] = df_history['price'].map('{:.2f} PLN'.format)
        
        # Pokazujemy ładną tabelę
        st.dataframe(df_history[['timestamp', 'action', 'stock', 'price']])
    else:
        st.write("Brak historii transakcji.")
else:
    st.write("Brak danych z topiku 'portfolio'.")
