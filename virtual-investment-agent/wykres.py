import streamlit as st
from kafka import KafkaConsumer
import json
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta

# Konfiguracja Kafka
KAFKA_TOPIC = 'portfolio'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

# Ustawienie Streamlit
st.set_page_config(layout="wide")
st.title("📈 Portfolio Dashboard")

@st.cache_data(ttl=60)  # buforuj dane na 60s
def load_data():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        group_id='streamlit-dashboard',  # unikalna grupa konsumentów
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=60000  # czekaj maksymalnie 60s
    )

    data = []
    now = datetime.utcnow()
    two_days_ago = now - timedelta(days=2)

    for message in consumer:
        msg = message.value

        if isinstance(msg, dict) and 'timestamp' in msg and 'portfolio_value' in msg:
            try:
                msg_time = datetime.fromisoformat(msg['timestamp'].replace('Z', '+00:00'))
                if msg_time >= two_days_ago:
                    data.append({'timestamp': msg_time, 'portfolio_value': msg['portfolio_value']})
            except Exception as e:
                print("Błąd przy przetwarzaniu wiadomości:", e)

    consumer.close()
    return pd.DataFrame(data)

df = load_data()

if df.empty:
    st.warning("Brak danych z ostatnich 2 dni w temacie Kafka.")
else:
    df.sort_values('timestamp', inplace=True)
    fig = px.line(df, x='timestamp', y='portfolio_value', title='Portfolio Value Over Time')
    st.plotly_chart(fig, use_container_width=True)
