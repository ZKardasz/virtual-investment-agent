import streamlit as st
from kafka import KafkaConsumer
import json
import pandas as pd
import matplotlib.pyplot as plt

st.set_page_config(layout="wide")  # szeroki layout

st.title("Dashboard Portfela Inwestycyjnego")

# Kafka settings
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
    try:
        for _ in range(100):
            msg = next(consumer)
            messages.append(msg.value)
    except StopIteration:
        # mniej niż 100 wiadomości dostępnych
        pass
    consumer.close()
    return messages

# Pobierz dane z Kafka
portfolio_data = consume_portfolio_messages()

if portfolio_data:
    # Konwersja na DataFrame
    df = pd.DataFrame(portfolio_data)

    # Konwersja timestamp na datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp')

    # Funkcja do wyliczenia wartości portfela (gotówka + wartość akcji)
    def calculate_portfolio_value(row):
        cash = row['cash']
        stocks = row['stocks']  # dict np. {'MSFT': 0, 'AAPL': 3, ...}
        history = row['history'] if 'history' in row else []

        # Ostatnie ceny akcji z historii (dla uproszczenia)
        prices = {}
        for stock in stocks.keys():
            filtered = [t for t in history if t['stock'] == stock]
            if filtered:
                prices[stock] = filtered[-1]['price']
            else:
                prices[stock] = 0

        stocks_value = sum(stocks[stk] * prices.get(stk, 0) for stk in stocks)
        return cash + stocks_value

    df['portfolio_value'] = df.apply(calculate_portfolio_value, axis=1)

    # Layout: lewy panel (gotówka, akcje, historia), prawy panel (wykres)
    left_col, right_col = st.columns([1, 3])

    with left_col:
        st.subheader("Aktualny stan portfela")
        latest = df.iloc[-1]
        st.write(f"**Gotówka:** {latest['cash']:.2f} PLN")

        st.write("**Akcje:**")
        for stock, amount in latest['stocks'].items():
            st.write(f"- {stock}: {amount}")

        # Historia transakcji
        if 'history' in latest and latest['history']:
            st.subheader("Historia transakcji")
            hist_df = pd.DataFrame(latest['history'])
            hist_df['timestamp'] = pd.to_datetime(hist_df['timestamp'])
            st.dataframe(hist_df[['timestamp', 'action', 'stock', 'price']].sort_values(by='timestamp', ascending=False))
        else:
            st.write("Brak historii transakcji.")

    with right_col:
        st.subheader("Wykres zmian wartości portfela w czasie")

        plt.figure(figsize=(10,5))
        plt.plot(df['timestamp'], df['portfolio_value'], marker='o', linestyle='-', color='blue')
        plt.xlabel("Czas")
        plt.ylabel("Wartość portfela (PLN)")
        plt.title("Zmiana wartości portfela w czasie")
        plt.grid(True)
        plt.tight_layout()

        st.pyplot(plt.gcf())
        plt.close()

else:
    st.write("Brak danych z topicu 'portfolio'.")
