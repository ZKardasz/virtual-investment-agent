import streamlit as st
from kafka import KafkaConsumer
import json
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt

st.title("Dashboard Portfela Inwestycyjnego")

KAFKA_TOPIC = 'portfolio'
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']

@st.cache_data(ttl=10)
def consume_portfolio_messages(max_messages=100):
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='portfolio_dashboard_group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    messages = []
    try:
        for _ in range(max_messages):
            msg = next(consumer)
            messages.append(msg.value)
    except StopIteration:
        pass
    consumer.close()
    return messages

messages = consume_portfolio_messages()

if messages:
    # Przetwarzamy dane: zamiana timestampów, wartości
    portfolio_states = []
    for msg in messages:
        # Parsuj timestamp jako datetime - zakładam, że timestamp jest wewnątrz historii lub dołożymy bieżący czas
        # Ponieważ w przesłanym przykładzie portfolio nie ma 'timestamp' w głównym dict,
        # ale ma go w historii, weźmy timestamp ostatniej transakcji jako przybliżenie.
        if msg.get('history'):
            last_ts_str = msg['history'][-1]['timestamp']
            ts = pd.to_datetime(last_ts_str)
        else:
            ts = pd.Timestamp.now()

        stocks = msg.get('stocks', {})
        cash = msg.get('cash', 0.0)
        history = msg.get('history', [])

        # Ostatnie ceny akcji z historii
        last_prices = {}
        for entry in reversed(history):
            stock = entry['stock']
            if stock not in last_prices:
                last_prices[stock] = entry['price']

        stock_value = sum(amount * last_prices.get(stock, 0) for stock, amount in stocks.items())
        total_value = cash + stock_value

        portfolio_states.append({'timestamp': ts, 'cash': cash, 'stock_value': stock_value, 'total_value': total_value,
                                 'stocks': stocks, 'history': history, 'raw': msg})

    # Sortujemy po timestamp rosnąco
    portfolio_states = sorted(portfolio_states, key=lambda x: x['timestamp'])

    # Bierzemy ostatni stan jako aktualny
    latest = portfolio_states[-1]

    left_col, right_col = st.columns([1, 2])

    with left_col:
        st.subheader("Gotówka")
        st.write(f"{latest['cash']:.2f} PLN")

        st.subheader("Akcje w portfelu")
        stocks = latest['stocks']
        if stocks:
            for stock, amount in stocks.items():
                st.write(f"- {stock}: {amount}")
        else:
            st.write("Brak akcji w portfelu.")

        st.subheader("Historia transakcji")
        history = latest['history']
        if history:
            df_history = pd.DataFrame(history)
            df_history['timestamp'] = pd.to_datetime(df_history['timestamp'])
            df_history['timestamp'] = df_history['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
            df_history['price'] = df_history['price'].map('{:.2f} PLN'.format)
            st.dataframe(df_history[['timestamp', 'action', 'stock', 'price']])
        else:
            st.write("Brak historii transakcji.")

    with right_col:
        st.subheader("Zmiana wartości portfela w czasie")

        df_values = pd.DataFrame(portfolio_states)
        df_values.set_index('timestamp', inplace=True)

        plt.figure(figsize=(10, 5))
        plt.plot(df_values.index, df_values['total_value'], label='Łączna wartość portfela')
        plt.plot(df_values.index, df_values['cash'], label='Gotówka')
        plt.plot(df_values.index, df_values['stock_value'], label='Wartość akcji')
        plt.xlabel('Czas')
        plt.ylabel('Wartość (PLN)')
        plt.legend()
        plt.grid(True)
        st.pyplot(plt)

else:
    st.write("Brak danych z topicu 'portfolio'.")
