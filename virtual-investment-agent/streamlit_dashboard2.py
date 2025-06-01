import streamlit as st
from kafka import KafkaConsumer
import json
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import time

st.set_page_config(layout="wide")
st.title("📊 Dashboard Portfela Inwestycyjnego")

# Kafka settings
KAFKA_TOPIC = 'portfolio'
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']

def consume_portfolio_messages():
    """Pobiera najnowsze wiadomości z Kafka"""
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='earliest',  # Zmienione na 'earliest' aby pobrać więcej danych
            enable_auto_commit=True,
            group_id='portfolio_dashboard_group',  # Zmieniona nazwa grupy
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=5000  # Timeout po 5 sekundach
        )
        
        messages = []
        start_time = time.time()
        
        # Pobierz wiadomości przez maksymalnie 5 sekund
        for message in consumer:
            messages.append(message.value)
            if time.time() - start_time > 5:  # Max 5 sekund
                break
                
        consumer.close()
        return messages
        
    except Exception as e:
        st.error(f"Błąd połączenia z Kafka: {e}")
        return []

def get_latest_prices_from_history(history):
    """Wyciąga najnowsze ceny dla każdej akcji z historii transakcji"""
    prices = {}
    if not history:
        return prices
        
    # Sortuj historię po timestamp
    sorted_history = sorted(history, key=lambda x: x['timestamp'], reverse=True)
    
    # Dla każdej akcji znajdź najnowszą cenę
    for transaction in sorted_history:
        stock = transaction['stock']
        if stock not in prices:
            prices[stock] = transaction['price']
    
    return prices

def calculate_portfolio_value(cash, stocks, history):
    """Oblicza całkowitą wartość portfela"""
    prices = get_latest_prices_from_history(history)
    stocks_value = sum(stocks.get(stock, 0) * prices.get(stock, 0) for stock in stocks.keys())
    return cash + stocks_value

# Automatyczne odświeżanie co 10 sekund
if 'last_refresh' not in st.session_state:
    st.session_state.last_refresh = time.time()

# Przycisk do ręcznego odświeżania
if st.button("🔄 Odśwież dane"):
    st.session_state.last_refresh = time.time()
    st.rerun()

# Auto-refresh co 10 sekund
if time.time() - st.session_state.last_refresh > 10:
    st.session_state.last_refresh = time.time()
    st.rerun()

# Pobierz dane z Kafka
with st.spinner("Pobieranie danych z Kafka..."):
    portfolio_data = consume_portfolio_messages()

if portfolio_data:
    st.success(f"Pobrano {len(portfolio_data)} wiadomości z topicu 'portfolio'")
    
    # Weź najnowsze dane
    latest_data = portfolio_data[-1]
    
    # Layout: lewy panel (stan portfela), prawy panel (wykres i historia)
    left_col, right_col = st.columns([1, 2])
    
    with left_col:
        st.subheader("💰 Aktualny stan portfela")
        
        cash = latest_data['cash']
        stocks = latest_data['stocks']
        history = latest_data.get('history', [])
        
        # Oblicz wartość portfela
        portfolio_value = calculate_portfolio_value(cash, stocks, history)
        
        st.metric("Gotówka", f"{cash:.2f} PLN")
        st.metric("Całkowita wartość portfela", f"{portfolio_value:.2f} PLN")
        
        st.subheader("📈 Posiadane akcje")
        if any(amount > 0 for amount in stocks.values()):
            prices = get_latest_prices_from_history(history)
            for stock, amount in stocks.items():
                if amount > 0:
                    current_price = prices.get(stock, 0)
                    value = amount * current_price
                    st.write(f"**{stock}**: {amount} szt. × {current_price:.2f} PLN = {value:.2f} PLN")
        else:
            st.write("Brak posiadanych akcji")
    
    with right_col:
        # Historia transakcji
        if history:
            st.subheader("📋 Historia transakcji")
            
            # Przygotuj DataFrame z historii
            hist_df = pd.DataFrame(history)
            hist_df['timestamp'] = pd.to_datetime(hist_df['timestamp'])
            hist_df = hist_df.sort_values('timestamp', ascending=False)
            
            # Dodaj kolumnę z wartością transakcji
            hist_df['value'] = hist_df['price']
            
            # Pokaż ostatnie 10 transakcji
            st.dataframe(
                hist_df[['timestamp', 'action', 'stock', 'price']].head(10),
                column_config={
                    'timestamp': st.column_config.DatetimeColumn('Czas', format='DD/MM/YYYY HH:mm:ss'),
                    'action': st.column_config.TextColumn('Akcja'),
                    'stock': st.column_config.TextColumn('Akcja'),
                    'price': st.column_config.NumberColumn('Cena', format='%.2f PLN')
                },
                use_container_width=True
            )

# Wykres zmian wartości portfela na podstawie snapshotów z Kafka
st.subheader("📊 Wykres wartości portfela (ostatnie 3 dni)")

timeline_data = []

for snapshot in portfolio_data:
    snapshot_time = pd.to_datetime(snapshot['timestamp']) if 'timestamp' in snapshot else None
    cash = snapshot.get('cash', 0)
    stocks = snapshot.get('stocks', {})
    history = snapshot.get('history', [])
    total_value = calculate_portfolio_value(cash, stocks, history)

    if snapshot_time:
        timeline_data.append({
            'timestamp': snapshot_time,
            'portfolio_value': total_value
        })

# Filtruj dane z ostatnich 3 dni
three_days_ago = datetime.utcnow() - timedelta(days=3)
filtered_data = [entry for entry in timeline_data if entry['timestamp'] >= three_days_ago]

if filtered_data:
    df_timeline = pd.DataFrame(filtered_data).sort_values('timestamp')

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(df_timeline['timestamp'], df_timeline['portfolio_value'], 
            marker='o', linestyle='-', color='#1f77b4', linewidth=2, markersize=4)
    ax.set_xlabel("Czas")
    ax.set_ylabel("Wartość portfela (PLN)")
    ax.set_title("Zmiana wartości portfela w czasie (ostatnie 3 dni)")
    ax.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    plt.tight_layout()
    st.pyplot(fig)
    plt.close()
else:
    st.info("Brak danych z ostatnich 3 dni do wygenerowania wykresu.")

          
            # Statystyki
            st.subheader("📊 Statystyki transakcji")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                buy_count = len([t for t in history if t['action'] == 'BUY'])
                st.metric("Transakcje kupna", buy_count)
            
            with col2:
                sell_count = len([t for t in history if t['action'] == 'SELL'])
                st.metric("Transakcje sprzedaży", sell_count)
            
            with col3:
                total_transactions = len(history)
                st.metric("Łączne transakcje", total_transactions)
        
        else:
            st.info("Brak historii transakcji do wyświetlenia")

else:
    st.warning("Brak danych z topicu 'portfolio'. Sprawdź czy:")
    st.write("1. Kafka jest uruchomiona")
    st.write("2. Topic 'portfolio' istnieje")
    st.write("3. Moduł portfela wysyła dane")
