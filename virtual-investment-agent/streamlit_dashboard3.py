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
    """Pobiera wiadomości z Kafka tylko z wczoraj i dzisiaj"""
    try:
        # Używamy unikalnej group_id za każdym razem, aby czytać od początku
        group_id = f'portfolio_dashboard_group_{int(time.time())}'
        
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            group_id=group_id,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=8000,     # Skrócony timeout
            fetch_max_wait_ms=1500,
            max_poll_records=500
        )
        
        messages = []
        start_time = time.time()
        empty_polls = 0
        max_empty_polls = 2  # Mniej pustych poll'ów
        
        # Definiuj dokładnie wczoraj i dzisiaj
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        yesterday = today - timedelta(days=1)
        tomorrow = today + timedelta(days=1)  # Do filtrowania
        
        st.info(f"🔍 Pobieranie danych tylko z {yesterday.strftime('%d.%m.%Y')} i {today.strftime('%d.%m.%Y')}...")
        
        while True:
            message_batch = consumer.poll(timeout_ms=2000)
            
            if message_batch:
                empty_polls = 0
                for topic_partition, records in message_batch.items():
                    for record in records:
                        messages.append(record.value)
                        
                st.info(f"📥 Pobrano: {len(messages)} wiadomości...")
            else:
                empty_polls += 1
                if empty_polls >= max_empty_polls:
                    break
            
            # Krótszy timeout - 10 sekund
            if time.time() - start_time > 10:
                st.warning("⏰ Timeout - używam dostępne dane")
                break
                
        consumer.close()
        
        # Filtruj TYLKO wczoraj i dzisiaj + usuń duplikaty
        filtered_messages = []
        seen_timestamps = set()
        
        for msg in messages:
            timestamp_str = msg.get('timestamp')
            if timestamp_str:
                try:
                    msg_timestamp = parse_timestamp(timestamp_str)
                    # Sprawdź czy wiadomość jest z wczoraj lub dzisiaj (ale nie z jutra)
                    if (msg_timestamp >= yesterday and 
                        msg_timestamp < tomorrow and 
                        timestamp_str not in seen_timestamps):
                        seen_timestamps.add(timestamp_str)
                        filtered_messages.append(msg)
                except:
                    continue
        
        # Sortuj chronologicznie
        filtered_messages.sort(key=lambda x: parse_timestamp(x.get('timestamp', '')))
        
        st.success(f"✅ Pobrano {len(filtered_messages)} wiadomości z {yesterday.strftime('%d.%m')} - {today.strftime('%d.%m')}")
        return filtered_messages
        
    except Exception as e:
        st.error(f"❌ Błąd połączenia z Kafka: {e}")
        return []

def get_latest_prices_from_history(history):
    prices = {}
    if not history:
        return prices
        
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

def parse_timestamp(timestamp_str):
    try:
        return pd.to_datetime(timestamp_str)
    except:
        return pd.Timestamp.now()

# --- Auto-refresh co 30 sekund ---
if 'last_refresh' not in st.session_state:
    st.session_state.last_refresh = time.time()

# Odświeżaj co 30 sekund (dane zmieniają się co 30s)
if time.time() - st.session_state.last_refresh > 30:
    st.session_state.last_refresh = time.time()
    st.experimental_set_query_params(refresh=int(time.time()))

# Przycisk ręcznego odświeżania
if st.button("🔄 Odśwież dane"):
    st.experimental_set_query_params(refresh=int(time.time()))

# Pobierz dane z Kafka
with st.spinner("Pobieranie danych z wczoraj i dzisiaj..."):
    portfolio_data = consume_portfolio_messages()

if portfolio_data:
    st.success(f"✅ Załadowano {len(portfolio_data)} snapshotów portfela")
    
    # Debug info - dokładny zakres czasowy
    if len(portfolio_data) > 1:
        timestamps = []
        for snapshot in portfolio_data:
            if 'timestamp' in snapshot:
                timestamps.append(parse_timestamp(snapshot['timestamp']))
        
        if timestamps:
            timestamps.sort()
            first_time = timestamps[0]
            last_time = timestamps[-1]
            time_span = last_time - first_time
            
            st.info(f"📅 Zakres danych: {first_time.strftime('%d.%m.%Y %H:%M')} - {last_time.strftime('%d.%m.%Y %H:%M')} ({time_span})")
    
    latest_data = portfolio_data[-1]
    
    left_col, right_col = st.columns([1, 2])
    
    with left_col:
        st.subheader("💰 Aktualny stan portfela")
        
        cash = latest_data.get('cash', 0)
        stocks = latest_data.get('stocks', {})
        history = latest_data.get('history', [])
        
        portfolio_value = calculate_portfolio_value(cash, stocks, history)
        
        st.metric("Gotówka", f"{cash:.2f} PLN")
        st.metric("Całkowita wartość portfela", f"{portfolio_value:.2f} PLN")
        
        st.subheader("📈 Posiadane akcje")
        if any(amount > 0 for amount in stocks.values() if isinstance(amount, (int, float))):
            prices = get_latest_prices_from_history(history)
            for stock, amount in stocks.items():
                if isinstance(amount, (int, float)) and amount > 0:
                    current_price = prices.get(stock, 0)
                    value = amount * current_price
                    st.write(f"**{stock}**: {amount} szt. × {current_price:.2f} PLN = {value:.2f} PLN")
        else:
            st.write("Brak posiadanych akcji")
    
    with right_col:
        if history:
            st.subheader("📋 Historia transakcji")
            hist_df = pd.DataFrame(history)
            hist_df['timestamp'] = hist_df['timestamp'].apply(parse_timestamp)
            hist_df = hist_df.sort_values('timestamp', ascending=False)
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
        
        # Przygotowanie danych do wykresu - zachowaj wszystkie punkty (30s interwały)
        timeline_data = []

        for snapshot in portfolio_data:
            if 'timestamp' in snapshot:
                snapshot_time = parse_timestamp(snapshot['timestamp'])
                
                cash = snapshot.get('cash', 0)
                stocks = snapshot.get('stocks', {})
                history = snapshot.get('history', [])
                total_value = calculate_portfolio_value(cash, stocks, history)

                timeline_data.append({
                    'timestamp': snapshot_time,
                    'portfolio_value': total_value,
                    'cash': cash,
                    'stocks_value': total_value - cash
                })

        if timeline_data:
            timeline_data.sort(key=lambda x: x['timestamp'])
            
            st.subheader("📊 Wykres wartości portfela (ostatnie 2 dni)")
            
            df_timeline = pd.DataFrame(timeline_data)
            
            # Wykres z 30-sekundowymi danymi, ale osią co 1h
            fig, ax = plt.subplots(figsize=(14, 7))
            ax.plot(df_timeline['timestamp'], df_timeline['portfolio_value'], 
                    marker='o', linestyle='-', color='#1f77b4', linewidth=2, 
                    markersize=3, label='Wartość portfela', alpha=0.8)
            
            ax.set_xlabel("Czas")
            ax.set_ylabel("Wartość portfela (PLN)")
            ax.set_title("Zmiana wartości portfela w czasie (dane co 30s, oś co 1h)")
            ax.grid(True, alpha=0.3)
            ax.legend()
            
            # Formatowanie osi X - znaczniki co 1 godzinę
            import matplotlib.dates as mdates
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%d.%m %H:%M'))
            ax.xaxis.set_major_locator(mdates.HourLocator(interval=1))  # Co 1 godzinę
            ax.xaxis.set_minor_locator(mdates.MinuteLocator(interval=30))  # Drobne znaczniki co 30 min
            
            plt.xticks(rotation=45)
            ax.tick_params(axis='x', labelsize=9)
            plt.tight_layout()
            
            col1, col2 = st.columns([3, 1])
            with col1:
                st.pyplot(fig)
            with col2:
                st.subheader("📈 Statystyki")
                if len(df_timeline) > 1:
                    start_value = df_timeline['portfolio_value'].iloc[0]
                    end_value = df_timeline['portfolio_value'].iloc[-1]
                    change = end_value - start_value
                    change_pct = (change / start_value * 100) if start_value != 0 else 0
                    
                    st.metric("Zmiana wartości", f"{change:+.2f} PLN", f"{change_pct:+.2f}%")
                    st.metric("Maksymalna wartość", f"{df_timeline['portfolio_value'].max():.2f} PLN")
                    st.metric("Minimalna wartość", f"{df_timeline['portfolio_value'].min():.2f} PLN")
                    
                    # Info o danych
                    first_date = df_timeline['timestamp'].iloc[0].strftime('%d.%m %H:%M')
                    last_date = df_timeline['timestamp'].iloc[-1].strftime('%d.%m %H:%M')
                    st.info(f"📊 {len(df_timeline)} punktów danych")
                    st.info(f"🕐 {first_date} - {last_date}")
                    
                    # Średni interwał między pomiarami
                    if len(df_timeline) > 1:
                        time_diffs = df_timeline['timestamp'].diff().dropna()
                        avg_interval = time_diffs.mean()
                        st.info(f"⏱️ Średni interwał: {int(avg_interval.total_seconds())}s")
        else:
            st.info("Brak danych do wygenerowania wykresu.")

        if history:
            st.subheader("📊 Statystyki transakcji")
            col1, col2, col3 = st.columns(3)
            with col1:
                buy_count = len([t for t in history if t.get('action') == 'BUY'])
                st.metric("Transakcje kupna", buy_count)
            with col2:
                sell_count = len([t for t in history if t.get('action') == 'SELL'])
                st.metric("Transakcje sprzedaży", sell_count)
            with col3:
                total_transactions = len(history)
                st.metric("Łączne transakcje", total_transactions)
        else:
            st.info("Brak historii transakcji do wyświetlenia")
else:
    st.warning("Brak danych z topiku 'portfolio'. Sprawdź czy:")
    st.write("1. Kafka jest uruchomiona")
    st.write("2. Topic 'portfolio' istnieje")  
    st.write("3. Moduł portfela wysyła dane co 30 sekund")
