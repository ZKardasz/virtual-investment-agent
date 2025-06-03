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
    """Pobiera wiadomości z Kafka z ostatnich 2 dni (wczoraj i dzisiaj)"""
    try:
        # Używamy unikalnej group_id za każdym razem, aby czytać od początku
        group_id = f'portfolio_dashboard_group_{int(time.time())}'
        
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='earliest',  # Czytaj od początku topiku
            enable_auto_commit=False,      # Nie commituj offsetów
            group_id=group_id,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=10000,     # Zwiększony timeout
            fetch_max_wait_ms=2000,       # Maksymalny czas oczekiwania na dane
            max_poll_records=1000         # Maksymalna liczba rekordów na poll
        )
        
        messages = []
        start_time = time.time()
        empty_polls = 0
        max_empty_polls = 3
        
        # Oblicz zakres dat - 2 czerwca i 3 czerwca 2025
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        yesterday = today - timedelta(days=1)
        
        st.info(f"🔍 Pobieranie danych z Kafka od {yesterday.strftime('%d.%m.%Y')} do {today.strftime('%d.%m.%Y')}...")
        
        while True:
            # Poll z większym timeout
            message_batch = consumer.poll(timeout_ms=3000)
            
            if message_batch:
                empty_polls = 0
                for topic_partition, records in message_batch.items():
                    for record in records:
                        messages.append(record.value)
                        
                st.info(f"📥 Pobrano dotychczas: {len(messages)} wiadomości...")
            else:
                empty_polls += 1
                if empty_polls >= max_empty_polls:
                    break
            
            # Bezpieczeństwo - nie czekaj więcej niż 15 sekund
            if time.time() - start_time > 15:
                st.warning("⏰ Timeout pobierania danych - używam dostępne dane")
                break
                
        consumer.close()
        
        # Filtruj wiadomości z ostatnich 2 dni (wczoraj i dzisiaj) i usuń duplikaty
        filtered_messages = []
        seen_timestamps = set()
        
        for msg in messages:
            timestamp_str = msg.get('timestamp')
            if timestamp_str:
                try:
                    msg_timestamp = parse_timestamp(timestamp_str)
                    # Sprawdź czy wiadomość jest z wczoraj lub dzisiaj
                    if msg_timestamp >= yesterday and timestamp_str not in seen_timestamps:
                        seen_timestamps.add(timestamp_str)
                        filtered_messages.append(msg)
                except:
                    continue
            else:
                # Jeśli brak timestamp, dodaj wiadomość (może być najnowsza)
                filtered_messages.append(msg)
        
        st.success(f"✅ Pobrano {len(filtered_messages)} wiadomości z okresu {yesterday.strftime('%d.%m')} - {today.strftime('%d.%m')} (z {len(messages)} całkowitych)")
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

def determine_time_period(timestamps):
    """Określa najlepszy okres czasowy do wyświetlenia na podstawie dostępnych danych"""
    if not timestamps:
        return timedelta(hours=1), "1 godzina"
    
    timestamps = sorted(timestamps)
    time_span = timestamps[-1] - timestamps[0]
    
    # Bardziej szczegółowe okresy
    if time_span >= timedelta(days=7):
        return timedelta(days=7), "7 dni"
    elif time_span >= timedelta(days=3):
        return timedelta(days=3), "3 dni"
    elif time_span >= timedelta(days=1):
        return timedelta(days=1), "1 dzień"
    elif time_span >= timedelta(hours=12):
        return timedelta(hours=12), "12 godzin"
    elif time_span >= timedelta(hours=6):
        return timedelta(hours=6), "6 godzin"
    elif time_span >= timedelta(hours=3):
        return timedelta(hours=3), "3 godziny"
    elif time_span >= timedelta(hours=1):
        return timedelta(hours=1), "1 godzina"
    elif time_span >= timedelta(minutes=30):
        return timedelta(minutes=30), "30 minut"
    else:
        # Dla bardzo krótkich okresów, pokaż wszystko bez filtrowania
        return timedelta(seconds=0), f"ostatnie {int(time_span.total_seconds() / 60)} minut"

# --- Prosty timer do odświeżania ---
if 'last_refresh' not in st.session_state:
    st.session_state.last_refresh = time.time()

if time.time() - st.session_state.last_refresh > 10:
    st.session_state.last_refresh = time.time()
    # Przekierowanie na siebie (odświeżenie strony)
    st.experimental_set_query_params(refresh=int(time.time()))

# Przycisk ręcznego odświeżania (opcjonalnie)
if st.button("🔄 Odśwież dane"):
    st.experimental_set_query_params(refresh=int(time.time()))

# Pobierz dane z Kafka
with st.spinner("Pobieranie danych z Kafka z ostatnich 2 dni (02.06 - 03.06)..."):
    portfolio_data = consume_portfolio_messages()

if portfolio_data:
    st.success(f"✅ Załadowano {len(portfolio_data)} snapshotów portfela z ostatnich 2 dni")
    
    # Debug info - pokaż zakres czasowy danych
    if len(portfolio_data) > 1:
        timestamps = []
        for snapshot in portfolio_data:
            if 'timestamp' in snapshot:
                timestamps.append(parse_timestamp(snapshot['timestamp']))
        
        if timestamps:
            timestamps.sort()
            time_span = timestamps[-1] - timestamps[0]
            if time_span.total_seconds() < 3600:
                span_text = f"{int(time_span.total_seconds() / 60)} minut"
            elif time_span.total_seconds() < 86400:
                span_text = f"{time_span.total_seconds() / 3600:.1f} godzin"
            else:
                span_text = f"{time_span.days} dni, {int((time_span.total_seconds() % 86400) / 3600)} godzin"
            
            st.info(f"📅 Dane obejmują okres: {span_text} (od {timestamps[0].strftime('%d.%m.%Y %H:%M')} do {timestamps[-1].strftime('%d.%m.%Y %H:%M')})")
    
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
        
        # Przygotowanie danych do wykresu
        timeline_data = []
        current_time = datetime.now()

        for i, snapshot in enumerate(portfolio_data):
            if 'timestamp' in snapshot:
                snapshot_time = parse_timestamp(snapshot['timestamp'])
            else:
                snapshot_time = current_time - timedelta(minutes=len(portfolio_data) - i)
            
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
            
            st.subheader("📊 Wykres wartości portfela (02.06 - 03.06)")
            
            if timeline_data:
                df_timeline = pd.DataFrame(timeline_data)
                
                # Sortuj według czasu
                df_timeline = df_timeline.sort_values('timestamp')
                
                # Prostszy wykres - tylko wartość portfela
                fig, ax = plt.subplots(figsize=(12, 6))
                ax.plot(df_timeline['timestamp'], df_timeline['portfolio_value'], 
                        marker='o', linestyle='-', color='#1f77b4', linewidth=3, 
                        markersize=4, label='Wartość portfela')
                
                ax.set_xlabel("Czas")
                ax.set_ylabel("Wartość portfela (PLN)")
                ax.set_title("Zmiana wartości portfela w czasie (02.06 - 03.06.2025)")
                ax.grid(True, alpha=0.3)
                ax.legend()
                
                # Lepsze formatowanie osi X - pokaż daty i godziny
                import matplotlib.dates as mdates
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%d.%m %H:%M'))
                ax.xaxis.set_major_locator(mdates.HourLocator(interval=2))  # Co 2 godziny
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
                        
                        # Info o liczbie punktów danych i zakresie dat
                        first_date = df_timeline['timestamp'].iloc[0].strftime('%d.%m %H:%M')
                        last_date = df_timeline['timestamp'].iloc[-1].strftime('%d.%m %H:%M')
                        st.info(f"📊 {len(df_timeline)} punktów danych")
                        st.info(f"🕐 Od {first_date} do {last_date}")
            else:
                st.info("Brak danych z okresu 02.06 - 03.06 do wygenerowania wykresu.")
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
    st.warning("Brak danych z topicu 'portfolio'. Sprawdź czy:")
    st.write("1. Kafka jest uruchomiona")
    st.write("2. Topic 'portfolio' istnieje")
    st.write("3. Moduł portfela wysyła dane")
