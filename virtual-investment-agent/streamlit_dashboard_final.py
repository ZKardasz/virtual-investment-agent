import streamlit as st
from kafka import KafkaConsumer
import json
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import time

st.set_page_config(layout="wide")
tab1, tab2 = st.tabs(["📊 Dashboard Portfela", "📈 Analiza wyników"])
with tab1:
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
                
                # Wykres zmian wartości portfela w czasie
                st.subheader("📊 Wykres wartości portfela")
                
                # Symuluj zmiany wartości portfela na podstawie transakcji
                portfolio_timeline = []
                current_cash = 10000  # Zakładam startową kwotę
                current_stocks = {stock: 0 for stock in stocks.keys()}
                
                for transaction in sorted(history, key=lambda x: x['timestamp']):
                    if transaction['action'] == 'BUY':
                        current_cash -= transaction['price']
                        current_stocks[transaction['stock']] += 1
                    elif transaction['action'] == 'SELL':
                        current_cash += transaction['price']
                        current_stocks[transaction['stock']] -= 1
                    
                    # Oblicz wartość portfela w tym momencie
                    prices = {transaction['stock']: transaction['price']}
                    stocks_value = sum(current_stocks[stock] * prices.get(stock, transaction['price']) 
                                     for stock in current_stocks.keys())
                    total_value = current_cash + stocks_value
                    
                    portfolio_timeline.append({
                        'timestamp': pd.to_datetime(transaction['timestamp']),
                        'portfolio_value': total_value
                    })
                
                if portfolio_timeline:
                    timeline_df = pd.DataFrame(portfolio_timeline)
                    
                    fig, ax = plt.subplots(figsize=(10, 5))
                    ax.plot(timeline_df['timestamp'], timeline_df['portfolio_value'], 
                           marker='o', linestyle='-', color='#1f77b4', linewidth=2, markersize=4)
                    ax.set_xlabel("Czas")
                    ax.set_ylabel("Wartość portfela (PLN)")
                    ax.set_title("Zmiana wartości portfela w czasie")
                    ax.grid(True, alpha=0.3)
                    plt.xticks(rotation=45)
                    plt.tight_layout()
                    st.pyplot(fig)
                    plt.close()
                
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

#Utworzenie drugiej zakładki
with tab2:
    st.title("📈 Analiza wyników inwestowania")
    
    if not portfolio_data:
        st.warning("Brak danych do analizy.")  #komunikat w razie braku danych
    else:
        history = portfolio_data[-1].get("history", []) #historia ostatniego stany portfela
        #W przypadku braku akcji kupna/sprzedazy wyswietlony zostanie pusty raport
        if not history:
            st.info("Brak historii transakcji do analizy.")
            #Podstawowe statystyki
            st.subheader("Podstawowe statystyki")
            col1, col2 = st.columns(2)
            col1.metric("Liczba transakcji KUPNA", "0")
            col2.metric("Liczba transakcji SPRZEDAŻY", "0")

            st.metric("Średni zysk/strata na transakcji", "Brak danych")
            st.metric("📈 Zwrot z inwestycji (ROI)", "Brak danych")

            st.subheader("📅 Najlepszy i najgorszy dzień")
            col1, col2 = st.columns(2)
            col1.write(" **Najlepszy dzień**: brak danych")
            col2.write(" **Najgorszy dzień**: brak danych")
            #Generowanie pustego wykresu
            st.subheader("📉 Wykres ROI w czasie")
            fig, ax = plt.subplots(figsize=(10, 4))
            ax.set_title("Zwrot z inwestycji w czasie")
            ax.set_ylabel("ROI (%)")
            ax.set_xlabel("Czas")
            ax.grid(True, alpha=0.3)
            st.pyplot(fig)
        else:
            #W przypadku podjecia decyzji o kupnie/sprzedazy wygenerowany zostanie kompletny raport
            hist_df = pd.DataFrame(history) 
            hist_df['timestamp'] = pd.to_datetime(hist_df['timestamp'])
            
            #Podstawowe statystyki
            buy_df = hist_df[hist_df['action'] == 'BUY']
            sell_df = hist_df[hist_df['action'] == 'SELL']
            
            buy_count = len(buy_df) #liczba zakupow
            sell_count = len(sell_df) #liczba sprzedazy
            
            st.subheader("Podstawowe statystyki")
            col1, col2 = st.columns(2)
            col1.metric("Liczba transakcji KUPNA", buy_count)
            col2.metric("Liczba transakcji SPRZEDAŻY", sell_count)
            
            #Sredni zysk/strata
            if not sell_df.empty and not buy_df.empty:
                merged = pd.merge(
                    buy_df.sort_values('timestamp'),
                    sell_df.sort_values('timestamp'),
                    on='stock',
                    suffixes=('_buy', '_sell')
                )
                merged = merged[merged['timestamp_sell'] > merged['timestamp_buy']]
                
                merged['profit'] = merged['price_sell'] - merged['price_buy']
                avg_profit = merged['profit'].mean()
                
                st.metric("Średni zysk/strata na transakcji", f"{avg_profit:.2f} PLN")
            else:
                st.info("Brak pełnych par KUPNA/SPRZEDAŻY do analizy zysków.")
            
            #Obliczenie ROI
            start_value = 10000  # zakładany kapitał początkowy
            latest_data = portfolio_data[-1]
            current_value = calculate_portfolio_value(
                latest_data['cash'],
                latest_data['stocks'],
                history
            )
            roi = (current_value - start_value) / start_value * 100
            st.metric("📈 Zwrot z inwestycji (ROI)", f"{roi:.2f}%")
            
            #Podanie informacji o najlepszym i najgorszym dniu
            hist_df['date'] = hist_df['timestamp'].dt.date
            daily_value = hist_df.groupby('date')['price'].sum()
            best_day = daily_value.idxmax()
            worst_day = daily_value.idxmin()
            st.subheader("📅 Najlepszy i najgorszy dzień")
            col1, col2 = st.columns(2)
            col1.write(f" **Najlepszy dzień**: {best_day} (+{daily_value[best_day]:.2f} PLN)")
            col2.write(f" **Najgorszy dzień**: {worst_day} ({daily_value[worst_day]:.2f} PLN)")
            
            #Generowanie wykresu ROI
            st.subheader("📉 Wykres ROI w czasie")
            timeline = []
            cash = 10000
            stocks = {}
            last_prices = {}

            for tx in hist_df.sort_values('timestamp'):
                stock = tx['stock']
                action = tx['action']
                price = tx['price']

                # Inicjalizacja
                if stock not in stocks:
                    stocks[stock] = 0

                # Aktualizacja stanu portfela
                if action == 'BUY':
                    cash -= price
                    stocks[stock] += 1
                elif action == 'SELL' and stocks[stock] > 0:
                    cash += price
                    stocks[stock] -= 1

                # Zaktualizuj ostatnią znaną cenę dla tej akcji
                last_prices[stock] = price

                # Oblicz wartość portfela
                stocks_value = sum(stocks[s] * last_prices.get(s, 0) for s in stocks)
                total_value = cash + stocks_value
                roi_val = (total_value - 10000) / 10000 * 100

                timeline.append({
                    'timestamp': tx['timestamp'],
                    'roi': roi_val
                })
            
            if timeline:
                df_roi = pd.DataFrame(timeline)
                fig, ax = plt.subplots(figsize=(10, 4))
                ax.plot(df_roi['timestamp'], df_roi['roi'], color='green')
                ax.set_title("Zwrot z inwestycji w czasie")
                ax.set_ylabel("ROI (%)")
                ax.set_xlabel("Czas")
                ax.grid(True, alpha=0.3)
                plt.xticks(rotation=45)
                st.pyplot(fig)