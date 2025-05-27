from kafka import KafkaConsumer, KafkaProducer
import json

INITIAL_CASH = 10000.0

class Portfolio:
    def __init__(self):
        self.cash = INITIAL_CASH
        self.stocks = {}  # np. { "AAPL": 2, "TSLA": 1 }
        self.history = []  # lista transakcji

    def process_signal(self, signal):
        symbol = signal['stock']
        action = signal['action']
        price = signal.get('price', 0.0)
        timestamp = signal['timestamp']

        if action == 'BUY':
            if self.cash >= price:
                self.cash -= price
                self.stocks[symbol] = self.stocks.get(symbol, 0) + 1
                self.history.append({
                    "timestamp": timestamp,
                    "action": "BUY",
                    "stock": symbol,
                    "price": price
                })
                print(f"Kupiono 1 akcję {symbol} za {price}, gotówka: {self.cash}")
            else:
                print(f"Za mało gotówki na zakup {symbol} ({price})")

        elif action == 'SELL':
            if self.stocks.get(symbol, 0) > 0:
                self.cash += price
                self.stocks[symbol] -= 1
                self.history.append({
                    "timestamp": timestamp,
                    "action": "SELL",
                    "stock": symbol,
                    "price": price
                })
                print(f"Sprzedano 1 akcję {symbol} za {price}, gotówka: {self.cash}")
            else:
                print(f"Brak akcji {symbol} do sprzedaży")

        else:
            print(f"Brak akcji (HOLD) dla {symbol}")

    def to_dict(self):
        return {
            "cash": round(self.cash, 2),
            "stocks": self.stocks,
            "history": self.history[-10:]  # ostatnie 10 transakcji
        }

def start_portfolio_manager():
    SERVER = 'localhost:9092'
    SIGNALS_TOPIC = 'signals'
    PORTFOLIO_TOPIC = 'portfolio'

    consumer = KafkaConsumer(
        SIGNALS_TOPIC,
        bootstrap_servers=SERVER,
        group_id='portfolio_manager_group',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    producer = KafkaProducer(
        bootstrap_servers=SERVER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    portfolio = Portfolio()

    for message in consumer:
        signal = message.value
        price = signal.get("price")  # może nie być w sygnale

        # Jeśli nie ma ceny, dodajmy ją sztucznie (dla demo, można to zmienić)
        if not price:
            print("Brak ceny w sygnale, pomijam")
            continue

        portfolio.process_signal(signal)
        state = portfolio.to_dict()

        producer.send(PORTFOLIO_TOPIC, value=state)
        producer.flush()

        print(f"Wysłano stan portfela: {state}")
