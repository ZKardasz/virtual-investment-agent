from kafka import KafkaConsumer
from kafka import KafkaProducer
import json 

def start_signals():
    SERVER = "localhost:9092"
    TOPIC  = "stock_data"

    # Konsumer do pobierania danych z Kafka
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=SERVER,
        group_id='stock_data_consumer_group ',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda m: json.dumps(m).encode('utf-8')
    )

    price_history = {}
    MAX_HISTORY = 5 #podejmujemy decyzje na podstawie 5 ostatnich wpisów

    for message in consumer:
        stock_message = message.value
        symbol = stock_message["stock"]
        price = stock_message["price"]
        ts = stock_message['timestamp']

        history = price_history.setdefault(symbol, []) # 1) Pobierz lub utwórz listę historii dla danego symbolu
        history.append(price) # 2) Dopisz nową cenę
        if len(history) > MAX_HISTORY: # 3) Jeśli dla tego symbolu zebrało się za dużo wpisów, usuń najstarszy
            history.pop(0)

        is_increasing = all(history[i] < history[i+1] for i in range(len(history)-1))
        is_decreasing = all(history[i] > history[i+1] for i in range(len(history)-1))

        if len(history) < MAX_HISTORY:
            action = 'HOLD'
        elif is_increasing:
            action = 'BUY'
        elif is_decreasing:
            action = 'SELL'
        else:
            action = 'HOLD'
        
        signal = {
            "stock": symbol,
            "action": action,
            "timestamp": ts
        }

        producer.send('signals', signal)
        producer.flush()
        
        print(f"SENT: {signal}")
