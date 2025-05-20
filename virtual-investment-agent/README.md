# Stock Data Producer

Moduł do pobierania danych giełdowych (AAPL, MSFT, TSLA) i publikowania ich do Apache Kafka (topic `stock_data`), lub lokalnego pliku JSON (np. w Google Colab).

## Uruchomienie lokalne
```bash
pip install -r requirements.txt
python run_producer.py
```

## Uruchomienie w Google Colab
Zobacz plik: `notebook/stock_producer_colab.ipynb`

## Struktura danych JSON
```json
{
  "timestamp": "2025-05-20T10:00:00Z",
  "stock": "AAPL",
  "price": 175.20
}
```