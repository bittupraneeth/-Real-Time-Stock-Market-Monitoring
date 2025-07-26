from kafka import KafkaProducer
import requests
import json
import time

# --------------------- Configuration ---------------------
API_KEY = 'd21hb59r01qpst751750d21hb59r01qpst75175g'  # Replace with your actual Finnhub API key
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'stock_prices'
API_RATE_LIMIT = 50  # Max 50 API calls per minute

# --------------------- Initialize Kafka Producer ---------------------
# producer = KafkaProducer(
#     bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
#     value_serializer=lambda v: json.dumps(v).encode('utf-8')
# )

# --------------------- NIFTY 50 Symbols (BSE Format) ---------------------
symbols = [
  "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "GOOG", "META", "AVGO", "BRK.B",
  "TSLA", "JPM", "LLY", "WMT", "V", "MA", "COST", "XOM", "JNJ", "UNH", "PG",
  "BAC", "ORCL", "KO", "NFLX", "C", "PEP", "CRM", "CSCO", "DIS", "CMCSA",
  "ABT", "CVX", "NKE", "VZ", "TMO", "DHR", "TXN", "MRK", "MDT", "UPS",
  "LOW", "HON", "MCD", "AMGN", "BMY", "QCOM", "WFC", "IBM", "GE", "CAT"
]
# Add .BSE suffix for Finnhub API
# symbols = [symbol + ".BSE" for symbol in symbols]

# --------------------- Helper Function ---------------------
def fetch_price(symbol):
    url = f"https://finnhub.io/api/v1/quote?symbol={symbol}&token={API_KEY}"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            quote = response.json()
            if quote.get('c') == 0:
                print(f"[WARN] No valid data for {symbol}")
                return None
            return {
                'symbol': symbol,
                'price': quote.get('c'),
                'high': quote.get('h'),
                'low': quote.get('l'),
                'open': quote.get('o'),
                'prev_close': quote.get('pc'),
                'timestamp': int(time.time())
            }
        else:
            print(f"[ERROR] HTTP {response.status_code} for {symbol}")
    except Exception as e:
        print(f"[EXCEPTION] {symbol}: {e}")
    return None

# --------------------- Producer Loop ---------------------
batch_size = 10
interval = 12  # seconds (10 calls every 12s = 50/min)

print("✅ Starting stock data producer...")

while True:
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i + batch_size]
        for symbol in batch:
            data = fetch_price(symbol)
            if data:
                print(f"📤 Sending to Kafka: {data}")
                # producer.send(KAFKA_TOPIC, value=data)
        time.sleep(interval)
