import requests
from kafka import KafkaProducer
from time import sleep
import json

def get_crypto_price(crypto):
    try:
        url = f"https://api.coingecko.com/api/v3/simple/price?ids={crypto}&vs_currencies=usd"
        response = requests.get(url)
        response.raise_for_status()  # Check for HTTP errors
        data = response.json()
        return data.get(crypto, {}).get('usd')
    except requests.RequestException as e:
        print(f"Error retrieving price for {crypto}: {e}")
        return None

# Create a Kafka producer instance
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: str(v).encode('utf-8')
    # Add any additional Kafka producer configurations here
)

try:
    while True:
        for crypto in ['bitcoin', 'ethereum']:
            price = get_crypto_price(crypto)
            if price is not None:
                producer.send(crypto, value=price)
        sleep(10)
except KeyboardInterrupt:
    print("Shutting down...")
finally:
    producer.close()