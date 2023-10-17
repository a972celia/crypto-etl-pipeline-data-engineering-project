import requests
import os
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


# Fetch the Kafka broker URL from the environment variables
KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL", "default_kafka_service:9092")

# Create a Kafka producer instance
producer = KafkaProducer(
    bootstrap_servers = KAFKA_BROKER_URL,
    value_serializer = lambda v: str(v).encode('utf-8')
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
except requests.RequestException as e:
    print(f"Network-related error occurred: {e}")
except KafkaError as e:
    print(f"Kafka error occurred: {e}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
finally:
    producer.close()