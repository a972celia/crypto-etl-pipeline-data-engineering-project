import locale
from kafka import KafkaConsumer
import json
from datetime import datetime
import matplotlib.pyplot as plt
import pandas as pd
import os

def setup_consumer():
    KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL", "default_kafka_service:9092")
    consumer = KafkaConsumer(
        'bitcoin',
        bootstrap_servers=KAFKA_BROKER_URL,
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )
    return consumer

def update_plot(df):
    plt.clf()
    plt.plot(df['Timestamp'], df['Price'], label='Bitcoin Price')
    plt.plot(df['Timestamp'], df['MA'], label='Moving Average (20 periods)')
    plt.xlabel('Time')
    plt.ylabel('Bitcoin Price (USD)')
    plt.title('Real-Time Bitcoin Price with Moving Average')
    plt.legend()
    plt.gca().get_yaxis().set_major_formatter(locale.currency)
    plt.gcf().autofmt_xdate()
    plt.pause(0.001)

def main():
    locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
    df = pd.DataFrame(columns=['Timestamp', 'Price'])
    
    consumer = setup_consumer()

    try:
        for message in consumer:
            value = message.value
            timestamp = datetime.fromtimestamp(message.timestamp / 1000.0)
            
            new_row = {'Timestamp': timestamp, 'Price': round(value)}
            df = df.append(new_row, ignore_index=True)

            df['MA'] = df['Price'].rolling(window=20).mean()

            update_plot(df)
            
    except KeyboardInterrupt:
        print("\nStopped by user.")
    finally:
        consumer.close()
        plt.show()

if __name__ == "__main__":
    main()
