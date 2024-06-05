import time
import json
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd
from kafka import KafkaProducer
import logging

# Ustawienia loggera
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FILE_PATH = 'RTA_today.xlsx'
INTERVAL = 5  # co 5 sekund
topic = 'test-topic'

def create_producer():
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    return producer

def send_message(producer, topic, message):
    future = producer.send(topic, message)
    try:
        record_metadata = future.get(timeout=10)
        logger.info(f"Message sent, partition {record_metadata.partition} offset {record_metadata.offset}")
    except Exception as e:
        logger.error(f"Failed to send message: {e}")


# Funkcja analizująca profil klienta według płci i wieku
def analyze_customer_profile(data):
    age_bins = [0, 18, 30, 40, 50, 60, 100]
    age_labels = ['<18', '19-30', '31-40', '41-50', '51-60', '60+']
    data['Age Group'] = pd.cut(data['Age'], bins=age_bins, labels=age_labels, right=False)
    customer_profile_summary = data.groupby(['Product Category', 'Gender', 'Age Group']).agg({
        'Quantity': 'sum',
        'Total Amount': 'sum'
    }).reset_index()
    #!!!!!
    customer_profile_summary['Date'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
    return customer_profile_summary

def analytics(producer, topic):

    start_index = 1

    while True:
        try:
            # Wczytaj 50 wiersze z pliku Excel
            data = pd.read_excel(FILE_PATH, engine='openpyxl', header=0, skiprows=range(1, start_index), nrows=50)

            if not data.empty:
                
                customer_profile_analysis = analyze_customer_profile(data)
                
                # Wysyłanie wiadomości do Kafka
                send_message(producer, topic, customer_profile_analysis.to_dict())
                
                # Aktualizuj ostatni wczytany indeks
                start_index += 50
            else:
                logger.info("No more data to process. Exiting.")
                break

            # Oczekuj przez ustalony interwał (5 sekund)
            time.sleep(INTERVAL)

        except Exception as e:
            logger.error(f"An error occurred: {e}")
            break

if __name__ == '__main__':
    try:
        producer = create_producer()
        analytics(producer, topic)
    finally:
        producer.close()
        logger.info("Producer closed.")
