import os
import json
import time
import requests
import logging
from kafka import KafkaProducer
from datetime import datetime, timedelta
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env'))

url_base = os.getenv('API_URL')
if not url_base:
    raise ValueError("API_URL is not set in the .env file.")

url = f"{url_base}?$limit=1000&$order=read_date DESC"
kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(',')
topic_name = "traffic-telemetry"

data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
sample_file_path = os.path.join(data_dir, 'sample_camera_data.json')

if not url:
    raise ValueError("API_URL is not set in the .env file.")

if not kafka_bootstrap_servers:
    raise ValueError("KAFKA_BOOTSTRAP_SERVERS is not set in the .env file.")

if not topic_name:
    raise ValueError("TOPIC_NAME is not set in the .env file.")

def delivery_report(err, msg):
    """ Callback function to handle delivery reports from Kafka producer."""
    if err is not None:
        logging.error(f"Delivery failed for record {msg.key()}: {err}")
    else:
        logging.debug(f"Record successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
        
def fetch_data_and_save():
    """ Fetches data from the API and saves it to a local JSON file for simulation purposes."""
    logging.info(f"Fetching data from API: {url.split('?')[0]}")
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    
    os.makedirs(data_dir, exist_ok=True)
    with open(sample_file_path, 'w') as f:
        json.dump(data, f, indent=4)
    logging.info(f"Data saved to {sample_file_path}, containing {len(data)} records.")
    
    return data

def create_producer():
    """ Creates and returns a Kafka producer instance."""
    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8'),
        retries=3
    )
    return producer

def run_simulation():
    """ Runs the simulation of real-time data ingestion to Kafka."""
    data = fetch_data_and_save()
    producer = create_producer()
    
    logging.info(f"Starting simulation of real-time data ingestion to Kafka topic '{topic_name}'")
    
    try:
        for idx, record in enumerate(data):
            sensor_id = str(record.get('camera_id', f"sensor_{idx}"))
            count = int(record.get('turn_count',1 ))
            
            try:
                event_time_obj = datetime.now()
            except Exception:
                event_time_obj = datetime.now()
            
            if idx > 0 and idx % 10 == 0:
                event_time_obj = event_time_obj - timedelta(seconds=8)
                logging.warning(f"Simulating out-of-order event for record {idx}: Adjusted event time to {event_time_obj.isoformat()}")
            
            formatted_event_time = event_time_obj.strftime('%Y-%m-%d %H:%M:%S.000')
            
            payload = {
                "event_id": f"evt_{sensor_id}_{idx}",
                "sensor_id": sensor_id,
                "turn_count": count,
                "count_date": formatted_event_time
            }
            
            producer.send(topic_name, key=sensor_id, value=payload).add_errback(delivery_report)
            logging.info(f"Produced record {idx} to topic '{topic_name}': {payload}")
            
            time.sleep(2)  # Simulate a delay of 2 seconds between records
    
    except KeyboardInterrupt:
        logging.info("Simulation interrupted by user.")
        
    finally:
        producer.flush()
        producer.close()
        logging.info("Kafka producer closed. Simulation ended.")


if __name__ == "__main__":
    run_simulation()
        