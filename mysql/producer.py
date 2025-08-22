import csv
import json
import time
import os
from kafka import KafkaProducer

def create_producer():
    retries = 5
    while retries > 0:
        try:
            return KafkaProducer(
                bootstrap_servers=['localhost:29092'],
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                api_version=(2, 8, 1)
            )
        except Exception as e:
            print(f"Connection failed, retrying... ({retries} attempts left). Error: {str(e)}")
            time.sleep(5)
            retries -= 1
    raise Exception("Failed to connect to Kafka after multiple attempts")

def get_csv_path():
    
    possible_paths = [
        os.path.join('data', 'country_wise_latest.csv'),  
        os.path.join(os.path.dirname(__file__), 'data', 'country_wise_latest.csv'),  
        'country_wise_latest.csv'  
    ]
    
    for path in possible_paths:
        if os.path.exists(path):
            print(f"Found CSV at: {os.path.abspath(path)}")
            return path
    raise FileNotFoundError("Could not find country_wise_latest.csv in any expected location")

try:
    producer = create_producer()
    print("Successfully connected to Kafka!")
    
    csv_path = get_csv_path()
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                producer.send('covid_data', value=row)
                print(f"Sent: {row['Country/Region']}")
            except Exception as e:
                print(f"Failed to send {row['Country/Region']}: {str(e)}")
    
    producer.flush()
    print("All data sent successfully!")

except Exception as e:
    print(f"Fatal error: {str(e)}")