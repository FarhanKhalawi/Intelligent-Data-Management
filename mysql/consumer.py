from kafka import KafkaConsumer
import mysql.connector
import json


db = mysql.connector.connect(
    host="127.0.0.1",
    port=23306,
    user="root",
    password="secret",
    database="covid_dw"
)
cursor = db.cursor()


consumer = KafkaConsumer(
    "covid_data_topic",
    bootstrap_servers="localhost:29092",
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Starting to listen for messages...")

try:
    for message in consumer:
        data = message.value
        
        # 1. Find the country ID
        cursor.execute("SELECT CountryID FROM Country WHERE Country_Name = %s", 
                      (data['Country/Region'],))
        country_id = cursor.fetchone()[0]
        
        # 2. Insert into fact table
        cursor.execute("""
            INSERT INTO COVID_Facts (
                CountryID, TimeID, Confirmed, Deaths, Recovered, Active,
                New_cases, New_deaths, New_recovered, Deaths_per_100_cases,
                Recovered_per_100_cases, Deaths_per_100_recovered,
                Confirmed_last_week, One_week_change, One_week_percent_increase
            ) VALUES (%s, 1, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                country_id,
                int(data['Confirmed']),
                int(data['Deaths']),
                int(data['Recovered']),
                int(data['Active']),
                int(data['New cases']),
                int(data['New deaths']),
                int(data['New recovered']),
                float(data['Deaths / 100 Cases']),
                float(data['Recovered / 100 Cases']),
                float(data['Deaths / 100 Recovered']),
                int(data['Confirmed last week']),
                int(data['1 week change']),
                float(data['1 week % increase'])
            ))
        
        db.commit()
        print(f"Inserted data for {data['Country/Region']}")

except KeyboardInterrupt:
    print("\nStopping...")
finally:
    cursor.close()
    db.close()
    consumer.close()