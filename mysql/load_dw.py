import mysql.connector
import csv
from pathlib import Path

def safe_float(value):
    """Convert value to float, handling 'inf' and empty strings"""
    if value == 'inf' or value == '':
        return None
    try:
        return float(value)
    except ValueError:
        return None

def safe_int(value):
    """Convert value to int, handling empty strings"""
    if value == '':
        return None
    try:
        return int(value)
    except ValueError:
        return None


current_dir = Path(__file__).parent
csv_path = current_dir.parent / 'data' / 'country_wise_latest.csv'


if not csv_path.exists():
    raise FileNotFoundError(f"CSV file not found at: {csv_path}")

# Connect to MySQL DW
try:
    conn = mysql.connector.connect(
        host="127.0.0.1",
        port=23306,
        user="root",
        password="secret",
        database="covid_dw"
    )
    cursor = conn.cursor()

    # Load Country Dimension
    with open(csv_path, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        countries = [(row['Country/Region'], row['WHO Region']) for row in reader]
        
        cursor.executemany("""
            INSERT INTO Country (Country_Name, WHO_Region)
            VALUES (%s, %s)
        """, countries)

    # Load Time Dimension 
    cursor.execute("INSERT INTO Time (Date) VALUES ('2023-07-01')")
    time_id = cursor.lastrowid

    # Load Fact Table
    with open(csv_path, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        
       
        cursor.execute("SELECT CountryID, Country_Name FROM Country")
        country_map = {name: id for id, name in cursor.fetchall()}
        
       
        batch = []
        for row in reader:
            country_id = country_map[row['Country/Region']]
            
            batch.append((
                country_id, time_id,
                safe_int(row['Confirmed']),
                safe_int(row['Deaths']),
                safe_int(row['Recovered']),
                safe_int(row['Active']),
                safe_int(row['New cases']),
                safe_int(row['New deaths']),
                safe_int(row['New recovered']),
                safe_float(row['Deaths / 100 Cases']),
                safe_float(row['Recovered / 100 Cases']),
                safe_float(row['Deaths / 100 Recovered']),
                safe_int(row['Confirmed last week']),
                safe_int(row['1 week change']),
                safe_float(row['1 week % increase'])
            ))
        
        
        cursor.executemany("""
            INSERT INTO COVID_Facts (
                CountryID, TimeID, Confirmed, Deaths, Recovered, Active,
                New_cases, New_deaths, New_recovered, Deaths_per_100_cases,
                Recovered_per_100_cases, Deaths_per_100_recovered,
                Confirmed_last_week, One_week_change, One_week_percent_increase
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, batch)

    conn.commit()
    print("Data loaded successfully!")

except Exception as e:
    print(f"Error: {e}")
    if 'conn' in locals():
        conn.rollback()

finally:
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()