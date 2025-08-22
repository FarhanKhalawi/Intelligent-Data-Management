import pymongo
import csv
from pathlib import Path

# 1. Establish one-time connection
client = pymongo.MongoClient(
    host="mongodb://root:secret@localhost:27017/",
    serverSelectionTimeoutMS=5000  # 5 second timeout
)

# 2. Access database and collections
db = client["covid_dw"]
countries = db["countries"]
daily_stats = db["daily_stats"]

# 3. Clear existing collections (if needed)
countries.drop()
daily_stats.drop()

# 4. Load and insert data
csv_path = Path(__file__).parent.parent / "data" / "country_wise_latest.csv"

with open(csv_path, 'r', encoding='utf-8') as file:
    reader = csv.DictReader(file)
    
    for row in reader:
        # Insert country
        country_id = countries.insert_one({
            "name": row["Country/Region"],
            "who_region": row["WHO Region"]
        }).inserted_id
        
        # Insert stats with reference
        daily_stats.insert_one({
            "country_id": country_id,
            "date": "2023-07-01",
            "confirmed": int(row["Confirmed"]),
            "deaths": int(row["Deaths"]),
            "recovered": int(row["Recovered"]),
            "active": int(row["Active"]),
            "new_cases": int(row["New cases"]),
            "new_deaths": int(row["New deaths"]),
            "new_recovered": int(row["New recovered"]),
            "death_rate": float(row["Deaths / 100 Cases"]),
            "recovery_rate": float(row["Recovered / 100 Cases"]),
            "death_recovery_ratio": None if row["Deaths / 100 Recovered"] == 'inf' 
                                   else float(row["Deaths / 100 Recovered"])
        })

print(f"Inserted {countries.count_documents({})} countries")
print(f"Inserted {daily_stats.count_documents({})} daily records")

# 5. Close connection
client.close()