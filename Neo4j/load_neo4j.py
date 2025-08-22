from neo4j import GraphDatabase
import csv
from pathlib import Path

# Neo4j connection
URI = "bolt://localhost:7687"
AUTH = ("neo4j", "StrongPassword123")  # Use your password

# CSV file path
csv_path = Path(__file__).parent.parent / "data" / "country_wise_latest.csv"

def load_data_to_neo4j():
    driver = GraphDatabase.driver(URI, auth=AUTH)
    
    with driver.session() as session:
        # Clear existing data
        session.run("MATCH (n) DETACH DELETE n")
        
        # Load countries and create relationships
        with open(csv_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            
            for row in reader:
                # Create Country node
                session.run("""
                    MERGE (c:Country {name: $name})
                    SET c.who_region = $who_region
                """, {
                    "name": row["Country/Region"],
                    "who_region": row["WHO Region"]
                })
                
                # Create DailyStats node and relationship
                session.run("""
                    MATCH (c:Country {name: $name})
                    CREATE (c)-[:HAS_STATS]->(s:DailyStats {
                        date: '2023-07-01',
                        confirmed: toInteger($confirmed),
                        deaths: toInteger($deaths),
                        recovered: toInteger($recovered),
                        active: toInteger($active),
                        new_cases: toInteger($new_cases),
                        new_deaths: toInteger($new_deaths),
                        new_recovered: toInteger($new_recovered),
                        death_rate: toFloat($death_rate),
                        recovery_rate: toFloat($recovery_rate),
                        confirmed_last_week: toInteger($confirmed_last_week)
                    })
                """, {
                    "name": row["Country/Region"],
                    "confirmed": row["Confirmed"],
                    "deaths": row["Deaths"],
                    "recovered": row["Recovered"],
                    "active": row["Active"],
                    "new_cases": row["New cases"],
                    "new_deaths": row["New deaths"],
                    "new_recovered": row["New recovered"],
                    "death_rate": row["Deaths / 100 Cases"],
                    "recovery_rate": row["Recovered / 100 Cases"],
                    "confirmed_last_week": row["Confirmed last week"]
                })
        
        print("Data loaded successfully to Neo4j!")
    
    driver.close()

if __name__ == "__main__":
    load_data_to_neo4j()