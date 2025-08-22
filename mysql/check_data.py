import mysql.connector

db = mysql.connector.connect(
    host="127.0.0.1",
    port=23306,
    user="root",
    password="secret",
    database="covid_dw"
)

cursor = db.cursor(dictionary=True)

cursor.execute("""
    SELECT c.Country_Name, f.Confirmed, f.Deaths, f.Recovered
    FROM COVID_Facts f
    JOIN Country c ON f.CountryID = c.CountryID
    ORDER BY f.FactID DESC
    LIMIT 5
""")

results = cursor.fetchall()
for row in results:
    print(f"{row['Country_Name']}: {row['Confirmed']} cases, {row['Deaths']} deaths, {row['Recovered']} recovered")

cursor.close()
db.close()