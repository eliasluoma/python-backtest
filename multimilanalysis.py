import sqlite3
import pandas as pd

# Yhdistä SQLite-tietokantaan
db_path = 'cache/pools.db'  # Korjattu oikeaksi poluksi
conn = sqlite3.connect(db_path)

# SQL-kysely
query = """
SELECT
    p5.poolAddress,
    p5.marketCap AS marketCap_5s,
    p1140.athMarketCap AS athMarketCap_1140s,
    (CAST(p1140.athMarketCap AS REAL) * 1.0 / CAST(p5.marketCap AS REAL)) AS multiplier
FROM
    market_data AS p5
JOIN
    market_data AS p1140
ON
    p5.poolAddress = p1140.poolAddress
    AND p1140.timeFromStart = 1140
WHERE
    p5.timeFromStart = 5
    AND CAST(p5.marketCap AS REAL) > 30000
ORDER BY
    multiplier DESC;
"""

# Suorita kysely ja lataa tulokset Pandas DataFrameen
df = pd.read_sql_query(query, conn)

# Sulje tietokantayhteys
conn.close()

# Tarkista, että dataa löytyi
if df.empty:
    print("Yhtään poolia ei löytynyt annetuilla ehdoilla.")
else:
    # Tulosta tulokset terminaaliin
    print("\nAnalyysin tulokset:")
    print(df.to_string(index=False))

    # Tallenna tulokset CSV-tiedostoon
    csv_filename = 'pool_analysis_results.csv'
    df.to_csv(csv_filename, index=False)
    print(f"\nTulokset tallennettu tiedostoon: {csv_filename}")