import psycopg2

# Connect to the PostgreSQL database
conn = psycopg2.connect(
    host="localhost",
    database="stock_data",
    user="postgres",
    password=""
)



# Set the schema name
schema_name = "raw_stocks"

# Create a cursor object
cur = conn.cursor()

# Create a new table in the desired schema
cur.execute(f"""CREATE TABLE {schema_name}.AAPL (
        time TIMESTAMP,
        open NUMERIC,
        high NUMERIC,
        low NUMERIC,
        close NUMERIC,
        volume NUMERIC
    )""")

# Commit the changes
conn.commit()

# Close the cursor and connection
cur.close()
conn.close()




