from snowflake.connector.pandas_tools import write_pandas
import snowflake.connector
import pandas

conn = snowflake.connector.connect(
    account='baxkfzd-sw19303',
    user='mani6301',
    password='Mani6301',
    database='PANDAS',
    schema='PUBLIC',
    warehouse='COMPUTE_WH',
    role='ACCOUNTADMIN')

# Create a cursor object
cur = conn.cursor()

# Debug: Print the current database and schema
cur.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")
print("Current Database and Schema: ", cur.fetchall())

# Ensure the correct database and schema are being used
cur.execute("USE DATABASE DF_CITY")
cur.execute("USE SCHEMA PUBLIC")

# Debug: Verify the table name and check its existence
table_name = 'CITY'
cur.execute(f"SHOW TABLES LIKE '{table_name}'")
tables = cur.fetchall()
print(f"Table {table_name} does not exist. Creating the table.")
cur.execute(f"""
        CREATE TABLE {table_name} (
            city STRING,
            state_name STRING,
            county_name STRING,
            population INT,
            zipcounts INT,
            presc_counts INT,
        )
""")
 
    # Debug: Print the structure of the table
cur.execute(f"DESCRIBE TABLE {table_name}")
print("Table Structure: ", cur.fetchall())

    # Write the Pandas DataFrame to Snowflake
write_pandas(conn, data_report_1_pd, table_name=table_name)

 # Close the cursor and connection
cur.close()
conn.close()