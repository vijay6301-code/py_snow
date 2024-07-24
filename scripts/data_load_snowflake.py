import get_env_variables as gav
from snowflake.connector.pandas_tools import write_pandas
import snowflake.connector
import pandas as pd
import os




def data_load_snowflake(df, table_name):
    conn = snowflake.connector.connect(
        account=gav.account,
        user=gav.user,
        password=gav.password,
        database=gav.database,
        schema=gav.schema,
        warehouse=gav.warehouse,
        role=gav.role
    )

    # Convert the Spark DataFrame to Pandas DataFrame
    df = df.toPandas()

    # Update the columns to uppercase
    df.columns = [col.upper() for col in df.columns]

    # Create a cursor object
    cur = conn.cursor()

    # Debug: Print the current database and schema
    cur.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")
    print("Current Database and Schema: ", cur.fetchall())

    # Ensure the correct database and schema are being used
    cur.execute(f"USE DATABASE {gav.database.upper()}")
    cur.execute(f"USE SCHEMA {gav.schema.upper()}")

    # Debug: Verify the table name and check its existence
    cur.execute(f"SHOW TABLES LIKE '{table_name.upper()}'")
    tables = cur.fetchall()
    if not tables:
        print(f"Table {table_name} does not exist. Creating the table.")
        # Create table dynamically based on DataFrame columns
        column_defs = ", ".join([f"{col} STRING" if dtype == 'object' else f"{col} FLOAT" if dtype == 'float64' else f"{col} INT" for col, dtype in zip(df.columns, df.dtypes)])
        create_table_query = f"CREATE TABLE {table_name.upper()} ({column_defs})"
        cur.execute(create_table_query)
    else:
        print(f"Table {table_name} exists.")

    # Debug: Print the structure of the table
    cur.execute(f"DESCRIBE TABLE {table_name.upper()}")
    print("Table Structure: ", cur.fetchall())

    # Write the Pandas DataFrame to Snowflake
    success, nchunks, nrows, output = write_pandas(conn, df, table_name=table_name.upper())
    print(f"Success: {success}")
    print(f"Number of chunks: {nchunks}")
    print(f"Number of rows successfully loaded into Snowflake: {nrows}")

    # Close the cursor and connection
    cur.close()
    conn.close()
    
   # for file_path in file_paths:
#    if os.path.isfile(file_path):
 #           os.remove(file_path)
 #           print(f"Source file {file_path} deleted.")       else:
  #          print(f"Source file {file_path} not found.")
            
    return success, nchunks, nrows, output