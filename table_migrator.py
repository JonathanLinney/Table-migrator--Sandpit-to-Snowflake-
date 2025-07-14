import pyodbc
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# --- SQL Server Connection Details ---
sql_server_connection_string = (
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=PSFADHSSTP01.AD.ELC.NHS.UK,1460;"
    "Database=Data_Lab_NCL_Dev;" #necessary for a connection (then actual db/schema/table is specified in the query)
    "Trusted_Connection=yes;"
)
sql_table_name = "[Data_Lab_NCL].[dbo].[wf_pwr_kpi]" # Table to pull data from

df = None # Initialize df outside the try block

try:
    # Establish connection to SQL Server
    cnxn = pyodbc.connect(sql_server_connection_string)
    cursor = cnxn.cursor()

    # Execute query to get data
    print(f"Attempting to select data from {sql_table_name}...")
    cursor.execute(f"SELECT * FROM {sql_table_name}")

    # Fetch all data
    rows = cursor.fetchall()

    # Get column names
    columns = [column[0] for column in cursor.description]

    print(f"Successfully pulled {len(rows)} rows from {sql_table_name}.")
    print("Columns found:", columns)

    # Convert to a Pandas DataFrame
    df = pd.DataFrame.from_records(rows, columns=columns)

    print("\nFirst 5 rows of the DataFrame:")
    print(df.head())

    print("\nData types (inferred by Pandas):")
    print(df.dtypes)

except pyodbc.Error as ex:
    sqlstate = ex.args[0]
    print(f"Error connecting to SQL Server or executing query: {sqlstate}")
    print(ex)
finally:
    if 'cnxn' in locals() and cnxn:
        cnxn.close()
        print("SQL Server connection closed.")

# --- Snowflake Data Loading ---

# Only proceed if the DataFrame was successfully created
if df is not None and not df.empty:
    # Snowflake Connection Details
    snowflake_account_identifier = "ATKJNCU-NCL"
    snowflake_database = "DATA_LAB_NCL_TRAINING_TEMP"
    snowflake_schema = "DATA_ENGINEER"
    snowflake_warehouse = "NCL_ANALYTICS_XS"
    snowflake_user = "jonathan.linney@nhs.net"
    snowflake_table_name = "WF_PWR_KPI_JL_TEST" # Name for new table in Snowflake

    # Function to map Pandas dtypes to Snowflake types
    def map_pandas_to_snowflake_type(pandas_dtype):
        if pd.api.types.is_integer_dtype(pandas_dtype):
            return "NUMBER"
        elif pd.api.types.is_float_dtype(pandas_dtype):
            return "FLOAT"
        elif pd.api.types.is_datetime64_any_dtype(pandas_dtype):
            return "TIMESTAMP_NTZ"
        elif pd.api.types.is_bool_dtype(pandas_dtype):
            return "BOOLEAN"
        else: # Default to VARCHAR for strings and objects
            return "VARCHAR(16777216)" # Max length for VARCHAR

    # Generate CREATE TABLE statement
    def generate_create_table_sql(dataframe, db_name, schema_name, table_name):
        columns_sql = []
        for col_name, col_dtype in dataframe.dtypes.items():
            snowflake_type = map_pandas_to_snowflake_type(col_dtype)
            # Enclose column names in quotes for case sensitivity and special characters
            columns_sql.append(f'"{col_name}" {snowflake_type}')

        create_sql = f'CREATE OR REPLACE TABLE "{db_name}"."{schema_name}"."{table_name}" (\n'
        create_sql += ",\n".join(columns_sql)
        create_sql += '\n);'
        return create_sql

    conn = None # Initialize conn outside the try block
    try:
        # Establish Snowflake connection (SSO will prompt if necessary)
        print("\nAttempting to connect to Snowflake...")
        conn = snowflake.connector.connect(
            account=snowflake_account_identifier,
            user=snowflake_user,
            database=snowflake_database,
            schema=snowflake_schema,
            warehouse=snowflake_warehouse,
            authenticator='externalbrowser' # This triggers SSO
        )
        print("Successfully connected to Snowflake.")

        cursor = conn.cursor()

        # Generate and execute CREATE TABLE statement
        create_table_sql = generate_create_table_sql(df, snowflake_database, snowflake_schema, snowflake_table_name)
        print(f"\nGenerated CREATE TABLE SQL:\n{create_table_sql}")

        print(f"Attempting to create/replace table {snowflake_table_name}...")
        cursor.execute(create_table_sql)
        print(f"Table {snowflake_table_name} created/replaced successfully.")

        # Load data using write_pandas
        print(f"Attempting to load data into {snowflake_table_name}...")
        # Newer versions of write_pandas return a single boolean for success
        success = write_pandas(conn, df, snowflake_table_name,
                                database=snowflake_database,
                                schema=snowflake_schema,
                                chunk_size=50000,
                                overwrite=True)

        # Get the number of rows directly from the DataFrame
        nrows_loaded = df.shape[0]

        print(f"Data loading complete. Success: {success}, Rows loaded (from DataFrame): {nrows_loaded}")

        if success:
            print(f"Successfully loaded {nrows_loaded} rows into {snowflake_database}.{snowflake_schema}.{snowflake_table_name}")
        else:
            print("Data loading failed.")

    except snowflake.connector.errors.ProgrammingError as e:
        # Specific error handling for Snowflake
        print(f"Snowflake Programming Error: {e.errno} - {e.msg}")
    except Exception as e:
        # General error handling
        print(f"An unexpected error occurred: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            print("Snowflake connection closed.")
else:
    print("\nSkipping Snowflake load as no data was pulled from SQL Server or DataFrame is empty.")