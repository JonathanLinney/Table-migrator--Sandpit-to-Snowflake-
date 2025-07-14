import pyodbc
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# --- SQL Server Connection Details ---
sql_server_connection_string = (
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=PSFADHSSTP01.AD.ELC.NHS.UK,1460;"
    "Database=Data_Lab_NCL_Dev;" #necessary for connection (actual db/schema/table is specified below)
    "Trusted_Connection=yes;"
)

# List of tables to migrate (SQL Server -> Snowflake)
tables = [
    ("[Data_Lab_NCL].[dbo].[wf_pwr_kpi]", "WF_PWR_KPI_JL_TEST"),
    ("[Data_Lab_NCL].[dbo].[wf_pwr_wte]", "WF_PWR_WTE_JL_TEST")
]

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
    else:  # Default to VARCHAR for strings and objects
        return "VARCHAR(16777216)"  # Max length for VARCHAR

# Generate CREATE TABLE SQL
def generate_create_table_sql(dataframe, db_name, schema_name, table_name):
    columns_sql = []
    for col_name, col_dtype in dataframe.dtypes.items():
        snowflake_type = map_pandas_to_snowflake_type(col_dtype)
        columns_sql.append(f'"{col_name}" {snowflake_type}')
    create_sql = f'CREATE OR REPLACE TABLE "{db_name}"."{schema_name}"."{table_name}" (\n'
    create_sql += ",\n".join(columns_sql)
    create_sql += '\n);'
    return create_sql

# Snowflake Connection Details
snowflake_account_identifier = "ATKJNCU-NCL"
snowflake_database = "DATA_LAB_NCL_TRAINING_TEMP"
snowflake_schema = "DATA_ENGINEER"
snowflake_warehouse = "NCL_ANALYTICS_XS"
snowflake_user = "jonathan.linney@nhs.net"

# --- Establish Snowflake Connection Once ---
conn = None

try:
    print("\nAttempting to connect to Snowflake...")
    conn = snowflake.connector.connect(
        account=snowflake_account_identifier,
        user=snowflake_user,
        database=snowflake_database,
        schema=snowflake_schema,
        warehouse=snowflake_warehouse,
        authenticator='externalbrowser'  # This triggers SSO
    )
    print("Successfully connected to Snowflake.")

    # Loop through tables to process each one
    for sql_table_name, snowflake_table_name in tables:
        df = None  # Initialize df for each iteration

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
        if df is not None and not df.empty:
            try:
                cursor = conn.cursor()

                # Generate and execute CREATE TABLE statement
                create_table_sql = generate_create_table_sql(
                    df,
                    snowflake_database,
                    snowflake_schema,
                    snowflake_table_name
                )
                print(f"\nGenerated CREATE TABLE SQL for {snowflake_table_name}:\n{create_table_sql}")

                print(f"Attempting to create/replace table {snowflake_table_name}...")
                cursor.execute(create_table_sql)
                print(f"Table {snowflake_table_name} created/replaced successfully.")

                # Load data using write_pandas
                print(f"Attempting to load data into {snowflake_table_name}...")
                success = write_pandas(
                    conn,
                    df,
                    snowflake_table_name,
                    database=snowflake_database,
                    schema=snowflake_schema,
                    chunk_size=50000,
                    overwrite=True
                )

                # Get the number of rows directly from the DataFrame
                nrows_loaded = df.shape[0]

                print(f"Data loading complete for {snowflake_table_name}. Success: {success}, Rows loaded: {nrows_loaded}")

                if success:
                    print(f"✅ Successfully loaded {nrows_loaded} rows into {snowflake_database}.{snowflake_schema}.{snowflake_table_name}")
                else:
                    print(f"❌ Data loading failed for {snowflake_table_name}.")

            except snowflake.connector.errors.ProgrammingError as e:
                print(f"Snowflake Programming Error: {e.errno} - {e.msg}")
            except Exception as e:
                print(f"An unexpected error occurred for {snowflake_table_name}: {e}")
            finally:
                print(f"Finished processing table {snowflake_table_name}.")
        else:
            print(f"\nSkipping Snowflake load for {sql_table_name} because no data was pulled or DataFrame is empty.")

except Exception as e:
    print(f"Failed to connect to Snowflake: {e}")

finally:
    if conn is not None:
        conn.close()
        print("Snowflake connection closed.")
