import pyodbc
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# --- SQL Server Connection Details ---
sql_server_connection_string = (
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=PSFADHSSTP01.AD.ELC.NHS.UK,1460;"
#    "Database=Data_Lab_NCL_Dev;" #not required here, DB's explicitely declared below
    "Trusted_Connection=yes;"
)

# List of tables to migrate (SQL Server -> Snowflake)
tables = [
    ("[Your_DB_1].[Your_Schema_1].[Your_Table_Name_1]", "YOUR_DESTINATION_TABLE_1"),
    ("[Your_DB_1].[Your_Schema_2].[Your_Table_Name_2]", "YOUR_DESTINATION_TABLE_2"),
]


# Function to map SQL Server types to Snowflake types

def map_sqlserver_to_snowflake_type(sql_type, char_length=None):
    # For character types, use the defined length if available
    char_types = ['char', 'varchar', 'nchar', 'nvarchar']
    if sql_type.lower() in char_types and char_length:
        # If length is -1 or None, use max
        if str(char_length) == '-1' or char_length is None:
            return f"VARCHAR(16777216)"
        else:
            return f"VARCHAR({char_length})"
    type_map = {
        'int': 'NUMBER',
        'bigint': 'NUMBER',
        'smallint': 'NUMBER',
        'tinyint': 'NUMBER',
        'bit': 'BOOLEAN',
        'decimal': 'NUMBER',
        'numeric': 'NUMBER',
        'float': 'FLOAT',
        'real': 'FLOAT',
        'money': 'FLOAT',
        'smallmoney': 'FLOAT',
        'date': 'DATE',
        'datetime': 'TIMESTAMP_NTZ',
        'datetime2': 'TIMESTAMP_NTZ',
        'smalldatetime': 'TIMESTAMP_NTZ',
        'datetimeoffset': 'TIMESTAMP_TZ',
        'time': 'TIME',
        'text': 'VARCHAR(16777216)',
        'ntext': 'VARCHAR(16777216)',
        'binary': 'BINARY',
        'varbinary': 'BINARY',
        'image': 'BINARY',
    }
    return type_map.get(sql_type.lower(), 'VARCHAR(16777216)')

# Generate CREATE TABLE SQL using SQL Server types
def generate_create_table_sql_from_sqlserver(columns_info, db_name, schema_name, table_name):
    columns_sql = []
    for col_name, sql_type, is_nullable, char_length in columns_info:
        snowflake_type = map_sqlserver_to_snowflake_type(sql_type, char_length)
        null_str = "NULL" if is_nullable else "NOT NULL"
        columns_sql.append(f'"{col_name}" {snowflake_type} {null_str}')
    create_sql = f'CREATE OR REPLACE TABLE "{db_name}"."{schema_name}"."{table_name}" (\n'
    create_sql += ",\n".join(columns_sql)
    create_sql += '\n);'
    return create_sql

# Snowflake Connection Details
snowflake_account_identifier = "ATKJNCU-NCL"
snowflake_database = "DATA_LAB_NCL_TRAINING_TEMP"
snowflake_schema = "NCL_DICTIONARY"
snowflake_warehouse = "NCL_ANALYTICS_XS"
snowflake_user = "<your email goes here>"

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


            # Get column names and SQL Server types
            columns = [column[0] for column in cursor.description]
            # Get SQL Server type names using INFORMATION_SCHEMA
            import re
            m = re.match(r'\[(.*?)\]\.\[(.*?)\]\.\[(.*?)\]', sql_table_name)
            if m:
                db_name, schema_name, tbl_name = m.groups()
            else:
                parts = sql_table_name.replace('[','').replace(']','').split('.')
                db_name, schema_name, tbl_name = parts if len(parts)==3 else (None,None,None)

            columns_info = []
            if db_name and schema_name and tbl_name:
                type_query = f"""
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH
                FROM [{db_name}].INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{tbl_name}'
                """
                cursor2 = cnxn.cursor()
                cursor2.execute(type_query)
                type_rows = cursor2.fetchall()
                type_map = {row[0]: (row[1], row[2], row[3]) for row in type_rows}
                for col in columns:
                    sql_type, is_nullable, char_length = type_map.get(col, ('varchar', 'YES', None))
                    columns_info.append((col, sql_type, is_nullable == 'YES', char_length))
            else:
                for col in columns:
                    columns_info.append((col, 'varchar', True, None))

            print(f"Successfully pulled {len(rows)} rows from {sql_table_name}.")
            print("Columns found:", columns)
            print("SQL Server types:", columns_info)

            df = pd.DataFrame.from_records(rows, columns=columns)

            print("\nFirst 5 rows of the DataFrame:")
            print(df.head())

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


                # Generate and execute CREATE TABLE statement using SQL Server types
                create_table_sql = generate_create_table_sql_from_sqlserver(
                    columns_info,
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
