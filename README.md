# SQL Server to Snowflake table migrator

## Overview

Extracts all tables from a given tuple of DB.SCHEMA.TABLE combinations 
from SQL Server Management Studio and copies them into Snowflake

## Features

Preserves field length and field types (converted to Snowflake equivalents)

## Prerequisites

* Python 3.x
* Libraries
  * pyodbc
  * pandas
  * snowflake-connector-python[pandas]

## Setup

1. Clone the repository:

```bash
git clone https://github.com/JonathanLinney/Table-migrator--Sandpit-to-Snowflake-.git
```

2. Create a virtual environment and install dependencies:

```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```
3. Replace the SQL Server Connection details in the script with your own (if different):

```bash
Change this:

sql_server_connection_string = (
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=PSFADHSSTP01.AD.ELC.NHS.UK,1460;"

To this:

sql_server_connection_string = (
    "Driver={your driver name here};"
    "Server=your server name here;"

Trusted Connection is set to "yes" for automatic connection to your SQL Server (change if connecting manually)
    "Trusted_Connection=yes;"
```

4. Replace the Snowflake Connection details in the script with your own required destination details:

```bash
Change this:

# Snowflake Connection Details
snowflake_account_identifier = "ATKJNCU-NCL"
snowflake_database = "DATA_LAB_NCL_TRAINING_TEMP"
snowflake_schema = "NCL_DICTIONARY"
snowflake_warehouse = "NCL_ANALYTICS_XS"
snowflake_user = "<your email goes here>"

To this:

# Snowflake Connection Details
snowflake_account_identifier = "YOUR SNOWFLAKE ACCOUNT IDENTIFIER"
snowflake_database = "YOUR DATABASE"
snowflake_schema = "YOUR SCHEMA"
snowflake_warehouse = "YOUR WAREHOUSE"
snowflake_user = "<your email goes here>"

   
## Usage

Run the script:

```bash
python table_migrator.py
```

The script will:

Pull tables (including all their data) from SQL Server to Snowflake, preserving column formats 

## Author

Jonathan Linney | NHS NCL ICB
