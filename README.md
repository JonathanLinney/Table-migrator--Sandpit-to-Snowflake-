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

## Usage

Run the script:

```bash
python table_migrator.py
```

The script will:

Pull tables (including all their data) from SQL Server to Snowflake, preserving column formats 

## Author

Jonathan Linney | NHS NCL ICB
