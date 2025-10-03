import os
import logging
import pandas as pd
import pyodbc
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
from datetime import datetime

# ===========================
# Load environment variables
# ===========================
load_dotenv(r'C:\Coding\python\Python Snowflake Projects\venv\Scripts\Secrets.env')

# ===========================
# Logging configuration
# ===========================

# Create logs folder if it doesn't exist
log_folder = r"C:\Coding\python\Python Snowflake Projects\Logs"
os.makedirs(log_folder, exist_ok=True)

# Generate log file name with current date and time
log_filename = f"migration_sqlserver_to_snowflake_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
log_path = os.path.join(log_folder, log_filename)

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_path, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

logging.info(f"Logging started. Log file: {log_path}")

# ===========================
# SQL Server Config
# ===========================
SQL_SERVER = os.getenv("SQL_SERVER")
SQL_DATABASE = os.getenv("SQL_DATABASE")
SQL_USERNAME = os.getenv("SQL_USERNAME")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")
SQL_DRIVER = os.getenv("SQL_DRIVER")  # Must match exactly installed driver

# ===========================
# Snowflake Config
# ===========================
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

# ===========================
# Tables & Primary Keys
# ===========================
TABLES_TO_MIGRATE = {
    "CUSTOMERS": "CUSTOMERID",
    "EMPLOYEES": "EMPLOYEEID",
    "INVOICES": "INVOICEID",
    "MAINTENANCE": "MAINTENANCEID",
    "PAYMENTS": "PAYMENTID",
    "RENTAL_AGREEMENTS": "AGREEMENTID",
    "RENTAL_ITEMS": "ITEMID"
}

# ===========================
# Migration function with MERGE logic
# ===========================
def migrate_table_merge(table_name: str, primary_key: str):
    """Migrate a table from SQL Server to Snowflake using MERGE logic (upsert)"""
    table_name = table_name.upper()
    df = None

    # ---------------------------
    # Fetch data from SQL Server
    # ---------------------------
    try:
        logging.info(f"Connecting to SQL Server: {table_name}")
        conn_str = (
            f"DRIVER={{{SQL_DRIVER}}};"
            f"SERVER={SQL_SERVER};"
            f"DATABASE={SQL_DATABASE};"
            f"UID={SQL_USERNAME};"
            f"PWD={SQL_PASSWORD};"
            f"TrustServerCertificate=yes;"
        )
        sql_conn = pyodbc.connect(conn_str)

        query = f"SELECT * FROM {table_name}"
        logging.info(f"Fetching data from SQL Server table: {table_name}")
        df = pd.read_sql(query, sql_conn)
        logging.info(f"Fetched {len(df)} rows from {table_name}")
        
        df.columns = [col.upper() for col in df.columns]  # Uppercase all columns
        primary_key = primary_key.upper()  # Uppercase primary key

    except Exception as e:
        logging.error(f"Error fetching data from SQL Server for table {table_name}: {e}")
        return
    finally:
        try:
            sql_conn.close()
        except:
            pass

    if df is None or df.empty:
        logging.warning(f"No data found for table {table_name}, skipping migration.")
        return

    # ---------------------------
    # Connect to Snowflake
    # ---------------------------
    try:
        logging.info(f"Connecting to Snowflake for table {table_name}")
        sf_conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            role=SNOWFLAKE_ROLE,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )

        # ---------------------------
        # Create temporary staging table
        # ---------------------------
        stage_table = f"{table_name}_STAGE"
        logging.info(f"Creating temporary staging table {stage_table}")
        cols = ", ".join([f"{col} STRING" for col in df.columns])  # Treat all columns as STRING
        sf_conn.cursor().execute(f"CREATE TEMPORARY TABLE {stage_table} ({cols})")

        # ---------------------------
        # Insert data into staging table
        # ---------------------------
        logging.info(f"Inserting data into staging table {stage_table}")
        success, nchunks, nrows, _ = write_pandas(sf_conn, df, table_name=stage_table)
        if success:
            logging.info(f"Inserted {nrows} rows into staging table {stage_table}")
        else:
            logging.error(f"Failed to insert into staging table {stage_table}")
            return

        # ---------------------------
        # MERGE into target table
        # ---------------------------
        logging.info(f"Merging data into target table {table_name}")
        update_columns = ", ".join([f'TARGET.{col} = SOURCE.{col}' for col in df.columns if col != primary_key])
        merge_sql = f"""
        MERGE INTO {table_name} AS TARGET
        USING {stage_table} AS SOURCE
        ON TARGET.{primary_key} = SOURCE.{primary_key}
        WHEN MATCHED THEN
            UPDATE SET {update_columns}
        WHEN NOT MATCHED THEN
            INSERT ({', '.join(df.columns)}) 
            VALUES ({', '.join(['SOURCE.' + col for col in df.columns])})
        """
        sf_conn.cursor().execute(merge_sql)
        logging.info(f"Merge completed for table {table_name}")

    except Exception as e:
        logging.error(f"Error merging data into Snowflake for table {table_name}: {e}")
    finally:
        try:
            sf_conn.close()
        except:
            pass

# ===========================
# Run migration for all tables
# ===========================
if __name__ == "__main__":
    for table, pk in TABLES_TO_MIGRATE.items():
        migrate_table_merge(table, pk)

    logging.info("Migration completed for all tables!")