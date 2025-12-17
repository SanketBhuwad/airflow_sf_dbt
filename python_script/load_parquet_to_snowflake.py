import snowflake.connector
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from get_snowflake_creds import get_snowflake_credentials_from_vault


def load_parquet_to_snowflake(parquet_file: str, sf_table: str, **context):
    df = pd.read_parquet(parquet_file)

    snowflake_creds = get_snowflake_credentials_from_vault()

    conn = snowflake.connector.connect(
        user=snowflake_creds["user"],
        password=snowflake_creds["password"],
        account=snowflake_creds["account"],
        role=snowflake_creds["role"],
        warehouse=snowflake_creds["warehouse"],
        database=snowflake_creds["database"],   # important
        schema=snowflake_creds["schema"],       # optional but recommended
    )
    cursor = conn.cursor()
    try:
        # Ensure current DB & schema for temp stage used by write_pandas
        cursor.execute(f"USE DATABASE {snowflake_creds['database']}")
        cursor.execute(f"USE SCHEMA {snowflake_creds['schema']}")

        # Optional: truncate table first
        cursor.execute(f"DELETE FROM {sf_table}")

        write_pandas(
            conn=conn,
            df=df,
            table_name=sf_table.split(".")[-1],
            database=snowflake_creds["database"],
            schema=snowflake_creds["schema"],
        )
    finally:
        cursor.close()
        conn.close()