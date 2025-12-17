import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from get_snowflake_creds import get_snowflake_credentials_from_vault
from decide_action import decide_action
from convert_csv_to_parquet import normalize_dates


def load_parquet_usecase3(parquet_file: str, sf_table: str, **context):
    # e.g. customer_2.parquet -> customer_2.csv
    csv_name = parquet_file.split("/")[-1].replace(".parquet", ".csv")
    action = decide_action(csv_name)
    print(f"[UC3] Loading {csv_name} into {sf_table} with action={action}")

    df = pd.read_parquet(parquet_file)
    df = normalize_dates(df)

    creds = get_snowflake_credentials_from_vault()

    conn = snowflake.connector.connect(
        user=creds["user"],
        password=creds["password"],
        account=creds["account"],
        role=creds["role"],
        warehouse=creds["warehouse"],
        database=creds["database"],
        schema=creds["schema"],
    )
    cur = conn.cursor()
    try:
        cur.execute(f"USE DATABASE {creds['database']}")
        cur.execute(f"USE SCHEMA {creds['schema']}")

        if action == "truncate":
            print(f"[UC3] Truncating table {sf_table}")
            cur.execute(f"TRUNCATE TABLE {sf_table}")

        write_pandas(
            conn=conn,
            df=df,
            table_name=sf_table.split(".")[-1],
            database=creds["database"],
            schema=creds["schema"],
        )
        print(f"[UC3] Loaded {len(df)} rows into {sf_table}")
    finally:
        cur.close()
        conn.close()