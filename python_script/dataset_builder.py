import os

BASE_DIR = "/opt/airflow/data"
LANDING_DIR = os.path.join(BASE_DIR, "landing")
FORMATTED_DIR = os.path.join(BASE_DIR, "formatted")
SNOWFLAKE_DB_SCHEMA_PREFIX = "PRACTICE_DB.RAW"

def build_datasets():
    datasets = []
    for folder_name in ["customer", "lineitem", "nation", "orders", "region"]:
        landing_subdir = os.path.join(LANDING_DIR, folder_name)
        formatted_subdir = os.path.join(FORMATTED_DIR, folder_name)
        os.makedirs(formatted_subdir, exist_ok=True)

        csv_files = sorted(
            [f for f in os.listdir(landing_subdir) if f.lower().endswith(".csv")]
        )
        if not csv_files:
            continue

        latest = csv_files[-1]   # e.g. customer_2.csv > customer.csv
        csv_file = os.path.join(landing_subdir, latest)
        base = os.path.splitext(latest)[0]
        parquet_file = os.path.join(formatted_subdir, f"{base}.parquet")
        table_name = f"{SNOWFLAKE_DB_SCHEMA_PREFIX}.{folder_name.upper()}"

        datasets.append(
            {
                "csv_file": csv_file,
                "parquet_file": parquet_file,
                "sf_table": table_name,
            }
        )
    return datasets

