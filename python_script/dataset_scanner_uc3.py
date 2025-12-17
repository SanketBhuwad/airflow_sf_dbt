import os
from typing import List, Dict

BASE_DIR = "/opt/airflow/data"
LANDING_DIR = os.path.join(BASE_DIR, "landing")
FORMATTED_DIR = os.path.join(BASE_DIR, "formatted")
SNOWFLAKE_DB_SCHEMA_PREFIX = "PRACTICE_DB.RAW"

def scan_datasets_uc3(**context) -> List[Dict]:
    """
    For each dataset folder, pick ONLY the latest CSV file.
    Old files are ignored and never appear as tasks.
    """
    datasets: List[Dict] = []

    for folder_name in ["customer", "lineitem", "nation", "orders", "region"]:
        landing_subdir = os.path.join(LANDING_DIR, folder_name)
        formatted_subdir = os.path.join(FORMATTED_DIR, folder_name)
        os.makedirs(formatted_subdir, exist_ok=True)

        if not os.path.exists(landing_subdir):
            continue

        csv_files = sorted(
            f for f in os.listdir(landing_subdir)
            if f.lower().endswith(".csv")
        )
        if not csv_files:
            continue

        latest = csv_files[-1]          # e.g. customer_2.csv
        csv_file = os.path.join(landing_subdir, latest)
        base = os.path.splitext(latest)[0]
        parquet_file = os.path.join(formatted_subdir, f"{base}.parquet")
        sf_table = f"{SNOWFLAKE_DB_SCHEMA_PREFIX}.{folder_name.upper()}"

        datasets.append(
            {
                "csv_file": csv_file,
                "parquet_file": parquet_file,
                "sf_table": sf_table,
                "file_base": base,
            }
        )

    return datasets