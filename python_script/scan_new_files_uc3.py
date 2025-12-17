import os
from typing import List, Dict

BASE_DIR = "/opt/airflow/data"
LANDING_DIR = os.path.join(BASE_DIR, "landing")
FORMATTED_DIR = os.path.join(BASE_DIR, "formatted")
SNOWFLAKE_DB_SCHEMA_PREFIX = "PRACTICE_DB.RAW"

def scan_new_files_uc3(**context) -> List[Dict]:
    """
    Decide which files to load in this run.
    Rule:
      - For each folder:
        * If multiple files, pick only the latest (by filename sort).
      - Return a list of dicts with csv/parquet/sf_table/file_base.
    """
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

        # only latest file per folder for this run
        latest = csv_files[-1]
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

    # return value is automatically pushed to XCom by PythonOperator
    return datasets