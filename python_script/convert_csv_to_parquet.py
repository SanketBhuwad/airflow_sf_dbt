import pandas as pd
from datetime import datetime
import re

def normalize_dates(df: pd.DataFrame) -> pd.DataFrame:
    # Pick columns whose names look like dates
    date_cols = [
        col for col in df.columns
        if re.search(r"date$", col, re.IGNORECASE) or "date" in col.lower()
    ]

    for col in date_cols:
        df[col] = pd.to_datetime(
            df[col],
            format="%d-%m-%Y",
            errors="coerce",
        ).dt.date

    return df


def convert_csv_to_parquet(csv_file: str, parquet_file: str, dag_id: str, **context):
    df = pd.read_csv(csv_file)

    # Uppercase all column names
    df.columns = [c.upper() for c in df.columns]

    # Add metadata columns
    dag_id = context['dag'].dag_id
    load_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df['DAG_ID'] = dag_id
    df['LOAD_TIME'] = load_time

    # Write to Parquet
    df.to_parquet(parquet_file, index=False)
