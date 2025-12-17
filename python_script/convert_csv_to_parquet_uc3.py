import pandas as pd
from datetime import datetime


def detect_date_columns(df: pd.DataFrame) -> list:
    """
    Detect columns whose names suggest they are dates.
    Example: ORDERDATE, SHIP_DATE, order_date, etc.
    """
    date_cols = []
    for col in df.columns:
        col_upper = col.upper()
        if "DATE" in col_upper:   # simple heuristic; extend if needed
            date_cols.append(col)
    return date_cols


def normalize_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert detected date-like columns from DD-MM-YYYY strings to date objects.
    """
    date_cols = detect_date_columns(df)
    for col in date_cols:
        df[col] = pd.to_datetime(
            df[col],
            format="%d-%m-%Y",   # matches values like 15-12-1995
            errors="coerce",
        ).dt.date
    return df


def convert_csv_to_parquet_uc3(csv_file: str, parquet_file: str, dag_id: str, **context):
    df = pd.read_csv(csv_file)

    # uppercase column names
    df.columns = [c.upper() for c in df.columns]

    # normalize dates based on column name pattern (after uppercasing)
    df = normalize_dates(df)

    # add metadata columns
    dag_id = context['dag'].dag_id
    load_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df['DAG_ID'] = dag_id
    df['LOAD_TIME'] = load_time

    df.to_parquet(parquet_file, index=False)
