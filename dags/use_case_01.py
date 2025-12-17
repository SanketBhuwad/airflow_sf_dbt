from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime
import sys

# Add python_script folder to path
sys.path.append("/opt/airflow/python_script")

from dataset_builder import build_datasets
from convert_csv_to_parquet import convert_csv_to_parquet
from load_parquet_to_snowflake import load_parquet_to_snowflake

with DAG(
    dag_id="dynamic_csv_to_parquet_snowflake_dag",
    start_date=datetime(2025, 10, 7),
    schedule_interval=None,
    catchup=False,
    tags=["csv", "parquet", "snowflake", "vault"],
) as dag:

    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    DATASETS = build_datasets()

    for dataset in DATASETS:
        dataset_name = dataset["sf_table"].split(".")[-1].lower()

        convert_task = PythonOperator(
            task_id=f"convert_{dataset_name}",
            python_callable=convert_csv_to_parquet,
            op_kwargs={
                "csv_file": dataset["csv_file"],
                "parquet_file": dataset["parquet_file"],
                "dag_id": dag.dag_id,
            },
        )

        load_task = PythonOperator(
            task_id=f"load_{dataset_name}_to_snowflake",
            python_callable=load_parquet_to_snowflake,
            op_kwargs={
                "parquet_file": dataset["parquet_file"],
                "sf_table": dataset["sf_table"],
            },
        )

        start >> convert_task >> load_task >> end