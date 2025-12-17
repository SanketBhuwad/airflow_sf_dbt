from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import sys

sys.path.append("/opt/airflow/python_script")

from dataset_scanner_uc3 import scan_datasets_uc3
from convert_csv_to_parquet_uc3 import convert_csv_to_parquet_uc3
from load_parquet_to_snowflake_uc3 import load_parquet_usecase3
from decide_skip_uc3 import should_load_file   # can be dropped if not needed now


with DAG(
    dag_id="usecase03_delete_or_append_dag",
    start_date=datetime(2025, 10, 7),
    schedule_interval=None,
    catchup=False,
    tags=["csv", "parquet", "snowflake", "vault", "usecase3"],
) as dag:

    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    DATASETS = scan_datasets_uc3()

    with TaskGroup(group_id="convert_group") as convert_group:
        pass

    with TaskGroup(group_id="load_group") as load_group:
        pass

    for ds in DATASETS:
        table_name = ds["sf_table"].split(".")[-1].lower()
        file_base = ds["file_base"].lower()

        convert_task = PythonOperator(
            task_id=f"convert_{table_name}",
            python_callable=convert_csv_to_parquet_uc3,
            op_kwargs={
                "csv_file": ds["csv_file"],
                "parquet_file": ds["parquet_file"],
                "dag_id": dag.dag_id,
            },
            task_group=convert_group,
        )

        load_task = PythonOperator(
            task_id=f"load_{table_name}",
            python_callable=load_parquet_usecase3,
            op_kwargs={
                "parquet_file": ds["parquet_file"],
                "sf_table": ds["sf_table"],
            },
            task_group=load_group,
        )

        start >> convert_task >> load_task >> end