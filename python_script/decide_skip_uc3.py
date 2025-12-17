from airflow.exceptions import AirflowSkipException

def should_load_file(file_key: str, **context) -> str:
    """
    Ensure each file_key is only loaded once per DAG run.
    Stores state in this task's XCom.
    """
    ti = context["ti"]

    # Read list from previous try of THIS task (or empty)
    processed = ti.xcom_pull(key="processed_files") or []

    if file_key in processed:
        raise AirflowSkipException(f"{file_key} already loaded in this run")

    processed.append(file_key)
    ti.xcom_push(key="processed_files", value=processed)

    return file_key
