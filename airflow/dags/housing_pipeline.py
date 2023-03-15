from airflow import DAG
from airflow.providers.papermill.operators.papermill import PapermillOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id="housing_pipeline",
    default_args={"owner": "airflow"},
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["is3107"],
) as dag:
    # hello_task = BashOperator(
    #     task_id="hello_task",
    #     bash_command="python /opt/airflow/extract/Resale_Flats.py",
    # )

    TASK_DEFS = {
        "test": {"path": "sql/test.sql"},
    }

    # create_table = BashOperator(
    #     task_id="create_table",
    #     bash_command="python /opt/airflow/load/create_table.py",
    # )

    create_tables_if_not_exists = {
        k: PostgresOperator(
            task_id=f"create_if_not_exists_{k}_table",
            postgres_conn_id="postgres_localhost",
            sql=v["path"],
        )
        for k, v in TASK_DEFS.items()
    }
