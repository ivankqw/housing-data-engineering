from airflow import DAG
from airflow.providers.papermill.operators.papermill import PapermillOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id="housing_pipeline",
    default_args={"owner": "airflow"},
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["is3107"],
) as dag:
    hello_task = BashOperator(
        task_id="hello_task",
        bash_command="python /opt/airflow/extract/Resale_Flats.py",
    )
