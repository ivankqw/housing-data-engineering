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
    notebook_task = PapermillOperator(
        task_id="notebook_task",
        input_nb="/opt/airflow/extract/Resale_Flats.ipynb",
        output_nb="/opt/airflow/extract/out-{{ execution_date }}.ipynb",
        parameters={"msgs": "Ran from Airflow at {{ execution_date }}!"},
    )

    hello_task = BashOperator(
        task_id="hello_task",
        bash_command="python /opt/airflow/extract/Resale_Flats.py",
    )
