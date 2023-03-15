from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2022, 1, 1),
    "retries": 1,
}

with DAG("mysql_test_dag", default_args=default_args, schedule_interval=None) as dag:

    test_connection = MySqlOperator(
        task_id="test_connection", mysql_conn_id="1", sql="SELECT 1;"
    )

    # print ok if connection is successful
    print_ok = BashOperator(task_id="print_ok", bash_command='echo "ok"')

    test_connection >> print_ok
