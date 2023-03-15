from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2022, 1, 1),
    "retries": 1,
}

with DAG("mysql_dag", default_args=default_args, schedule_interval=None) as dag:

    create_table = MySqlOperator(
        task_id="create_table",
        mysql_conn_id="10",
        sql="""
            CREATE TABLE IF NOT EXISTS my_table (
                id INT NOT NULL,
                name VARCHAR(255),
                PRIMARY KEY (id)
            );
        """,
    )

    insert_data = MySqlOperator(
        task_id="insert_data",
        mysql_conn_id="10",
        sql="""
            INSERT INTO my_table (id, name) VALUES
            (1, 'Alice'),
            (2, 'Bob'),
            (3, 'Charlie');
        """,
    )

    create_table >> insert_data
