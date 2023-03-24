from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import queries

default_args = {
    "owner": "airflow",
    "start_date": days_ago(2),
    "tags": ["is3107"]
}

with DAG(dag_id="insert", default_args=default_args, schedule_interval=None) as dag:

    # create a postgres operator to execute the create_tables_query
    create_tables = PostgresOperator(
        task_id="create_tables",
        postgres_conn_id="db_localhost",
        sql=queries.CREATE_TABLES,
    )

    insert_salesperson_information = PythonOperator(
        task_id="insert_salesperson_information",
        python_callable=lambda sql, filepath: PostgresHook(postgres_conn_id="db_localhost").copy_expert(
            sql=sql,
            filename=filepath
        ),
        op_kwargs={
            "sql": queries.INSERT_SALESPERSON_INFORMATION,
            "filepath": "/opt/airflow/dags/data/salesperson_info.csv"
        },
    )

    insert_salesperson_transactions = PythonOperator(
        task_id="insert_salesperson_transactions",
        python_callable=lambda sql, filepath: PostgresHook(postgres_conn_id="db_localhost").copy_expert(
            sql=sql,
            filename=filepath
        ),
        op_kwargs={
            "sql": queries.INSERT_SALESPERSON_TRANSACTIONS,
            "filepath": "/opt/airflow/dags/data/salesperson_transactions.csv"
        },
    )

    insert_districts = PythonOperator(
        task_id="insert_districts",
        python_callable=lambda sql, filepath: PostgresHook(postgres_conn_id="db_localhost").copy_expert(
            sql=sql,
            filename=filepath
        ),
        op_kwargs={
            "sql": queries.INSERT_DISTRICTS,
            "filepath": "/opt/airflow/dags/data/districts_transformed.csv"
        },
    )

    insert_private_transactions = PythonOperator(
        task_id="insert_private_transactions",
        python_callable=lambda sql, filepath: PostgresHook(postgres_conn_id="db_localhost").copy_expert(
            sql=sql,
            filename=filepath
        ),
        op_kwargs={
            "sql": queries.INSERT_PRIVATE_TRANSACTIONS,
            "filepath": "/opt/airflow/dags/data/private_transactions_transformed.csv"
        },
    )

    insert_private_rental = PythonOperator(
        task_id="insert_private_rental",
        python_callable=lambda sql, filepath: PostgresHook(postgres_conn_id="db_localhost").copy_expert(
            sql=sql,
            filename=filepath
        ),
        op_kwargs={
            "sql": queries.INSERT_PRIVATE_RENTAL,
            "filepath": "/opt/airflow/dags/data/private_rental_transformed.csv"
        },
    )

    insert_hdb_information = PythonOperator(
        task_id="insert_hdb_information",
        python_callable=lambda sql, filepath: PostgresHook(postgres_conn_id="db_localhost").copy_expert(
            sql=sql,
            filename=filepath
        ),
        op_kwargs={
            "sql": queries.INSERT_HDB_INFORMATION,
            "filepath": "/opt/airflow/dags/data/hdb_information.csv"
        },
    )

    insert_resale_flats = PythonOperator(
        task_id="insert_resale_flats",
        python_callable=lambda sql, filepath: PostgresHook(postgres_conn_id="db_localhost").copy_expert(
            sql=sql,
            filename=filepath
        ),
        op_kwargs={
            "sql": queries.INSERT_RESALE_FLATS,
            "filepath": "/opt/airflow/dags/data/resale_flats_transformed.csv"
        },
    )

    insert_rental_flats = PythonOperator(
        task_id="insert_rental_flats",
        python_callable=lambda sql, filepath: PostgresHook(postgres_conn_id="db_localhost").copy_expert(
            sql=sql,
            filename=filepath
        ),
        op_kwargs={
            "sql": queries.INSERT_RENTAL_FLATS,
            "filepath": "/opt/airflow/dags/data/flat_rental.csv"
        },
    )

    create_tables >> [insert_salesperson_information, insert_salesperson_transactions, insert_districts, insert_private_transactions, insert_private_rental, insert_hdb_information, insert_resale_flats, insert_rental_flats]


