from airflow import DAG
# from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import ura
import datagovsg

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

    def extract_ura_data(**kwargs):
        df_private_transactions, df_private_rental, df_planning_decisions = ura.get_all_ura()

        # push to task instance
        ti = kwargs['ti']
        ti.xcom_push('df_private_transactions', df_private_transactions)
        ti.xcom_push('df_private_rental', df_private_rental)
        ti.xcom_push('df_planning_decisions', df_planning_decisions)

    def extract_datagovsg_data(**kwargs):
        df_resale_flat_transactions = datagovsg.get_resale_flat_transactions()
        df_salesperson_info = datagovsg.get_salesperson_info()
        df_salesperson_trans = datagovsg.get_salesperson_transactions()
        df_flat_rental = datagovsg.get_renting_out_of_flats_2023()
        df_hdb_information = datagovsg.get_hdb_property_information()

        # push to task instance
        ti = kwargs['ti']
        ti.xcom_push('df_resale_flat_transactions', df_resale_flat_transactions)
        ti.xcom_push('df_salesperson_info', df_salesperson_info)
        ti.xcom_push('df_salesperson_trans', df_salesperson_trans)
        ti.xcom_push('df_flat_rental', df_flat_rental)
        ti.xcom_push('df_hdb_information', df_hdb_information)
        

    # TASK_DEFS = {
    #     "test": {"path": "sql/test.sql"},
    # }

    # create_table = BashOperator(
    #     task_id="create_table",
    #     bash_command="python /opt/airflow/load/create_table.py",
    # )
    extract_ura_data_task = PythonOperator(
        task_id='extract_ura_data',
        python_callable=extract_ura_data,
    )

    extract_datagovsg_data_task = PythonOperator(
        task_id='extract_datagovsg_data',
        python_callable=extract_datagovsg_data,
    )

    # create_tables_if_not_exists = {
    #     k: PostgresOperator(
    #         task_id=f"create_if_not_exists_{k}_table",
    #         postgres_conn_id="db_localhost",
    #         sql=v["path"],
    #     )
    #     for k, v in TASK_DEFS.items()
    # }

    extract_ura_data_task >> extract_datagovsg_data_task