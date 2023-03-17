from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import ura
import datagovsg
import os

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
        # print current working directory
        print("Current working directory: ", os.getcwd())
        df_private_transactions, df_private_rental, df_planning_decisions = ura.get_all_ura()
        data_path = '/opt/airflow/dags/data'
        data_path_private_transactions = data_path + "/private_transactions.csv"
        data_path_private_rental = data_path + "/private_rental.csv"
        data_path_planning_decisions = data_path + "/planning_decisions.csv"

        # save to csv
        df_private_transactions.to_csv(data_path_private_transactions, index=False)
        df_private_rental.to_csv(data_path_private_rental, index=False)
        df_planning_decisions.to_csv(data_path_planning_decisions, index=False)

        # push to task instance
        ti = kwargs['ti']
        ti.xcom_push('df_private_transactions', data_path_private_transactions)
        ti.xcom_push('df_private_rental', data_path_private_rental)
        ti.xcom_push('df_planning_decisions', data_path_planning_decisions)

    def extract_datagovsg_data(**kwargs):
        print("Getting resale flat transactions...")
        df_resale_flat_transactions = datagovsg.get_resale_flat_transactions()
        print("Getting salesperson information...")
        df_salesperson_info = datagovsg.get_salesperson_information()
        print("Getting salesperson transactions...")
        df_salesperson_trans = datagovsg.get_salesperson_transactions()
        print("Getting renting out of flats...")
        df_flat_rental = datagovsg.get_renting_out_of_flats_2023()
        print("Getting HDB property information...")
        df_hdb_information = datagovsg.get_hdb_property_information()

        data_path = '/opt/airflow/dags/data'
        data_path_resale_flat_transactions = data_path + "/resale_flat_transactions.csv"
        data_path_salesperson_info = data_path + "/salesperson_info.csv"
        data_path_salesperson_trans = data_path + "/salesperson_trans.csv"
        data_path_flat_rental = data_path + "/flat_rental.csv"
        data_path_hdb_information = data_path + "/hdb_information.csv"
        
        # save to csv
        data_path_resale_flat_transactions.to_csv(data_path_resale_flat_transactions, index=False)
        data_path_salesperson_info.to_csv(data_path_salesperson_info, index=False)
        data_path_salesperson_trans.to_csv(data_path_salesperson_trans, index=False)
        data_path_flat_rental.to_csv(data_path_flat_rental, index=False)
        data_path_hdb_information.to_csv(data_path_hdb_information, index=False)
        
        # push to task instance
        ti = kwargs['ti']
        ti.xcom_push('df_resale_flat_transactions', data_path_resale_flat_transactions)
        ti.xcom_push('df_salesperson_info', data_path_salesperson_info)
        ti.xcom_push('df_salesperson_trans', data_path_salesperson_trans)
        ti.xcom_push('df_flat_rental', data_path_flat_rental)
        ti.xcom_push('df_hdb_information', data_path_hdb_information)

        
    def transform(**kwargs):
        print("Transforming data...")
        ti = kwargs['ti']
        # get all the data from task instance
        df_private_transactions_filename = ti.xcom_pull(task_ids='extract_ura_data', key='df_private_transactions')
        df_private_rental_filename = ti.xcom_pull(task_ids='extract_ura_data', key='df_private_rental')
        df_planning_decisions_filename = ti.xcom_pull(task_ids='extract_ura_data', key='df_planning_decisions')

        df_resale_flat_transactions_filename = ti.xcom_pull(task_ids='extract_datagovsg_data', key='df_resale_flat_transactions')
        df_salesperson_info_filename = ti.xcom_pull(task_ids='extract_datagovsg_data', key='df_salesperson_info')
        df_salesperson_trans_filename = ti.xcom_pull(task_ids='extract_datagovsg_data', key='df_salesperson_trans')
        df_flat_rental_filename = ti.xcom_pull(task_ids='extract_datagovsg_data', key='df_flat_rental')
        df_hdb_information_filename = ti.xcom_pull(task_ids='extract_datagovsg_data', key='df_hdb_information')

        # print all filenames
        print("df_private_transactions_filename: ", df_private_transactions_filename)
        print("df_private_rental_filename: ", df_private_rental_filename)
        print("df_planning_decisions_filename: ", df_planning_decisions_filename)
        print("df_resale_flat_transactions_filename: ", df_resale_flat_transactions_filename)
        print("df_salesperson_info_filename: ", df_salesperson_info_filename)
        print("df_salesperson_trans_filename: ", df_salesperson_trans_filename)
        print("df_flat_rental_filename: ", df_flat_rental_filename)
        print("df_hdb_information_filename: ", df_hdb_information_filename)
        
        
        


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

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform,
    )

    # create_tables_if_not_exists = {
    #     k: PostgresOperator(
    #         task_id=f"create_if_not_exists_{k}_table",
    #         postgres_conn_id="db_localhost",
    #         sql=v["path"],
    #     )
    #     for k, v in TASK_DEFS.items()
    # }

    [extract_ura_data_task, extract_datagovsg_data_task] >> transform_task
