from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import ura
import datagovsg
import transform
import os
import queries

with DAG(
    dag_id="housing_pipeline",
    default_args={"owner": "airflow"},
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["is3107"],
) as dag:

    def extract_ura_data(**kwargs):
        # print current working directory
        print("Current working directory: ", os.getcwd())
        print("Getting URA data...")
        (
            df_private_transactions,
            df_private_rental,
            df_planning_decisions,
        ) = ura.get_all_ura()
        print("URA data obtained.")

        data_path = "/opt/airflow/dags/data"
        data_path_private_transactions = data_path + "/private_transactions.csv"
        data_path_private_rental = data_path + "/private_rental.csv"
        data_path_planning_decisions = data_path + "/planning_decisions.csv"

        # save to csv
        df_private_transactions.to_csv(data_path_private_transactions, index=False)
        df_private_rental.to_csv(data_path_private_rental, index=False)
        df_planning_decisions.to_csv(data_path_planning_decisions, index=False)

        # push to task instance
        ti = kwargs["ti"]
        ti.xcom_push("df_private_transactions", data_path_private_transactions)
        ti.xcom_push("df_private_rental", data_path_private_rental)
        ti.xcom_push("df_planning_decisions", data_path_planning_decisions)

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
        # rename columns in df_hdb_information
        df_hdb_information = df_hdb_information.rename(
            columns={
                "1room_sold": "one_room_sold",
                "2room_sold": "two_room_sold",
                "3room_sold": "three_room_sold",
                "4room_sold": "four_room_sold",
                "5room_sold": "five_room_sold",
                "1room_rental": "one_room_rental",
                "2room_rental": "two_room_rental",
                "3room_rental": "three_room_rental"
                })

        data_path = "/opt/airflow/dags/data"
        data_path_resale_flat_transactions = data_path + "/resale_flat_transactions.csv"
        data_path_salesperson_info = data_path + "/salesperson_info.csv"
        data_path_salesperson_trans = data_path + "/salesperson_transactions.csv"
        data_path_flat_rental = data_path + "/flat_rental.csv"
        data_path_hdb_information = data_path + "/hdb_information.csv"

        # save to csv
        df_resale_flat_transactions.to_csv(
            data_path_resale_flat_transactions, index=False
        )
        df_salesperson_info.to_csv(data_path_salesperson_info, index=False)
        df_salesperson_trans.to_csv(data_path_salesperson_trans, index=False)
        df_flat_rental.to_csv(data_path_flat_rental, index=False)
        df_hdb_information.to_csv(data_path_hdb_information, index=False)

        # push to task instance
        ti = kwargs["ti"]
        ti.xcom_push("df_resale_flat_transactions", data_path_resale_flat_transactions)
        ti.xcom_push("df_salesperson_info", data_path_salesperson_info)
        ti.xcom_push("df_salesperson_trans", data_path_salesperson_trans)
        ti.xcom_push("df_flat_rental", data_path_flat_rental)
        ti.xcom_push("df_hdb_information", data_path_hdb_information)

    def transform_first(**kwargs):
        print("Transforming data...")
        ti = kwargs["ti"]
        # get all the data from task instance
        df_private_transactions_filename = ti.xcom_pull(
            task_ids="extract_ura_data", key="df_private_transactions"
        )
        df_private_rental_filename = ti.xcom_pull(
            task_ids="extract_ura_data", key="df_private_rental"
        )
        df_planning_decisions_filename = ti.xcom_pull(
            task_ids="extract_ura_data", key="df_planning_decisions"
        )

        df_resale_flat_transactions_filename = ti.xcom_pull(
            task_ids="extract_datagovsg_data", key="df_resale_flat_transactions"
        )
        df_salesperson_info_filename = ti.xcom_pull(
            task_ids="extract_datagovsg_data", key="df_salesperson_info"
        )
        df_salesperson_trans_filename = ti.xcom_pull(
            task_ids="extract_datagovsg_data", key="df_salesperson_trans"
        )
        df_flat_rental_filename = ti.xcom_pull(
            task_ids="extract_datagovsg_data", key="df_flat_rental"
        )
        df_hdb_information_filename = ti.xcom_pull(
            task_ids="extract_datagovsg_data", key="df_hdb_information"
        )

        # print all filenames
        print("df_private_transactions_filename: ", df_private_transactions_filename)
        print("df_private_rental_filename: ", df_private_rental_filename)
        print("df_planning_decisions_filename: ", df_planning_decisions_filename)
        print(
            "df_resale_flat_transactions_filename: ",
            df_resale_flat_transactions_filename,
        )
        print("df_salesperson_info_filename: ", df_salesperson_info_filename)
        print("df_salesperson_trans_filename: ", df_salesperson_trans_filename)
        print("df_flat_rental_filename: ", df_flat_rental_filename)
        print("df_hdb_information_filename: ", df_hdb_information_filename)

        # transform
        print("Transforming..")
        print("Transforming resale flats...")
        df_districts = transform.read_and_transform_districts()
        df_resale_flats = transform.transform_resale_flats(df_resale_flat_transactions_filename, df_districts)
        print("Transforming private transactions and rental...")
        df_private_transactions, df_private_rental = transform.transform_private_transactions_and_rental(df_private_transactions_filename, df_private_rental_filename, df_districts)
        print("Transformed!")

        
        print("Saving to csv...")
        # save to csv
        data_path = "/opt/airflow/dags/data"
        data_path_resale_flats = data_path + "/resale_flats_transformed.csv"
        data_path_private_transactions = data_path + "/private_transactions_transformed.csv"
        data_path_private_rental = data_path + "/private_rental_transformed.csv"
        data_path_districts = data_path + "/districts_transformed.csv"
    
        df_resale_flats.to_csv(data_path_resale_flats, index=False)
        df_private_transactions.to_csv(data_path_private_transactions, index=False)
        df_private_rental.to_csv(data_path_private_rental, index=False)
        df_districts.to_csv(data_path_districts, index=False)
        print("Saved!")

        # push to task instance
        ti.xcom_push("df_resale_flats_transformed", data_path_resale_flats)
        ti.xcom_push("df_private_transactions_transformed", data_path_private_transactions)
        ti.xcom_push("df_private_rental_transformed", data_path_private_rental)

    extract_ura_data_task = PythonOperator(
        task_id="extract_ura_data",
        python_callable=extract_ura_data,
    )

    extract_datagovsg_data_task = PythonOperator(
        task_id="extract_datagovsg_data",
        python_callable=extract_datagovsg_data,
    )

    transform_task = PythonOperator(
        task_id="transform_first",
        python_callable=transform_first,
    )

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

    # create a postgres operator to execute the create_tables_query
    alter_tables = PostgresOperator(
        task_id="alter_tables",
        postgres_conn_id="db_localhost",
        sql=queries.ALTER_TABLES,
    )

    [extract_ura_data_task, extract_datagovsg_data_task] >> transform_task >> create_tables >> [insert_salesperson_information, insert_salesperson_transactions, insert_districts, insert_private_transactions, insert_private_rental, insert_hdb_information, insert_resale_flats, insert_rental_flats] >> alter_tables

