from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timezone
from dag_utils.ingest_data import ingest_data
from dag_utils.execute_sql import execute_sql
from dag_utils.run_dq_check import run_dq_check
from dag_utils.create_table import create_table
from dq_check.stg_event_stream import DQ_CHECKS

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 6, 12, tzinfo=timezone.utc),
}

"""
This pipeline is responsible for ingesting raw data from a CSV file into a stage table in Postgres, after peforming necessary transformations and data quality checks, publishing the fct table. Data modeling will be carried out using dbt in later stage. 

The pipeline consists of the following steps:
1. Create a staging table `stg_event_stream` in Postgres. (create_stg_table)
2. Ingest data from a CSV file into the staging table. (load_stg)
3. Perform data quality checks on the staging table. (dq_tasks: list of dq_check tasks)
4. Create a fact table `fct_event_stream` in Postgres. (create_fct_table)
5. Load the fact table with data from the staging table. create_stg_table
"""

with DAG (
    "hm_datamart",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag: 

    create_stg_table = PythonOperator(
        task_id='create_stg_table',
        python_callable=create_table,
        op_kwargs = {
            "table_schema":"/opt/airflow/table_schema/stg_event_stream.sql",
            "log_message":"Successfully created stage table stg_event_stream",
        },
    )

    load_stg = PythonOperator(
        task_id="ingest_data",
        python_callable=ingest_data,
        op_kwargs={
            "file_path":"/opt/airflow/raw_data/event_stream.csv", 
            "table_name":"stg_event_stream",
        },
    )

    dq_tasks = []
    for check_type, check_content in DQ_CHECKS.items():
        for col_name, sql_query in check_content.items():
            f"dq_task_{check_type}_{col_name}" = PythonOperator(
                task_id=f"dq_check_{check_type}_{col_name}",
                python_callable=run_dq_check,
                op_kwargs={
                    "check_name": f"{check_type}_{col_name}",
                    "sql_query": sql_query,
                },
            )
            dq_tasks.append(f"dq_task_{check_type}_{col_name}")

    create_fct_table = PythonOperator(
        task_id='create_fct_table',
        python_callable=create_table,
        op_kwargs = {
            "table_schema":"/opt/airflow/table_schema/fct_event_stream.sql",
            "log_message":"Successfully created fct table fct_event_stream",
        },
    )

    load_fct = PythonOperator(
        task_id="load_fct",
        python_callable=execute_sql,
        op_kwargs={
            "sql_query":"SELECT",
            "log_message":"Successfully loaded fct_event_stream table",
        },
    )

    create_stg_table >> load_stg >> dq_tasks >> create_fct_table >> load_fct