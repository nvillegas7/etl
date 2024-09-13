from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from tasks.extractor import extractor
from tasks.parser import parser
from tasks.loader import loader


# Define the DAG for the ETL pipeline
with DAG(
    dag_id='etl_pipeline_dag',
    start_date=datetime(2023, 9, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    # Extractor task to get raw CSV data
    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extractor,
        op_kwargs={'url': '{{ var.value.covid_data_url }}'}  # Pass the URL from Airflow Variables
    )

    # Parser task to convert raw CSV data to DataFrame
    parse_task = PythonOperator(
        task_id='parse_task',
        python_callable=parser,
        provide_context=True,  # Enables XComs to pass data between tasks
        op_args=['{{ ti.xcom_pull(task_ids="extract_task") }}']  # Pull raw data from Extractor task
    )

    # Loader task to load parsed data into PostgreSQL
    load_task = PythonOperator(
        task_id='load_task',
        python_callable=loader,
        provide_context=True,  # Enables XComs to pass data between tasks
        op_args=['{{ ti.xcom_pull(task_ids="parse_task") }}']  # Pull parsed data from Parser task
    )

    # Set task dependencies
    extract_task >> parse_task >> load_task
