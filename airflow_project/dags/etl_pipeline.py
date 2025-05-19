from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from tasks.extractor import extractor
from tasks.parser import parser
from tasks.loader import loader


# Define the DAG for the ETL pipeline
with DAG(
    dag_id='ingest',
    start_date=datetime(2023, 9, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    # Extractor task to get raw CSV data
    extractor = PythonOperator(
        task_id='extractor',
        python_callable=extractor,
        op_kwargs={'web_proxy': '{{ var.value.web_proxy }}'}  # Pass the URL from Airflow Variables
    )

    # Parser task to convert raw CSV data to DataFrame
    parser = PythonOperator(
        task_id='parser',
        python_callable=parser,
        provide_context=True,  # Enables XComs to pass data between tasks
        op_args=['{{ ti.xcom_pull(task_ids="extractor") }}']  # Pull raw data from Extractor task
    )

    # Loader task to load parsed data into PostgreSQL
    loader = PythonOperator(
        task_id='loader',
        python_callable=loader,
        provide_context=True,  # Enables XComs to pass data between tasks
        op_args=['{{ ti.xcom_pull(task_ids="parser") }}']  # Pull parsed data from Parser task
    )

    # Set task dependencies
    extractor >> parser >> loader
