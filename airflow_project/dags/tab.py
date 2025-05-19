from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from tasks.extractor import extractor
from tasks.parser import parser
from tasks.loader import loader


# Define the DAG for the ETL pipeline
with DAG(
    dag_id='tab',
    start_date=datetime(2023, 9, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    # Extractor task to get raw CSV data
    extractor = PythonOperator(
        task_id='extractor',
        python_callable=extractor,
        op_kwargs={'proxy_url': '{{ var.value.proxy_url }}'}  # Pass the URL from Airflow Variables
    )

    # Loader task to load parsed data into PostgreSQL
    loader = PythonOperator(
        task_id='loader',
        python_callable=loader,
        provide_context=True,  # Enables XComs to pass data between tasks
        op_args=['{{ ti.xcom_pull(task_ids="extractor") }}']  # Pull parsed data from Parser task
    )

    # Set task dependencies
    extractor >> loader
