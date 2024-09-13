from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'dbt_run_dag',
    default_args=default_args,
    description='Run DBT transformations as part of the COVID-19 ETL',
    schedule_interval='@daily',  # Set your desired schedule
    start_date=datetime(2023, 9, 1),
    catchup=False,
) as dag:

    # Task to trigger DBT run command
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='dbt run --project-dir /usr/app/dbt/covid_etl_dbt --profiles-dir /usr/app/dbt'
    )

    # Task to trigger DBT test command (optional)
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='dbt test --project-dir /usr/app/dbt/covid_etl_dbt --profiles-dir /usr/app/dbt'
    )

    # Define the order of tasks
    dbt_run >> dbt_test
