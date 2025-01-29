from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 28),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'articles_processing_pipeline',
    default_args=default_args,
    description='A DAG to extract, process, and load article data',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    # Task 1: Extract data from API
    extract_data_task = BashOperator(
        task_id='extract_data_api',
        bash_command='python extract_data_api.py',
    )

    # Task 2: Process articles using PySpark
    process_articles_task = BashOperator(
        task_id='process_articles',
        bash_command='spark-submit process_articles.py',
    )

    # Task 3: Load processed data into the data model
    upload_data_model_task = BashOperator(
        task_id='create_upload_data_model',
        bash_command='python load_tables.py',
    )

    # Define task dependencies
    extract_data_task >> process_articles_task >> upload_data_model_task