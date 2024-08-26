"""
sales_data_pipeline.py

This module defines an Apache Airflow DAG for managing a sales data pipeline. The pipeline performs the following tasks:
1. Checks for the existence of a specified table in a PostgreSQL database and creates it if it does not exist.
2. Downloads raw sales data CSV files from an S3 bucket to a local directory.
3. Truncates the target table in the PostgreSQL database.
4. Ingests the downloaded CSV files into the PostgreSQL database.
5. Cleans up local files after ingestion.
6. Sends an email notification if any task fails.

The module retrieves configuration settings and connection details from Airflow variables and environment variables. It also includes functionality to handle errors by sending email notifications.

Key Components:
- `load_environment_variables()`: Retrieves AWS and PostgreSQL connection IDs from environment variables, with default values if not set.
- `check_create_table`: Checks if a PostgreSQL table exists and creates it if necessary.
- `download_sales_data`: Downloads CSV files from S3 to a local directory.
- `truncate_target_table`: Truncates the target table in PostgreSQL.
- `ingest_sales_data`: Ingests CSV data from the local directory into PostgreSQL.
- `cleanup_local_files`: Cleans up local files after processing.
- `notify_failure_email`: Sends an email notification if any task in the DAG fails.

Configuration:
- Environment Variables:
  - `AWS_CONN_ID`: AWS connection ID (default: 'aws_default')
  - `POSTGRES_CONN_ID`: PostgreSQL connection ID (default: 'postgres_default')
- Airflow Variables:
  - `EMAIL_RECIPIENT`: Recipient email address for failure notifications.
  - `FAILURE_EMAIL_SUBJECT`: Subject line for failure notifications.
  - `FAILURE_EMAIL_CONTENT`: HTML content for failure notifications.
  - `DBT_DIR`: Directory path for DBT project.
  - `SALES_PROJECT_NAME`: Name of the sales project.
  - `SALES_BUCKET_NAME`: S3 bucket name for sales data.
  - `SALES_FILE_PREFIX`: Prefix for sales data files.
  - `SALES_RAW_DATA_TABLE_NAME`: Name of the PostgreSQL table for raw sales data.
  - `SALES_TABLE_SQL`: SQL query to create the sales table.
  - `SALES_DATABASE`: Name of the PostgreSQL database.

Dependencies:
- Requires `utils.utils` module for utility functions used in the pipeline.

Usage:
To use this script, ensure that all required environment variables and Airflow variables are set. The DAG can be triggered manually or scheduled to run at specified intervals.
"""

import os

from airflow.models import Variable
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from utils.utils import (
    check_and_create_table,
    clean_local_files,
    download_files_from_s3,
    ingest_files_to_postgres,
    truncate_table,
)

from airflow import DAG
from airflow.utils.dates import days_ago


def load_environment_variables():
    """
    Load environment variables required for the DAG.

    This function retrieves the necessary environment variables used in the Airflow DAG,
    such as AWS and Postgres connection IDs. If these environment variables are not set,
    default values are used.

    Returns:
        dict: A dictionary containing the connection IDs for AWS and Postgres, 
        with keys 'aws_conn_id' and 'postgres_conn_id'. Defaults to 'aws_default' 
        and 'postgres_default' if the corresponding environment variables are not found.

    Example:
        env_vars = load_environment_variables()
        print(env_vars['aws_conn_id'])  # Output: 'aws_default' (if AWS_CONN_ID is not set)
    """
    return {
        'aws_conn_id': os.getenv('AWS_CONN_ID', 'aws_default'),
        'postgres_conn_id': os.getenv('POSTGRES_CONN_ID', 'postgres_default')
    }


env_vars = load_environment_variables()

# Set variables
email_recipient = Variable.get(
    "EMAIL_RECIPIENT", default_var='rakesh.singh@tothenew.com')
failure_email_subject = Variable.get(
    "FAILURE_EMAIL_SUBJECT", default_var='DAG Failure Notification')
failure_email_content = Variable.get(
    "FAILURE_EMAIL_CONTENT",
    default_var="""<h3>Dear Team,</h3><p>The DAG <strong>'sales_data_pipeline'</strong> failed.</p>"""
)
dbt_directory = Variable.get(
    "DBT_DIR", default_var='/Users/rakeshsingh/Personal/dbt')
project_name = Variable.get(
    "SALES_PROJECT_NAME", default_var='Sales-Analytics')
sales_bucket_name = Variable.get(
    "SALES_BUCKET_NAME", default_var='dbt-sales-data')
# This need to configure for which file need to pick
file_prefix = Variable.get("SALES_FILE_PREFIX", default_var='Sales_')
# This need to configure in which table data ned to ingest
sales_raw_data_table_name = Variable.get(
    "SALES_RAW_DATA_TABLE_NAME", default_var='raw_sales_data')
# This need to configure for SQL need to use to create table
sales_raw_data_table_sql = Variable.get("SALES_TABLE_SQL")
sales_database = Variable.get("SALES_DATABASE", default_var='sales_analytics')
data_directory = f'{dbt_directory}/{project_name}/data'

# Ensure the data directory exists
os.makedirs(data_directory, exist_ok=True)

# Define default_args
default_args = {
    'owner': 'airflow',
    'retries': 0
}


# DAG for pipeline to download sales raw data and ingest
dag = DAG(
    'sales_data_pipeline',
    default_args=default_args,
    max_active_runs=1,
    description='Pipeline to download sales raw data from S3, ingest into Postgres, and cleanup local files.',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False
)


# Task to check if table exists and create if not
check_create_table = PythonOperator(
    task_id='check_create_table',
    python_callable=check_and_create_table,
    op_kwargs={
        'database_name': sales_database,
        'table_name': sales_raw_data_table_name,
        'postgres_conn_id': env_vars['postgres_conn_id'],
        'create_table_sql': sales_raw_data_table_sql
    },
    trigger_rule='always',
    dag=dag,
)

# Task to download raw sales data CSV files from S3 bucket to local data directory
download_sales_data = PythonOperator(
    task_id='download_sales_data_from_s3',
    python_callable=download_files_from_s3,
    op_kwargs={
        'bucket_name': sales_bucket_name,
        'file_prefix': file_prefix,
        'local_path_dir': data_directory,
        'aws_conn_id': env_vars['aws_conn_id']
    },
    trigger_rule='all_success',
    dag=dag,
)

# Task to truncate the target table
truncate_target_table = PythonOperator(
    task_id='truncate_target_table',
    python_callable=truncate_table,
    op_kwargs={
        'database_name': sales_database,
        'table_name': sales_raw_data_table_name,
        'postgres_conn_id': env_vars['postgres_conn_id']
    },
    trigger_rule='all_success',
    dag=dag,
)

# Task to ingest CSV files from local data directory to Postgres database table
ingest_sales_data = PythonOperator(
    task_id='ingest_sales_data_into_postgres',
    python_callable=ingest_files_to_postgres,
    op_kwargs={
        'local_path_dir': data_directory,
        'file_prefix': file_prefix,
        'database_name': sales_database,
        'table_name': sales_raw_data_table_name,
        'postgres_conn_id': env_vars['postgres_conn_id']
    },
    trigger_rule='all_success',
    dag=dag,
)

# Task to cleanup the local data directory
cleanup_local_files = PythonOperator(
    task_id='cleanup_local_files',
    python_callable=clean_local_files,
    op_kwargs={'local_path_dir': data_directory},
    trigger_rule='all_done',
    dag=dag,
)

# Task to notify if pipeline or any tasks fails
notify_failure_email = EmailOperator(
    task_id='notify_failure_email',
    to=[email_recipient],
    subject=failure_email_subject,
    html_content=failure_email_content.replace('{dag.dag_id}', dag.dag_id),
    trigger_rule='one_failed',
    dag=dag,
)

# Set task dependencies
check_create_table.set_downstream(download_sales_data)
download_sales_data.set_downstream(truncate_target_table)
truncate_target_table.set_downstream(ingest_sales_data)
ingest_sales_data.set_downstream(cleanup_local_files)

# Set failure notification for all tasks
check_create_table.set_downstream(notify_failure_email)
download_sales_data.set_downstream(notify_failure_email)
truncate_target_table.set_downstream(notify_failure_email)
ingest_sales_data.set_downstream(notify_failure_email)
cleanup_local_files.set_downstream(notify_failure_email)
