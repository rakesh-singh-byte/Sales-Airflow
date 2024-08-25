from airflow.models import Variable
import os
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from utils.utils import (
    download_files_from_s3,
    ingest_files_to_postgres,
    clean_local_files,
    check_and_create_table,
    truncate_table
)


def load_environment_variables():
    return {
        'aws_conn_id': os.getenv('AWS_CONN_ID', 'aws_default'),
        'postgres_conn_id': os.getenv('POSTGRES_CONN_ID', 'postgres_default')
    }


env_vars = load_environment_variables()

# Set variables
email_recipient = Variable.get("EMAIL_RECIPIENT", default_var='rakesh.singh@tothenew.com')
failure_email_subject = Variable.get("FAILURE_EMAIL_SUBJECT", default_var='DAG Failure Notification')
failure_email_content = Variable.get(
    "FAILURE_EMAIL_CONTENT",
    default_var="""<h3>Dear Team,</h3><p>The DAG <strong>'sales_data_pipeline'</strong> failed.</p>"""
)
dbt_directory = Variable.get("DBT_DIR", default_var='/Users/rakeshsingh/Personal/dbt')
project_name = Variable.get("SALES_PROJECT_NAME", default_var='Sales-Analytics')
sales_bucket_name = Variable.get("SALES_BUCKET_NAME", default_var='dbt-sales-data')
file_prefix = Variable.get("SALES_FILE_PREFIX", default_var='Sales_') # This need to configure for which file need to pick
sales_raw_data_table_name = Variable.get("SALES_RAW_DATA_TABLE_NAME", default_var='raw_sales_data') # This need to configure in which table data ned to ingest
sales_raw_data_table_sql = Variable.get("SALES_TABLE_SQL") # This need to configure for SQL need to use to create table
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
check_create_table >> notify_failure_email # type: ignore 
download_sales_data >> notify_failure_email # type: ignore
truncate_target_table >> notify_failure_email # type: ignore
ingest_sales_data >> notify_failure_email # type: ignore
cleanup_local_files >> notify_failure_email # type: ignore
check_create_table >> download_sales_data # type: ignore
download_sales_data >> truncate_target_table # type: ignore
truncate_target_table >> ingest_sales_data # type: ignore
ingest_sales_data >> cleanup_local_files # type: ignore
