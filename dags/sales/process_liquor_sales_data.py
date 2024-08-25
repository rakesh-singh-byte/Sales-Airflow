from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowException
import subprocess
import os
from airflow.models import Variable

dbt_directory = Variable.get("DBT_DIR", default_var='/Users/rakeshsingh/Personal/dbt/')
project_name = Variable.get("SALES_PROJECT_NAME", default_var='Sales-Analytics')
email_recipient = Variable.get("EMAIL_RECIPIENT", default_var='rakesh.singh@tothenew.com')
failure_email_subject = Variable.get("FAILURE_EMAIL_SUBJECT", default_var='DAG Failure Notification')
failure_email_content = Variable.get(
    "FAILURE_EMAIL_CONTENT",
    default_var="""<h3>Dear Team,</h3><p>The DAG <strong>'sales_data_pipeline'</strong> failed.</p>"""
)
dbt_sales_project_directory = f'{dbt_directory}/{project_name}/'


# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 0
}

# Define the DAG
dag = DAG(
    'process_liquor_sales_data',
    default_args=default_args,
    max_active_runs=1,
    description='Advanced DAG for running dbt models with error handling and notifications',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
)

# Helper function to run dbt commands
def run_dbt_command(command: str, cwd: str = dbt_sales_project_directory) -> None:
    try:
        result = subprocess.run(
            command,
            shell=True,
            check=True,
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        print(result.stdout.decode())
        if result.stderr:
            print(result.stderr.decode())
    except subprocess.CalledProcessError as e:
        raise AirflowException(f"Error running dbt command '{command}': {e}")

# Task to run dbt run
def run_dbt_run():
    run_dbt_command("dbt run")

# Task to run dbt test
def run_dbt_test():
    run_dbt_command("dbt test")

# Task to generate dbt docs
def generate_dbt_docs():
    run_dbt_command("dbt docs generate")


# Define tasks in a TaskGroup for better organization
with TaskGroup(group_id='dbt_tasks', dag=dag) as dbt_tasks:
    run_dbt = PythonOperator(
        task_id='run_dbt',
        python_callable=run_dbt_run,
        dag=dag,
    )

    test_dbt = PythonOperator(
        task_id='test_dbt',
        python_callable=run_dbt_test,
        dag=dag,
    )

    generate_docs = PythonOperator(
        task_id='generate_dbt_docs',
        python_callable=generate_dbt_docs,
        dag=dag,
    )

    run_dbt >> test_dbt >> generate_docs # type: ignore

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
dbt_tasks # type: ignore
dbt_tasks >> notify_failure_email # type: ignore