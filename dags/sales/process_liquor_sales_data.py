"""
process_liquor_sales_data.py

This module defines an Apache Airflow DAG for processing liquor sales data using dbt (data build tool).
The DAG runs dbt commands to execute, test, and generate documentation for dbt models. It also includes error handling
and notifications for task failures.

Key Components:
- `run_dbt_command(command: str, cwd: str)`: Helper function to run dbt commands.
- `run_dbt_run()`: Task to run dbt models.
- `run_dbt_test()`: Task to run dbt tests.
- `generate_dbt_docs()`: Task to generate dbt documentation.
- `dbt_tasks`: Task group for dbt-related tasks.
- `notify_failure_email`: Task to send an email notification if any task fails.

Configuration:
- Airflow Variables:
  - `DBT_DIR`: Directory path for the DBT project (default: '/Users/rakeshsingh/Personal/dbt/')
  - `SALES_PROJECT_NAME`: Name of the sales project (default: 'Sales-Analytics')
  - `EMAIL_RECIPIENT`: Recipient email address for failure notifications (default: 'rakesh.singh@tothenew.com')
  - `FAILURE_EMAIL_SUBJECT`: Subject line for failure notifications (default: 'DAG Failure Notification')
  - `FAILURE_EMAIL_CONTENT`: HTML content for failure notifications (default: "<h3>Dear Team,</h3><p>The DAG <strong>'sales_data_pipeline'</strong> failed.</p>")

Dependencies:
- Requires `subprocess` module for running shell commands.

Usage:
To use this script, ensure that all required Airflow variables are set. The DAG can be triggered manually or scheduled to run at specified intervals.
"""

import subprocess

from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

# Retrieve configuration settings from Airflow Variables
dbt_directory = Variable.get(
    "DBT_DIR", default_var='/Users/rakeshsingh/Personal/dbt/')
project_name = Variable.get(
    "SALES_PROJECT_NAME", default_var='Sales-Analytics')
email_recipient = Variable.get(
    "EMAIL_RECIPIENT", default_var='rakesh.singh@tothenew.com')
failure_email_subject = Variable.get(
    "FAILURE_EMAIL_SUBJECT", default_var='DAG Failure Notification')
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


def run_dbt_command(command: str, cwd: str = dbt_sales_project_directory) -> None:
    """
    Run a dbt command in a specified directory.

    Args:
        command (str): The dbt command to execute.
        cwd (str): The working directory where the command will be executed. Default is the dbt sales project directory.

    Raises:
        AirflowException: If the command returns a non-zero exit code.
    """
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
        raise AirflowException(
            f"Error running dbt command '{command}': {e}") from e


def run_dbt_run() -> None:
    """
    Task function to run dbt models.
    """
    run_dbt_command("dbt run")


def run_dbt_test() -> None:
    """
    Task function to run dbt tests.
    """
    run_dbt_command("dbt test")


def generate_dbt_docs() -> None:
    """
    Task function to generate dbt documentation.
    """
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

    run_dbt >> test_dbt >> generate_docs  # type: ignore

# Task to notify if the pipeline or any tasks fail
notify_failure_email = EmailOperator(
    task_id='notify_failure_email',
    to=[email_recipient],
    subject=failure_email_subject,
    html_content=failure_email_content.replace('{dag.dag_id}', dag.dag_id),
    trigger_rule='one_failed',
    dag=dag,
)

# Set task dependencies
dbt_tasks >> notify_failure_email  # type: ignore
