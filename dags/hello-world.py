
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from datetime import timedelta
from airflow.utils.dates import days_ago

with DAG(dag_id="hello-airflow",
         description='A simple hello world example',
         schedule_interval=timedelta(days=1),  # Schedule to run once a day
         start_date=days_ago(1),  # Start date of the DAG
         catchup=False,  # Do not perform backfill
         tags=['example'],
         ) as dag:

    # Task to print hello
    hello = BashOperator(task_id="hello", bash_command="echo hello")

    # Task to print airflow
    @task()
    def airflow():
        print("airflow")

    # Define the task dependencies
    hello >> airflow()