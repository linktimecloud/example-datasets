
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


# if __name__ == "__main__":
#     dag.test()

# kubectl cp demo.py $(kubectl get pods -n kdp-data -l release=airflow,component=worker -o jsonpath='{.items[0].metadata.name}'):/opt/airflow/dags -c worker -n kdp-data

# kubectl cp demo.py $(kubectl get pods -n kdp-data -l release=airflow,component=scheduler -o jsonpath='{.items[0].metadata.name}'):/opt/airflow/dags -c scheduler  -n kdp-data


# conda create -n airflow python=3.12.3
# conda activate airflow

# macos https://github.com/google/re2/issues/453
# brew install re2 abseil
# pip install pybind11
# CFLAGS='-std=c++17' pip install google-re2
# pip install "apache-airflow[celery]==2.9.1" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.1/constraints-3.8.txt"
# airflow config get-value database sql_alchemy_conn
# airflow standalone
# cd ~/airflow cat standalone_admin_password.txt

# airflow config get-value core DAGS_FOLDER
# /Users/mark/airflow/dags


# Variable: AIRFLOW__CORE__TEST_CONNECTION
#
# test_connection = Disabled
# https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html#testing-connections
