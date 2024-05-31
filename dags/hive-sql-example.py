from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from pyhive import hive
from datetime import timedelta

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Function to get Hive connection


def get_hive_connection():
    return hive.Connection(
        host='hive-server2',  # The hostname or IP address of the Hive server
        port=10000,           # The port number of the Hive server (default is 10000)
        username='root',      # The username to connect to Hive
        database='default',    # The database to connect to (default is 'default')
        auth='NOSASL'         # The authentication method
    )


# Define the DAG
with DAG(
    dag_id='hive_student_dag',
    default_args=default_args,
    description='A DAG to create Hive table and insert data, then find top students',
    schedule_interval=timedelta(days=1),  # Schedule to run once a day
    start_date=days_ago(1),  # Start date of the DAG
    catchup=False,  # Do not perform backfill
    tags=['example'],
) as dag:

    @task()
    def create_and_insert():
        # Establish a connection to the Hive server
        conn = get_hive_connection()

        # Create a cursor object to interact with the database
        cursor = conn.cursor()

        try:
            # Drop table if exists
            cursor.execute('DROP TABLE IF EXISTS default.airflow_student')

            # Create table
            cursor.execute('''
                CREATE TABLE default.airflow_student (
                    name STRING,
                    gender STRING,
                    subject STRING,
                    score INT
                )
            ''')

            # Insert data
            cursor.execute('''
                INSERT INTO default.airflow_student (name, gender, subject, score) VALUES
                ('Alice', 'F', 'Math', 95),
                ('Bob', 'M', 'Math', 89),
                ('Charlie', 'M', 'Math', 92),
                ('David', 'M', 'Math', 85),
                ('Eve', 'F', 'Math', 97),
                ('Alice', 'F', 'Science', 88),
                ('Bob', 'M', 'Science', 92),
                ('Charlie', 'M', 'Science', 91),
                ('David', 'M', 'Science', 90),
                ('Eve', 'F', 'Science', 94)
            ''')
        finally:
            # Close the cursor and the connection
            cursor.close()
            conn.close()

    @task()
    def find_top_students():
        # Establish a connection to the Hive server
        conn = get_hive_connection()

        # Create a cursor object to interact with the database
        cursor = conn.cursor()

        try:
            # Drop table if exists
            cursor.execute('DROP TABLE IF EXISTS default.airflow_top_student')

            # Create table
            cursor.execute('''
                CREATE TABLE default.airflow_top_student (
                    name STRING,
                    gender STRING,
                    subject STRING,
                    score INT
                )
            ''')

            # Find top students and insert into new table
            cursor.execute('''
                INSERT INTO default.airflow_top_student
                SELECT name, gender, subject, score
                FROM (
                    SELECT name, gender, subject, score,
                           ROW_NUMBER() OVER (PARTITION BY subject ORDER BY score DESC) as rank
                    FROM airflow_student
                ) tmp
                WHERE rank = 1
            ''')
        finally:
            # Close the cursor and the connection
            cursor.close()
            conn.close()

    # Define the task dependencies
    create_and_insert_task = create_and_insert()
    find_top_students_task = find_top_students()

    create_and_insert_task >> find_top_students_task
