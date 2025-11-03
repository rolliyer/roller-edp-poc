from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

def my_python_function():
    print("Hello from Airflow's PythonOperator!")
    # You can add any Python logic here, e.g., data processing, API calls, etc.

# Define the DAG
with DAG(
    dag_id='edp_test',
    start_date=datetime(2025, 11, 2),
    schedule=None,  # Run manually or define a schedule
    catchup=False,
    tags=['example', 'test'],
) as dag:
    # Define the task using PythonOperator
    t1 = PythonOperator(
        task_id='execute_my_function',
        python_callable=my_python_function,
    )