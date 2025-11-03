from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
}

with DAG(
    dag_id="dbt_bash_oauth_dag",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
) as dag:
    
    whoami = BashOperator(
        task_id="whoami",
        bash_command=("echo `whoami`")
    )

    pwd = BashOperator(
        task_id="pwd",
        bash_command=("echo `pwd`")
    )

    ls = BashOperator(
        task_id="ls",
        bash_command=("echo `ls -ltr`")
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=(
            "export GOOGLE_APPLICATION_CREDENTIALS=/home/airflow/.config/gcloud/application_default_credentials.json && "
            "dbt seed"
        ),
        cwd="/opt/airflow/dbt/sales_analytics"
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "export GOOGLE_APPLICATION_CREDENTIALS=/home/airflow/.config/gcloud/application_default_credentials.json && "
            "dbt run"
        ),
        cwd="/opt/airflow/dbt/sales_analytics"
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "export GOOGLE_APPLICATION_CREDENTIALS=/home/airflow/.config/gcloud/application_default_credentials.json && "
            "dbt test"
        ),
        cwd="/opt/airflow/dbt/sales_analytics"
    )

    whoami >> pwd >> ls >> dbt_seed >> dbt_run >> dbt_test
