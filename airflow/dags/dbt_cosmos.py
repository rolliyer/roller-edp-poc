from datetime import datetime
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import BigQueryProfileMapping

# Paths inside Airflow container
DBT_PROJECT_PATH = "/opt/airflow/dags/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dags/dbt"

# Cosmos DAG definition
dbt_bigquery_oauth_dag = DbtDag(
    project_config=ProjectConfig(
        project_name="my_dbt_project",  # from dbt_project.yml
        project_root=DBT_PROJECT_PATH,
    ),
    profile_config=ProfileConfig(
        profile_name="default",  # from profiles.yml
        target_name="prod",
        profile_mapping=BigQueryProfileMapping(
            conn_id=None,
            profile_args={
                "method": "oauth",
                "project": "ent-data-warehouse-dev",
                "dataset": "sales_analytics_test",
                "location": "US",  # update region
            },
        ),
    ),
    execution_config=ExecutionConfig(
        dbt_executable_path="/usr/local/bin/dbt"  # path to dbt binary inside container
    ),
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    dag_id="dbt_bigquery_oauth_cosmos_dag",
)
