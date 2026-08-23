from datetime import datetime

from airflow.sdk import DAG

from cosmos import DbtTaskGroup
from cosmos.config import ProjectConfig, ProfileConfig

with DAG(
    dag_id="cosmos_test_dbt",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
):

    DbtTaskGroup(
        group_id="dbt",
        project_config=ProjectConfig(
            dbt_project_path="/opt/airflow/dbt/data_platform",
        ),
        profile_config=ProfileConfig(
            profile_name="data_platform",
            target_name="dev",
            profiles_yml_filepath="/opt/airflow/dbt/data_platform/profiles.yml",
        ),
    )