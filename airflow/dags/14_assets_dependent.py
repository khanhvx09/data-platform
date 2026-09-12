from airflow.sdk import dag, task, asset
import pendulum
import os
from assets_13 import fetch_data

@asset(
    schedule = fetch_data,
    uri = "/opt/airflow/logs/data/14_data_processed.txt",
    name = "14_process_data"
)
def process_data(self):
    os.makedirs(os.path.dirname(self.uri), exist_ok=True)

    with open(self.uri, "w") as f:
        f.write(f"Data processed on {pendulum.now().to_iso8601_string()}\n")

    print(f"Data processed and written to {self.uri}")