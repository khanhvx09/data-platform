from airflow.sdk import dag, task, asset
import pendulum
import os


@asset(
    schedule = "@daily",
    uri = "/opt/airflow/logs/data/13_data_extract.txt",
    name = "13_fetch_data"
)
def fetch_data(self):
    os.makedirs(os.path.dirname(self.uri), exist_ok=True)

    with open(self.uri, "w") as f:
        f.write(f"Data fetched on {pendulum.now().to_iso8601_string()}\n")

    print(f"Data fetched and written to {self.uri}")