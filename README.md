# data-platform

Local data platform sandbox: Spark + Delta Lake on MinIO (S3-compatible storage) with a
Hive Metastore catalog, orchestrated by Airflow, transformed with dbt, and explorable via
Jupyter or Zeppelin notebooks. Includes a sample `zomato` project (batch ingestion, dbt
models, and a RAG/text-to-SQL chat app over the data).

## Prerequisites

- Docker + Docker Compose
- At least 4 GB RAM / 2 CPUs / 10 GB disk free for Docker (Airflow's init container warns
  if you're short)

## 1. One-time setup

**Hive Metastore JDBC driver** — the metastore image doesn't bundle a Postgres driver, so
fetch it once before first boot:

```bash
mkdir -p drivers
curl -fsSL -o drivers/postgresql.jar \
  https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar
```

**Environment file** — create a `.env` in the repo root (git-ignored) with the variables
the Airflow and zomato services expect:

```bash
AIRFLOW_UID=50000
AIRFLOW_PROJ_DIR=./airflow
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow

# Used by the zomato RAG chat / text-to-SQL app
OPENAI_API_KEY=
DATABRICKS_SERVER_HOSTNAME=
DATABRICKS_HTTP_PATH=
DATABRICKS_TOKEN=

# Optional: LangSmith tracing for the chat app
LANGSMITH_TRACING=
LANGSMITH_ENDPOINT=
LANGSMITH_API_KEY=
LANGSMITH_PROJECT=
```

## 2. Start the stack

Everything is wired through the scripts in [scripts/](scripts/) — run these instead of
calling `docker compose` directly:

```bash
./scripts/run-spark.sh        # Spark + Delta Lake + MinIO + Hive Metastore (docker-compose.yml)
./scripts/rebuild-airflow.sh  # Builds the Airflow image and starts it (docker-compose-airflow-local.yaml)
```

`run-spark.sh` must be started first — it creates the `data-platform-net` network that the
Airflow compose file joins as external. `rebuild-airflow.sh` rebuilds the image on every
run (picks up `airflow/Dockerfile` / dbt / DAG changes), so it's safe to re-run whenever
you change something under `airflow/`.

Both scripts just wrap `docker compose -f <file> up -d` and print `docker compose ps`
afterwards — see the scripts themselves if you need to pass extra flags or target a single
service.

### Optional add-ons (not started by the scripts above)

- **Zeppelin** notebook UI instead of/alongside Jupyter:
  ```bash
  docker compose -f docker-compose-zeppelin.yml up -d
  ```
- **Spark Declarative Pipelines** (`spark-pipelines` CLI) — needs a Spark Connect server
  that normal startup doesn't start:
  ```bash
  docker exec spark-master bash /opt/startup-sdp.sh
  # then, inside the container:
  $SPARK_HOME/bin/spark-pipelines run --remote sc://localhost:15002
  ```
- **Full Airflow (CeleryExecutor + Redis + Flower)** instead of the lightweight
  LocalExecutor setup `rebuild-airflow.sh` uses:
  ```bash
  docker compose -f docker-compose-airflow.yaml build
  docker compose -f docker-compose-airflow.yaml up -d
  ```

## 3. Service URLs

| Service              | URL                              | Notes                                   |
|-----------------------|-----------------------------------|------------------------------------------|
| Jupyter Lab           | http://localhost:8888             | Runs inside `spark-master`, driver + master |
| Spark master Web UI   | http://localhost:8080             |                                          |
| Spark worker Web UI   | http://localhost:8081             |                                          |
| Spark Application UI  | http://localhost:4040              | Only while a job/session is active       |
| MinIO S3 API          | http://localhost:9000             | `minioadmin` / `minioadmin`             |
| MinIO console         | http://localhost:9001             | `minioadmin` / `minioadmin`             |
| Hive Metastore        | thrift://localhost:9083           | Backed by `metastore-db` (Postgres)     |
| Airflow UI            | http://localhost:8090             | `airflow` / `airflow` (from `.env`)     |
| Zeppelin (optional)   | http://localhost:8082             | If started                              |

Spark is preconfigured to read/write `s3a://<bucket>/...` against MinIO and uses the Hive
Metastore as its catalog — see `hive-metastore/core-site.xml` for the S3A wiring.

## 4. Stopping / resetting

```bash
docker compose down                                        # Spark/MinIO/metastore
docker compose -f docker-compose-airflow-local.yaml down    # Airflow
```

Data on disk (`data_platform/`, `drivers/`, Spark warehouse dirs, Airflow logs) is
git-ignored — add `-v` to also drop the Postgres volumes if you want a clean slate.

## Project layout

- `scripts/` — entry-point scripts for bringing up each stack (see above)
- `startup.sh` / `startup-sdp.sh` — run *inside* `spark-master`; not invoked directly on the host
- `docker-compose.yml` — Spark, MinIO, Hive Metastore
- `docker-compose-airflow-local.yaml` — Airflow (LocalExecutor, lightweight, used by `rebuild-airflow.sh`)
- `docker-compose-airflow.yaml` — Airflow (CeleryExecutor, closer to production topology)
- `docker-compose-zeppelin.yml` — optional Zeppelin notebook UI
- `airflow/dags/` — DAGs, including `spark_jobs/` (SparkSubmitOperator jobs) and dbt/Cosmos DAGs
- `airflow/dbt/` — dbt projects (`zomato`, `data_platform`) run by Airflow via Cosmos
- `project/zomato/` — sample dataset, DDL, and a RAG chat / text-to-SQL app over it
- `work/` — notebooks (Delta Lake tutorial series, Spark Declarative Pipelines example)
- `report/` — write-ups (e.g. managed vs. external Delta tables)
