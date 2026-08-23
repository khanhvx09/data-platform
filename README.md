# data-platform

## Hive Metastore

The metastore image doesn't bundle a Postgres JDBC driver, so fetch it once before first boot:

```bash
mkdir -p drivers
curl -fsSL -o drivers/postgresql.jar \
  https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar
```

```bash
docker compose up -d hive-metastore
```

- Thrift API: thrift://localhost:9083

Backed by the `metastore-db` Postgres service (schema is initialized automatically
on first boot). `./drivers` is git-ignored.

## Delta Lake quickstart

```bash
docker compose up -d spark-master spark-worker
```

Jupyter Lab: http://localhost:8888
Spark master Web UI: http://localhost:8080 · worker Web UI: http://localhost:8081

`spark-master` doubles as the Jupyter/driver container (see `startup.sh`) and the Spark
standalone master; jobs run distributed across `spark-worker`. Spark is preconfigured to
read/write `s3a://<bucket>/...` paths directly against MinIO, and uses `hive-metastore`
(thrift://hive-metastore:9083) as its catalog.

## MinIO

```bash
docker compose up -d minio
```

- S3 API: http://localhost:9000
- Web console: http://localhost:9001 (default login `minioadmin` / `minioadmin`)

Data is stored on the host under `data_platform/` (git-ignored).
