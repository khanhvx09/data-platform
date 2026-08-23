#!/bin/bash
set -e

# Runs only on first boot of an empty metastore_db_data volume.
# Adds a separate role/database for Airflow inside the shared postgres
# instance, alongside the hive/hive metastore database.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER airflow WITH PASSWORD 'airflow';
    CREATE DATABASE airflow OWNER airflow;
EOSQL
