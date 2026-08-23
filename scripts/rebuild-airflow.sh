#!/usr/bin/env bash
# Rebuild the Airflow Docker image and (re)start the stack from docker-compose-airflow.yaml.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/docker-compose-airflow-local.yaml"

cd "$ROOT_DIR"

echo "==> Building Airflow image..."
docker compose -f "$COMPOSE_FILE" build

echo "==> Starting stack..."
docker compose -f "$COMPOSE_FILE" up -d

echo "==> Done. Current status:"
docker compose -f "$COMPOSE_FILE" ps
