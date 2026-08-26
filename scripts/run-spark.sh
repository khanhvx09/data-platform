#!/usr/bin/env bash
# Start (or restart) the Spark stack from docker-compose.yml.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/docker-compose.yml"

cd "$ROOT_DIR"

echo "==> Starting stack..."
docker compose -f "$COMPOSE_FILE" up -d

echo "==> Done. Current status:"
docker compose -f "$COMPOSE_FILE" ps
