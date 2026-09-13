#!/usr/bin/env bash
# Requires Git Bash or WSL on Windows. See scripts/start_dev.ps1 for PowerShell equivalent.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${PROJECT_ROOT}"

if [[ ! -f .env ]]; then
  echo "Warning: .env not found. Copy .env.example to .env and set NEO4J_PASSWORD." >&2
  exit 1
fi

wait_for_healthy() {
  local container_name="$1"
  local service_name="$2"
  local deadline=$((SECONDS + 120))
  local status

  echo "Waiting for ${service_name} to be healthy..."
  while ((SECONDS < deadline)); do
    status="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "${container_name}" 2>/dev/null || true)"
    case "${status}" in
      healthy)
        echo "${service_name} is ready."
        return 0
        ;;
      unhealthy)
        echo "${service_name} became unhealthy. Recent logs:" >&2
        docker compose logs --tail=100 "${service_name}" >&2 || true
        return 1
        ;;
    esac
    sleep 3
  done

  echo "${service_name} did not become healthy within 120 seconds. Recent logs:" >&2
  docker compose logs --tail=100 "${service_name}" >&2 || true
  return 1
}

echo "Starting Graph Loader dev stack..."
docker compose up -d

wait_for_healthy graph-loader-neo4j neo4j
wait_for_healthy graph-loader-kafka kafka

echo
echo "Dev stack is up:"
echo "  Neo4j Browser : http://localhost:7474"
echo "  Neo4j Bolt    : bolt://localhost:7687"
echo "  Kafka         : localhost:9092"
