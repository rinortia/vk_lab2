#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
docker compose up -d --build
ready=0
for i in $(seq 1 90); do
  if curl -fsS http://localhost:8083/connectors >/dev/null; then
    ready=1
    break
  fi
  sleep 2
done
[ "$ready" = 1 ] || { echo 'Kafka Connect REST is unavailable' >&2; exit 1; }
curl --fail-with-body -sS -X PUT \
  http://localhost:8083/connectors/bank-accounts/config \
  -H 'Content-Type: application/json' --data-binary @config/connector.json
printf '\nConnector status (tasks may need a few seconds to start):\n'
curl -fsS http://localhost:8083/connectors/bank-accounts/status
printf '\n'
