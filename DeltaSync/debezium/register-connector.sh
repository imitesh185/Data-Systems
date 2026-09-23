#!/bin/sh
set -eu

connect_url="${CONNECT_URL:-http://connect:8083}"
connector_name="${CONNECTOR_NAME:-deltasync-mysql}"
config_file="${CONNECTOR_CONFIG:-/config/connector.json}"

echo "Registering ${connector_name} at ${connect_url}"
curl --fail-with-body --silent --show-error \
  --request PUT \
  --header "Content-Type: application/json" \
  --data-binary "@${config_file}" \
  "${connect_url}/connectors/${connector_name}/config"
echo

attempt=1
while [ "${attempt}" -le 30 ]; do
  # Status can briefly return 404 right after registration; keep polling.
  status="$(curl --silent --show-error \
    "${connect_url}/connectors/${connector_name}/status" || true)"
  echo "${status}"

  if echo "${status}" | grep -q '"state"[[:space:]]*:[[:space:]]*"FAILED"'; then
    echo "Connector or task entered FAILED state" >&2
    exit 1
  fi

  running_count="$(echo "${status}" |
    grep -o '"state"[[:space:]]*:[[:space:]]*"RUNNING"' |
    wc -l |
    tr -d ' ')"
  if [ "${running_count}" -ge 2 ]; then
    echo "Connector and task are running"
    exit 0
  fi

  attempt=$((attempt + 1))
  sleep 2
done

echo "Timed out waiting for connector and task to run" >&2
exit 1
