#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
. "${SCRIPT_DIR}/streamcraft-env.sh"

if [ ! -f "${STREAMCRAFT_PID_FILE}" ]; then
  echo "StreamCraft service is not running"
  exit 0
fi

PID="$(cat "${STREAMCRAFT_PID_FILE}")"
if ! kill -0 "${PID}" >/dev/null 2>&1; then
  rm -f "${STREAMCRAFT_PID_FILE}"
  echo "Removed stale pid file"
  exit 0
fi

kill "${PID}"
for _ in $(seq 1 30); do
  if ! kill -0 "${PID}" >/dev/null 2>&1; then
    rm -f "${STREAMCRAFT_PID_FILE}"
    echo "StreamCraft service stopped"
    exit 0
  fi
  sleep 1
done

echo "StreamCraft service did not stop within 30 seconds, pid=${PID}"
exit 1
