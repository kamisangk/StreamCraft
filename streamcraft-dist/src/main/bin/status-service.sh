#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
. "${SCRIPT_DIR}/streamcraft-env.sh"

if [ -f "${STREAMCRAFT_PID_FILE}" ] && kill -0 "$(cat "${STREAMCRAFT_PID_FILE}")" >/dev/null 2>&1; then
  echo "StreamCraft service is running, pid=$(cat "${STREAMCRAFT_PID_FILE}")"
  exit 0
fi

echo "StreamCraft service is not running"
exit 3
