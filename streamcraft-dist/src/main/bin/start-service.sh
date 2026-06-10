#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
. "${SCRIPT_DIR}/streamcraft-env.sh"

JAVA_BIN="java"
if [ -n "${JAVA_HOME}" ]; then
  JAVA_BIN="${JAVA_HOME}/bin/java"
fi

mkdir -p "${STREAMCRAFT_LOG_DIR}" "${STREAMCRAFT_DATA_DIR}"

if [ -f "${STREAMCRAFT_PID_FILE}" ] && kill -0 "$(cat "${STREAMCRAFT_PID_FILE}")" >/dev/null 2>&1; then
  echo "StreamCraft service is already running, pid=$(cat "${STREAMCRAFT_PID_FILE}")"
  exit 0
fi

APP_ARGS=("--spring.config.additional-location=${STREAMCRAFT_CONF_DIR}/")
if [ -n "${SPRING_PROFILES_ACTIVE}" ]; then
  APP_ARGS+=("--spring.profiles.active=${SPRING_PROFILES_ACTIVE}")
fi

nohup "${JAVA_BIN}" ${JAVA_OPTS} \
  -cp "${STREAMCRAFT_CONF_DIR}:${STREAMCRAFT_HOME}/libs/*" \
  "${STREAMCRAFT_SERVICE_MAIN_CLASS}" "${APP_ARGS[@]}" \
  > "${STREAMCRAFT_LOG_DIR}/streamcraft-service.out" 2> "${STREAMCRAFT_LOG_DIR}/streamcraft-service.err" &

echo "$!" > "${STREAMCRAFT_PID_FILE}"
echo "StreamCraft service started, pid=$(cat "${STREAMCRAFT_PID_FILE}")"
