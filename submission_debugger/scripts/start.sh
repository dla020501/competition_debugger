#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_FILE="$ROOT_DIR/data/server.pid"
LOG_FILE="$ROOT_DIR/data/server.log"

ENV_FILE_DEFAULT="$ROOT_DIR/.env"
ENV_FILE="${ENV_FILE:-$ENV_FILE_DEFAULT}"
if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  . "$ENV_FILE"
  set +a
fi

PYTHON_BIN="${PYTHON_BIN:-/opt/conda/envs/colmap_env/bin/python}"

SD_HOST="${SD_HOST:-0.0.0.0}"
SD_PORT="${SD_PORT:-18080}"
SD_WORKERS="${SD_WORKERS:-1}"
SD_RELOAD="${SD_RELOAD:-0}"
SD_PROXY_HEADERS="${SD_PROXY_HEADERS:-1}"
SD_FORWARDED_ALLOW_IPS="${SD_FORWARDED_ALLOW_IPS:-127.0.0.1}"

mkdir -p "$ROOT_DIR/data"

if [[ ! -x "$PYTHON_BIN" ]]; then
  if command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="$(command -v python3)"
  elif command -v python >/dev/null 2>&1; then
    PYTHON_BIN="$(command -v python)"
  else
    echo "No usable Python interpreter found. Set PYTHON_BIN explicitly."
    exit 1
  fi
fi

if [[ -f "$PID_FILE" ]]; then
  OLD_PID="$(cat "$PID_FILE")"
  if ps -p "$OLD_PID" >/dev/null 2>&1; then
    echo "Server already running (pid=$OLD_PID)"
    exit 0
  fi
  rm -f "$PID_FILE"
fi

cd "$ROOT_DIR"
CMD=(
  "$PYTHON_BIN" -m uvicorn app:app
  --host "$SD_HOST"
  --port "$SD_PORT"
  --workers "$SD_WORKERS"
  --forwarded-allow-ips "$SD_FORWARDED_ALLOW_IPS"
)

if [[ "$SD_RELOAD" =~ ^([1Tt][Rr][Uu][Ee]|[Yy][Ee][Ss]|[Oo][Nn]|1)$ ]]; then
  CMD+=(--reload)
fi

if [[ "$SD_PROXY_HEADERS" =~ ^([1Tt][Rr][Uu][Ee]|[Yy][Ee][Ss]|[Oo][Nn]|1)$ ]]; then
  CMD+=(--proxy-headers)
else
  CMD+=(--no-proxy-headers)
fi

nohup "${CMD[@]}" > "$LOG_FILE" 2>&1 &
NEW_PID=$!
echo "$NEW_PID" > "$PID_FILE"

sleep 1
if ps -p "$NEW_PID" >/dev/null 2>&1; then
  echo "Server started (pid=$NEW_PID)"
  echo "Log: $LOG_FILE"
else
  echo "Server failed to start. Check log: $LOG_FILE"
  exit 1
fi
