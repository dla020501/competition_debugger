#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

ENV_FILE_DEFAULT="$ROOT_DIR/.env"
ENV_FILE="${ENV_FILE:-$ENV_FILE_DEFAULT}"
if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  . "$ENV_FILE"
  set +a
fi

PYTHON_BIN="${PYTHON_BIN:-/home/milab/miniconda3/bin/python}"
SD_HOST="${SD_HOST:-0.0.0.0}"
SD_PORT="${SD_PORT:-18080}"
SD_WORKERS="${SD_WORKERS:-1}"
SD_RELOAD="${SD_RELOAD:-0}"
SD_PROXY_HEADERS="${SD_PROXY_HEADERS:-1}"
SD_FORWARDED_ALLOW_IPS="${SD_FORWARDED_ALLOW_IPS:-127.0.0.1}"

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

exec "${CMD[@]}"
