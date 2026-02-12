#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-$(cd "$(dirname "$0")/.." && pwd)}"
DB_PATH="${DB_PATH:-$APP_DIR/agency.db}"
API_HOST="${API_HOST:-127.0.0.1}"
API_PORT="${API_PORT:-8787}"
POLL_SECONDS="${POLL_SECONDS:-20}"
LEAD_SECONDS="${LEAD_SECONDS:-300}"
JITTER_SECONDS="${JITTER_SECONDS:-20}"
REWARD_PER_WINDOW="${REWARD_PER_WINDOW:-1.0}"
STREAM_URL="${STREAM_URL:-https://stream.claws.network/stream}"
INTAKE_PROBE_STREAM="${INTAKE_PROBE_STREAM:-true}"
WITH_SCHEDULER="${WITH_SCHEDULER:-true}"

cmd=(
  python3 "$APP_DIR/stream_agency.py"
  --db "$DB_PATH"
  api
  --api-host "$API_HOST"
  --api-port "$API_PORT"
  --poll-seconds "$POLL_SECONDS"
  --lead-seconds "$LEAD_SECONDS"
  --jitter-seconds "$JITTER_SECONDS"
  --reward-per-window "$REWARD_PER_WINDOW"
  --stream-url "$STREAM_URL"
)

if [[ -n "${API_TOKEN:-}" ]]; then
  cmd+=(--api-token "$API_TOKEN")
fi

if [[ "$WITH_SCHEDULER" == "true" ]]; then
  cmd+=(--with-scheduler)
fi

if [[ "$INTAKE_PROBE_STREAM" != "true" ]]; then
  cmd+=(--intake-no-probe-stream)
fi

if [[ "${BILLING_ENABLED:-false}" == "true" ]]; then
  : "${ESCROW_CONTRACT:?ESCROW_CONTRACT must be set when BILLING_ENABLED=true}"
  : "${OPERATOR_PEM:?OPERATOR_PEM must be set when BILLING_ENABLED=true}"

  cmd+=(
    --billing-enabled
    --escrow-contract "$ESCROW_CONTRACT"
    --operator-pem "$OPERATOR_PEM"
    --epoch-api-url "${EPOCH_API_URL:-https://api.claws.network}"
    --billing-proxy "${BILLING_PROXY:-https://api.claws.network}"
    --billing-chain "${BILLING_CHAIN:-C}"
    --billing-gas-limit "${BILLING_GAS_LIMIT:-25000000}"
    --billing-gas-price "${BILLING_GAS_PRICE:-20000000000000}"
  )
fi

exec "${cmd[@]}"
