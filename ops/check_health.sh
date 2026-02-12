#!/usr/bin/env bash
set -euo pipefail

HEALTHCHECK_URL="${HEALTHCHECK_URL:-http://127.0.0.1:8787/health}"
ALERT_WEBHOOK_URL="${ALERT_WEBHOOK_URL:-}"
ALERT_COOLDOWN_SECONDS="${ALERT_COOLDOWN_SECONDS:-600}"
ALERT_NAME="${ALERT_NAME:-stream-agency}"
STATE_DIR="${STATE_DIR:-/var/lib/stream-agency}"
STATE_FILE="$STATE_DIR/healthcheck.last_alert"

mkdir -p "$STATE_DIR"

if curl -fsS --max-time 8 "$HEALTHCHECK_URL" >/dev/null; then
  exit 0
fi

now="$(date +%s)"
last="0"
if [[ -f "$STATE_FILE" ]]; then
  last="$(cat "$STATE_FILE" 2>/dev/null || echo 0)"
fi

if (( now - last < ALERT_COOLDOWN_SECONDS )); then
  echo "healthcheck failed, cooldown active" >&2
  exit 1
fi

echo "$now" > "$STATE_FILE"

msg="[$ALERT_NAME] healthcheck failed at $(date -u +%Y-%m-%dT%H:%M:%SZ): $HEALTHCHECK_URL"
logger -t stream-agency-healthcheck "$msg"
echo "$msg" >&2

if [[ -n "$ALERT_WEBHOOK_URL" ]]; then
  payload=$(printf '{"text":"%s"}' "$msg")
  curl -fsS -X POST "$ALERT_WEBHOOK_URL" \
    -H 'Content-Type: application/json' \
    --data "$payload" >/dev/null || true
fi

exit 1
