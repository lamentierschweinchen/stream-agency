#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
  echo "Run as root: sudo bash ops/install_systemd.sh" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
APP_DIR="${APP_DIR:-/opt/stream-agency}"
ENV_DIR="/etc/stream-agency"
ENV_FILE="$ENV_DIR/stream-agency.env"
SYSTEMD_DIR="/etc/systemd/system"
SERVICE_USER="streamagency"

if ! id "$SERVICE_USER" >/dev/null 2>&1; then
  useradd --system --home-dir /var/lib/stream-agency --shell /usr/sbin/nologin "$SERVICE_USER"
fi

mkdir -p "$APP_DIR" "$ENV_DIR" /var/lib/stream-agency /var/log/stream-agency

if command -v rsync >/dev/null 2>&1; then
  rsync -a --delete \
    --exclude '.git' \
    --exclude '__pycache__' \
    --exclude '*.db' \
    "$REPO_DIR/" "$APP_DIR/"
else
  rm -rf "$APP_DIR/ops" "$APP_DIR/stream_agency.py" "$APP_DIR/README.md" "$APP_DIR/LAUNCH_BLUEPRINT.md"
  cp -a "$REPO_DIR/ops" "$APP_DIR/"
  cp -a "$REPO_DIR/stream_agency.py" "$APP_DIR/"
  cp -a "$REPO_DIR/README.md" "$APP_DIR/"
  cp -a "$REPO_DIR/LAUNCH_BLUEPRINT.md" "$APP_DIR/"
fi

install -m 0644 "$REPO_DIR/ops/systemd/stream-agency.service" "$SYSTEMD_DIR/stream-agency.service"
install -m 0644 "$REPO_DIR/ops/systemd/stream-agency-healthcheck.service" "$SYSTEMD_DIR/stream-agency-healthcheck.service"
install -m 0644 "$REPO_DIR/ops/systemd/stream-agency-healthcheck.timer" "$SYSTEMD_DIR/stream-agency-healthcheck.timer"

if [[ ! -f "$ENV_FILE" ]]; then
  install -m 0640 "$REPO_DIR/ops/stream-agency.env.example" "$ENV_FILE"
  chown root:"$SERVICE_USER" "$ENV_FILE"
  echo "Created $ENV_FILE from template. Edit it before starting the service."
fi

chmod 0755 "$APP_DIR/ops/run_stream_agency.sh" "$APP_DIR/ops/check_health.sh"
chmod 0644 "$APP_DIR/stream_agency.py" "$APP_DIR/README.md"

chown -R "$SERVICE_USER:$SERVICE_USER" /var/lib/stream-agency /var/log/stream-agency
chown -R root:root "$APP_DIR" "$ENV_DIR"
if [[ -f "$ENV_FILE" ]]; then
  chown root:"$SERVICE_USER" "$ENV_FILE"
  chmod 0640 "$ENV_FILE"
fi

systemctl daemon-reload

echo "Install complete. Next steps:"
echo "1) Edit $ENV_FILE"
echo "2) Place operator PEM at path set in OPERATOR_PEM (chown root:$SERVICE_USER, chmod 640)"
echo "3) Start services:"
echo "   systemctl enable --now stream-agency.service"
echo "   systemctl enable --now stream-agency-healthcheck.timer"
echo "4) Check logs: journalctl -u stream-agency -f"
