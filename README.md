# Stream Agency Daemon + Intake API

Automated stream continuity + automated epoch billing bridge, with optional HTTP intake API.

## What it does

1. Keeps enrolled agent streams alive (`/stream` renewals)
2. Tracks successful protected windows per chain epoch
3. Optionally calls `billEpoch(agent, epoch, windows)` automatically on `StreamAgencyEscrow`

## Quick start

```bash
cd /Users/ls/Documents/Claws\ Network
python3 stream-agency/stream_agency.py init-db
```

Enroll from PEM:

```bash
python3 stream-agency/stream_agency.py enroll-from-pem \
  --pem /path/to/agent.pem \
  --fee-bps 500
```

Run daemon only (no on-chain billing):

```bash
python3 stream-agency/stream_agency.py run
```

Run daemon with automated epoch billing:

```bash
python3 stream-agency/stream_agency.py run \
  --billing-enabled \
  --escrow-contract claw1contract... \
  --operator-pem /path/to/operator.pem
```

Run intake API only:

```bash
python3 stream-agency/stream_agency.py api \
  --api-host 0.0.0.0 \
  --api-port 8787 \
  --api-token change-me
```

Run intake API + scheduler in one process:

```bash
python3 stream-agency/stream_agency.py api \
  --with-scheduler \
  --poll-seconds 20 \
  --billing-enabled \
  --escrow-contract claw1contract... \
  --operator-pem /path/to/operator.pem \
  --api-token change-me
```

## Production deployment (systemd + alerts)

Ops files are in `ops/`:
- `ops/install_systemd.sh`
- `ops/stream-agency.env.example`
- `ops/run_stream_agency.sh`
- `ops/check_health.sh`
- `ops/systemd/stream-agency.service`
- `ops/systemd/stream-agency-healthcheck.service`
- `ops/systemd/stream-agency-healthcheck.timer`
- `ops/SUBAGENT_INSTRUCTIONS.md`

Install on server:

```bash
cd /opt/stream-agency
sudo bash ops/install_systemd.sh
```

Then edit `/etc/stream-agency/stream-agency.env` and set:
- `API_TOKEN`
- `ESCROW_CONTRACT`
- `OPERATOR_PEM`
- Optional `ALERT_WEBHOOK_URL`

Set operator key permissions so the service user can read it:

```bash
sudo chown root:streamagency /etc/stream-agency/operator.pem
sudo chmod 640 /etc/stream-agency/operator.pem
```

Start services:

```bash
sudo systemctl enable --now stream-agency.service
sudo systemctl enable --now stream-agency-healthcheck.timer
```

Check status/logs:

```bash
sudo systemctl status stream-agency --no-pager
journalctl -u stream-agency -f
curl -sS http://127.0.0.1:8787/health
```

## Commands

- `init-db`
- `enroll --address --signature --fee-bps`
- `enroll-from-pem --pem --fee-bps`
- `pause --address`
- `resume --address`
- `remove --address`
- `tick` (single cycle)
- `run` (continuous)
- `api` (HTTP intake server)
- `report`
- `attempts --address --limit`
- `billing-attempts --limit`

## Runtime options (`tick` / `run`)

Core:
- `--lead-seconds` (default `360`)
- `--jitter-seconds` (default `20`)
- `--stream-url` (default `https://stream.claws.network/stream`)
- `--intake-no-probe-stream` (skip `/stream` probe during API enrollment)

Billing automation:
- `--billing-enabled`
- `--escrow-contract`
- `--operator-pem`
- `--epoch-api-url` (default `https://api.claws.network`)
- `--billing-proxy` (default `https://api.claws.network`)
- `--billing-chain` (default `C`)
- `--billing-gas-limit` (default `25000000`)
- `--billing-gas-price` (default `20000000000000`)

API mode:
- `--api-host` (default `0.0.0.0`)
- `--api-port` (default `8787`)
- `--api-token` (optional auth token; required for non-health endpoints if set)
- `--with-scheduler` (run scheduler loop in-process)

## Intake API endpoints

- `GET /health` (no auth)
- `GET /report`
- `GET /agent?address=claw1...`
- `POST /enroll`
- `POST /pause`
- `POST /resume`
- `POST /remove`
- `POST /tick`

If `--api-token` is set, use either:
- `Authorization: Bearer <token>`
- `X-API-Key: <token>`

### Enroll request example

```bash
curl -sS -X POST http://127.0.0.1:8787/enroll \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer change-me" \
  -d '{
    "address": "claw1...",
    "signature": "abcd1234...",
    "fee_bps": 500
  }'
```

## Data model (SQLite)

- `agents`: enrollment + scheduler status
- `attempts`: `/stream` attempt log
- `usage_windows`: windows counted per `(agent, epoch)` and billed flag
- `billing_attempts`: `billEpoch` tx submission history

## Security notes

- Stream signatures are sensitive secrets.
- This MVP stores them in plaintext SQLite.
- Production should use encrypted secret storage + key management.
