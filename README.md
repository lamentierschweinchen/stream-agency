# Stream Agency Daemon

Automated stream continuity + automated epoch billing bridge.

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

## Commands

- `init-db`
- `enroll --address --signature --fee-bps`
- `enroll-from-pem --pem --fee-bps`
- `pause --address`
- `resume --address`
- `remove --address`
- `tick` (single cycle)
- `run` (continuous)
- `report`
- `attempts --address --limit`
- `billing-attempts --limit`

## Runtime options (`tick` / `run`)

Core:
- `--lead-seconds` (default `360`)
- `--jitter-seconds` (default `20`)
- `--stream-url` (default `https://stream.claws.network/stream`)

Billing automation:
- `--billing-enabled`
- `--escrow-contract`
- `--operator-pem`
- `--epoch-api-url` (default `https://api.claws.network`)
- `--billing-proxy` (default `https://api.claws.network`)
- `--billing-chain` (default `C`)
- `--billing-gas-limit` (default `25000000`)
- `--billing-gas-price` (default `20000000000000`)

## Data model (SQLite)

- `agents`: enrollment + scheduler status
- `attempts`: `/stream` attempt log
- `usage_windows`: windows counted per `(agent, epoch)` and billed flag
- `billing_attempts`: `billEpoch` tx submission history

## Security notes

- Stream signatures are sensitive secrets.
- This MVP stores them in plaintext SQLite.
- Production should use encrypted secret storage + key management.
