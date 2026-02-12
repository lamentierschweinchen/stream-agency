#!/usr/bin/env python3
"""Stream Agency daemon.

- Maintains Claws stream windows alive for enrolled agents.
- Tracks covered windows per chain epoch.
- Optionally auto-submits on-chain billEpoch() calls to StreamAgencyEscrow.

Dependency-free: Python stdlib only.
"""

from __future__ import annotations

import argparse
import json
import random
import re
import sqlite3
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

DEFAULT_DB_PATH = "stream-agency/agency.db"
STREAM_URL = "https://stream.claws.network/stream"
DEFAULT_API_URL = "https://api.claws.network"


@dataclass
class Config:
    lead_seconds: int
    jitter_seconds: int
    reward_per_window: float
    poll_interval_seconds: int
    stream_url: str
    billing_enabled: bool
    escrow_contract: str | None
    operator_pem: str | None
    billing_proxy: str
    billing_chain: str
    billing_gas_limit: int
    billing_gas_price: int
    epoch_api_url: str
    intake_probe_stream: bool


def now_ms() -> int:
    return int(time.time() * 1000)


def fmt_ts(epoch_ms: int | None) -> str:
    if not epoch_ms:
        return "-"
    return datetime.fromtimestamp(epoch_ms / 1000.0, tz=timezone.utc).isoformat()


def connect_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS agents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            address TEXT UNIQUE NOT NULL,
            stream_signature TEXT NOT NULL,
            fee_bps INTEGER NOT NULL DEFAULT 500,
            status TEXT NOT NULL DEFAULT 'active',
            expected_end_ms INTEGER,
            next_attempt_ms INTEGER,
            retry_step INTEGER NOT NULL DEFAULT 0,
            success_count INTEGER NOT NULL DEFAULT 0,
            failure_count INTEGER NOT NULL DEFAULT 0,
            fee_due_claw REAL NOT NULL DEFAULT 0,
            last_success_ms INTEGER,
            last_error TEXT,
            created_ms INTEGER NOT NULL,
            updated_ms INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS attempts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            agent_id INTEGER NOT NULL,
            attempted_ms INTEGER NOT NULL,
            ok INTEGER NOT NULL,
            status_code INTEGER NOT NULL,
            reason TEXT,
            end_stream_ms INTEGER,
            response_body TEXT,
            FOREIGN KEY(agent_id) REFERENCES agents(id)
        );

        CREATE TABLE IF NOT EXISTS usage_windows (
            agent_id INTEGER NOT NULL,
            epoch INTEGER NOT NULL,
            windows INTEGER NOT NULL DEFAULT 0,
            billed INTEGER NOT NULL DEFAULT 0,
            billed_at_ms INTEGER,
            last_error TEXT,
            PRIMARY KEY(agent_id, epoch),
            FOREIGN KEY(agent_id) REFERENCES agents(id)
        );

        CREATE TABLE IF NOT EXISTS billing_attempts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            agent_id INTEGER NOT NULL,
            epoch INTEGER NOT NULL,
            windows INTEGER NOT NULL,
            attempted_ms INTEGER NOT NULL,
            ok INTEGER NOT NULL,
            return_code INTEGER NOT NULL,
            stdout TEXT,
            stderr TEXT,
            FOREIGN KEY(agent_id) REFERENCES agents(id)
        );

        CREATE INDEX IF NOT EXISTS idx_agents_next_attempt
            ON agents(status, next_attempt_ms);
        CREATE INDEX IF NOT EXISTS idx_attempts_agent_time
            ON attempts(agent_id, attempted_ms DESC);
        CREATE INDEX IF NOT EXISTS idx_usage_epoch
            ON usage_windows(epoch, billed);
        """
    )
    conn.commit()


def _normalize_signature(sig: str) -> str:
    s = sig.strip()
    if s.startswith("0x"):
        s = s[2:]
    return s


def _run_clawpy(args: list[str]) -> str:
    proc = subprocess.run(
        ["clawpy", *args],
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"clawpy {' '.join(args)} failed ({proc.returncode})\n"
            f"stdout:\n{proc.stdout}\n"
            f"stderr:\n{proc.stderr}"
        )
    return (proc.stdout + "\n" + proc.stderr).strip()


def _extract_address(text: str) -> str:
    match = re.search(r"(claw1[0-9a-z]+)", text)
    if not match:
        raise RuntimeError(f"Unable to parse claw address from output:\n{text}")
    return match.group(1)


def _extract_signature(text: str) -> str:
    match = re.search(r"(0x[0-9a-fA-F]+)", text)
    if not match:
        raise RuntimeError(f"Unable to parse signature from output:\n{text}")
    return match.group(1)


def enroll_from_pem(conn: sqlite3.Connection, pem: str, fee_bps: int) -> str:
    address_output = _run_clawpy(
        [
            "wallet",
            "convert",
            "--infile",
            pem,
            "--in-format",
            "pem",
            "--out-format",
            "address-bech32",
        ]
    )
    address = _extract_address(address_output)
    signature_output = _run_clawpy(
        ["wallet", "sign-message", "--pem", pem, "--message", "stream"]
    )
    signature = _extract_signature(signature_output)
    enroll_agent(conn, address, signature, fee_bps)
    return address


def enroll_agent(conn: sqlite3.Connection, address: str, signature: str, fee_bps: int) -> None:
    ts = now_ms()
    conn.execute(
        """
        INSERT INTO agents(address, stream_signature, fee_bps, status, next_attempt_ms, created_ms, updated_ms)
        VALUES (?, ?, ?, 'active', ?, ?, ?)
        ON CONFLICT(address) DO UPDATE SET
            stream_signature = excluded.stream_signature,
            fee_bps = excluded.fee_bps,
            status = 'active',
            updated_ms = excluded.updated_ms
        """,
        (address.strip(), _normalize_signature(signature), fee_bps, ts, ts, ts),
    )
    conn.commit()


def set_status(conn: sqlite3.Connection, address: str, status: str) -> None:
    ts = now_ms()
    cur = conn.execute(
        "UPDATE agents SET status = ?, updated_ms = ? WHERE address = ?",
        (status, ts, address),
    )
    conn.commit()
    if cur.rowcount == 0:
        raise RuntimeError(f"Agent not found: {address}")


def remove_agent(conn: sqlite3.Connection, address: str) -> None:
    row = conn.execute("SELECT id FROM agents WHERE address = ?", (address,)).fetchone()
    if not row:
        raise RuntimeError(f"Agent not found: {address}")
    aid = int(row["id"])
    conn.execute("DELETE FROM billing_attempts WHERE agent_id = ?", (aid,))
    conn.execute("DELETE FROM usage_windows WHERE agent_id = ?", (aid,))
    conn.execute("DELETE FROM attempts WHERE agent_id = ?", (aid,))
    conn.execute("DELETE FROM agents WHERE id = ?", (aid,))
    conn.commit()


def list_agents(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return list(
        conn.execute(
            """
            SELECT
                id, address, fee_bps, status,
                expected_end_ms, next_attempt_ms,
                success_count, failure_count,
                fee_due_claw, last_success_ms, last_error
            FROM agents
            ORDER BY id
            """
        )
    )


def _post_stream(stream_url: str, address: str, signature: str) -> tuple[bool, int, str, dict[str, Any] | None]:
    payload = {
        "signature": _normalize_signature(signature),
        "message": "stream",
        "address": address,
    }
    req = Request(
        stream_url,
        method="POST",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )

    try:
        with urlopen(req, timeout=20) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            parsed = json.loads(body) if body else None
            return True, resp.status, body, parsed
    except HTTPError as e:
        body = e.read().decode("utf-8", errors="replace") if e.fp else str(e)
        parsed = None
        try:
            parsed = json.loads(body)
        except json.JSONDecodeError:
            parsed = None
        return False, e.code, body, parsed
    except URLError as e:
        return False, 0, f"URLError: {e}", None


def _get_json(url: str) -> dict[str, Any]:
    req = Request(url, method="GET")
    with urlopen(req, timeout=20) as resp:
        body = resp.read().decode("utf-8", errors="replace")
        return json.loads(body)


def get_chain_epoch(api_base: str) -> int:
    base = api_base.rstrip("/")
    last_error: Exception | None = None
    data = None
    for path in ("/network/status/4294967295", "/network/status"):
        try:
            data = _get_json(f"{base}{path}")
            break
        except Exception as exc:
            last_error = exc
    if data is None:
        raise RuntimeError(f"Unable to fetch chain epoch: {last_error}")

    status = data.get("data", {}).get("status", {})

    for key in ("erd_epoch", "erd_epoch_number", "epoch"):
        value = status.get(key)
        if isinstance(value, int):
            return value

    metrics = data.get("data", {}).get("metrics", {})
    value = metrics.get("erd_epoch")
    if isinstance(value, int):
        return value

    raise RuntimeError(f"Unable to parse chain epoch from /network/status response: {data}")


def _extract_end_stream_ms(parsed: dict[str, Any] | None) -> int | None:
    if not parsed:
        return None
    value = parsed.get("end_stream") or parsed.get("can_stream_again_at")
    return value if isinstance(value, int) else None


def _record_attempt(
    conn: sqlite3.Connection,
    agent_id: int,
    ok: bool,
    status_code: int,
    reason: str,
    end_stream_ms: int | None,
    response_body: str,
) -> None:
    conn.execute(
        """
        INSERT INTO attempts(agent_id, attempted_ms, ok, status_code, reason, end_stream_ms, response_body)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            agent_id,
            now_ms(),
            1 if ok else 0,
            status_code,
            reason,
            end_stream_ms,
            response_body[:4000],
        ),
    )


def _increment_usage_window(conn: sqlite3.Connection, agent_id: int, epoch: int) -> None:
    conn.execute(
        """
        INSERT INTO usage_windows(agent_id, epoch, windows, billed)
        VALUES (?, ?, 1, 0)
        ON CONFLICT(agent_id, epoch) DO UPDATE SET windows = windows + 1
        """,
        (agent_id, epoch),
    )


def _next_planned_attempt(end_stream_ms: int, lead_seconds: int, jitter_seconds: int) -> int:
    jitter_ms = random.randint(0, max(0, jitter_seconds) * 1000)
    return end_stream_ms - (lead_seconds * 1000) + jitter_ms


def _fee_for_success(reward_per_window: float, fee_bps: int) -> float:
    return reward_per_window * (fee_bps / 10_000.0)


def _schedule_retry(ts_ms: int, retry_step: int) -> tuple[int, int]:
    delays = [30, 60, 120]
    idx = min(retry_step, len(delays) - 1)
    delay = delays[idx] if retry_step < len(delays) else 180
    return ts_ms + delay * 1000, retry_step + 1


def process_due_agents(conn: sqlite3.Connection, cfg: Config, chain_epoch: int | None) -> dict[str, int]:
    ts_ms = now_ms()
    due_agents = list(
        conn.execute(
            """
            SELECT * FROM agents
            WHERE status = 'active'
              AND (next_attempt_ms IS NULL OR next_attempt_ms <= ?)
            ORDER BY COALESCE(next_attempt_ms, 0) ASC
            """,
            (ts_ms,),
        )
    )

    stats = {"processed": 0, "ok": 0, "fail": 0, "usage_windows_added": 0}

    for agent in due_agents:
        stats["processed"] += 1
        aid = int(agent["id"])
        address = str(agent["address"])
        sig = str(agent["stream_signature"])
        fee_bps = int(agent["fee_bps"])
        retry_step = int(agent["retry_step"])

        ok, status_code, body, parsed = _post_stream(cfg.stream_url, address, sig)
        end_stream_ms = _extract_end_stream_ms(parsed)

        reason = "ok" if ok else "error"
        if not ok and status_code == 403 and "already streaming" in body.lower():
            reason = "already_streaming"

        _record_attempt(conn, aid, ok, status_code, reason, end_stream_ms, body)

        if ok and end_stream_ms:
            fee = _fee_for_success(cfg.reward_per_window, fee_bps)
            next_attempt = _next_planned_attempt(end_stream_ms, cfg.lead_seconds, cfg.jitter_seconds)
            conn.execute(
                """
                UPDATE agents
                SET
                    expected_end_ms = ?,
                    next_attempt_ms = ?,
                    retry_step = 0,
                    success_count = success_count + 1,
                    fee_due_claw = fee_due_claw + ?,
                    last_success_ms = ?,
                    last_error = NULL,
                    updated_ms = ?
                WHERE id = ?
                """,
                (end_stream_ms, next_attempt, fee, ts_ms, ts_ms, aid),
            )
            if chain_epoch is not None:
                _increment_usage_window(conn, aid, chain_epoch)
                stats["usage_windows_added"] += 1
            stats["ok"] += 1
            continue

        if reason == "already_streaming" and end_stream_ms:
            next_attempt = _next_planned_attempt(end_stream_ms, cfg.lead_seconds, cfg.jitter_seconds)
            conn.execute(
                """
                UPDATE agents
                SET
                    expected_end_ms = ?,
                    next_attempt_ms = ?,
                    retry_step = 0,
                    updated_ms = ?
                WHERE id = ?
                """,
                (end_stream_ms, next_attempt, ts_ms, aid),
            )
            stats["ok"] += 1
            continue

        retry_at, next_retry_step = _schedule_retry(ts_ms, retry_step)
        conn.execute(
            """
            UPDATE agents
            SET
                next_attempt_ms = ?,
                retry_step = ?,
                failure_count = failure_count + 1,
                last_error = ?,
                updated_ms = ?
            WHERE id = ?
            """,
            (retry_at, next_retry_step, f"{status_code}: {body[:300]}", ts_ms, aid),
        )
        stats["fail"] += 1

    conn.commit()
    return stats


def _run_bill_epoch(cfg: Config, agent_address: str, epoch: int, windows: int) -> subprocess.CompletedProcess:
    if not cfg.escrow_contract or not cfg.operator_pem:
        raise RuntimeError("Billing requires --escrow-contract and --operator-pem")

    cmd = [
        "clawpy",
        "contract",
        "call",
        cfg.escrow_contract,
        "--function",
        "billEpoch",
        "--arguments",
        agent_address,
        str(epoch),
        str(windows),
        "--gas-limit",
        str(cfg.billing_gas_limit),
        "--gas-price",
        str(cfg.billing_gas_price),
        "--pem",
        cfg.operator_pem,
        "--chain",
        cfg.billing_chain,
        "--proxy",
        cfg.billing_proxy,
        "--send",
    ]
    return subprocess.run(cmd, capture_output=True, text=True)


def bill_closed_epochs(conn: sqlite3.Connection, cfg: Config, chain_epoch: int | None) -> dict[str, int]:
    stats = {"billing_candidates": 0, "billing_ok": 0, "billing_fail": 0}
    if not cfg.billing_enabled or chain_epoch is None:
        return stats

    candidates = list(
        conn.execute(
            """
            SELECT uw.agent_id, uw.epoch, uw.windows, a.address
            FROM usage_windows uw
            JOIN agents a ON a.id = uw.agent_id
            WHERE uw.billed = 0
              AND uw.epoch < ?
              AND uw.windows > 0
              AND a.status IN ('active', 'paused', 'suspended')
            ORDER BY uw.epoch ASC, uw.agent_id ASC
            """,
            (chain_epoch,),
        )
    )

    stats["billing_candidates"] = len(candidates)

    for row in candidates:
        agent_id = int(row["agent_id"])
        epoch = int(row["epoch"])
        windows = int(row["windows"])
        address = str(row["address"])

        proc = _run_bill_epoch(cfg, address, epoch, windows)
        ok = proc.returncode == 0

        conn.execute(
            """
            INSERT INTO billing_attempts(agent_id, epoch, windows, attempted_ms, ok, return_code, stdout, stderr)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                agent_id,
                epoch,
                windows,
                now_ms(),
                1 if ok else 0,
                proc.returncode,
                (proc.stdout or "")[:4000],
                (proc.stderr or "")[:4000],
            ),
        )

        if ok:
            conn.execute(
                """
                UPDATE usage_windows
                SET billed = 1, billed_at_ms = ?, last_error = NULL
                WHERE agent_id = ? AND epoch = ?
                """,
                (now_ms(), agent_id, epoch),
            )
            stats["billing_ok"] += 1
        else:
            conn.execute(
                """
                UPDATE usage_windows
                SET last_error = ?
                WHERE agent_id = ? AND epoch = ?
                """,
                ((proc.stderr or proc.stdout or "billing failed")[:300], agent_id, epoch),
            )
            stats["billing_fail"] += 1

    conn.commit()
    return stats


def execute_tick(conn: sqlite3.Connection, cfg: Config) -> dict[str, Any]:
    chain_epoch = None
    epoch_error = None
    if cfg.billing_enabled:
        try:
            chain_epoch = get_chain_epoch(cfg.epoch_api_url)
        except Exception as exc:
            epoch_error = str(exc)

    stream_stats = process_due_agents(conn, cfg, chain_epoch)
    bill_stats = bill_closed_epochs(conn, cfg, chain_epoch)
    return {
        "stream": stream_stats,
        "billing": bill_stats,
        "chain_epoch": chain_epoch,
        "epoch_error": epoch_error,
    }


def collect_report_data(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    usage = {
        int(r["agent_id"]): (int(r["pending"] or 0), int(r["billed"] or 0))
        for r in conn.execute(
            """
            SELECT
                agent_id,
                SUM(CASE WHEN billed = 0 THEN windows ELSE 0 END) AS pending,
                SUM(CASE WHEN billed = 1 THEN windows ELSE 0 END) AS billed
            FROM usage_windows
            GROUP BY agent_id
            """
        )
    }

    rows = []
    for row in list_agents(conn):
        pending, billed = usage.get(int(row["id"]), (0, 0))
        rows.append(
            {
                "id": int(row["id"]),
                "address": str(row["address"]),
                "fee_bps": int(row["fee_bps"]),
                "status": str(row["status"]),
                "success_count": int(row["success_count"]),
                "failure_count": int(row["failure_count"]),
                "pending_windows": pending,
                "billed_windows": billed,
                "next_attempt_ms": row["next_attempt_ms"],
                "expected_end_ms": row["expected_end_ms"],
                "last_success_ms": row["last_success_ms"],
                "last_error": row["last_error"],
            }
        )
    return rows


def print_report(conn: sqlite3.Connection) -> None:
    rows = collect_report_data(conn)
    if not rows:
        print("No agents enrolled.")
        return

    print(
        "address                              status  fee_bps  ok/fail  pending/billed  next_attempt(UTC)                 expected_end(UTC)"
    )
    print("-" * 146)
    for row in rows:
        addr = row["address"]
        if len(addr) > 34:
            addr = addr[:31] + "..."
        print(
            f"{addr:<34} {row['status']:<9} {row['fee_bps']:<7} "
            f"{row['success_count']}/{row['failure_count']:<7} "
            f"{row['pending_windows']}/{row['billed_windows']:<13} "
            f"{fmt_ts(row['next_attempt_ms']):<32} "
            f"{fmt_ts(row['expected_end_ms']):<32}"
        )


def print_attempts(conn: sqlite3.Connection, address: str, limit: int) -> None:
    row = conn.execute("SELECT id FROM agents WHERE address = ?", (address,)).fetchone()
    if not row:
        raise RuntimeError(f"Agent not found: {address}")

    aid = int(row["id"])
    attempts = list(
        conn.execute(
            """
            SELECT attempted_ms, ok, status_code, reason, end_stream_ms, response_body
            FROM attempts
            WHERE agent_id = ?
            ORDER BY attempted_ms DESC
            LIMIT ?
            """,
            (aid, limit),
        )
    )

    if not attempts:
        print("No attempts recorded.")
        return

    for att in attempts:
        print(
            f"{fmt_ts(att['attempted_ms'])} ok={att['ok']} status={att['status_code']} reason={att['reason']} "
            f"end={fmt_ts(att['end_stream_ms'])}"
        )
        print(f"  body={att['response_body'][:280]}")


def print_billing_attempts(conn: sqlite3.Connection, limit: int) -> None:
    rows = list(
        conn.execute(
            """
            SELECT ba.attempted_ms, ba.epoch, ba.windows, ba.ok, ba.return_code, a.address, ba.stderr
            FROM billing_attempts ba
            JOIN agents a ON a.id = ba.agent_id
            ORDER BY ba.attempted_ms DESC
            LIMIT ?
            """,
            (limit,),
        )
    )

    if not rows:
        print("No billing attempts recorded.")
        return

    for row in rows:
        print(
            f"{fmt_ts(row['attempted_ms'])} agent={row['address']} epoch={row['epoch']} windows={row['windows']} "
            f"ok={row['ok']} rc={row['return_code']}"
        )
        if row["stderr"]:
            print(f"  stderr={row['stderr'][:260]}")


def _is_authorized(handler: BaseHTTPRequestHandler, token: str | None) -> bool:
    if not token:
        return True

    auth = handler.headers.get("Authorization", "")
    if auth == f"Bearer {token}":
        return True

    api_key = handler.headers.get("X-API-Key", "")
    return api_key == token


def _read_json_body(handler: BaseHTTPRequestHandler) -> dict[str, Any]:
    length_raw = handler.headers.get("Content-Length", "0")
    try:
        length = int(length_raw)
    except ValueError as exc:
        raise RuntimeError(f"Invalid Content-Length: {length_raw}") from exc
    if length <= 0:
        return {}
    raw = handler.rfile.read(length)
    if not raw:
        return {}
    try:
        parsed = json.loads(raw.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError("Body must be valid JSON") from exc
    if not isinstance(parsed, dict):
        raise RuntimeError("JSON payload must be an object")
    return parsed


def _write_json(handler: BaseHTTPRequestHandler, status: int, payload: dict[str, Any]) -> None:
    body = json.dumps(payload).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def _validate_agent_address(address: str) -> bool:
    return bool(re.fullmatch(r"claw1[0-9a-z]+", address))


def _enroll_via_api(
    conn: sqlite3.Connection,
    cfg: Config,
    address: str,
    signature: str,
    fee_bps: int,
) -> dict[str, Any]:
    if not _validate_agent_address(address):
        raise RuntimeError("Invalid Claws address")
    if not (0 <= fee_bps <= 10_000):
        raise RuntimeError("fee_bps must be between 0 and 10000")
    if not signature:
        raise RuntimeError("Missing stream signature")

    probe = {
        "ok": True,
        "status_code": 0,
        "reason": "skipped",
        "end_stream_ms": None,
        "response_body": "",
    }
    if cfg.intake_probe_stream:
        ok, status_code, body, parsed = _post_stream(cfg.stream_url, address, signature)
        end_stream_ms = _extract_end_stream_ms(parsed)
        reason = "ok" if ok else "error"
        if not ok and status_code == 403 and "already streaming" in body.lower():
            reason = "already_streaming"

        # Accept a valid active stream as proof; reject everything else.
        if not ok and not (reason == "already_streaming" and end_stream_ms):
            raise RuntimeError(
                f"Stream signature probe failed (status={status_code}): {body[:220]}"
            )

        probe = {
            "ok": ok,
            "status_code": status_code,
            "reason": reason,
            "end_stream_ms": end_stream_ms,
            "response_body": body[:500],
        }

    enroll_agent(conn, address, signature, fee_bps)

    if probe["end_stream_ms"]:
        ts = now_ms()
        next_attempt = _next_planned_attempt(
            int(probe["end_stream_ms"]), cfg.lead_seconds, cfg.jitter_seconds
        )
        conn.execute(
            """
            UPDATE agents
            SET expected_end_ms = ?, next_attempt_ms = ?, retry_step = 0, last_error = NULL, updated_ms = ?
            WHERE address = ?
            """,
            (int(probe["end_stream_ms"]), next_attempt, ts, address),
        )
        conn.commit()

    return {
        "address": address,
        "fee_bps": fee_bps,
        "probe": probe,
    }


def make_api_handler(
    db_path: str,
    cfg: Config,
    api_token: str | None,
) -> type[BaseHTTPRequestHandler]:
    class IntakeApiHandler(BaseHTTPRequestHandler):
        def _require_auth(self) -> bool:
            if _is_authorized(self, api_token):
                return True
            _write_json(self, 401, {"ok": False, "error": "Unauthorized"})
            return False

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            path = parsed.path

            if path == "/health":
                _write_json(
                    self,
                    200,
                    {"ok": True, "time_ms": now_ms(), "billing_enabled": cfg.billing_enabled},
                )
                return

            if not self._require_auth():
                return

            if path == "/report":
                with connect_db(db_path) as conn:
                    data = collect_report_data(conn)
                _write_json(self, 200, {"ok": True, "agents": data})
                return

            if path == "/agent":
                query = parse_qs(parsed.query)
                address = (query.get("address") or [""])[0].strip()
                if not address:
                    _write_json(self, 400, {"ok": False, "error": "Missing address query parameter"})
                    return
                with connect_db(db_path) as conn:
                    row = conn.execute(
                        "SELECT id FROM agents WHERE address = ?",
                        (address,),
                    ).fetchone()
                    if not row:
                        _write_json(self, 404, {"ok": False, "error": "Agent not found"})
                        return
                    aid = int(row["id"])
                    attempts = list(
                        conn.execute(
                            """
                            SELECT attempted_ms, ok, status_code, reason, end_stream_ms
                            FROM attempts
                            WHERE agent_id = ?
                            ORDER BY attempted_ms DESC
                            LIMIT 10
                            """,
                            (aid,),
                        )
                    )
                _write_json(
                    self,
                    200,
                    {
                        "ok": True,
                        "address": address,
                        "recent_attempts": [
                            {
                                "attempted_ms": int(a["attempted_ms"]),
                                "ok": bool(a["ok"]),
                                "status_code": int(a["status_code"]),
                                "reason": str(a["reason"] or ""),
                                "end_stream_ms": a["end_stream_ms"],
                            }
                            for a in attempts
                        ],
                    },
                )
                return

            _write_json(self, 404, {"ok": False, "error": "Not found"})

        def do_POST(self) -> None:  # noqa: N802
            if not self._require_auth():
                return

            path = urlparse(self.path).path

            try:
                payload = _read_json_body(self)
            except Exception as exc:
                _write_json(self, 400, {"ok": False, "error": str(exc)})
                return

            try:
                if path == "/enroll":
                    address = str(payload.get("address", "")).strip()
                    signature = str(payload.get("signature", "")).strip()
                    fee_bps = int(payload.get("fee_bps", 500))
                    with connect_db(db_path) as conn:
                        result = _enroll_via_api(conn, cfg, address, signature, fee_bps)
                    _write_json(self, 200, {"ok": True, **result})
                    return

                if path == "/pause":
                    address = str(payload.get("address", "")).strip()
                    with connect_db(db_path) as conn:
                        set_status(conn, address, "paused")
                    _write_json(self, 200, {"ok": True, "address": address, "status": "paused"})
                    return

                if path == "/resume":
                    address = str(payload.get("address", "")).strip()
                    with connect_db(db_path) as conn:
                        set_status(conn, address, "active")
                    _write_json(self, 200, {"ok": True, "address": address, "status": "active"})
                    return

                if path == "/remove":
                    address = str(payload.get("address", "")).strip()
                    with connect_db(db_path) as conn:
                        remove_agent(conn, address)
                    _write_json(self, 200, {"ok": True, "address": address, "removed": True})
                    return

                if path == "/tick":
                    with connect_db(db_path) as conn:
                        result = execute_tick(conn, cfg)
                    _write_json(self, 200, {"ok": True, **result})
                    return

                _write_json(self, 404, {"ok": False, "error": "Not found"})
            except Exception as exc:
                _write_json(self, 400, {"ok": False, "error": str(exc)})

        def log_message(self, fmt: str, *args: Any) -> None:
            sys.stderr.write(f"{self.log_date_time_string()} api {fmt % args}\n")

    return IntakeApiHandler


def run_scheduler_loop(conn: sqlite3.Connection, cfg: Config, stop: threading.Event) -> None:
    print(
        f"Scheduler loop started: poll={cfg.poll_interval_seconds}s lead={cfg.lead_seconds}s "
        f"jitter={cfg.jitter_seconds}s reward/window={cfg.reward_per_window} billing={cfg.billing_enabled}"
    )
    while not stop.is_set():
        result = execute_tick(conn, cfg)
        stream_stats = result["stream"]
        bill_stats = result["billing"]
        if stream_stats["processed"] > 0 or bill_stats["billing_candidates"] > 0:
            print(
                f"{datetime.now(tz=timezone.utc).isoformat()} "
                f"stream processed={stream_stats['processed']} ok={stream_stats['ok']} fail={stream_stats['fail']} "
                f"usage+={stream_stats['usage_windows_added']} "
                f"bill cand={bill_stats['billing_candidates']} ok={bill_stats['billing_ok']} fail={bill_stats['billing_fail']}"
            )
        if result["epoch_error"]:
            print(f"{datetime.now(tz=timezone.utc).isoformat()} epoch-fetch-error: {result['epoch_error']}")
        stop.wait(cfg.poll_interval_seconds)


def run_api_server(
    db_path: str,
    cfg: Config,
    host: str,
    port: int,
    api_token: str | None,
    with_scheduler: bool,
) -> None:
    handler_class = make_api_handler(db_path=db_path, cfg=cfg, api_token=api_token)
    server = ThreadingHTTPServer((host, port), handler_class)
    server.daemon_threads = True

    scheduler_stop = threading.Event()
    scheduler_thread = None
    scheduler_conn = None
    if with_scheduler:
        scheduler_conn = connect_db(db_path)
        init_db(scheduler_conn)
        scheduler_thread = threading.Thread(
            target=run_scheduler_loop,
            args=(scheduler_conn, cfg, scheduler_stop),
            daemon=True,
        )
        scheduler_thread.start()

    auth_msg = "disabled" if not api_token else "enabled"
    print(
        f"Intake API listening on http://{host}:{port} "
        f"(auth={auth_msg}, scheduler={with_scheduler}, probe_stream={cfg.intake_probe_stream})"
    )

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping API server...")
    finally:
        server.shutdown()
        server.server_close()
        scheduler_stop.set()
        if scheduler_thread:
            scheduler_thread.join(timeout=3)
        if scheduler_conn:
            scheduler_conn.close()


def run_forever(conn: sqlite3.Connection, cfg: Config) -> None:
    print("Starting Stream Agency loop...")
    print(
        f"poll={cfg.poll_interval_seconds}s lead={cfg.lead_seconds}s jitter={cfg.jitter_seconds}s "
        f"reward/window={cfg.reward_per_window} billing={cfg.billing_enabled}"
    )

    try:
        while True:
            chain_epoch = None
            if cfg.billing_enabled:
                try:
                    chain_epoch = get_chain_epoch(cfg.epoch_api_url)
                except Exception as exc:
                    print(f"{datetime.now(tz=timezone.utc).isoformat()} epoch-fetch-error: {exc}")

            stream_stats = process_due_agents(conn, cfg, chain_epoch)
            bill_stats = bill_closed_epochs(conn, cfg, chain_epoch)

            if stream_stats["processed"] > 0 or bill_stats["billing_candidates"] > 0:
                print(
                    f"{datetime.now(tz=timezone.utc).isoformat()} "
                    f"stream processed={stream_stats['processed']} ok={stream_stats['ok']} fail={stream_stats['fail']} "
                    f"usage+={stream_stats['usage_windows_added']} "
                    f"bill cand={bill_stats['billing_candidates']} ok={bill_stats['billing_ok']} fail={bill_stats['billing_fail']}"
                )

            time.sleep(cfg.poll_interval_seconds)
    except KeyboardInterrupt:
        print("\nStopped.")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Stream Agency daemon")
    p.add_argument("--db", default=DEFAULT_DB_PATH, help="SQLite DB path")

    sub = p.add_subparsers(dest="command", required=True)

    sub.add_parser("init-db", help="Create or migrate local database")

    sp_enroll = sub.add_parser("enroll", help="Enroll/update an agent")
    sp_enroll.add_argument("--address", required=True)
    sp_enroll.add_argument("--signature", required=True, help="Reusable signature for message 'stream'")
    sp_enroll.add_argument("--fee-bps", type=int, default=500)

    sp_enroll_pem = sub.add_parser("enroll-from-pem", help="Enroll agent directly from wallet PEM")
    sp_enroll_pem.add_argument("--pem", required=True)
    sp_enroll_pem.add_argument("--fee-bps", type=int, default=500)

    sp_pause = sub.add_parser("pause", help="Pause an agent")
    sp_pause.add_argument("--address", required=True)

    sp_resume = sub.add_parser("resume", help="Resume an agent")
    sp_resume.add_argument("--address", required=True)

    sp_remove = sub.add_parser("remove", help="Delete an agent and all local records")
    sp_remove.add_argument("--address", required=True)

    def add_runtime_args(sp: argparse.ArgumentParser) -> None:
        sp.add_argument("--lead-seconds", type=int, default=360)
        sp.add_argument("--jitter-seconds", type=int, default=20)
        sp.add_argument("--reward-per-window", type=float, default=1.0)
        sp.add_argument("--stream-url", default=STREAM_URL)
        sp.add_argument(
            "--intake-no-probe-stream",
            action="store_true",
            help="Skip stream signature probe during API /enroll (not recommended).",
        )

        sp.add_argument("--billing-enabled", action="store_true")
        sp.add_argument("--escrow-contract", default="")
        sp.add_argument("--operator-pem", default="")
        sp.add_argument("--epoch-api-url", default=DEFAULT_API_URL)
        sp.add_argument("--billing-proxy", default=DEFAULT_API_URL)
        sp.add_argument("--billing-chain", default="C")
        sp.add_argument("--billing-gas-limit", type=int, default=25_000_000)
        sp.add_argument("--billing-gas-price", type=int, default=20_000_000_000_000)

    sp_tick = sub.add_parser("tick", help="Run one scheduling cycle (+ optional auto billing)")
    add_runtime_args(sp_tick)

    sp_run = sub.add_parser("run", help="Run continuous scheduler loop (+ optional auto billing)")
    sp_run.add_argument("--poll-seconds", type=int, default=20)
    add_runtime_args(sp_run)

    sp_api = sub.add_parser("api", help="Run intake HTTP API server")
    sp_api.add_argument("--poll-seconds", type=int, default=20)
    sp_api.add_argument("--api-host", default="0.0.0.0")
    sp_api.add_argument("--api-port", type=int, default=8787)
    sp_api.add_argument(
        "--api-token",
        default="",
        help="Optional bearer/API key token for all endpoints except /health",
    )
    sp_api.add_argument(
        "--with-scheduler",
        action="store_true",
        help="Run scheduler loop in-process alongside the API server",
    )
    add_runtime_args(sp_api)

    sub.add_parser("report", help="Show enrolled agents and local usage summary")

    sp_attempts = sub.add_parser("attempts", help="Show recent stream attempts for one agent")
    sp_attempts.add_argument("--address", required=True)
    sp_attempts.add_argument("--limit", type=int, default=20)

    sp_ba = sub.add_parser("billing-attempts", help="Show recent billEpoch attempt history")
    sp_ba.add_argument("--limit", type=int, default=20)

    return p.parse_args()


def _build_config(args: argparse.Namespace) -> Config:
    if args.billing_enabled and (not args.escrow_contract or not args.operator_pem):
        raise RuntimeError("--billing-enabled requires --escrow-contract and --operator-pem")

    return Config(
        lead_seconds=args.lead_seconds,
        jitter_seconds=args.jitter_seconds,
        reward_per_window=args.reward_per_window,
        poll_interval_seconds=getattr(args, "poll_seconds", 20),
        stream_url=args.stream_url,
        billing_enabled=args.billing_enabled,
        escrow_contract=args.escrow_contract or None,
        operator_pem=args.operator_pem or None,
        billing_proxy=args.billing_proxy,
        billing_chain=args.billing_chain,
        billing_gas_limit=args.billing_gas_limit,
        billing_gas_price=args.billing_gas_price,
        epoch_api_url=args.epoch_api_url,
        intake_probe_stream=not getattr(args, "intake_no_probe_stream", False),
    )


def main() -> int:
    args = parse_args()
    db_path = str(Path(args.db))

    conn = connect_db(db_path)
    init_db(conn)

    if args.command == "init-db":
        print(f"DB ready: {db_path}")
        return 0

    if args.command == "enroll":
        if not (0 <= args.fee_bps <= 10_000):
            raise RuntimeError("fee-bps must be between 0 and 10000")
        enroll_agent(conn, args.address, args.signature, args.fee_bps)
        print(f"Enrolled: {args.address}")
        return 0

    if args.command == "enroll-from-pem":
        if not (0 <= args.fee_bps <= 10_000):
            raise RuntimeError("fee-bps must be between 0 and 10000")
        address = enroll_from_pem(conn, args.pem, args.fee_bps)
        print(f"Enrolled from PEM: {address}")
        return 0

    if args.command == "pause":
        set_status(conn, args.address, "paused")
        print(f"Paused: {args.address}")
        return 0

    if args.command == "resume":
        set_status(conn, args.address, "active")
        print(f"Resumed: {args.address}")
        return 0

    if args.command == "remove":
        remove_agent(conn, args.address)
        print(f"Removed: {args.address}")
        return 0

    if args.command in ("tick", "run"):
        cfg = _build_config(args)
        if args.command == "tick":
            print(json.dumps(execute_tick(conn, cfg), indent=2))
            return 0

        run_forever(conn, cfg)
        return 0

    if args.command == "api":
        cfg = _build_config(args)
        run_api_server(
            db_path=db_path,
            cfg=cfg,
            host=args.api_host,
            port=args.api_port,
            api_token=args.api_token or None,
            with_scheduler=args.with_scheduler,
        )
        return 0

    if args.command == "report":
        print_report(conn)
        return 0

    if args.command == "attempts":
        print_attempts(conn, args.address, args.limit)
        return 0

    if args.command == "billing-attempts":
        print_billing_attempts(conn, args.limit)
        return 0

    raise RuntimeError(f"Unknown command: {args.command}")


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise
