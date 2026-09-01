from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import os
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, AsyncIterator

import httpx

LOG = logging.getLogger("signal_group_ingest")


@dataclass(frozen=True)
class Settings:
    signal_cli_url: str
    signal_group_id: str
    signal_account: str | None
    downstream_webhook_url: str | None
    downstream_bearer_token: str | None
    outbox_db: Path
    retry_interval_seconds: float = 5.0

    @classmethod
    def from_env(cls) -> "Settings":
        group_id = os.getenv("SIGNAL_GROUP_ID", "").strip()
        if not group_id:
            raise RuntimeError("SIGNAL_GROUP_ID is required")

        account = os.getenv("SIGNAL_ACCOUNT", "").strip() or None
        webhook = os.getenv("DOWNSTREAM_WEBHOOK_URL", "").strip() or None
        token = os.getenv("DOWNSTREAM_BEARER_TOKEN", "").strip() or None
        db_path = Path(os.getenv("SIGNAL_OUTBOX_DB", "./data/signal_outbox.sqlite3"))

        return cls(
            signal_cli_url=os.getenv("SIGNAL_CLI_URL", "http://127.0.0.1:8080").rstrip("/"),
            signal_group_id=group_id,
            signal_account=account,
            downstream_webhook_url=webhook,
            downstream_bearer_token=token,
            outbox_db=db_path,
            retry_interval_seconds=float(os.getenv("SIGNAL_RETRY_INTERVAL_SECONDS", "5")),
        )


class Outbox:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS outbox (
                message_id TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0,
                next_attempt_at REAL NOT NULL DEFAULT 0,
                last_error TEXT,
                created_at REAL NOT NULL,
                delivered_at REAL
            )
            """
        )
        self.conn.commit()

    def enqueue(self, message_id: str, payload: dict[str, Any]) -> bool:
        cursor = self.conn.execute(
            """
            INSERT OR IGNORE INTO outbox(message_id, payload, created_at)
            VALUES (?, ?, ?)
            """,
            (message_id, json.dumps(payload, ensure_ascii=False), time.time()),
        )
        self.conn.commit()
        return cursor.rowcount == 1

    def pending(self, limit: int = 50) -> list[sqlite3.Row]:
        return list(
            self.conn.execute(
                """
                SELECT message_id, payload, attempts
                FROM outbox
                WHERE status = 'pending' AND next_attempt_at <= ?
                ORDER BY created_at ASC
                LIMIT ?
                """,
                (time.time(), limit),
            )
        )

    def mark_delivered(self, message_id: str) -> None:
        self.conn.execute(
            """
            UPDATE outbox
            SET status = 'delivered', delivered_at = ?, last_error = NULL
            WHERE message_id = ?
            """,
            (time.time(), message_id),
        )
        self.conn.commit()

    def mark_failed(self, message_id: str, attempts: int, error: str) -> None:
        next_attempt = attempts + 1
        delay = min(300.0, 2.0 ** min(next_attempt, 8))
        self.conn.execute(
            """
            UPDATE outbox
            SET attempts = ?, next_attempt_at = ?, last_error = ?
            WHERE message_id = ?
            """,
            (next_attempt, time.time() + delay, error[:1000], message_id),
        )
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()


def unwrap_receive_event(event: dict[str, Any]) -> dict[str, Any]:
    """Return the object that contains account/envelope for direct or JSON-RPC events."""
    if event.get("method") == "receive":
        params = event.get("params") or {}
        result = params.get("result")
        if isinstance(result, dict):
            return result
        return params
    return event


def _data_message(envelope: dict[str, Any]) -> dict[str, Any] | None:
    direct = envelope.get("dataMessage")
    if isinstance(direct, dict):
        return direct

    edit = envelope.get("editMessage")
    if isinstance(edit, dict) and isinstance(edit.get("dataMessage"), dict):
        return edit["dataMessage"]

    return None


def normalize_group_message(
    event: dict[str, Any], expected_group_id: str
) -> tuple[str, dict[str, Any]] | None:
    body = unwrap_receive_event(event)
    envelope = body.get("envelope")
    if not isinstance(envelope, dict):
        return None

    data_message = _data_message(envelope)
    if not data_message:
        return None

    group_info = data_message.get("groupInfo")
    if not isinstance(group_info, dict):
        return None

    group_id = group_info.get("groupId")
    if group_id != expected_group_id:
        return None

    text = data_message.get("message")
    attachments = data_message.get("attachments") or []
    if (text is None or text == "") and not attachments:
        return None

    timestamp = int(data_message.get("timestamp") or envelope.get("timestamp") or 0)
    sender = (
        envelope.get("sourceNumber")
        or envelope.get("sourceUuid")
        or envelope.get("source")
        or "unknown"
    )
    sender_uuid = envelope.get("sourceUuid")
    account = body.get("account")

    identity = "|".join(
        [
            str(account or ""),
            str(group_id),
            str(sender_uuid or sender),
            str(timestamp),
            str(text or ""),
        ]
    )
    message_id = hashlib.sha256(identity.encode("utf-8")).hexdigest()

    payload = {
        "message_id": message_id,
        "source": "signal",
        "account": account,
        "group_id": group_id,
        "group_name": group_info.get("groupName"),
        "sender": sender,
        "sender_name": envelope.get("sourceName"),
        "sender_uuid": sender_uuid,
        "timestamp": timestamp,
        "text": text,
        "attachments": attachments,
        "is_edit": "editMessage" in envelope,
    }
    return message_id, payload


async def iter_sse_json(response: httpx.Response) -> AsyncIterator[dict[str, Any]]:
    data_lines: list[str] = []

    async for raw_line in response.aiter_lines():
        line = raw_line.rstrip("\r")

        if line == "":
            if data_lines:
                raw = "\n".join(data_lines)
                data_lines.clear()
                try:
                    value = json.loads(raw)
                except json.JSONDecodeError:
                    LOG.warning("Ignoring malformed SSE JSON frame")
                    continue
                if isinstance(value, dict):
                    yield value
            continue

        if line.startswith(":"):
            continue

        if line.startswith("data:"):
            data_lines.append(line[5:].lstrip())
            continue

        # Tolerate implementations that emit one JSON object per line.
        if line.startswith("{") and not data_lines:
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                LOG.warning("Ignoring malformed JSON line from Signal stream")
                continue
            if isinstance(value, dict):
                yield value

    if data_lines:
        try:
            value = json.loads("\n".join(data_lines))
        except json.JSONDecodeError:
            return
        if isinstance(value, dict):
            yield value


async def deliver_pending(
    client: httpx.AsyncClient,
    settings: Settings,
    outbox: Outbox,
) -> None:
    if not settings.downstream_webhook_url:
        return

    headers = {"Content-Type": "application/json"}
    if settings.downstream_bearer_token:
        headers["Authorization"] = f"Bearer {settings.downstream_bearer_token}"

    for row in outbox.pending():
        message_id = row["message_id"]
        payload = json.loads(row["payload"])
        attempts = int(row["attempts"])
        try:
            response = await client.post(
                settings.downstream_webhook_url,
                json=payload,
                headers=headers,
            )
            response.raise_for_status()
        except Exception as exc:  # network/status errors are retried via outbox
            outbox.mark_failed(message_id, attempts, str(exc))
            LOG.warning("Downstream delivery failed id=%s attempt=%s", message_id[:12], attempts + 1)
            continue

        outbox.mark_delivered(message_id)
        LOG.info("Delivered id=%s", message_id[:12])


async def retry_worker(
    client: httpx.AsyncClient,
    settings: Settings,
    outbox: Outbox,
) -> None:
    while True:
        await deliver_pending(client, settings, outbox)
        await asyncio.sleep(settings.retry_interval_seconds)


async def consume_events(
    client: httpx.AsyncClient,
    settings: Settings,
    outbox: Outbox,
) -> None:
    events_url = f"{settings.signal_cli_url}/api/v1/events"
    backoff = 1.0

    while True:
        try:
            async with client.stream("GET", events_url) as response:
                response.raise_for_status()
                LOG.info("Connected to Signal event stream")
                backoff = 1.0

                async for event in iter_sse_json(response):
                    normalized = normalize_group_message(event, settings.signal_group_id)
                    if not normalized:
                        continue

                    message_id, payload = normalized
                    if outbox.enqueue(message_id, payload):
                        LOG.info(
                            "Queued Signal message id=%s sender=%s timestamp=%s",
                            message_id[:12],
                            payload.get("sender"),
                            payload.get("timestamp"),
                        )
                        await deliver_pending(client, settings, outbox)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            LOG.warning("Signal stream disconnected: %s; reconnecting in %.0fs", exc, backoff)
            await asyncio.sleep(backoff)
            backoff = min(30.0, backoff * 2.0)


async def list_groups(client: httpx.AsyncClient, settings: Settings) -> list[dict[str, Any]]:
    params: dict[str, Any] = {}
    if settings.signal_account:
        params["account"] = settings.signal_account

    request: dict[str, Any] = {
        "jsonrpc": "2.0",
        "method": "listGroups",
        "id": "signal-group-ingest-list-groups",
    }
    if params:
        request["params"] = params

    response = await client.post(f"{settings.signal_cli_url}/api/v1/rpc", json=request)
    response.raise_for_status()
    body = response.json()
    result = body.get("result", [])
    if not isinstance(result, list):
        raise RuntimeError(f"Unexpected listGroups response: {body}")
    return result


async def run(settings: Settings) -> None:
    outbox = Outbox(settings.outbox_db)
    timeout = httpx.Timeout(connect=10.0, read=None, write=20.0, pool=20.0)

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            retry_task = asyncio.create_task(retry_worker(client, settings, outbox))
            try:
                await consume_events(client, settings, outbox)
            finally:
                retry_task.cancel()
                await asyncio.gather(retry_task, return_exceptions=True)
    finally:
        outbox.close()


async def print_groups(settings: Settings) -> None:
    timeout = httpx.Timeout(20.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        groups = await list_groups(client, settings)
    for group in groups:
        print(f"{group.get('id', '')}\t{group.get('name', '')}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest messages from one Signal group")
    parser.add_argument(
        "--list-groups",
        action="store_true",
        help="Print Signal group IDs and names, then exit",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    args = parse_args()
    settings = Settings.from_env()

    if args.list_groups:
        asyncio.run(print_groups(settings))
        return

    if not settings.downstream_webhook_url:
        LOG.warning(
            "DOWNSTREAM_WEBHOOK_URL is not set; messages will be safely queued in %s",
            settings.outbox_db,
        )
    asyncio.run(run(settings))


if __name__ == "__main__":
    main()
