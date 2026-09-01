# Signal Group Ingest — implementation status

**Date:** 2026-09-01  
**Branch:** `feature/signal-group-ingest`  
**Owner:** ChatGPT  
**Status:** code complete / live binding pending

## Completed

- Implemented `signal-cli` HTTP/SSE consumer.
- Exact filter by Signal internal `group_id`.
- Normalized event payload for downstream processing.
- SQLite outbox with deduplication, retry and delivery state.
- Automatic SSE reconnect with bounded exponential backoff.
- `--list-groups` discovery before `SIGNAL_GROUP_ID` is known.
- Environment-only runtime configuration; secrets are not committed.
- Local `.env`, SQLite outbox and virtualenv are ignored by Git.
- Unit tests cover group filtering, JSON-RPC wrapper, attachment-only events, empty events and outbox deduplication.
- GitHub Actions runs `py_compile` and `pytest -q` on module changes.

## Verification evidence

Workflow run `33453334142` on commit `0a3b34313107ab81414924cfa19f56eae147813e` completed successfully.

- Python: 3.12.14
- `python -m py_compile signal_ingest.py`: success
- `python -m pytest -q`: `6 passed in 0.07s`

A new CI run is required after any subsequent module change before merge.

## Production binding still required

The adapter itself is ready, but production cannot be enabled without runtime-specific values:

1. A legitimately linked `signal-cli` account that is already a member of the target group.
2. The target group's internal `SIGNAL_GROUP_ID` from `python signal_ingest.py --list-groups`.
3. The real endpoint/contract of the existing parser or bot in `DOWNSTREAM_WEBHOOK_URL`.
4. A live smoke test with one non-sensitive test message before operational use.

## Do not change

- Do not write directly into the existing report sheets from this adapter unless that is a separately approved iteration.
- Do not expose the `signal-cli` HTTP daemon publicly; keep it on localhost/private network.
- Do not commit Signal credentials, `.env`, message outbox data, or message content logs.
