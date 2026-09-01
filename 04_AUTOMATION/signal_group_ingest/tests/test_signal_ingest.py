import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from signal_ingest import Outbox, normalize_group_message, unwrap_receive_event


def make_event(group_id="group-123", message="hello", attachments=None):
    return {
        "account": "+380000000000",
        "envelope": {
            "sourceNumber": "+380111111111",
            "sourceUuid": "sender-uuid",
            "sourceName": "Operator",
            "timestamp": 1725148800000,
            "dataMessage": {
                "timestamp": 1725148800000,
                "message": message,
                "groupInfo": {
                    "groupId": group_id,
                    "groupName": "Test group",
                },
                "attachments": attachments or [],
            },
        },
    }


def test_normalizes_message_from_target_group():
    result = normalize_group_message(make_event(), "group-123")

    assert result is not None
    message_id, payload = result
    assert len(message_id) == 64
    assert payload["source"] == "signal"
    assert payload["group_id"] == "group-123"
    assert payload["sender"] == "+380111111111"
    assert payload["text"] == "hello"
    assert payload["timestamp"] == 1725148800000


def test_ignores_message_from_other_group():
    assert normalize_group_message(make_event(group_id="other-group"), "group-123") is None


def test_unwraps_json_rpc_receive_event():
    inner = make_event()
    wrapped = {
        "jsonrpc": "2.0",
        "method": "receive",
        "params": {"result": inner},
    }

    assert unwrap_receive_event(wrapped) == inner
    result = normalize_group_message(wrapped, "group-123")
    assert result is not None
    assert result[1]["text"] == "hello"


def test_accepts_attachment_only_message():
    event = make_event(
        message=None,
        attachments=[{"contentType": "image/jpeg", "filename": "photo.jpg"}],
    )

    result = normalize_group_message(event, "group-123")
    assert result is not None
    assert result[1]["text"] is None
    assert result[1]["attachments"][0]["filename"] == "photo.jpg"


def test_ignores_empty_non_attachment_message():
    assert normalize_group_message(make_event(message=""), "group-123") is None


def test_outbox_deduplicates_and_marks_delivered(tmp_path):
    outbox = Outbox(tmp_path / "outbox.sqlite3")
    payload = {"message_id": "abc", "text": "hello"}

    try:
        assert outbox.enqueue("abc", payload) is True
        assert outbox.enqueue("abc", payload) is False
        assert len(outbox.pending()) == 1

        outbox.mark_delivered("abc")
        assert outbox.pending() == []
    finally:
        outbox.close()
