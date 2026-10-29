import json
from dataclasses import replace
from typing import Any
from uuid import uuid4

import pytest

from devrelay.application.contracts.processes import ProcessStopStatus
from devrelay.application.contracts.trusted_processes import (
    TrustedProcessResult,
    TrustedProcessStopReason,
)
from devrelay.infrastructure.codex_events import (
    AdapterFailure,
    CodexJsonlDecoder,
    EventLimits,
)


def events() -> bytes:
    values = [
        {"type": "thread.started", "thread_id": str(uuid4())},
        {"type": "turn.started"},
        {
            "type": "item.completed",
            "item": {
                "id": "cmd",
                "type": "command_execution",
                "command": "test -e absent",
                "aggregated_output": "",
                "exit_code": 1,
                "status": "failed",
            },
        },
        {
            "type": "item.completed",
            "item": {"id": "answer", "type": "agent_message", "text": "Fixed ✔"},
        },
        {
            "type": "turn.completed",
            "usage": {"input_tokens": 1, "cached_input_tokens": 0, "output_tokens": 1},
        },
    ]
    return b"".join(
        json.dumps(item, ensure_ascii=False).encode() + b"\n" for item in values
    )


def process(raw: bytes) -> TrustedProcessResult:
    return TrustedProcessResult(
        execution_id=uuid4(),
        reason=TrustedProcessStopReason.EXITED,
        exit_code=0,
        stdout=raw,
        stderr=b"",
        stop_status=ProcessStopStatus.EMPTY_VERIFIED,
    )


def test_recovered_exploratory_failure_and_split_utf8_are_valid() -> None:
    raw = events()
    decoder = CodexJsonlDecoder()
    for byte in raw:
        decoder.feed(bytes([byte]))
    result = process(raw)
    assert decoder.finish(result, result.execution_id).final_text == "Fixed ✔"
    with pytest.raises(AdapterFailure):
        decoder.finish(result, result.execution_id)


@pytest.mark.parametrize(
    "change",
    [
        {"exit_code": 1},
        {"stop_status": ProcessStopStatus.UNKNOWN},
        {"stdout": b"different"},
        {"execution_id": uuid4()},
    ],
)
def test_model_completion_cannot_replace_fresh_process_settlement(
    change: dict[str, Any],
) -> None:
    raw = events()
    result = process(raw)
    decoder = CodexJsonlDecoder()
    decoder.feed(raw)
    with pytest.raises(AdapterFailure):
        decoder.finish(replace(result, **change), result.execution_id)


@pytest.mark.parametrize(
    "raw",
    [
        b'{"type":"thread.started","type":"error"}\n',
        events().replace(b'"input_tokens": 1', b'"input_tokens": NaN'),
        events().replace(b'"command_execution"', b'"mcp_tool_call"'),
        events() + b'{"type":"turn.started"}\n',
        b"\xff\n",
    ],
)
def test_malformed_or_unapproved_events_fail_sticky(raw: bytes) -> None:
    decoder = CodexJsonlDecoder()
    with pytest.raises(AdapterFailure):
        decoder.feed(raw)
    with pytest.raises(AdapterFailure):
        decoder.feed(events())


def test_unterminated_stream_and_oversized_event_fail() -> None:
    raw = events()
    decoder = CodexJsonlDecoder()
    decoder.feed(raw[:-1])
    with pytest.raises(AdapterFailure):
        decoder.finish(process(raw[:-1]), uuid4())
    decoder = CodexJsonlDecoder(EventLimits(maximum_line_bytes=8))
    with pytest.raises(AdapterFailure):
        decoder.feed(b"x" * 9)


def test_denied_tool_diagnostic_can_recover_but_cannot_replace_completion() -> None:
    raw = events().replace(
        b'{"type": "turn.started"}\n',
        b'{"type": "turn.started"}\n{"type":"error","message":"Tool permission denied"}\n',
    )
    decoder = CodexJsonlDecoder()
    decoder.feed(raw)
    result = process(raw)
    assert decoder.finish(result, result.execution_id).final_text == "Fixed ✔"
    decoder = CodexJsonlDecoder()
    incomplete = raw[: raw.rfind(b'{"type": "turn.completed"')]
    decoder.feed(incomplete)
    with pytest.raises(AdapterFailure):
        decoder.finish(process(incomplete), result.execution_id)
