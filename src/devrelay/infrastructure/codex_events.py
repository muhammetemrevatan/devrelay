"""Bounded one-turn Codex events; model text never supplies fixed-check authority."""

import hashlib
import json
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, NoReturn
from uuid import UUID

from devrelay.application.contracts.processes import ProcessStopStatus
from devrelay.application.contracts.trusted_processes import (
    TrustedProcessResult,
    TrustedProcessStopReason,
)


class FailureCode(StrEnum):
    MALFORMED = "malformed_events"
    LIMIT = "event_limit"
    INCOMPLETE = "incomplete_events"
    RUNTIME = "runtime_error"
    PROCESS = "process_not_verified"
    POLICY = "invocation_policy"


class AdapterFailure(ValueError):
    def __init__(self, code: FailureCode) -> None:
        self.code = code
        super().__init__("Codex adapter rejected execution: " + code.value)


def _reject(code: FailureCode = FailureCode.MALFORMED) -> NoReturn:
    raise AdapterFailure(code)


def _object(
    value: object, required: set[str], optional: set[str] | frozenset[str] = frozenset()
) -> dict[str, Any]:
    if type(value) is not dict or not required <= value.keys() <= required | optional:
        _reject()
    return value


def _text(value: object, maximum: int = 65536) -> str:
    if type(value) is not str or len(value.encode("utf-8", "surrogatepass")) > maximum:
        _reject()
    if any(0xD800 <= ord(char) <= 0xDFFF for char in value):
        _reject()
    return value


def _integer(value: object) -> int:
    if type(value) is not int or not 0 <= value <= 2**53 - 1:
        _reject()
    return value


def _pairs(values: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in values:
        if key in result:
            _reject()
        result[key] = value
    return result


def _noninteger(_: str) -> None:
    _reject()


@dataclass(frozen=True, slots=True)
class EventLimits:
    maximum_bytes: int = 1024 * 1024
    maximum_line_bytes: int = 64 * 1024
    maximum_events: int = 4096
    maximum_items: int = 512
    maximum_depth: int = 8
    maximum_nodes: int = 2048
    maximum_text_bytes: int = 64 * 1024

    def __post_init__(self) -> None:
        caps = (1024 * 1024, 64 * 1024, 4096, 512, 8, 2048, 64 * 1024)
        for name, cap in zip(self.__dataclass_fields__, caps, strict=True):
            value = getattr(self, name)
            if type(value) is not int or not 1 <= value <= cap:
                _reject(FailureCode.POLICY)


@dataclass(frozen=True, slots=True)
class CodexCompletion:
    execution_id: UUID
    thread_id: UUID
    event_count: int
    item_count: int
    final_text: str = field(repr=False)


_DEFAULT_LIMITS = EventLimits()


class CodexJsonlDecoder:
    """One fresh thread/turn only; bounded chunks and sticky parse failures."""

    def __init__(self, limits: EventLimits = _DEFAULT_LIMITS) -> None:
        limits.__post_init__()
        self.limits = limits
        self._buffer = bytearray()
        self._digest = hashlib.sha256()
        self._bytes = self._events = 0
        self._thread: UUID | None = None
        self._turn = self._terminal = self._closed = False
        self._failure: FailureCode | None = None
        self._items: dict[str, tuple[str, bool]] = {}
        self._text_bytes = 0
        self._final: str | None = None

    def _shape(self, line: bytes) -> None:
        depth = nodes = 0
        quoted = escaped = False
        for char in line:
            if quoted:
                if escaped:
                    escaped = False
                elif char == 92:
                    escaped = True
                elif char == 34:
                    quoted = False
            elif char == 34:
                quoted = True
            elif char in (123, 91):
                depth += 1
                nodes += 1
                if depth > self.limits.maximum_depth:
                    _reject(FailureCode.LIMIT)
            elif char in (125, 93):
                depth -= 1
            elif char in (44, 58):
                nodes += 1
            if nodes > self.limits.maximum_nodes:
                _reject(FailureCode.LIMIT)
        if quoted or depth != 0:
            _reject()

    def feed(self, chunk: bytes) -> None:
        if self._closed or self._failure not in (None, FailureCode.RUNTIME):
            _reject(self._failure or FailureCode.MALFORMED)
        try:
            if type(chunk) is not bytes:
                _reject()
            self._bytes += len(chunk)
            if self._bytes > self.limits.maximum_bytes:
                _reject(FailureCode.LIMIT)
            self._digest.update(chunk)
            # Never buffer an entire oversized chunk. Each retained suffix is
            # bounded by the line budget before extending the buffer.
            offset = 0
            while offset < len(chunk):
                end = chunk.find(b"\n", offset)
                stop = len(chunk) if end < 0 else end
                if len(self._buffer) + stop - offset > self.limits.maximum_line_bytes:
                    _reject(FailureCode.LIMIT)
                self._buffer.extend(chunk[offset:stop])
                if end < 0:
                    break
                line = bytes(self._buffer)
                self._buffer.clear()
                self._line(line)
                offset = end + 1
        except AdapterFailure as error:
            self._failure = error.code
            raise
        except ValueError, UnicodeError, TypeError, RecursionError:
            self._failure = FailureCode.MALFORMED
            raise AdapterFailure(FailureCode.MALFORMED) from None

    def _line(self, line: bytes) -> None:
        if not line or line.endswith(b"\r") or self._terminal:
            _reject()
        self._events += 1
        if self._events > self.limits.maximum_events:
            _reject(FailureCode.LIMIT)
        self._shape(line)
        event = json.loads(
            line.decode("utf-8", "strict"),
            object_pairs_hook=_pairs,
            parse_constant=_noninteger,
            parse_float=_noninteger,
        )
        if type(event) is not dict:
            _reject()
        kind = _text(event.get("type"), 64)
        if kind == "error":
            _object(event, {"type", "message"})
            _text(event["message"])
            # The pinned CLI also emits this diagnostic for a denied tool that
            # the turn can recover from. Only verified completion/exit below
            # establishes a completed turn; a diagnostic grants no authority.
            return
        if kind == "thread.started":
            _object(event, {"type", "thread_id"})
            if self._thread is not None or self._turn:
                _reject()
            thread = _text(event["thread_id"], 36)
            parsed = UUID(thread)
            if str(parsed) != thread:
                _reject()
            self._thread = parsed
            return
        if self._thread is None:
            _reject()
        if kind == "turn.started":
            _object(event, {"type"})
            if self._turn:
                _reject()
            self._turn = True
        elif kind in ("turn.completed", "turn.failed"):
            if not self._turn:
                _reject()
            if kind == "turn.failed":
                _object(event, {"type", "error"})
                error = _object(event["error"], {"message"})
                _text(error["message"])
                self._failure = FailureCode.RUNTIME
            else:
                _object(event, {"type", "usage"})
                usage = _object(
                    event["usage"],
                    {"input_tokens", "cached_input_tokens", "output_tokens"},
                    {"cache_write_input_tokens", "reasoning_output_tokens"},
                )
                for number in usage.values():
                    _integer(number)
                if any(not complete for _, complete in self._items.values()):
                    _reject(FailureCode.INCOMPLETE)
            self._terminal = True
        elif kind in ("item.started", "item.updated", "item.completed"):
            _object(event, {"type", "item"})
            self._item(event["item"], kind)
        else:
            _reject()

    def _item(self, value: object, event: str) -> None:
        item = _object(
            value,
            {"id", "type"},
            set(value) - {"id", "type"} if type(value) is dict else set(),
        )
        identity, kind = _text(item["id"], 128), _text(item["type"], 64)
        if not identity or (not self._turn and kind != "error"):
            _reject()
        previous = self._items.get(identity)
        completed = event == "item.completed"
        if previous is None:
            if event == "item.updated":
                _reject()
            if len(self._items) >= self.limits.maximum_items:
                _reject(FailureCode.LIMIT)
        elif previous[0] != kind or previous[1] or event == "item.started":
            _reject()
        if kind in ("agent_message", "reasoning"):
            _object(item, {"id", "type", "text"})
            text = _text(item["text"])
            self._text_bytes += len(text.encode("utf-8"))
            if self._text_bytes > self.limits.maximum_text_bytes:
                _reject(FailureCode.LIMIT)
            if kind == "agent_message" and completed:
                self._final = text
        elif kind == "error":
            _object(item, {"id", "type", "message"})
            _text(item["message"])
        elif kind == "command_execution":
            _object(
                item,
                {"id", "type", "command", "aggregated_output", "exit_code", "status"},
            )
            _text(item["command"], 32768)
            _text(item["aggregated_output"])
            if (
                item["exit_code"] is not None
                and _integer(item["exit_code"]) > 0xFFFFFFFF
            ):
                _reject()
            if (
                completed
                and item["status"] == "completed"
                and item["exit_code"] is None
            ) or (not completed and item["exit_code"] is not None):
                _reject()
            self._status(item, completed)
        elif kind == "file_change":
            _object(item, {"id", "type", "changes", "status"})
            if (
                type(item["changes"]) is not list
                or not 1 <= len(item["changes"]) <= 128
            ):
                _reject()
            for change in item["changes"]:
                _object(change, {"path", "kind"})
                _text(change["path"], 4096)
                if change["kind"] not in ("add", "delete", "update"):
                    _reject()
            self._status(item, completed)
        elif kind == "todo_list":
            _object(item, {"id", "type", "items"})
            if type(item["items"]) is not list or len(item["items"]) > 128:
                _reject()
            for todo in item["items"]:
                _object(todo, {"text", "completed"})
                _text(todo["text"], 4096)
                if type(todo["completed"]) is not bool:
                    _reject()
        else:
            # MCP, web, new item/tool variants require an explicit profile update.
            _reject()
        self._items[identity] = (kind, completed)

    def _status(self, item: dict[str, Any], completed: bool) -> None:
        if item["status"] != ("completed" if completed else "in_progress"):
            if completed and item["status"] == "failed":
                pass  # A failed exploratory tool can be recovered within the turn.
            else:
                _reject()

    def finish(
        self, result: TrustedProcessResult, execution_id: UUID
    ) -> CodexCompletion:
        if self._closed:
            _reject()
        self._closed = True
        if self._failure is not None:
            _reject(self._failure)
        if (
            self._buffer
            or not self._turn
            or not self._terminal
            or self._thread is None
            or self._final is None
        ):
            _reject(FailureCode.INCOMPLETE)
        if not isinstance(result, TrustedProcessResult):
            _reject(FailureCode.PROCESS)
        try:
            result.__post_init__()
        except ValueError, TypeError:
            raise AdapterFailure(FailureCode.PROCESS) from None
        if (
            result.execution_id != execution_id
            or result.reason is not TrustedProcessStopReason.EXITED
            or result.exit_code != 0
            or result.stop_status is not ProcessStopStatus.EMPTY_VERIFIED
            or hashlib.sha256(result.stdout).digest() != self._digest.digest()
            or len(result.stdout) != self._bytes
        ):
            _reject(FailureCode.PROCESS)
        return CodexCompletion(
            execution_id, self._thread, self._events, len(self._items), self._final
        )
