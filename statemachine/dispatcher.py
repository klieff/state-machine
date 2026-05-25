from __future__ import annotations

from contextvars import ContextVar
from collections.abc import Callable
from dataclasses import dataclass, field
from time import time
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from .audit import AuditRecord, MicroStep


active_audit_record: ContextVar[AuditRecord] = ContextVar("active_audit_record")
active_log_record: ContextVar[LogRecord] = ContextVar("active_audit_record")


@dataclass(slots=True, frozen=True)
class BaseLog:
    type: str  # "ACTION_EXECUTION", "GUARD_EVALUATION", "TRANSITION_LEG"
    timestamp: float = field(default_factory=time)


@dataclass(slots=True, frozen=True)
class CallbackLog(BaseLog):
    name: str = ""  # e.g., "check_inventory_guard"
    status: str = ""  # e.g., "PASSED", "FAILED", "EXECUTED"
    error: Any = None  # Captured exception if it failed


@dataclass(slots=True, frozen=True)
class TransitionLog(BaseLog):
    source: str = ""
    target: str = ""
    event: str = ""
    payload: Any = None


@dataclass(slots=True)
class LogRecord:
    event: str
    success: bool = False
    exception: Any = None
    timeline: list[CallbackLog | TransitionLog] = field(default_factory=list)


# TODO: Placeholder exceptions - to be streamlined
#       Express callbacks as CallbackSpec objects
#       Implement logging levels: "SPARSE", "DEBUG", "VERBOSE" etc.
class EventDispatcher:
    def __init__(self) -> None:
        self._listeners = []

    def subscribe(self, callback: Callable[[AuditRecord], None]) -> None:
        self._listeners.append(callback)

    def emit(self, record: AuditRecord) -> None:
        for callback in self._listeners:
            try:
                callback(record)
            except Exception as e:
                raise RuntimeError("Dispatch error on emit.") from e

    def create_log(self, event: str, success: bool) -> LogRecord | None:
        if self._listeners:
            return LogRecord(event=event, success=success)

    def log_callback(
        self, type: str, name: str, status: str, error: Any = None
    ) -> None:
        if not self._listeners:
            return

        log = CallbackLog(type=type, name=name, status=status, error=error)
        try:
            record = active_log_record.get()
            record.timeline.append(log)
        except Exception as e:
            raise RuntimeError("Audit error on logging microstep.") from e

    def log_transition(
        self, type: str, source: str, target: str, event: str, payload: Any = None
    ) -> None:
        if not self._listeners:
            return

        log = TransitionLog(
            type=type, source=source, target=target, event=event, payload=payload
        )
        try:
            record = active_log_record.get()
            record.timeline.append(log)
        except Exception as e:
            raise RuntimeError("Audit error on logging microstep.") from e

    def log_micro_step(self, microstep: MicroStep):
        try:
            record = active_audit_record.get()
            record.timeline.append(microstep)
        except Exception as e:
            raise RuntimeError("Audit error on logging microstep.") from e

    def log_to_active_audit(self, field_name: str, value: str) -> None:
        try:
            record = active_audit_record.get()
            getattr(record, field_name).append(value)
        except LookupError as e:
            raise LookupError("Audit error on log record.") from e
