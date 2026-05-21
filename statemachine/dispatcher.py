from __future__ import annotations

from contextvars import ContextVar
from collections.abc import Callable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .audit import AuditRecord, MicroStep


active_audit_record: ContextVar[AuditRecord] = ContextVar("active_audit_record")


# TODO: Placeholder exceptions - to be streamlined
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
