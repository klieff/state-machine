from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from time import time

from .definitions import StateSpec, EventSpec, TransitionInfo


@dataclass(slots=True)
class MicroStep:
    micro_step: str = ""
    target: str = ""
    result: Any = None
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass(slots=True)
class BaseHistory:
    type: str  # "ACTION_EXECUTION", "GUARD_EVALUATION", "TRANSITION_LEG"
    timestamp: float = field(default_factory=time)


@dataclass(slots=True)
class CallbackHistory(BaseHistory):
    name: str = ""  # e.g., "check_inventory_guard"
    status: str = ""  # e.g., "PASSED", "FAILED", "EXECUTED"
    error: Any = None  # Captured exception if it failed
    passed_vars: list[str] | None = None


@dataclass(slots=True)
class TransitionHistory(BaseHistory):
    source: str = ""
    target: str = ""
    event: str = ""
    payload: Any = None


a = TransitionHistory


@dataclass(slots=True)
class LogRecord:
    initial_event: str
    status: str = "PENDING"
    timeline: list[CallbackHistory | TransitionHistory] = field(default_factory=list)
    exception: Any = None


@dataclass(slots=True)
class AuditRecord:
    event: EventSpec | None
    source: StateSpec = ""
    target: StateSpec = ""
    trigger_event: EventSpec | None = None
    success: bool = True
    exception: Exception | None = None
    transitions: list[TransitionInfo] = field(default_factory=list)
    timeline: list[MicroStep] = field(default_factory=list)


class AuditLogger:
    def __init__(self) -> None:
        pass
