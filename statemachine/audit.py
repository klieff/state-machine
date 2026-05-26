from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from .definitions import StateSpec, EventSpec, TransitionInfo


@dataclass(slots=True)
class MicroStep:
    micro_step: str = ""
    target: str = ""
    result: Any = None
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass(slots=True)
class AuditRecord:
    event: EventSpec | None
    source: StateSpec = ""
    target: StateSpec = ""
    trigger_event: EventSpec | None = None
    success: bool = False
    exception: Exception | None = None
    transitions: list[TransitionInfo] = field(default_factory=list)
    timeline: list[MicroStep] = field(default_factory=list)


class AuditLogger:
    def __init__(self) -> None:
        pass
