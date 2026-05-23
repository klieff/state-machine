from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from enum import Enum


@dataclass(slots=True)
class MicroStep:
    micro_step: str = ""
    target: str = ""
    result: Any = None
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass(slots=True)
class AuditRecord:
    machine_event: str = ""
    source: Enum | str = ""
    target: Enum | str = ""
    trigger_event: str = ""
    payload: object | None = None
    success: bool = True
    timeline: list[MicroStep] = field(default_factory=list)


class AuditLogger:
    def __init__(self) -> None:
        pass
