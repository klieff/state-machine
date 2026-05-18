from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

from .definitions import EngineEvent
from .exceptions import InvalidState, TransitionMapError

if TYPE_CHECKING:
    from .definitions import EventRecord, StateMachineConfig


def format_event_log(record: EventRecord) -> str:
    details = record.details
    timestamp = record.timestamp.strftime("%H:%M:%S.%f")[:-3]
    line = f"[{timestamp}] <{record.machine}> {record.machine_event:<20} | Source: {details.source}"

    detail_str = ""
    if EngineEvent.EVENT_TRIGGER.name in record.machine_event:
        detail_str += f" Event: {details.event}"
    if details.target and record.machine_event != EngineEvent.GUARD_SKIP.name:
        detail_str += f" -> Target: {details.target}"
    if details.action:
        detail_str += f" Action: {details.action}"
    if details.guard:
        res = "PASS" if details.passed else "FAIL"
        detail_str += f" Guard [{res}]: {details.guard}"
    if details.error_message:
        detail_str += f" Exception [{details.error_type}]: {details.error_message}"

    return f"{line}{detail_str}"


def get_obj_name(obj) -> str:
    name = getattr(obj, "name", None)
    return name or getattr(obj, "__name__", type(obj).__name__)


def ensure_tuple(obj) -> tuple:
    if callable(obj):
        return (obj,)
    elif isinstance(obj, (list, set, tuple)):
        return tuple(obj)
    return ()


def validate_config(config: StateMachineConfig) -> None:
    initial_state = config.initial_state
    if not config.transitions and initial_state not in config.on_exit:
        raise TransitionMapError(machine_name=config.name)

    if initial_state not in config.states:
        raise InvalidState(initial_state=initial_state)

    state_type = type(initial_state)
    for state in config.states:
        if not isinstance(state, Enum):
            raise TypeError(
                f"State '{state}' must be an Enum, not {type(state).__name__}."
            )

        if not isinstance(state, state_type):
            raise TypeError(
                f"Inconsistent Enum class: '{state}' is a {type(state).__name__}, "
                f"but the machine expects {state_type.__name__}."
            )

    for event in config.events:
        if not isinstance(event, Enum) and event is not None:
            raise TypeError(
                f"Event '{event}' must be an Enum, not {type(event).__name__}."
            )
