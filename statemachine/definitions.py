from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto

from .exceptions import InvalidState, TransitionMapError

type Action[C] = Callable[[C], Awaitable[None] | None]
type Guard[C] = Callable[[C], Awaitable[bool] | bool]
type EntryExitAction[S, C] = dict[S, list[Action[C]]]
type TransitionAction[S, C] = dict[tuple[S, S], list[Action[C]]]
type TransitionMap[S, E, C] = dict[
    tuple[S, E | None], list[tuple[S, tuple[Action[C]] | None, tuple[Guard[C]] | None]]
]


class EngineEvent(Enum):
    MACHINE_START = auto()
    EVENT_TRIGGER = auto()
    TRANSITION_START = auto()
    TRANSITION_ACTION = auto()
    TRANSITION_COMPLETE = auto()
    TRANSITION_FAIL = auto()
    GUARD_SKIP = auto()
    GUARD_EVALUATE = auto()
    ON_ENTRY = auto()
    ON_EXIT = auto()
    ON_TRANSITION = auto()
    STATE_CHANGE = auto()
    EXCEPTION = auto()


@dataclass(frozen=True)
class StateMachineConfig[S: Enum, E: Enum, C]:
    name: str
    initial_state: S
    events: set[E]
    states: set[S]
    on_entry: EntryExitAction[S, C]
    on_exit: EntryExitAction[S, C]
    on_transition: TransitionAction[S, C]
    transitions: TransitionMap[S, E, C]
    verbose: bool

    def __post_init__(self) -> None:
        initial_state = self.initial_state
        if not self.transitions and initial_state not in self.on_exit:
            raise TransitionMapError(machine_name=self.name)

        if initial_state not in self.states:
            raise InvalidState(initial_state=initial_state)

        state_type = type(initial_state)
        for state in self.states:
            if not isinstance(state, Enum):
                raise TypeError(
                    f"State '{state}' must be an Enum, not {type(state).__name__}."
                )

            if not isinstance(state, state_type):
                raise TypeError(
                    f"Inconsistent Enum class: '{state}' is a {type(state).__name__}, "
                    f"but the machine expects {state_type.__name__}."
                )

        for event in self.events:
            if not isinstance(event, Enum) and event is not None:
                raise TypeError(
                    f"Event '{event}' must be an Enum, not {type(event).__name__}."
                )


@dataclass(slots=True)
class EventDetails[S: Enum, E: Enum, C]:
    source: S | None = None
    target: S | None = None
    event: E | None = None
    action: Action[C] | None = None
    action_type: str | None = None
    guard: Guard[C] | None = None
    passed: bool | None = None
    error_type: str | None = None
    error_message: str | None = None
    original_exception: str | None = None


@dataclass(slots=True)
class EventRecord[S: Enum, E: Enum, C]:
    machine: str
    machine_event: str
    details: EventDetails[S, E, C]
    timestamp: datetime = field(default_factory=datetime.now)
