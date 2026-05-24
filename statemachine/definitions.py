from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, Iterable

from .callbacks import CallbackSpec
from .exceptions import InvalidState, TransitionMapError

type EventSpec = Enum | str
type StateSpec = Enum | str
type RouterSpec = CallbackSpec
type Callbacks = Iterable[Callable] | Callable
type EntryExitAction[S] = dict[S, list[State]]
type TransitionAction[S] = dict[tuple[S, S], list[CallbackSpec]]
type TransitionMap = dict[tuple[StateSpec, EventSpec | None], list[Transition]]

# type TransitionMap[S, E, C, I] = dict[tuple[S, E | None], list[Transition[S, C, I]]]
# type Action[C, I] = Callable[[C, I], Awaitable[None] | None]
# type Guard[C, I] = Callable[[C, I], Awaitable[bool] | bool]
# type EntryExitAction[S, C, I] = dict[S, list[Action[C, I]]]
# type TransitionAction[S, C, I] = dict[tuple[S, S], list[Action[C, I]]]
# type Transition[S, C, I] = tuple[
#     S, tuple[Action[C, I]] | None, tuple[Guard[C, I]] | None
# ]
# type TransitionMap[S, E, C, I] = dict[tuple[S, E | None], list[Transition[S, C, I]]]


class EngineEvent(Enum):
    MACHINE_START = auto()
    MACHINE_STOP = auto()
    MICRO_STEP = auto()
    EVENT_TRIGGER = auto()
    TRANSITION_START = auto()
    TRANSITION_COMPLETE = auto()
    TRANSITION_FAIL = auto()
    NULL_TRANSITION = auto()
    EXCEPTION = auto()


class EngineStep(Enum):
    GUARD_SKIP = auto()
    GUARD_EVALUATE = auto()
    ON_ENTRY = auto()
    ON_EXIT = auto()
    ON_TRANSITION = auto()
    STATE_CHANGE = auto()
    TRANSITION_ACTION = auto()


@dataclass(slots=True)
class State:
    state: StateSpec
    on_exit: list[CallbackSpec | None]
    on_entry: list[CallbackSpec | None]
    final_state: bool = False


@dataclass(slots=True)
class Transition:
    source: StateSpec
    event: EventSpec | None
    target: StateSpec | None
    actions: list[CallbackSpec | None]
    guards: list[CallbackSpec | None]
    router: RouterSpec | None = None


# TODO: Context that is passed to user-defined callbacks
@dataclass(slots=True, frozen=True)
class MachineContext[S: Enum, E: Enum]:
    source: S
    target: S
    event: E
    payload: Any
    machine_instance: Any


@dataclass(frozen=True)
class StateMachineConfig:
    name: str
    events: dict[EventSpec, EventSpec]
    states: dict[StateSpec, State]
    transitions: TransitionMap
    # on_transition: TransitionAction
    verbose: bool

    def __post_init__(self) -> None:
        if not self.transitions:
            raise TransitionMapError(machine_name=self.name)

        for state in self.states.keys():
            if not isinstance(state, (Enum, str)):
                raise TypeError(
                    f"State '{state}' must be an Enum, not {type(state).__name__}."
                )

        for event in self.events:
            if not isinstance(event, (Enum, str)) and event is not None:
                raise TypeError(
                    f"Event '{event}' must be an Enum, not {type(event).__name__}."
                )


# @dataclass(slots=True)
# class EventDetails[S: Enum, E: Enum, C]:
#     source: S | None = None
#     target: S | None = None
#     event: E | None = None
#     action: Action[C] | None = None
#     action_type: str | None = None
#     guard: Guard[C] | None = None
#     passed: bool | None = None
#     error_type: str | None = None
#     error_message: str | None = None
#     original_exception: str | None = None
#
#
# @dataclass(slots=True)
# class EventRecord[S: Enum, E: Enum, C]:
#     machine: str
#     machine_event: str
#     details: EventDetails[S, E, C]
#     timestamp: datetime = field(default_factory=datetime.now)
