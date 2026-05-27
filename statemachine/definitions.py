from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, Iterable

from .callbacks import CallbackSpec
from .exceptions import TransitionMapError

type EventSpec = Enum | str
type StateSpec = Enum | str
type RouterSpec = CallbackSpec
type Callbacks = Iterable[Callable] | Callable
type EntryExitAction[S] = dict[S, list[State]]
type TransitionAction[S] = dict[tuple[S, S], list[CallbackSpec]]
type TransitionMap = dict[tuple[StateSpec, EventSpec | None], list[Transition]]


class EngineEvent(Enum):
    MACHINE_START = auto()
    MACHINE_STOP = auto()
    MICRO_STEP = auto()
    EVENT_TRIGGER = auto()
    TRANSITION_START = auto()
    TRANSITION_COMPLETE = auto()
    TRANSITION_FAIL = auto()
    AUTOMATIC_TRANSITION = auto()
    DYNAMIC_TRANSITION = auto()
    EXCEPTION = auto()


class EngineStep(Enum):
    GUARD_EVALUATE = auto()
    ON_ENTRY_EVALUATE = auto()
    ON_EXIT_EVALUATE = auto()
    ON_TRANSITION = auto()
    ACTION_EVALUATE = auto()
    ROUTER_EVALUATE = auto()
    STATE_CHANGE = auto()


class StateType(Enum):
    STANDARD = auto()
    AUTOMATIC = auto()
    CHOICE = auto()


@dataclass(slots=True)
class State:
    type: StateType
    name: str
    state: StateSpec
    on_exit: list[CallbackSpec]
    on_entry: list[CallbackSpec]
    final_state: bool = False


@dataclass(slots=True)
class Transition:
    source: State
    event: EventSpec
    target: State
    actions: list[CallbackSpec]
    guards: list[CallbackSpec]
    router: RouterSpec | None = None


@dataclass(slots=True)
class TransitionInfo:
    machine: Any
    payload: Any = None
    source: StateSpec | None = None
    event: EventSpec | None = None
    target: StateSpec | None = None
    step: str = ""


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
                    f"State '{state}' must be an Enum or str, not {type(state).__name__}."
                )

        for event in self.events:
            if not isinstance(event, (Enum, str)) and event is not None:
                raise TypeError(
                    f"Event '{event}' must be an Enum or str, not {type(event).__name__}."
                )

        for transitions in self.transitions.values():
            for t in transitions:
                source = t.source.name
                target = t.target.name if t.target else t.target
                if source not in self.states:
                    raise TypeError(f"Source state '{source}' is not a valid state.")

                if target not in self.states and target is not None:
                    raise TypeError(f"Target state '{target}' is not a valid state.")
