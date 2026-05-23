from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, Iterable

from .callbacks import CallbackSpec
from .exceptions import InvalidState, TransitionMapError

type Callbacks = Iterable[Callable] | Callable | None
type EntryExitAction[S] = dict[S, list[State]]
type TransitionAction[S] = dict[tuple[S, S], list[CallbackSpec]]
type TransitionMap[S, E] = dict[tuple[S, E | None], list[Transition]]

# type TransitionMap[S, E, C, I] = dict[tuple[S, E | None], list[Transition[S, C, I]]]
# type Action[C, I] = Callable[[C, I], Awaitable[None] | None]
# type Guard[C, I] = Callable[[C, I], Awaitable[bool] | bool]
# type EntryExitAction[S, C, I] = dict[S, list[Action[C, I]]]
# type TransitionAction[S, C, I] = dict[tuple[S, S], list[Action[C, I]]]
# type Transition[S, C, I] = tuple[
#     S, tuple[Action[C, I]] | None, tuple[Guard[C, I]] | None
# ]
# type TransitionMap[S, E, C, I] = dict[tuple[S, E | None], list[Transition[S, C, I]]]


@dataclass(slots=True)
class State[S: Enum]:
    state: S
    on_exit: list[CallbackSpec | None]
    on_entry: list[CallbackSpec | None]


@dataclass(slots=True)
class Transition[S: Enum, E: Enum]:
    source: S
    target: S | None
    event: E | None
    actions: list[CallbackSpec | None]
    guards: list[CallbackSpec | None]


# TODO: Maybe use a dedicated Transition object rather than a generic
@dataclass(slots=True, frozen=True)
class TransitionInfo[S: Enum, E: Enum]:
    source: S
    target: S
    event: E | None
    guards: tuple[Callable] | None = None
    actions: tuple[Callable] | None = None


# TODO: Context that is passed to user-defined callbacks
@dataclass(slots=True, frozen=True)
class MachineContext[S: Enum, E: Enum]:
    source: S
    target: S
    event: E
    payload: Any
    machine_instance: Any


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


@dataclass(frozen=True)
class StateMachineConfig[S: Enum, E: Enum]:
    name: str
    initial_state: S
    events: set[E]
    states: set[S]
    on_entry: EntryExitAction[S]
    on_exit: EntryExitAction[S]
    on_transition: TransitionAction[S]
    transitions: TransitionMap[S, E]
    verbose: bool

    def __post_init__(self) -> None:
        initial_state = self.initial_state
        if not self.transitions and initial_state not in self.on_exit:
            raise TransitionMapError(machine_name=self.name)

        if initial_state not in self.states:
            raise InvalidState(initial_state=initial_state)

        state_type = type(initial_state)
        for state in self.states:
            if not isinstance(state, (Enum, Callable, type(None))):
                raise TypeError(
                    f"State '{state}' must be an Enum, not {type(state).__name__}."
                )

            if not isinstance(state, (state_type, Callable, type(None))):
                raise TypeError(
                    f"Inconsistent Enum class: '{state}' is a {type(state).__name__}, "
                    f"but the machine expects {state_type.__name__}."
                )

        for event in self.events:
            if not isinstance(event, Enum) and event is not None:
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
