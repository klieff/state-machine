from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any

from .exceptions import InvalidState, TransitionMapError

type Action2[CTX, PAY] = Callable[[CTX, PAY], Awaitable[None] | None]
type Action[C] = Callable[[C], Awaitable[None] | None]
type Guard[C] = Callable[[C], Awaitable[bool] | bool]
type EntryExitAction[S, C] = dict[S, list[Action[C]]]
type TransitionAction[S, C] = dict[tuple[S, S], list[Action[C]]]
type Transition[S, C] = tuple[S, tuple[Action[C]] | None, tuple[Guard[C]] | None]
type TransitionMap[S, E, C] = dict[tuple[S, E | None], list[Transition[S, C]]]

type State = Enum
type Event = Enum


# TODO: Maybe use a dedicated Transition object rather than a generic
@dataclass(slots=True, frozen=True)
class TransitionObject:
    source: State
    target: State
    event: Event | None
    guards: tuple[Guard] | None = None
    actions: tuple[Action] | None = None


# TODO: Context that is passed to user-defined callbacks
@dataclass(slots=True, frozen=True)
class MachineContext:
    source: State
    target: State
    event: Event
    payload: Any
    machine: Any  # reference to SM instance


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
