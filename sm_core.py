import itertools
from collections import deque
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum, IntEnum, StrEnum, auto
from inspect import isawaitable
from types import MappingProxyType
from typing import ClassVar
from exceptions import (
    ActionError,
    BlockedTransition,
    GuardError,
    InvalidState,
    InvalidTransition,
    TransitionMapError,
)

MAX_EVENTLOG = 200

type Action[C] = Callable[[C], Awaitable[None] | None]
type Guard[C] = Callable[[C], Awaitable[bool] | bool]
type EntryExitAction[S, C] = dict[S, list[Action[C]]]
type TransitionAction[S, C] = dict[tuple[S, S], list[Action[C]]]
type TransitionMap[S, E, C] = dict[
    tuple[S, E], list[tuple[S, Action[C] | None, Guard[C] | None]]
]
type ProxyEntryExitAction[S, C] = MappingProxyType[S, tuple[Action[C], ...]]
type ProxyTransitionAction[S, C] = MappingProxyType[tuple[S, S], tuple[Action[C], ...]]
type ProxyTransitionMap[S, E, C] = MappingProxyType[
    tuple[S, E], tuple[tuple[S, Action[C] | None, Guard[C] | None], ...]
]


class InternalEvent(Enum):
    EVENT_TRIGGER = auto()
    TRANSITION_START = auto()
    TRANSITION_ACTION = auto()
    TRANSITION_COMPLETE = auto()
    GUARD_SKIP = auto()
    GUARD_EVALUATE = auto()
    ON_ENTRY = auto()
    ON_EXIT = auto()
    ON_TRANSITION = auto()
    STATE_CHANGE = auto()
    EXCEPTION = auto()


class ReserveredEvent(Enum):
    ANY = auto()
    ERROR = auto()


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
    details: EventDetails[S, E, C] = field(default_factory=EventDetails)
    timestamp: datetime = field(default_factory=datetime.now)


def _format_eventlog(record: EventRecord) -> str:
    details = record.details
    timestamp = record.timestamp.strftime("%H:%M:%S")
    line = f"[{timestamp}] <{record.machine}> {record.machine_event:<20} | Source: {details.source}"

    detail_str = ""
    if InternalEvent.EVENT_TRIGGER.name in record.machine_event:
        detail_str += f" Event: {details.event}"
    if details.target and record.machine_event != InternalEvent.GUARD_SKIP.name:
        detail_str += f" -> Target: {details.target}"
    if details.action:
        detail_str += f" Action: {details.action}"
    if details.guard:
        res = "PASS" if details.passed else "FAIL"
        detail_str += f" Guard [{res}]: {details.guard}"
    if details.error_message:
        detail_str += f" Exception [{details.error_type}]: {details.error_message}"

    return f"{line}{detail_str}"


class StateMachineMixin[S: Enum, E: Enum, C]:
    __slots__ = ()
    _name: str
    _transitions: ProxyTransitionMap[S, E, C]
    _eventlog: deque[EventRecord]
    _verbose: bool

    def get_state_events(self, state: S) -> list[E]:
        return [event for (s, event) in self._transitions.keys() if s == state]

    def _apply_transition(self, event: E, target_state: S) -> None:
        source_state = self._state
        self._state = target_state
        self._dispatch_eventlog(
            machine_event=InternalEvent.STATE_CHANGE,
            source=source_state,
            target=target_state,
            event=event,
        )

    def _dispatch_eventlog(self, machine_event: InternalEvent, **kwargs) -> None:
        record = self._record_eventlog(machine_event, **kwargs)
        if self._verbose:
            print(_format_eventlog(record))

    def _get_name(self, obj) -> str:
        name = getattr(obj, "name", None)
        return name or getattr(obj, "__name__", type(obj).__name__)

    def _record_eventlog(
        self, machine_event: InternalEvent, **kwargs
    ) -> EventRecord[S, E, C]:
        func_keys = frozenset(("action", "action_type", "guard"))
        for key in func_keys:
            if key in kwargs:
                kwargs[key] = self._get_name(kwargs[key])

        record = EventRecord[S, E, C](
            machine=self._name,
            machine_event=machine_event.name,
            details=EventDetails[S, E, C](**kwargs),
        )

        self._eventlog.append(record)
        return record

    def _resolve_transitions(
        self, event: E
    ) -> tuple[tuple[S, Action[C] | None, Guard[C] | None], ...]:
        if not (transitions := self._transitions.get((self._state, event))):
            self._dispatch_eventlog(
                machine_event=InternalEvent.EXCEPTION,
                source=self._state,
                event=event,
                error_type=InvalidTransition.__name__,
                error_message=f"No transition map registered for {event}",
            )
            raise InvalidTransition(record=asdict(self._eventlog[-1]))

        return transitions


@dataclass(slots=True)
class StateMachine[S: Enum, E: Enum, C](StateMachineMixin[S, E, C]):
    _name: str
    _state: S
    _transitions: ProxyTransitionMap[S, E, C]
    _on_entry: ProxyEntryExitAction[S, C]
    _on_exit: ProxyEntryExitAction[S, C]
    _on_transition: ProxyTransitionAction[S, C]
    _eventlog: deque[EventRecord] = field(
        default_factory=lambda: deque(maxlen=MAX_EVENTLOG)
    )
    _verbose: bool = False

    def trigger(self, event: E, context: C) -> dict[str, S | E]:
        source_state = self._state
        self._dispatch_eventlog(
            machine_event=InternalEvent.EVENT_TRIGGER, event=event, source=self._state
        )

        transitions = self._resolve_transitions(event)
        target_state, action = self._evaluate_guards(event, context, transitions)

        self._dispatch_eventlog(
            machine_event=InternalEvent.TRANSITION_START,
            source=source_state,
            target=target_state,
            event=event,
        )

        self._execute_on_exit(event, context)
        self._execute_transition_action(event, action, context)
        self._apply_transition(event, target_state)
        self._execute_on_entry(event, context)
        self._execute_on_transition(source_state, event, target_state, context)

        self._dispatch_eventlog(
            machine_event=InternalEvent.TRANSITION_COMPLETE,
            source=source_state,
            target=target_state,
            event=event,
        )
        return dict(source=source_state, target=target_state, event=event)

    def _execute_guard(
        self, event: E, context: C, guard: Guard[C], target_state: S
    ) -> bool | Awaitable[bool]:
        try:
            passed = guard(context)
            self._dispatch_eventlog(
                machine_event=InternalEvent.GUARD_EVALUATE,
                source=self._state,
                # target=target_state,
                event=event,
                guard=guard,
                passed=passed,
            )
            return passed
        except Exception as e:
            self._dispatch_eventlog(
                machine_event=InternalEvent.EXCEPTION,
                source=self._state,
                # target=target_state,
                event=event,
                guard=guard,
                error_type=GuardError.__name__,
                error_message=f"<{type(e).__name__}>: {e}",
            )
            raise GuardError(record=asdict(self._eventlog[-1])) from e

    def _evaluate_guards(
        self,
        event: E,
        context: C,
        transitions: tuple[tuple[S, Action[C] | None, Guard[C] | None], ...],
    ) -> tuple[S, Action[C] | None]:

        for target_state, action, guard in transitions:
            if guard is None:
                if len(transitions) > 1:
                    self._dispatch_eventlog(
                        machine_event=InternalEvent.GUARD_SKIP,
                        source=self._state,
                        target=target_state,
                        event=event,
                    )
                return (target_state, action)

            if self._execute_guard(event, context, guard, target_state):
                return (target_state, action)

        self._dispatch_eventlog(
            machine_event=InternalEvent.EXCEPTION,
            source=self._state,
            event=event,
            error_type=BlockedTransition.__name__,
            error_message=f"No guards passed for event {event}",
        )
        raise BlockedTransition(record=asdict(self._eventlog[-1]))

    def _execute_on_entry(self, event: E, context: C) -> None:
        self._run_actions(
            event=event,
            context=context,
            actions=self._on_entry.get(self._state),
            action_type=InternalEvent.ON_ENTRY,
        )

    def _execute_on_exit(self, event: E, context: C) -> None:
        self._run_actions(
            event=event,
            context=context,
            actions=self._on_exit.get(self._state),
            action_type=InternalEvent.ON_EXIT,
        )

    def _execute_on_transition(
        self, source_state: S, event: E, target_state: S, context: C
    ) -> None:
        self._run_actions(
            event=event,
            context=context,
            actions=self._on_transition.get((source_state, target_state)),
            action_type=InternalEvent.ON_TRANSITION,
        )

    def _execute_transition_action(
        self, event: E, action: Action[C] | None, context: C
    ) -> None:
        if action:
            self._run_action(
                event=event,
                context=context,
                action=action,
                action_type=InternalEvent.TRANSITION_ACTION,
            )

    def _run_action(
        self,
        event: E,
        context: C,
        action: Action[C],
        action_type: InternalEvent,
    ) -> None:
        try:
            action(context)
            self._dispatch_eventlog(
                machine_event=action_type,
                source=self._state,
                event=event,
                action=action,
                action_type=action_type,
            )
        except Exception as e:
            self._dispatch_eventlog(
                machine_event=InternalEvent.EXCEPTION,
                source=self._state,
                event=event,
                action=action,
                action_type=action_type,
                error_type=ActionError.__name__,
                error_message=f"<{type(e).__name__}>: {e}",
            )
            raise ActionError(record=asdict(self._eventlog[-1])) from e

    def _run_actions(
        self,
        event: E,
        context: C,
        actions: Iterable[Action[C]] | None,
        action_type: InternalEvent,
    ) -> None:
        if actions:
            for action in actions:
                self._run_action(
                    event=event, context=context, action=action, action_type=action_type
                )


@dataclass(slots=True)
class AsyncStateMachine[S: Enum, E: Enum, C](StateMachineMixin[S, E, C]):
    _name: str
    _state: S
    _transitions: ProxyTransitionMap[S, E, C]
    _on_entry: ProxyEntryExitAction[S, C]
    _on_exit: ProxyEntryExitAction[S, C]
    _on_transition: ProxyTransitionAction[S, C]
    _eventlog: deque[EventRecord] = field(
        default_factory=lambda: deque(maxlen=MAX_EVENTLOG)
    )
    _verbose: bool = False

    async def trigger(self, event: E, context: C) -> dict[str, S | E]:
        source_state = self._state
        self._dispatch_eventlog(
            machine_event=InternalEvent.EVENT_TRIGGER, event=event, source=self._state
        )

        transitions = self._resolve_transitions(event)
        target_state, action = await self._evaluate_guards(event, context, transitions)

        self._dispatch_eventlog(
            machine_event=InternalEvent.TRANSITION_START,
            source=source_state,
            target=target_state,
            event=event,
        )

        await self._execute_on_exit(event, context)
        await self._execute_transition_action(event, action, context)
        self._apply_transition(event, target_state)
        await self._execute_on_entry(event, context)
        await self._execute_on_transition(source_state, event, target_state, context)

        self._dispatch_eventlog(
            machine_event=InternalEvent.TRANSITION_COMPLETE,
            source=source_state,
            target=target_state,
            event=event,
        )
        return dict(source=source_state, target=target_state, event=event)

    async def _execute_guard(
        self, event: E, context: C, guard: Guard[C], target_state: S
    ) -> bool | Awaitable[bool]:
        try:
            result = guard(context)
            passed = await result if isawaitable(result) else result
            self._dispatch_eventlog(
                machine_event=InternalEvent.GUARD_EVALUATE,
                source=self._state,
                # target=target_state,
                event=event,
                guard=guard,
                passed=passed,
            )
            return passed
        except Exception as e:
            self._dispatch_eventlog(
                machine_event=InternalEvent.EXCEPTION,
                source=self._state,
                # target=target_state,
                event=event,
                guard=guard,
                error_type=GuardError.__name__,
                error_message=f"<{type(e).__name__}>: {e}",
            )
            raise GuardError(record=asdict(self._eventlog[-1])) from e

    async def _evaluate_guards(
        self,
        event: E,
        context: C,
        transitions: tuple[tuple[S, Action[C] | None, Guard[C] | None], ...],
    ) -> tuple[S, Action[C] | None]:

        for target_state, action, guard in transitions:
            if guard is None:
                if len(transitions) > 1:
                    self._dispatch_eventlog(
                        machine_event=InternalEvent.GUARD_SKIP,
                        source=self._state,
                        target=target_state,
                        event=event,
                    )
                return (target_state, action)

            if await self._execute_guard(event, context, guard, target_state):
                return (target_state, action)

        self._dispatch_eventlog(
            machine_event=InternalEvent.EXCEPTION,
            source=self._state,
            event=event,
            error_type=BlockedTransition.__name__,
            error_message=f"No guards passed for event {event}",
        )
        raise BlockedTransition(record=asdict(self._eventlog[-1]))

    async def _execute_on_entry(self, event: E, context: C) -> None:
        await self._run_actions(
            event=event,
            context=context,
            actions=self._on_entry.get(self._state),
            action_type=InternalEvent.ON_ENTRY,
        )

    async def _execute_on_exit(self, event: E, context: C) -> None:
        await self._run_actions(
            event=event,
            context=context,
            actions=self._on_exit.get(self._state),
            action_type=InternalEvent.ON_EXIT,
        )

    async def _execute_on_transition(
        self, source_state: S, event: E, target_state: S, context: C
    ) -> None:
        await self._run_actions(
            event=event,
            context=context,
            actions=self._on_transition.get((source_state, target_state)),
            action_type=InternalEvent.ON_TRANSITION,
        )

    async def _execute_transition_action(
        self, event: E, action: Action[C] | None, context: C
    ) -> None:
        if action:
            await self._run_action(
                event=event,
                context=context,
                action=action,
                action_type=InternalEvent.TRANSITION_ACTION,
            )

    async def _run_action(
        self, event: E, context: C, action: Action[C], action_type: InternalEvent
    ) -> None:
        try:
            result = action(context)
            if isawaitable(result):
                await result

            self._dispatch_eventlog(
                machine_event=action_type,
                source=self._state,
                event=event,
                action=action,
                action_type=action_type,
            )
        except Exception as e:
            self._dispatch_eventlog(
                machine_event=InternalEvent.EXCEPTION,
                source=self._state,
                event=event,
                action=action,
                action_type=action_type,
                error_type=ActionError.__name__,
                error_message=f"<{type(e).__name__}>: {e}",
            )
            raise ActionError(record=asdict(self._eventlog[-1])) from e

    async def _run_actions(
        self,
        event: E,
        context: C,
        actions: Iterable[Action[C]] | None,
        action_type: InternalEvent,
    ) -> None:
        if actions:
            for action in actions:
                await self._run_action(
                    event=event, context=context, action=action, action_type=action_type
                )


@dataclass(slots=True)
class StateMachineBuilder[S: Enum, E: Enum, C]:
    _events: set[E] = field(default_factory=set, init=False)
    _states: set[S] = field(default_factory=set, init=False)
    _transitions: TransitionMap[S, E, C] = field(default_factory=dict, init=False)
    _on_entry: EntryExitAction[S, C] = field(default_factory=dict, init=False)
    _on_exit: EntryExitAction[S, C] = field(default_factory=dict, init=False)
    _on_transition: TransitionAction[S, C] = field(default_factory=dict, init=False)
    _audit_sink: Callable | None = field(default=None, init=False)
    _counter: ClassVar = itertools.count(start=1)

    @classmethod
    def _get_name(cls, base_name: str = "SM"):
        return f"{base_name}_{next(cls._counter)}"

    def add_audit_sink(
        self, audit_sink: Callable | None = None
    ) -> "StateMachineBuilder[S, E, C]":
        if callable(audit_sink):
            self._audit_sink = audit_sink
        return self

    def add_transition(
        self,
        source_state: S,
        event: E,
        target_state: S,
        action: Action[C] | None = None,
        guard: Guard[C] | None = None,
    ) -> "StateMachineBuilder[S, E, C]":
        self._events.add(event)
        self._states.add(source_state)
        self._states.add(target_state)
        self._transitions.setdefault((source_state, event), []).append(
            (target_state, action, guard)
        )
        return self

    def on_entry(self, state: S, action: Action[C]) -> "StateMachineBuilder[S, E, C]":
        self._states.add(state)
        self._on_entry.setdefault(state, []).append(action)
        return self

    def on_exit(self, state: S, action: Action[C]) -> "StateMachineBuilder[S, E, C]":
        self._states.add(state)
        self._on_exit.setdefault(state, []).append(action)
        return self

    def on_transition(
        self, source_state: S, target_state: S, action: Action[C]
    ) -> "StateMachineBuilder[S, E, C]":
        self._states.add(source_state)
        self._states.add(target_state)
        self._on_transition.setdefault((source_state, target_state), []).append(action)
        return self

    def build(
        self, initial_state: S, name: str | None = None, verbose: bool = False
    ) -> StateMachine[S, E, C]:
        name = name if name else StateMachineBuilder._get_name()

        self._states.add(initial_state)
        self._validate_model(name, initial_state)
        sm_settings = self._prepare_immutables()

        return StateMachine(
            _name=name, _state=initial_state, _verbose=verbose, **sm_settings
        )

    def build_async(
        self, initial_state: S, name: str | None = None, verbose: bool = False
    ) -> AsyncStateMachine[S, E, C]:
        name = name if name else StateMachineBuilder._get_name()

        self._validate_model(name, initial_state)
        sm_settings = self._prepare_immutables()

        return AsyncStateMachine(
            _name=name, _state=initial_state, _verbose=verbose, **sm_settings
        )

    def _prepare_immutables(self) -> dict:
        template = dict(
            _transitions=self._transitions,
            _on_entry=self._on_entry,
            _on_exit=self._on_exit,
            _on_transition=self._on_transition,
        )
        return {
            k: MappingProxyType({kk: tuple(vv) for kk, vv in v.items()})
            for k, v in template.items()
        }

    def _validate_model(self, name, initial_state) -> None:
        if not self._transitions:
            raise TransitionMapError(machine_name=name)

        if initial_state not in self._states:
            raise InvalidState(initial_state=initial_state)

        state_type = type(initial_state)
        for state in self._states:
            if not isinstance(state, Enum):
                raise TypeError(
                    f"State '{state}' must be an Enum, not {type(state).__name__}."
                )

            if not isinstance(state, state_type):
                raise TypeError(
                    f"Inconsistent Enum class: '{state}' is a {type(state).__name__}, "
                    f"but the machine expects {state_type.__name__}."
                )

        for event in self._events:
            if not isinstance(event, Enum):
                raise TypeError(
                    f"Event '{event}' must be an Enum, not {type(event).__name__}."
                )
