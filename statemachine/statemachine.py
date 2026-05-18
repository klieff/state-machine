import itertools
from collections import deque
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import asdict, dataclass, field
from enum import Enum
from inspect import isawaitable
from typing import TYPE_CHECKING, ClassVar, Collection

from .definitions import EngineEvent, EventDetails, EventRecord, StateMachineConfig
from .engine import AsyncEngine, SyncEngine
from .exceptions import (
    ActionError,
    BlockedTransition,
    GuardError,
    InvalidState,
    InvalidTransition,
    TransitionMapError,
    UninitializedError,
)

if TYPE_CHECKING:
    from .definitions import (
        Action,
        EntryExitAction,
        Guard,
        TransitionAction,
        TransitionMap,
    )

MAX_EVENT_LOG = 200


def _format_event_log(record: EventRecord) -> str:
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


class StateMachineMixin[S: Enum, E: Enum, C]:
    __slots__ = ()
    _name: str
    _state: S
    _event: E | None
    _target: S | None
    _verbose: bool
    _transitions: TransitionMap[S, E, C]
    _event_log: deque[EventRecord]

    # def get_state_events(self, state: S) -> list[E]:
    #     return [event for (s, event) in self._transitions.keys() if s == state]

    def _apply_transition(self, target_state: S) -> None:
        source_state = self._state
        self._state = target_state
        self._dispatch_event(
            machine_event=EngineEvent.STATE_CHANGE,
            source=source_state,
            target=target_state,
        )

    def _dispatch_event(self, machine_event: EngineEvent, **kwargs) -> EventRecord:
        record = self._record_event(machine_event, **kwargs)
        if self._verbose:
            print(_format_event_log(record))
        return record

    def _get_name(self, obj) -> str:
        name = getattr(obj, "name", None)
        return name or getattr(obj, "__name__", type(obj).__name__)

    def _record_event(
        self, machine_event: EngineEvent, **kwargs
    ) -> EventRecord[S, E, C]:
        details = EventDetails[S, E, C](
            source=self._state, target=self._target, event=self._event
        )
        for key, value in kwargs.items():
            if key in {"action", "action_type", "guard", "error_type"}:
                value = self._get_name(value)
            setattr(details, key, value)

        record = EventRecord[S, E, C](
            machine=self._name,
            machine_event=machine_event.name,
            details=details,
        )

        self._event_log.append(record)
        return record


# TODO: Use a single SM object but delegate callbacks to separate sync/async engines
class StateMachine[S: Enum, E: Enum, C](StateMachineMixin[S, E, C]):
    _engine: SyncEngine | AsyncEngine

    def __init__(
        self, config: StateMachineConfig, audit_sink: Callable | None = None
    ) -> None:
        self._state = config.initial_state
        self._event = None
        self._target = None
        self._config: StateMachineConfig = config
        self._audit_sink = audit_sink
        self._event_log = deque(maxlen=MAX_EVENT_LOG)
        self._initialized = False

    def start(self, context: C) -> dict[str, S | E | None] | None:
        if self._initialized:
            return

        self._initialized = True
        self._dispatch_event(machine_event=EngineEvent.MACHINE_START)
        self._engine._execute_on_entry(context)

        source_state, target_state = self._engine._state_transition(
            event=None, context=context
        )
        return dict(source=source_state, target=target_state, event=None)

    def trigger(self, event: E, context: C) -> dict[str, S | E | None]:
        if not self._initialized:
            raise UninitializedError(machine_name=self._name)

        self._event = event
        self._dispatch_event(machine_event=EngineEvent.EVENT_TRIGGER)

        source_state, target_state = self._engine._state_transition(
            event=event, context=context
        )
        self._event = None
        return dict(source=source_state, target=target_state, event=event)


@dataclass(slots=True)
class AsyncStateMachine[S: Enum, E: Enum, C](StateMachineMixin[S, E, C]):
    _name: str
    _state: S
    _transitions: TransitionMap[S, E, C]
    _on_entry: EntryExitAction[S, C]
    _on_exit: EntryExitAction[S, C]
    _on_transition: TransitionAction[S, C]
    _event_log: deque[EventRecord] = field(
        default_factory=lambda: deque(maxlen=MAX_EVENT_LOG)
    )
    _event: E | None = None
    _target: S | None = None
    _verbose: bool = False

    async def trigger(self, event: E, context: C) -> dict[str, S | E]:
        self._event = event
        self._dispatch_event(machine_event=EngineEvent.EVENT_TRIGGER)

        source_state = self._state
        transitions = self._resolve_transitions(source_state, event)
        target_state, action = await self._evaluate_guards(context, transitions)

        self._dispatch_event(machine_event=EngineEvent.TRANSITION_START)

        await self._execute_on_exit(context)
        await self._execute_transition_action(action, context)
        self._apply_transition(target_state)
        await self._execute_on_entry(context)
        await self._execute_on_transition(source_state, target_state, context)

        self._dispatch_event(
            machine_event=EngineEvent.TRANSITION_COMPLETE,
            source=source_state,
            target=target_state,
        )
        return dict(source=source_state, target=target_state, event=event)

    async def _execute_guard(
        self, guard: Guard[C], context: C
    ) -> bool | Awaitable[bool]:
        try:
            result = guard(context)
            passed = await result if isawaitable(result) else result
            self._dispatch_event(
                machine_event=EngineEvent.GUARD_EVALUATE, guard=guard, passed=passed
            )
            return passed
        except Exception as e:
            event_record = self._dispatch_event(
                machine_event=EngineEvent.EXCEPTION,
                guard=guard,
                error_type=GuardError,
                error_message=f"<{type(e).__name__}>: {e}",
            )
            raise GuardError(event_record=asdict(event_record)) from e

    async def _evaluate_guards(
        self,
        context: C,
        transitions: tuple[tuple[S, Action[C] | None, Guard[C] | None], ...],
    ) -> tuple[S, Action[C] | None]:

        for target_state, action, guard in transitions:
            self._target = target_state
            if guard is None:
                if len(transitions) > 1:
                    self._dispatch_event(machine_event=EngineEvent.GUARD_SKIP)
                return (target_state, action)

            if await self._execute_guard(guard, context):
                return (target_state, action)

        event_record = self._dispatch_event(
            machine_event=EngineEvent.EXCEPTION,
            error_type=BlockedTransition,
            error_message=f"No guards passed for event {self._event}",
        )
        raise BlockedTransition(event_record=asdict(event_record))

    async def _execute_on_entry(self, context: C) -> None:
        await self._run_actions(
            context=context,
            actions=self._on_entry.get(self._state),
            action_type=EngineEvent.ON_ENTRY,
        )

    async def _execute_on_exit(self, context: C) -> None:
        await self._run_actions(
            context=context,
            actions=self._on_exit.get(self._state),
            action_type=EngineEvent.ON_EXIT,
        )

    async def _execute_on_transition(self, source: S, target: S, context: C) -> None:
        await self._run_actions(
            context=context,
            source=source,
            actions=self._on_transition.get((source, target)),
            action_type=EngineEvent.ON_TRANSITION,
        )

    async def _execute_transition_action(
        self, action: Action[C] | None, context: C
    ) -> None:
        if action:
            await self._run_action(
                context=context,
                action=action,
                action_type=EngineEvent.TRANSITION_ACTION,
            )

    async def _run_action(
        self,
        context: C,
        action: Action[C],
        action_type: EngineEvent,
        source: S | None = None,
    ) -> None:
        try:
            result = action(context)
            if isawaitable(result):
                await result

            self._dispatch_event(
                machine_event=action_type,
                source=source or self._state,
                action=action,
                action_type=action_type,
            )
        except Exception as e:
            event_record = self._dispatch_event(
                machine_event=EngineEvent.EXCEPTION,
                source=source or self._state,
                action=action,
                action_type=action_type,
                error_type=ActionError,
                error_message=f"<{type(e).__name__}>: {e}",
            )
            raise ActionError(event_record=asdict(event_record)) from e

    async def _run_actions(
        self,
        context: C,
        actions: Iterable[Action[C]] | None,
        action_type: EngineEvent,
        source: S | None = None,
    ) -> None:
        if actions:
            for action in actions:
                await self._run_action(
                    context=context,
                    action=action,
                    action_type=action_type,
                    source=source,
                )


def ensure_tuple(obj) -> tuple:
    if callable(obj):
        return (obj,)
    elif isinstance(obj, (list, set, tuple)):
        return tuple(obj)
    return ()


def validate_model(config: StateMachineConfig) -> None:
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


class StateMachineBuilder[S: Enum, E: Enum, C]:
    _events: set[E | None]
    _states: set[S]
    _transitions: TransitionMap[S, E, C]
    _on_entry: EntryExitAction[S, C]
    _on_exit: EntryExitAction[S, C]
    _on_transition: TransitionAction[S, C]
    _audit_sink: Callable | None
    _counter: ClassVar = itertools.count(start=1)

    @classmethod
    def _get_unique_name(cls, base_name: str = "SM"):
        return f"{base_name}_{next(cls._counter)}"

    def add_audit_sink(self, audit_sink: Callable) -> "StateMachineBuilder[S, E, C]":
        if callable(audit_sink):
            self._audit_sink = audit_sink
        return self

    def add_transition(
        self,
        source_state: S,
        event: E | None,
        target_state: S,
        action: Collection[Action[C]] | Action[C] | None = None,
        guard: Collection[Guard[C]] | Guard[C] | None = None,
    ) -> "StateMachineBuilder[S, E, C]":
        self._events.add(event)
        self._states.add(source_state)
        self._states.add(target_state)
        self._transitions.setdefault((source_state, event), []).append(
            (target_state, ensure_tuple(action), ensure_tuple(guard))
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
        self._states.add(initial_state)

        config = StateMachineConfig[S, E, C](
            name=name if name else StateMachineBuilder._get_unique_name(),
            initial_state=initial_state,
            events=self._events,
            states=self._states,
            transitions=self._transitions,
            on_entry=self._on_entry,
            on_exit=self._on_exit,
            on_transition=self._on_transition,
            verbose=verbose,
        )
        validate_model(config)
        return StateMachine[S, E, C](config=config, audit_sink=self._audit_sink)

    def build_async(
        self, initial_state: S, name: str | None = None, verbose: bool = False
    ) -> AsyncStateMachine[S, E, C]:
        self._states.add(initial_state)

        config = StateMachineConfig[S, E, C](
            name=name if name else StateMachineBuilder._get_unique_name(),
            initial_state=initial_state,
            events=self._events,
            states=self._states,
            transitions=self._transitions,
            on_entry=self._on_entry,
            on_exit=self._on_exit,
            on_transition=self._on_transition,
            verbose=verbose,
        )
        validate_model(config)
        return AsyncStateMachine[S, E, C](config=config, audit_sink=self._audit_sink)
