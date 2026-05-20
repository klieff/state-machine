from __future__ import annotations

import itertools
from collections import deque
from collections.abc import Awaitable, Callable
from enum import Enum
from typing import TYPE_CHECKING, ClassVar, Collection

from .definitions import EngineEvent, EventDetails, EventRecord, StateMachineConfig

# from .engine import AsyncEngine, SyncEngine
from .engine_async import AsyncEngine
from .exceptions import UninitializedError
from .utils import ensure_tuple, format_event_log, get_obj_name

if TYPE_CHECKING:
    from .definitions import (
        Action,
        EntryExitAction,
        Guard,
        StateMachineConfig,
        TransitionAction,
        TransitionMap,
    )


MAX_EVENT_LOG = 200


class StateMachineBuilder[S: Enum, E: Enum, C]:
    _counter: ClassVar = itertools.count(start=1)

    def __init__(self) -> None:
        self._name = f"SM_{next(StateMachineBuilder._counter)}"
        self._events: set[E] = set()
        self._states: set[S] = set()
        self._transitions: TransitionMap[S, E, C] = dict()
        self._on_entry: EntryExitAction[S, C] = dict()
        self._on_exit: EntryExitAction[S, C] = dict()
        self._on_transition: TransitionAction[S, C] = dict()
        self._audit_sink: Callable | None = None

    def add_audit_sink(self, audit_sink: Callable) -> StateMachineBuilder[S, E, C]:
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
    ) -> StateMachineBuilder[S, E, C]:
        if event is not None:
            self._events.add(event)

        self._states.add(source_state)
        self._states.add(target_state)
        self._transitions.setdefault((source_state, event), []).append(
            (target_state, ensure_tuple(action), ensure_tuple(guard))
        )
        return self

    def on_entry(self, state: S, action: Action[C]) -> StateMachineBuilder[S, E, C]:
        self._states.add(state)
        self._on_entry.setdefault(state, []).append(action)
        return self

    def on_exit(self, state: S, action: Action[C]) -> StateMachineBuilder[S, E, C]:
        self._states.add(state)
        self._on_exit.setdefault(state, []).append(action)
        return self

    def on_transition(
        self, source_state: S, target_state: S, action: Action[C]
    ) -> StateMachineBuilder[S, E, C]:
        self._states.add(source_state)
        self._states.add(target_state)
        self._on_transition.setdefault((source_state, target_state), []).append(action)
        return self

    def build(
        self, initial_state: S, name: str | None = None, verbose: bool = False
    ) -> StateMachine[S, E, C]:
        self._states.add(initial_state)

        config = StateMachineConfig[S, E, C](
            name=name or self._name,
            initial_state=initial_state,
            events=self._events,
            states=self._states,
            transitions=self._transitions,
            on_entry=self._on_entry,
            on_exit=self._on_exit,
            on_transition=self._on_transition,
            verbose=verbose,
        )
        return StateMachine[S, E, C](
            config=config, audit_sink=self._audit_sink, is_async=False
        )

    def build_async(
        self, initial_state: S, name: str | None = None, verbose: bool = False
    ) -> StateMachine[S, E, C]:
        self._states.add(initial_state)

        config = StateMachineConfig[S, E, C](
            name=name or self._name,
            initial_state=initial_state,
            events=self._events,
            states=self._states,
            transitions=self._transitions,
            on_entry=self._on_entry,
            on_exit=self._on_exit,
            on_transition=self._on_transition,
            verbose=verbose,
        )
        return StateMachine[S, E, C](
            config=config, audit_sink=self._audit_sink, is_async=True
        )


class StateMachine[S: Enum, E: Enum, C]:  # , bool]:
    def __init__(
        self,
        config: StateMachineConfig,
        audit_sink: Callable | None = None,
        is_async: bool = False,
    ) -> None:
        self._state = config.initial_state
        self._event = None
        self._target = None
        self._config = config
        self._engine = AsyncEngine(sm=self, transition_depth=10)
        self._event_log = deque(maxlen=MAX_EVENT_LOG)
        self._audit_sink = audit_sink
        self._initialized = False
        self._is_async = is_async

    def start(self, context: C) -> Awaitable | None:
        if self._initialized:
            return

        self._initialized = True
        self._dispatch_event(machine_event=EngineEvent.MACHINE_START)

        return self._engine.start_engine(
            state=self._config.initial_state, context=context, is_async=self._is_async
        )

    def stop(self, force: bool = False):
        self._initialized = False
        return self._engine.stop_engine(is_async=self._is_async, force=force)

    def trigger(self, event: E, context: C) -> Awaitable | None:
        if not self._initialized:
            raise UninitializedError(machine_name=self._config.name)

        self._event = event
        self._dispatch_event(machine_event=EngineEvent.EVENT_TRIGGER)
        self._event = None

        return self._engine.event_trigger(
            event=event, context=context, is_async=self._is_async
        )

    def _apply_transition(self, target: S) -> None:
        source = self._state
        self._state = target
        self._dispatch_event(
            machine_event=EngineEvent.STATE_CHANGE, source=source, target=target
        )

    def _dispatch_event(self, machine_event: EngineEvent, **kwargs) -> EventRecord:
        record = self._record_event(machine_event, **kwargs)
        if self._config.verbose:
            print(format_event_log(record))
        return record

    def _record_event(
        self, machine_event: EngineEvent, **kwargs
    ) -> EventRecord[S, E, C]:
        details = EventDetails[S, E, C](
            source=self._state, target=self._target, event=self._event
        )
        for key, value in kwargs.items():
            if key in {"action", "action_type", "guard", "error_type"}:
                value = get_obj_name(value)
            setattr(details, key, value)

        record = EventRecord[S, E, C](
            machine=self._config.name,
            machine_event=machine_event.name,
            details=details,
        )

        self._event_log.append(record)
        return record
