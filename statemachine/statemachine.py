from __future__ import annotations

import itertools
from collections.abc import Awaitable, Callable
from enum import Enum
from typing import TYPE_CHECKING, ClassVar, Collection

from .audit import AuditRecord
from .definitions import EngineEvent, EngineStep, StateMachineConfig
from .dispatcher import EventDispatcher
from .engine_async import AsyncEngine
from .exceptions import UninitializedError
from .utils import ensure_tuple

if TYPE_CHECKING:
    from .definitions import (
        Action,
        EntryExitAction,
        Guard,
        StateMachineConfig,
        TransitionAction,
        TransitionMap,
    )


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
        self,
        initial_state: S,
        name: str | None = None,
        verbose: bool = False,
        is_async: bool = False,
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
            config=config, audit_sink=self._audit_sink, is_async=is_async
        )

    def build_async(
        self, initial_state: S, name: str | None = None, verbose: bool = False
    ) -> StateMachine[S, E, C]:
        return self.build(
            initial_state=initial_state, name=name, verbose=verbose, is_async=True
        )


# FIX: TEST AUDIT CALLBACK - REMOVE
def audit_sink_callback(record: AuditRecord) -> None:
    for step in record.timeline:
        timestamp = step.timestamp.strftime("%H:%M:%S.%f")[:-3]
        microstep = f"[{step.micro_step}]" if step.micro_step else ""
        event = f"{record.machine_event} {microstep}"
        line = f"[{timestamp}] {event:<35} Source: {record.source_state}"

        detail_str = ""
        if EngineEvent.EVENT_TRIGGER.name in record.machine_event:
            detail_str += f" Event: {record.trigger_event}"
        if EngineEvent.NULL_TRANSITION.name in record.machine_event:
            detail_str += f" Event: {record.trigger_event}"
        if record.target_state and step.micro_step != EngineStep.GUARD_SKIP.name:
            detail_str += f" -> Target: {record.target_state}"
        if step.target:
            res = "PASS" if step.result else "FAIL"
            detail_str += f" Action [{res}]: {step.target}"
        # if details.error_message:
        #     detail_str += f" Exception [{details.error_type}]: {details.error_message}"

        print(f"{line}{detail_str}")


class StateMachine[S: Enum, E: Enum, C]:  # , bool]:
    def __init__(
        self,
        config: StateMachineConfig,
        audit_sink: Callable | None = None,
        is_async: bool = False,
    ) -> None:
        dispatcher = EventDispatcher()
        dispatcher.subscribe(callback=audit_sink_callback)

        self._config = config
        self._is_async = is_async
        self._initialized = False
        self._engine = AsyncEngine(
            config=config, dispatcher=dispatcher, transition_depth=10
        )

    def start(self, context: C) -> Awaitable | None:
        if self._initialized:
            return

        self._initialized = True
        return self._engine.start_engine(
            state=self._config.initial_state, context=context, is_async=self._is_async
        )

    def stop(self, force: bool = False):
        self._initialized = False
        return self._engine.stop_engine(is_async=self._is_async, force=force)

    def trigger(self, event: E, context: C) -> Awaitable | None:
        if not self._initialized:
            raise UninitializedError(machine_name=self._config.name)

        return self._engine.event_trigger(
            event=event, context=context, is_async=self._is_async
        )

    # FIX: Create a dedicated state retriever in engine_async.py
    def get_state(self) -> S:
        return self._engine._state
