from __future__ import annotations

import itertools
from collections.abc import Awaitable, Callable
from enum import Enum
from typing import TYPE_CHECKING, Any, ClassVar

from .audit import AuditRecord
from .callbacks import prepare_callbacks
from .definitions import EngineEvent, EngineStep, StateMachineConfig, State, Transition
from .dispatcher import EventDispatcher
from .engine import AsyncEngine
from .exceptions import InvalidState, UninitializedError

if TYPE_CHECKING:
    from .definitions import (
        Callbacks,
        StateMachineConfig,
        TransitionAction,
        TransitionMap,
    )


class StateMachineBuilder[S: Enum, E: Enum]:
    _counter: ClassVar = itertools.count(start=1)

    def __init__(self) -> None:
        self._id = id(self)
        self._name = f"SM_{next(StateMachineBuilder._counter)}"
        self._events: set[E] = set()
        self._states: dict[S, State] = dict()
        self._transitions: TransitionMap[S, E] = dict()
        self._on_transition: TransitionAction[S] = dict()
        self._audit_sink: Callable | None = None
        self._is_async = False

    def add_audit_sink(self, audit_sink: Callable) -> StateMachineBuilder[S, E]:
        if callable(audit_sink):
            self._audit_sink = audit_sink
        return self

    def add_state(
        self, state: S, on_entry: Callbacks = None, on_exit: Callbacks = None
    ) -> StateMachineBuilder[S, E]:
        new_state = State(
            state=state,
            on_exit=prepare_callbacks(on_exit),
            on_entry=prepare_callbacks(on_entry),
        )
        self._states[state] = new_state
        return self

    # TODO: Guard against infinite loops, e.g., add_transition(State_X, None, State_X)
    def add_transition(
        self,
        source: S,
        event: E | None = None,
        target: S | None = None,
        actions: Callbacks = None,
        guards: Callbacks = None,
        router: Callbacks = None,
    ) -> StateMachineBuilder[S, E]:
        if event is not None:
            self._events.add(event)

        self._transitions.setdefault((source, event), []).append(
            Transition(
                source=source,
                target=target,
                event=event,
                actions=prepare_callbacks(actions),
                guards=prepare_callbacks(guards),
                router=prepare_callbacks(router).pop(),
            )
        )
        return self

    def on_transition(
        self, source: S, target: S, actions: Callbacks
    ) -> StateMachineBuilder[S, E]:
        self._on_transition.setdefault((source, target), []).extend(
            prepare_callbacks(actions)
        )
        return self

    def build(
        self, name: str | None = None, verbose: bool = False
    ) -> StateMachine[S, E]:
        config = StateMachineConfig[S, E](
            name=name or self._name,
            events=self._events,
            states=self._states,
            transitions=self._transitions,
            on_transition=self._on_transition,
            verbose=verbose,
        )
        return StateMachine[S, E](
            config=config, audit_sink=self._audit_sink, is_async=self._is_async
        )

    def build_async(
        self, name: str | None = None, verbose: bool = False
    ) -> StateMachine[S, E]:
        self._is_async = True
        return self.build(name=name, verbose=verbose)


# FIX: TEST AUDIT CALLBACK - REMOVE
def audit_sink_callback(record: AuditRecord) -> None:
    for step in record.timeline:
        timestamp = step.timestamp.strftime("%H:%M:%S.%f")[:-3]
        microstep = f"[{step.micro_step}]" if step.micro_step else ""
        success = f"{'SUCCESS' if record.success else 'FAILED'}"
        event = f"{record.machine_event} {microstep}"
        line = f"[{timestamp}] {event:<35} | {success:<7} | Source: {record.source}"

        detail_str = ""
        if EngineEvent.EVENT_TRIGGER.name in record.machine_event:
            detail_str += f" Event: {record.trigger_event}"
        if EngineEvent.NULL_TRANSITION.name in record.machine_event:
            detail_str += f" Event: {record.trigger_event}"
        if record.target and step.micro_step != EngineStep.GUARD_SKIP.name:
            detail_str += f" -> Target: {record.target}"
        if EngineStep.GUARD_EVALUATE.name in step.micro_step:
            res = "PASS" if step.result else "FAIL"
            detail_str += f" Guard [{res}]: {step.target}"
        elif step.target:
            detail_str += f" Action: {step.target}"
        # if details.error_message:
        #     detail_str += f" Exception [{details.error_type}]: {details.error_message}"

        print(f"{line}{detail_str}")


class StateMachine[S: Enum, E: Enum]:
    def __init__(
        self,
        config: StateMachineConfig[S, E],
        audit_sink: Callable | None = None,
        is_async: bool = False,
    ) -> None:
        dispatcher = EventDispatcher()
        # dispatcher.subscribe(callback=audit_sink_callback)

        self._config = config
        self._is_async = is_async
        self._running = False
        self._engine = AsyncEngine(
            config=config, dispatcher=dispatcher, transition_depth=10
        )

    def start(self, initial_state: S, context: Any) -> Awaitable | None:
        if self._running:
            return

        state = self._config.states.get(initial_state)
        if state is None:
            raise InvalidState

        self._running = True
        return self._engine.start_engine(
            initial_state=state, context=context, is_async=self._is_async
        )

    def stop(self, force: bool = False):
        self._running = False
        return self._engine.stop_engine(is_async=self._is_async, force=force)

    def trigger(self, event: E, payload: object | None = None) -> Awaitable | None:
        if not self._running:
            raise UninitializedError(machine_name=self._config.name)

        return self._engine.event_trigger(
            event=event, payload=payload, is_async=self._is_async
        )

    # FIX: Define a dedicated state getter in engine class def
    def get_state(self) -> Enum:
        return self._engine._state.state
