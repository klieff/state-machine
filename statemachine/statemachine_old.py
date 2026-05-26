from __future__ import annotations

import itertools
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, ClassVar

from .audit import AuditRecord
from .callbacks import prepare_callbacks
from .definitions import (
    EngineEvent,
    EngineStep,
    EventSpec,
    StateMachineConfig,
    State,
    StateSpec,
    StateType,
    Transition,
)
from .dispatcher import EventDispatcher
from .engine import AsyncEngine
from .utils import normalize_state_event

if TYPE_CHECKING:
    from .definitions import (
        Callbacks,
        TransitionAction,
        TransitionMap,
    )


# FIX: TEST AUDIT CALLBACK - REMOVE
def audit_sink_callback(record: AuditRecord) -> None:
    for step in record.timeline:
        timestamp = step.timestamp.strftime("%H:%M:%S.%f")[:-3]
        microstep = f"[{step.micro_step}]" if step.micro_step else ""
        success = f"{'SUCCESS' if record.success else 'FAILED'}"
        event = f"{record.event} {microstep}"
        line = f"[{timestamp}] {event:<40} | {success:<7} | Source: {record.source}"

        detail_str = ""
        if EngineEvent.EVENT_TRIGGER.name == record.event:
            detail_str += f" Event: {record.trigger_event}"
        if EngineEvent.AUTOMATIC_TRANSITION.name == record.event:
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


def _prepare_state(
    type: StateType,
    state: StateSpec,
    on_entry: Callbacks | None = None,
    on_exit: Callbacks | None = None,
) -> State:
    state_name = normalize_state_event(state)
    state_obj = State(
        type=type,
        name=state_name,
        state=state,
        on_exit=prepare_callbacks(on_exit),
        on_entry=prepare_callbacks(on_entry),
    )

    return state_obj


class StateMachineBuilder:
    _counter: ClassVar = itertools.count(start=1)

    def __init__(self) -> None:
        self._name = f"SM_{next(StateMachineBuilder._counter)}"
        self._id = id(self)
        self._events: dict[EventSpec, EventSpec] = dict()
        self._states: dict[StateSpec, State] = dict()
        self._transitions: TransitionMap = dict()
        self._on_transition: TransitionAction[StateSpec] = dict()
        self._audit_sink: Callable | None = None
        self._is_async = False

    def add_audit_sink(self, audit_sink: Callable) -> StateMachineBuilder:
        if callable(audit_sink):
            self._audit_sink = audit_sink
        return self

    def add_state(
        self,
        state: StateSpec,
        on_entry: Callbacks | None = None,
        on_exit: Callbacks | None = None,
        final_state: bool = False,
    ) -> StateMachineBuilder:
        state_name: StateSpec = normalize_state_event(state)
        if state_name in self._states:
            raise RuntimeError(f"State '{state_name}' is already registered.")

        state_obj = _prepare_state(
            type=StateType.STANDARD, state=state, on_entry=on_entry, on_exit=on_exit
        )
        state_obj.final_state = final_state
        self._states[state_name] = state_obj
        return self

    def add_choice_state(
        self,
        state: StateSpec,
        router: Callable[..., StateSpec],
        on_entry: Callbacks | None = None,
        on_exit: Callbacks | None = None,
        actions: Callbacks | None = None,
        guards: Callbacks | None = None,
    ) -> StateMachineBuilder:
        state_name = normalize_state_event(state)
        if state_name in self._states:
            raise RuntimeError(f"State '{state_name}' is already registered.")

        source_state = _prepare_state(
            type=StateType.CHOICE, state=state, on_entry=on_entry, on_exit=on_exit
        )

        event = EngineEvent.DYNAMIC_TRANSITION
        choice_transition = Transition(
            source=source_state,
            event=event.name,
            target=None,
            router=prepare_callbacks(router).pop(),
            actions=prepare_callbacks(actions),
            guards=prepare_callbacks(guards),
        )

        self._states[state_name] = source_state
        self._events[event.name] = event
        self._transitions.setdefault((state_name, event.name), []).append(
            choice_transition
        )
        return self

    def add_transient_state(
        self,
        source: StateSpec,
        target: StateSpec,
        on_entry: Callbacks | None = None,
        on_exit: Callbacks | None = None,
        actions: Callbacks | None = None,
        guards: Callbacks | None = None,
    ) -> StateMachineBuilder:
        source_name = normalize_state_event(source)
        target_name = normalize_state_event(target)

        if source_name in self._states:
            raise RuntimeError(f"Source state '{source_name}' is already registered.")
        if target_name in self._states:
            raise RuntimeError(f"Target state '{target_name}' is not registered.")

        source_state = _prepare_state(
            type=StateType.TRANSIENT, state=source, on_entry=on_entry, on_exit=on_exit
        )

        event = EngineEvent.AUTOMATIC_TRANSITION
        transient_transition = Transition(
            source=source_state,
            event=event.name,
            target=self._states[target_name],
            actions=prepare_callbacks(actions),
            guards=prepare_callbacks(guards),
        )

        self._states[source_name] = source_state
        self._events[event.name] = event
        self._transitions.setdefault((source_name, event.name), []).append(
            transient_transition
        )
        return self

    # TODO: Guard against infinite loops, e.g., add_transition(State_X, None, State_X)
    def add_transition(
        self,
        source: StateSpec,
        event: EventSpec,
        target: StateSpec,
        actions: Callbacks | None = None,
        guards: Callbacks | None = None,
    ) -> StateMachineBuilder:
        source_name = normalize_state_event(source)
        target_name = normalize_state_event(target)
        event_name = normalize_state_event(event)

        self._events[event_name] = event
        self._transitions.setdefault((source_name, event_name), []).append(
            Transition(
                source=self._states[source_name],
                target=self._states[target_name],
                event=event_name,
                actions=prepare_callbacks(actions),
                guards=prepare_callbacks(guards),
            )
        )
        return self

    # def on_transition(
    #     self, source: S, target: S, actions: Callbacks
    # ) -> StateMachineBuilder[S, E]:
    #     self._on_transition.setdefault((source, target), []).extend(
    #         prepare_callbacks(actions)
    #     )
    #     return self

    def build(self, name: str | None = None, verbose: bool = False) -> SyncStateMachine:
        config = StateMachineConfig(
            name=name or self._name,
            events=self._events,
            states=self._states,
            transitions=self._transitions,
            # on_transition=self._on_transition,
            verbose=verbose,
        )
        return SyncStateMachine(config=config, audit_sink=self._audit_sink)

    def build_async(
        self, name: str | None = None, verbose: bool = False
    ) -> AsyncStateMachine:
        self._is_async = True

        config = StateMachineConfig(
            name=name or self._name,
            events=self._events,
            states=self._states,
            transitions=self._transitions,
            # on_transition=self._on_transition,
            verbose=verbose,
        )
        return AsyncStateMachine(config=config, audit_sink=self._audit_sink)


class SyncStateMachine:
    def __init__(
        self,
        config: StateMachineConfig,
        audit_sink: Callable | None = None,
    ) -> None:
        dp = EventDispatcher()
        # dp.subscribe(callback=audit_sink_callback)

        self._config = config
        self._engine = AsyncEngine(sm=self, config=config, dispatcher=dp, depth=100)

    def start(self, initial_state: StateSpec, context: Any) -> None:
        return self._engine.start_engine(
            initial_state=initial_state, context=context, is_async=False
        )  # type: ignore[return-value]

    def stop(self, force: bool = False) -> None:
        return self._engine.stop_engine(is_async=False, force=force)  # type: ignore[return-value]

    def trigger(self, event: EventSpec, payload: Any = None) -> None:
        return self._engine.event_trigger(
            event=event,
            payload=payload,
            is_async=False,
        )  # type: ignore[return-value]

    # FIX: Define a dedicated state getter in engine class def
    def get_state(self) -> StateSpec:
        return self._engine._state.state


class AsyncStateMachine:
    def __init__(
        self,
        config: StateMachineConfig,
        audit_sink: Callable | None = None,
    ) -> None:
        dp = EventDispatcher()
        # dp.subscribe(callback=audit_sink_callback)

        self._config = config
        self._engine = AsyncEngine(sm=self, config=config, dispatcher=dp, depth=100)

    async def start(self, initial_state: StateSpec, context: Any) -> None:
        await self._engine.start_engine(
            initial_state=initial_state, context=context, is_async=True
        )  # type: ignore[return-value]

    async def stop(self, force: bool = False) -> None:
        await self._engine.stop_engine(is_async=True, force=force)  # type: ignore[return-value]

    async def trigger(self, event: EventSpec, payload: Any = None) -> None:
        await self._engine.event_trigger(
            event=event,
            payload=payload,
            is_async=True,
        )  # type: ignore[return-value]

    # FIX: Define a dedicated state getter in engine class def
    def get_state(self) -> StateSpec:
        return self._engine._state.state
