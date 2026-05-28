from __future__ import annotations

import itertools
from collections.abc import Callable
from inspect import iscoroutine
from typing import TYPE_CHECKING, Any, ClassVar

from .audit import AuditRecord
from .configuration import ConfigSpec, StateMachineConfigs
from .definitions import EngineEvent, EngineStep, StateType
from .dispatcher import EventDispatcher
from .engine import AsyncEngine

if TYPE_CHECKING:
    from .definitions import Callbacks, EventSpec, StateSpec


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


class StateMachineBuilder:
    _counter: ClassVar = itertools.count(start=1)

    def __init__(self) -> None:
        self._name = f"SM_{next(StateMachineBuilder._counter)}"
        self._id = id(self)
        self._config = StateMachineConfigs()
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
        self._config.add_state(
            type=StateType.STANDARD,
            state=state,
            on_entry=on_entry,
            on_exit=on_exit,
            final_state=final_state,
        )
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
        self._config.add_state(
            type=StateType.CHOICE, state=state, on_entry=on_entry, on_exit=on_exit
        )
        self._config.add_transition(
            source=state,
            event=EngineEvent.DYNAMIC_TRANSITION,
            target=EngineEvent.DYNAMIC_TRANSITION,
            actions=actions,
            guards=guards,
            router=router,
        )
        return self

    def add_automatic_state(
        self,
        source: StateSpec,
        target: StateSpec,
        on_entry: Callbacks | None = None,
        on_exit: Callbacks | None = None,
        actions: Callbacks | None = None,
        guards: Callbacks | None = None,
    ) -> StateMachineBuilder:
        self._config.add_state(
            type=StateType.AUTOMATIC, state=source, on_entry=on_entry, on_exit=on_exit
        )
        self._config.add_transition(
            source=source,
            event=EngineEvent.AUTOMATIC_TRANSITION,
            target=target,
            actions=actions,
            guards=guards,
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
        self._config.add_transition(
            source=source, event=event, target=target, actions=actions, guards=guards
        )
        return self

    # def on_transition(
    #     self, source: S, target: S, actions: Callbacks
    # ) -> StateMachineBuilder[S, E]:
    #     self._on_transition.setdefault((source, target), []).extend(
    #         prepare_callbacks(actions)
    #     )
    #     return self

    def build(self, name: str | None = None) -> SyncStateMachine:
        config = self._config.create_config(name or self._name)
        return SyncStateMachine(config=config, audit_sink=self._audit_sink)

    def build_async(self, name: str | None = None) -> AsyncStateMachine:
        config = self._config.create_config(name or self._name)
        return AsyncStateMachine(config=config, audit_sink=self._audit_sink)


class SyncStateMachine:
    def __init__(
        self,
        config: ConfigSpec,
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

    def stop(self) -> None:
        return self._engine.stop_engine(is_async=False)  # type: ignore[return-value]

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
        config: ConfigSpec,
        audit_sink: Callable | None = None,
    ) -> None:
        dp = EventDispatcher()
        # dp.subscribe(callback=audit_sink_callback)

        self._config = config
        self._engine = AsyncEngine(sm=self, config=config, dispatcher=dp, depth=100)

    async def start(self, initial_state: StateSpec, context: Any) -> None:
        result = self._engine.start_engine(
            initial_state=initial_state, context=context, is_async=True
        )
        result = await result if iscoroutine(result) else result

    async def stop(self) -> None:
        await self._engine.stop_engine(is_async=True)  # type: ignore[return-value]

    async def trigger(self, event: EventSpec, payload: Any = None) -> None:
        await self._engine.event_trigger(
            event=event,
            payload=payload,
            is_async=True,
        )  # type: ignore[return-value]

    # FIX: Define a dedicated state getter in engine class def
    def get_state(self) -> StateSpec:
        return self._engine._state.state
