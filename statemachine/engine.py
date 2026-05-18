from collections.abc import Awaitable, Callable, Iterable
from dataclasses import asdict
from enum import Enum
from typing import TYPE_CHECKING

from .definitions import EngineEvent
from .exceptions import (
    ActionError,
    BlockedTransition,
    GuardError,
    InvalidTransition,
)

if TYPE_CHECKING:
    from .definitions import Action, Guard
    from .statemachine import StateMachine


class BaseEngine[S: Enum, E: Enum, C]:
    def __init__(self, sm: StateMachine):
        self.sm = sm

    def _apply_transition(self, source_state: S, target_state: S) -> S:
        self.sm._dispatch_event(
            machine_event=EngineEvent.STATE_CHANGE,
            source=source_state,
            target=target_state,
        )
        return target_state

    def _resolve_transitions(
        self, source: S, event: E | None
    ) -> list[tuple[S, tuple[Action[C]] | None, tuple[Guard[C]] | None]] | None:
        transitions = self.sm._config.transitions.get((source, event))
        if not transitions and event is not None:
            self.sm._dispatch_event(
                machine_event=EngineEvent.EXCEPTION,
                error_type=InvalidTransition,
                error_message=f"No transition map registered for {event}",
            )
            raise InvalidTransition(event_record=asdict(self.sm._event_log[-1]))

        return transitions


class SyncEngine[S: Enum, E: Enum, C](BaseEngine):
    def _state_transition(
        self, state: S, event: E | None, context: C
    ) -> tuple[S, S | None]:
        original_state = state
        target_state = None

        transitions = self._resolve_transitions(original_state, event=event)
        # FIX: Add a MAX_TRANSITION_DEPTH to avoid infinite loops
        #      If a source state has multiple automatic transitions decide
        #      on how to evaluate them - maybe based on priority
        while transitions:
            source_state = self.sm._state
            target_state, actions = self._evaluate_guards(
                event=event, context=context, transitions=transitions
            )

            if target_state is None:
                target_state = self.sm._state
                break

            self.sm._dispatch_event(machine_event=EngineEvent.TRANSITION_START)

            self._execute_on_exit(context)
            self._execute_transition_action(actions, context)
            self.sm._apply_transition(target_state)
            self._execute_on_entry(context)
            self._execute_on_transition(source_state, target_state, context)

            self.sm._dispatch_event(
                machine_event=EngineEvent.TRANSITION_COMPLETE,
                source=source_state,
                target=target_state,
            )
            event = None
            transitions = self._resolve_transitions(target_state, event=event)

        return (original_state, target_state)

    # FIX: An awaitable guard should NEVER be passed to this function
    def _execute_guard(self, guard: Guard[C], context: C) -> bool | Awaitable[bool]:
        try:
            passed = guard(context)
        except Exception as e:
            event_record = self.sm._dispatch_event(
                machine_event=EngineEvent.EXCEPTION,
                guard=guard,
                error_type=GuardError,
                error_message=f"<{type(e).__name__}>: {e}",
            )
            raise GuardError(event_record=asdict(event_record)) from e
        else:
            self.sm._dispatch_event(
                machine_event=EngineEvent.GUARD_EVALUATE, guard=guard, passed=passed
            )
            return passed

    def _execute_guards(
        self, guards: tuple[Guard[C]], context: C
    ) -> bool | Awaitable[bool]:
        passed = True
        for guard in guards:
            if passed := self._execute_guard(guard, context):
                break
        return passed

    def _evaluate_guards(
        self,
        event: E | None,
        context: C,
        transitions: list[tuple[S, tuple[Action[C]] | None, tuple[Guard[C]] | None]],
    ) -> tuple[S | None, tuple[Action[C]] | None]:

        for target_state, actions, guards in transitions:
            self._target = target_state
            if not guards:
                # FIX: Double check the logic
                if len(transitions) > 1:
                    self.sm._dispatch_event(machine_event=EngineEvent.GUARD_SKIP)
                return (target_state, actions)

            if self._execute_guards(guards, context):
                return (target_state, actions)

        if event is None:
            self.sm._dispatch_event(machine_event=EngineEvent.TRANSITION_FAIL)
            return (None, None)

        event_record = self.sm._dispatch_event(
            machine_event=EngineEvent.EXCEPTION,
            error_type=BlockedTransition,
            error_message=f"No guards passed for event {self.sm._event}",
        )
        raise BlockedTransition(event_record=asdict(event_record))

    def _execute_on_entry(self, context: C) -> None:
        self._run_actions(
            context=context,
            actions=self.sm._config.on_entry.get(self.sm._state),
            action_type=EngineEvent.ON_ENTRY,
        )

    def _execute_on_exit(self, context: C) -> None:
        self._run_actions(
            context=context,
            actions=self.sm._config.on_exit.get(self.sm._state),
            action_type=EngineEvent.ON_EXIT,
        )

    def _execute_on_transition(self, source: S, target: S, context: C) -> None:
        self._run_actions(
            context=context,
            source=source,
            actions=self.sm._config.on_transition.get((source, target)),
            action_type=EngineEvent.ON_TRANSITION,
        )

    def _execute_transition_action(
        self, actions: tuple[Action[C]] | None, context: C
    ) -> None:
        if actions:
            self._run_actions(
                context=context,
                actions=actions,
                action_type=EngineEvent.TRANSITION_ACTION,
            )

    def _run_action(
        self,
        context: C,
        action: Action[C],
        action_type: EngineEvent,
        source: S | None = None,
    ) -> None:
        try:
            action(context)
            self.sm._dispatch_event(
                machine_event=action_type,
                source=source or self.sm._state,
                action=action,
                action_type=action_type,
            )
        except Exception as e:
            event_record = self.sm._dispatch_event(
                machine_event=EngineEvent.EXCEPTION,
                source=source or self.sm._state,
                action=action,
                action_type=action_type,
                error_type=ActionError,
                error_message=f"<{type(e).__name__}>: {e}",
            )
            raise ActionError(event_record=asdict(event_record)) from e

    def _run_actions(
        self,
        context: C,
        actions: Iterable[Action[C]] | None,
        action_type: EngineEvent,
        source: S | None = None,
    ) -> None:
        if actions:
            for action in actions:
                self._run_action(
                    context=context,
                    action=action,
                    action_type=action_type,
                    source=source,
                )


class AsyncEngine[S: Enum, E: Enum, C](BaseEngine):
    _name: str
    _state: S
    _transitions: ProxyTransitionMap[S, E, C]
    _on_entry: ProxyEntryExitAction[S, C]
    _on_exit: ProxyEntryExitAction[S, C]
    _on_transition: ProxyTransitionAction[S, C]
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
