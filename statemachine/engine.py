from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable
from dataclasses import asdict
from enum import Enum
from inspect import isawaitable
from typing import TYPE_CHECKING

from .definitions import EngineEvent
from .exceptions import (
    ActionError,
    GuardError,
    InvalidTransition,
)

if TYPE_CHECKING:
    from .definitions import Action, Guard
    from .statemachine import StateMachine


class BaseEngine[S: Enum, E: Enum, C]:
    def __init__(self, sm: "StateMachine", transition_depth: int = 10):
        self.sm = sm
        self._transition_depth = transition_depth

    def resolve_transitions(
        self, state: S, event: E | None
    ) -> list[tuple[S, tuple[Action[C]] | None, tuple[Guard[C]] | None]] | None:
        transitions = self.sm._config.transitions.get((state, event))
        if not transitions and event is not None:
            self.sm._dispatch_event(
                machine_event=EngineEvent.EXCEPTION,
                error_type=InvalidTransition,
                error_message=f"No transition map registered for {event}",
            )
            raise InvalidTransition(event_record=asdict(self.sm._event_log[-1]))

        return transitions


class SyncEngine[S: Enum, E: Enum, C](BaseEngine):
    def start_engine(self, state: S, context: C) -> None:
        self.evaluate_on_entry(state=state, context=context)
        self.evaluate_transitions(source=state, event=None, context=context)

    def stop_engine(self) -> None:
        raise RuntimeError("Engine forcefully halted.")

    def evaluate_transitions(self, source: S, event: E | None, context: C) -> None:
        source_state = source
        transitions = self.resolve_transitions(state=source_state, event=event)

        # TODO : a source state has multiple automatic transitions decide
        #        on how to evaluate them - maybe based on priority
        transition_depth = 0
        while transitions and transition_depth < self._transition_depth:
            target_state, actions = self.evaluate_guards(
                context=context, transitions=transitions
            )

            if target_state is None:
                break

            self.sm._dispatch_event(
                machine_event=EngineEvent.TRANSITION_START,
                source=source_state,
                target=target_state,
                event=event,
            )
            self.evaluate_on_exit(state=source_state, context=context)
            self.evaluate_transition_action(
                state=source_state, actions=actions, context=context
            )
            self.sm._apply_transition(target=target_state)
            self.evaluate_on_entry(state=target_state, context=context)
            self.evaluate_on_transition(
                source=source_state, target=target_state, context=context
            )
            self.sm._dispatch_event(
                machine_event=EngineEvent.TRANSITION_COMPLETE,
                source=source_state,
                target=target_state,
                event=event,
            )
            event = None
            source_state = target_state
            transitions = self.resolve_transitions(state=source_state, event=event)
            transition_depth += 1

    def evaluate_guards(
        self,
        context: C,
        transitions: list[tuple[S, tuple[Action[C]] | None, tuple[Guard[C]] | None]],
    ) -> tuple[S | None, tuple[Action[C]] | None]:

        for target_state, actions, guards in transitions:
            if not guards or self.execute_guards(guards=guards, context=context):
                return (target_state, actions)

        return (None, None)

    def evaluate_on_entry(self, state: S, context: C) -> None:
        self.execute_actions(
            source=state,
            context=context,
            actions=self.sm._config.on_entry.get(state),
            action_type=EngineEvent.ON_ENTRY,
        )

    def evaluate_on_exit(self, state: S, context: C) -> None:
        self.execute_actions(
            source=state,
            context=context,
            actions=self.sm._config.on_exit.get(state),
            action_type=EngineEvent.ON_EXIT,
        )

    def evaluate_on_transition(self, source: S, target: S, context: C) -> None:
        self.execute_actions(
            source=source,
            context=context,
            actions=self.sm._config.on_transition.get((source, target)),
            action_type=EngineEvent.ON_TRANSITION,
        )

    def evaluate_transition_action(
        self, state: S, actions: tuple[Action[C]] | None, context: C
    ) -> None:
        if actions:
            self.execute_actions(
                source=state,
                context=context,
                actions=actions,
                action_type=EngineEvent.TRANSITION_ACTION,
            )

    def execute_guards(self, guards: tuple[Guard[C]], context: C) -> bool:
        for guard in guards:
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

            self.sm._dispatch_event(
                machine_event=EngineEvent.GUARD_EVALUATE, guard=guard, passed=passed
            )

            if not passed:
                return False

        return True

    def execute_actions(
        self,
        source: S,
        context: C,
        actions: Iterable[Action[C]] | None,
        action_type: EngineEvent,
    ) -> None:
        if not actions:
            return

        for action in actions:
            try:
                action(context)
                self.sm._dispatch_event(
                    machine_event=action_type,
                    source=source,
                    action=action,
                    action_type=action_type,
                )
            except Exception as e:
                event_record = self.sm._dispatch_event(
                    machine_event=EngineEvent.EXCEPTION,
                    source=source,
                    action=action,
                    action_type=action_type,
                    error_type=ActionError,
                    error_message=f"<{type(e).__name__}>: {e}",
                )
                raise ActionError(event_record=asdict(event_record)) from e

    # def execute_validators(self) -> None:
    #     event_record = self.sm._dispatch_event(
    #         machine_event=EngineEvent.EXCEPTION,
    #         source=source_state,
    #         target=target_state,
    #         event=event,
    #         error_type=BlockedTransition,
    #         error_message=f"No guards passed for {event = }",
    #     )
    #     raise BlockedTransition(event_record=asdict(event_record))


# class AsyncEngine[S: Enum, E: Enum, C](BaseEngine):
#     _name: str
#     _state: S
#     _transitions: ProxyTransitionMap[S, E, C]
#     _on_entry: ProxyEntryExitAction[S, C]
#     _on_exit: ProxyEntryExitAction[S, C]
#     _on_transition: ProxyTransitionAction[S, C]
#     _event_log: deque[EventRecord] = field(
#         default_factory=lambda: deque(maxlen=MAX_EVENT_LOG)
#     )
#     _event: E | None = None
#     _target: S | None = None
#     _verbose: bool = False
#
#     async def trigger(self, event: E, context: C) -> dict[str, S | E]:
#         self._event = event
#         self._dispatch_event(machine_event=EngineEvent.EVENT_TRIGGER)
#
#         source_state = self._state
#         transitions = self._resolve_transitions(source_state, event)
#         target_state, action = await self._evaluate_guards(context, transitions)
#
#         self._dispatch_event(machine_event=EngineEvent.TRANSITION_START)
#
#         await self._execute_on_exit(context)
#         await self._execute_transition_action(action, context)
#         self._apply_transition(target_state)
#         await self._execute_on_entry(context)
#         await self._execute_on_transition(source_state, target_state, context)
#
#         self._dispatch_event(
#             machine_event=EngineEvent.TRANSITION_COMPLETE,
#             source=source_state,
#             target=target_state,
#         )
#         return dict(source=source_state, target=target_state, event=event)
#
#     async def _execute_guard(
#         self, guard: Guard[C], context: C
#     ) -> bool | Awaitable[bool]:
#         try:
#             result = guard(context)
#             passed = await result if isawaitable(result) else result
#             self._dispatch_event(
#                 machine_event=EngineEvent.GUARD_EVALUATE, guard=guard, passed=passed
#             )
#             return passed
#         except Exception as e:
#             event_record = self._dispatch_event(
#                 machine_event=EngineEvent.EXCEPTION,
#                 guard=guard,
#                 error_type=GuardError,
#                 error_message=f"<{type(e).__name__}>: {e}",
#             )
#             raise GuardError(event_record=asdict(event_record)) from e
#
#     async def _evaluate_guards(
#         self,
#         context: C,
#         transitions: tuple[tuple[S, Action[C] | None, Guard[C] | None], ...],
#     ) -> tuple[S, Action[C] | None]:
#
#         for target_state, action, guard in transitions:
#             self._target = target_state
#             if guard is None:
#                 if len(transitions) > 1:
#                     self._dispatch_event(machine_event=EngineEvent.GUARD_SKIP)
#                 return (target_state, action)
#
#             if await self._execute_guard(guard, context):
#                 return (target_state, action)
#
#         event_record = self._dispatch_event(
#             machine_event=EngineEvent.EXCEPTION,
#             error_type=BlockedTransition,
#             error_message=f"No guards passed for event {self._event}",
#         )
#         raise BlockedTransition(event_record=asdict(event_record))
#
#     async def _execute_on_entry(self, context: C) -> None:
#         await self._run_actions(
#             context=context,
#             actions=self._on_entry.get(self._state),
#             action_type=EngineEvent.ON_ENTRY,
#         )
#
#     async def _execute_on_exit(self, context: C) -> None:
#         await self._run_actions(
#             context=context,
#             actions=self._on_exit.get(self._state),
#             action_type=EngineEvent.ON_EXIT,
#         )
#
#     async def _execute_on_transition(self, source: S, target: S, context: C) -> None:
#         await self._run_actions(
#             context=context,
#             source=source,
#             actions=self._on_transition.get((source, target)),
#             action_type=EngineEvent.ON_TRANSITION,
#         )
#
#     async def _execute_transition_action(
#         self, action: Action[C] | None, context: C
#     ) -> None:
#         if action:
#             await self._run_action(
#                 context=context,
#                 action=action,
#                 action_type=EngineEvent.TRANSITION_ACTION,
#             )
#
#     async def _run_action(
#         self,
#         context: C,
#         action: Action[C],
#         action_type: EngineEvent,
#         source: S | None = None,
#     ) -> None:
#         try:
#             result = action(context)
#             if isawaitable(result):
#                 await result
#
#             self._dispatch_event(
#                 machine_event=action_type,
#                 source=source or self._state,
#                 action=action,
#                 action_type=action_type,
#             )
#         except Exception as e:
#             event_record = self._dispatch_event(
#                 machine_event=EngineEvent.EXCEPTION,
#                 source=source or self._state,
#                 action=action,
#                 action_type=action_type,
#                 error_type=ActionError,
#                 error_message=f"<{type(e).__name__}>: {e}",
#             )
#             raise ActionError(event_record=asdict(event_record)) from e
#
#     async def _run_actions(
#         self,
#         context: C,
#         actions: Iterable[Action[C]] | None,
#         action_type: EngineEvent,
#         source: S | None = None,
#     ) -> None:
#         if actions:
#             for action in actions:
#                 await self._run_action(
#                     context=context,
#                     action=action,
#                     action_type=action_type,
#                     source=source,
#                 )
