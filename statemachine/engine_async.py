from __future__ import annotations

import asyncio
import threading
from concurrent.futures import Future
from collections.abc import Callable, Iterable, Awaitable, Coroutine
from dataclasses import asdict
from enum import Enum
from inspect import isawaitable
from typing import TYPE_CHECKING, Any, Literal
from dataclasses import dataclass

from .definitions import EngineEvent
from .exceptions import (
    ActionError,
    GuardError,
    InvalidTransition,
)

if TYPE_CHECKING:
    from .definitions import Action, Guard
    from .statemachine import StateMachine


@dataclass
class TriggerRequest:
    task: Coroutine
    result: asyncio.Future[None]


class EngineRuntime:
    def __init__(self) -> None:
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            name="EngineRuntime",
            target=self._run_loop,
            daemon=True,
        )

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        return self._loop

    def _run_loop(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def submit[R](self, coro: Coroutine[Any, Any, R]) -> Future[R]:
        return asyncio.run_coroutine_threadsafe(coro=coro, loop=self._loop)

    async def submit_async[R](self, coro: Coroutine[Any, Any, R]) -> R:
        future = self.submit(coro)
        return await asyncio.wrap_future(future)

    def is_running(self):
        return self._thread.is_alive()

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join()


class BaseEngine[S: Enum, E: Enum, C]:
    _runtime: EngineRuntime = EngineRuntime()

    def __init__(self, sm: StateMachine, transition_depth: int = 10):
        self.sm = sm
        self._state = sm._config.initial_state
        self._transition_depth = transition_depth

    def _start_engine(self) -> None:
        if not self._runtime.is_running():
            self._runtime.start()

        setup_future = self._runtime.submit(self._setup_on_runtime_loop())
        setup_future.result()

    async def _setup_on_runtime_loop(self) -> None:
        # TODO : Can be moved to __init__
        self._queue: asyncio.Queue[TriggerRequest | Literal["STOP"]] = asyncio.Queue(
            maxsize=10
        )
        self._worker_task = asyncio.create_task(self._worker())

    async def _get_state_impl(self) -> S:
        self._assert_runtime_thread()
        return self._state

    async def _enqueue_task(self, task: Coroutine) -> None:
        self._assert_runtime_thread()

        result = self._runtime.loop.create_future()
        await self._queue.put(TriggerRequest(task=task, result=result))
        await result

    async def _worker(self) -> None:
        while True:
            request = await self._queue.get()

            if request == "STOP":
                self._queue.task_done()
                break

            try:
                await request.task
                request.result.set_result(None)
            except Exception as e:
                request.result.set_exception(e)
            finally:
                self._queue.task_done()

    async def _close_impl(self) -> None:
        self._assert_runtime_thread()

        if self._worker_task.done():
            return

        await self._queue.put("STOP")
        await self._queue.join()
        await self._worker_task

    def _assert_runtime_thread(self) -> None:
        if threading.current_thread() is not self._runtime._thread:
            raise RuntimeError(
                "State machine internals must run on the runtime thread."
            )

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
            self._runtime.submit(self._close_impl())
            # self.stop_engine()
            raise InvalidTransition(event_record=asdict(self.sm._event_log[-1]))

        return transitions


class AsyncEngine[S: Enum, E: Enum, C](BaseEngine):
    def stop_engine(self) -> None:
        self._runtime.submit(self._close_impl())

    async def stop_engine_async(self) -> None:
        await self._runtime.submit_async(self._close_impl())

    def force_stop_engine(self) -> None:
        self._runtime.stop()
        raise RuntimeError("Engine forcefully halted.")

    def start_engine(self, state: S, context: C, is_async: bool) -> None:
        self._start_engine()
        coro = self.evaluate_initial_state(state=state, context=context)

        # if is_async:
        #     await self._runtime.submit_async(self._enqueue_task(coro))
        # else:
        #     self._runtime.submit(self._enqueue_task(coro)).result()
        self._runtime.submit(self._enqueue_task(coro)).result()

    async def start_engine_async(self, state: S, context: C, is_async: bool) -> None:
        self._start_engine()
        coro = self.evaluate_initial_state(state=state, context=context)

        # if is_async:
        #     await self._runtime.submit_async(self._enqueue_task(coro))
        # else:
        #     self._runtime.submit(self._enqueue_task(coro)).result()
        await self._runtime.submit_async(self._enqueue_task(coro))

    def event_trigger(self, event: E, context: C, is_async: bool) -> None:
        coro = self.evaluate_transitions(event=event, context=context)

        self._runtime.submit(self._enqueue_task(coro)).result()

    async def event_trigger_async(self, event: E, context: C, is_async: bool) -> None:
        coro = self.evaluate_transitions(event=event, context=context)

        await self._runtime.submit_async(self._enqueue_task(coro))

    async def evaluate_initial_state(self, state: S, context: C) -> None:
        await self.evaluate_on_entry(state=state, context=context)
        await self.evaluate_transitions(event=None, context=context)

    async def evaluate_transitions(self, event: E | None, context: C) -> None:
        source_state = self._state
        transitions = self.resolve_transitions(state=source_state, event=event)

        # TODO : a source state has multiple automatic transitions decide
        #        on how to evaluate them - maybe based on priority
        transition_depth = 0
        while transitions and transition_depth < self._transition_depth:
            target_state, actions = await self.evaluate_guards(
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
            await self.evaluate_on_exit(state=source_state, context=context)
            await self.evaluate_transition_action(
                state=source_state, actions=actions, context=context
            )
            self._state = target_state
            self.sm._apply_transition(target=target_state)
            await self.evaluate_on_entry(state=target_state, context=context)
            await self.evaluate_on_transition(
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

    async def evaluate_guards(
        self,
        context: C,
        transitions: list[tuple[S, tuple[Action[C]] | None, tuple[Guard[C]] | None]],
    ) -> tuple[S | None, tuple[Action[C]] | None]:

        for target_state, actions, guards in transitions:
            if not guards or await self.execute_guards(guards=guards, context=context):
                return (target_state, actions)

        return (None, None)

    async def evaluate_on_entry(self, state: S, context: C) -> None:
        await self.execute_actions(
            source=state,
            context=context,
            actions=self.sm._config.on_entry.get(state),
            action_type=EngineEvent.ON_ENTRY,
        )

    async def evaluate_on_exit(self, state: S, context: C) -> None:
        await self.execute_actions(
            source=state,
            context=context,
            actions=self.sm._config.on_exit.get(state),
            action_type=EngineEvent.ON_EXIT,
        )

    async def evaluate_on_transition(self, source: S, target: S, context: C) -> None:
        await self.execute_actions(
            source=source,
            context=context,
            actions=self.sm._config.on_transition.get((source, target)),
            action_type=EngineEvent.ON_TRANSITION,
        )

    async def evaluate_transition_action(
        self, state: S, actions: tuple[Action[C]] | None, context: C
    ) -> None:
        await self.execute_actions(
            source=state,
            context=context,
            actions=actions,
            action_type=EngineEvent.TRANSITION_ACTION,
        )

    async def execute_guards(self, guards: tuple[Guard[C]], context: C) -> bool:
        for guard in guards:
            try:
                result = guard(context)
                passed = await result if isawaitable(result) else result
            except Exception as e:
                event_record = self.sm._dispatch_event(
                    machine_event=EngineEvent.EXCEPTION,
                    guard=guard,
                    error_type=GuardError,
                    error_message=f"<{type(e).__name__}>: {e}",
                )
                await self.stop_engine_async()
                raise GuardError(event_record=asdict(event_record)) from e

            self.sm._dispatch_event(
                machine_event=EngineEvent.GUARD_EVALUATE, guard=guard, passed=passed
            )

            if not passed:
                return False

        return True

    async def execute_actions(
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
                result = action(context)
                result = await result if isawaitable(result) else result
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
                await self.stop_engine_async()
                raise ActionError(event_record=asdict(event_record)) from e

    # async def execute_validators(self) -> None:
    #     event_record = self.sm._dispatch_event(
    #         machine_event=EngineEvent.EXCEPTION,
    #         source=source_state,
    #         target=target_state,
    #         event=event,
    #         error_type=BlockedTransition,
    #         error_message=f"No guards passed for {event = }",
    #     )
    #     raise BlockedTransition(event_record=asdict(event_record))
