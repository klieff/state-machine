from __future__ import annotations

import asyncio
import threading
from collections.abc import Awaitable, Coroutine, Iterable, Callable
from concurrent.futures import Future
from dataclasses import dataclass
from enum import Enum
from inspect import isawaitable
from typing import TYPE_CHECKING, Any, Literal

from .audit import AuditRecord, MicroStep
from .dispatcher import active_audit_record
from .definitions import EngineEvent, EngineStep
from .exceptions import ActionError, GuardError

if TYPE_CHECKING:
    from .dispatcher import EventDispatcher
    from .definitions import Action, Guard, Transition, StateMachineConfig


@dataclass(slots=True)
class TaskRequest:
    task: Coroutine
    future: asyncio.Future[None]


class EngineRuntime:
    def __init__(self) -> None:
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            name="SMEngineThread",
            target=self._run_loop,
            daemon=True,
        )

    def _run_loop(self) -> None:
        asyncio.set_event_loop(loop=self._loop)
        self._loop.run_forever()

    def call_soon(self, callback: Callable[..., Any], *args: Any) -> None:
        self._loop.call_soon_threadsafe(callback, *args)

    def create_future(self) -> asyncio.Future:
        self.assert_runtime_thread()
        return self._loop.create_future()

    def create_task[R](self, coro: Coroutine[Any, Any, R]) -> asyncio.Task[R]:
        self.assert_runtime_thread()
        return self._loop.create_task(coro=coro)

    def submit[R](self, coro: Coroutine[Any, Any, R]) -> Future[R]:
        return asyncio.run_coroutine_threadsafe(coro=coro, loop=self._loop)

    async def submit_async[R](self, coro: Coroutine[Any, Any, R]) -> R:
        future = self.submit(coro=coro)
        return await asyncio.wrap_future(future)

    def start(self) -> None:
        if not self.is_running():
            self._thread.start()

    def shutdown(self) -> None:
        if self.is_running():
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join()

    def is_running(self) -> bool:
        return self._thread.is_alive()

    def is_runtime_thread(self) -> bool:
        return threading.current_thread() is self._thread

    def assert_runtime_thread(self) -> None:
        if not self.is_runtime_thread():
            raise RuntimeError(
                "State machine internals must run on the runtime thread."
            )


# TODO: Implement a drain queue instead of a permanent queue per instance
class BaseEngine[S: Enum]:
    _runtime: EngineRuntime = EngineRuntime()

    def __init__(
        self,
        config: StateMachineConfig,
        dispatcher: EventDispatcher,
        transition_depth: int = 10,
    ):
        self._config = config
        self._dispatcher = dispatcher
        self._state = config.initial_state
        self._transition_depth = transition_depth
        self._running = False

    async def _start_on_runtime_loop(self) -> None:
        self._queue: asyncio.Queue[TaskRequest | Literal["STOP"]] = asyncio.Queue()
        self._worker_task = self._runtime.create_task(self._worker())

    def _start_worker(self) -> None:
        self._runtime.start()
        self._runtime.submit(self._start_on_runtime_loop()).result()
        self._running = True

        record = AuditRecord(
            machine_event=EngineEvent.MACHINE_START.name,
            source_state=self._state.name,
            trigger_event="None",
            success=self._running,
            timeline=[MicroStep()],
        )
        self._dispatcher.emit(record)

    async def _stop_worker(self) -> None:
        if self._running:
            await self._queue.put("STOP")
            await self._queue.join()
            await self._worker_task
            self._running = False

            record = AuditRecord(
                machine_event=EngineEvent.MACHINE_STOP.name,
                source_state=self._state.name,
                trigger_event="None",
                success=True,
                timeline=[MicroStep()],
            )
            self._dispatcher.emit(record=record)

    async def _queue_task(self, task: Coroutine) -> None:
        future = self._runtime.create_future()
        await self._queue.put(TaskRequest(task=task, future=future))
        await future

    async def _worker(self) -> None:
        while True:
            request = await self._queue.get()
            if request == "STOP":
                self._queue.task_done()
                break

            try:
                await request.task
                request.future.set_result(None)
            except Exception as e:
                request.future.set_exception(e)
            finally:
                self._queue.task_done()

    async def _get_state(self) -> S:
        self._runtime.assert_runtime_thread()
        return self._state

    def _dispatch_internal_event(self, machine_event: EngineEvent) -> None:
        record = AuditRecord(
            machine_event=machine_event.name, source_state=self._state.name
        )

        token = active_audit_record.set(record)
        active_audit_record.reset(token)
        self._dispatcher.emit(record=record)


# TODO: Implement a dedicated error handler
class AsyncEngine[S: Enum, E: Enum, C](BaseEngine):
    def start_engine(self, context: C, is_async: bool) -> Awaitable | None:
        if self._running:
            return

        self._start_worker()
        return self.event_trigger(event=None, context=context, is_async=is_async)

    def stop_engine(self, is_async: bool, force: bool = False) -> Awaitable | None:
        if not self._running:
            return

        coro = self._stop_worker()
        if is_async:
            return self._runtime.submit_async(coro=coro)

        self._runtime.submit(coro=coro).result()

    def event_trigger(
        self, event: E | None, context: C, is_async: bool
    ) -> Awaitable | None:
        coro = self.processing_loop(event=event, context=context)
        if is_async:
            return self._runtime.submit_async(coro=self._queue_task(coro))

        return self._runtime.submit(coro=self._queue_task(coro)).result()

    async def processing_loop(self, event: E | None, context: C) -> None:
        source_state = self._state

        if event is None:
            await self.evaluate_on_entry(state=source_state, context=context)

        transitions = self.resolve_transitions(state=source_state, event=event)

        # TODO: If a source state has multiple automatic transitions decide
        #       on how to evaluate them - maybe based on a priority flag.
        #       Currently, no audit record is logged if no event-triggered transition
        #       exists or if MAX_TRANSITION_DEPTH has been exceeded.
        transition_depth = 0
        while transitions and transition_depth < self._transition_depth:
            machine_event = (
                EngineEvent.EVENT_TRIGGER if event else EngineEvent.NULL_TRANSITION
            )
            record = AuditRecord(
                machine_event=machine_event.name,
                source_state=source_state.name,
                trigger_event="None" if event is None else event.name,
                success=False,
            )
            token = active_audit_record.set(record)

            try:
                target_state, actions = await self.evaluate_guards(
                    context=context, transitions=transitions
                )

                if target_state is None:
                    break

                await self.evaluate_on_exit(state=source_state, context=context)
                await self.evaluate_transition_action(actions=actions, context=context)

                self.apply_state_mutation(state=target_state)
                record.target_state = target_state.name
                record.success = True

                await self.evaluate_on_entry(state=target_state, context=context)
                await self.evaluate_on_transition(
                    source=source_state, target=target_state, context=context
                )
            except Exception as e:
                raise RuntimeError("BIGLY error") from e
            finally:
                active_audit_record.reset(token)
                self._dispatcher.emit(record)

            event = None
            source_state = target_state
            transitions = self.resolve_transitions(state=source_state, event=event)
            transition_depth += 1

    def apply_state_mutation(self, state: S) -> None:
        self._state = state
        self._dispatcher.log_micro_step(
            MicroStep(micro_step=EngineStep.STATE_CHANGE.name, result=True)
        )

    async def evaluate_guards(
        self,
        context: C,
        transitions: list[Transition],
    ) -> tuple[S | None, tuple[Action[C]] | None]:

        for target_state, actions, guards in transitions:
            if not guards or await self.execute_guards(guards=guards, context=context):
                return (target_state, actions)

        return (None, None)

    async def evaluate_on_entry(self, state: S, context: C) -> None:
        await self.execute_actions(
            context=context,
            actions=self._config.on_entry.get(state),
            action_type=EngineStep.ON_ENTRY,
        )

    async def evaluate_on_exit(self, state: S, context: C) -> None:
        await self.execute_actions(
            context=context,
            actions=self._config.on_exit.get(state),
            action_type=EngineStep.ON_EXIT,
        )

    async def evaluate_on_transition(self, source: S, target: S, context: C) -> None:
        await self.execute_actions(
            context=context,
            actions=self._config.on_transition.get((source, target)),
            action_type=EngineStep.ON_TRANSITION,
        )

    async def evaluate_transition_action(
        self, actions: tuple[Action[C]] | None, context: C
    ) -> None:
        await self.execute_actions(
            context=context,
            actions=actions,
            action_type=EngineStep.TRANSITION_ACTION,
        )

    async def execute_guards(self, guards: tuple[Guard[C]], context: C) -> bool:
        for guard in guards:
            microstep = MicroStep(
                micro_step=EngineStep.GUARD_EVALUATE.name, target=self._state.name
            )
            try:
                result = guard(context)
                passed = await result if isawaitable(result) else result
                microstep.result = passed
            except Exception as e:
                microstep.micro_step = EngineEvent.EXCEPTION.name
                raise GuardError from e
            finally:
                self._dispatcher.log_micro_step(microstep)

            if not passed:
                return False

        return True

    async def execute_actions(
        self,
        context: C,
        actions: Iterable[Action[C]] | None,
        action_type: EngineStep,
    ) -> None:
        if not actions:
            return

        for action in actions:
            microstep = MicroStep(micro_step=action_type.name, target=self._state.name)
            try:
                result = action(context)
                result = await result if isawaitable(result) else result
            except Exception as e:
                microstep.micro_step = EngineEvent.EXCEPTION.name
                raise ActionError from e
            finally:
                self._dispatcher.log_micro_step(microstep)

    def resolve_transitions(
        self, state: S, event: E | None
    ) -> list[Transition[S, C]] | None:
        return self._config.transitions.get((state, event))

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
