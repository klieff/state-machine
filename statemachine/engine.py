from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable, Coroutine, Iterable
from concurrent.futures import Future as ConcurrentFuture
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from .audit import AuditRecord, MicroStep
from .callbacks import CallbackSpec
from .definitions import EngineEvent, EngineStep, RouterSpec, StateType, TransitionInfo
from .dispatcher import active_audit_record
from .exceptions import (
    ActionError,
    GuardError,
    InvalidEvent,
    InvalidState,
    UninitializedError,
)

__all__ = ["AsyncEngine"]

if TYPE_CHECKING:
    from .configuration import ConfigSpec
    from .definitions import EventSpec, State, StateSpec, Transition
    from .dispatcher import EventDispatcher
    from .statemachine import AsyncStateMachine, SyncStateMachine


class InvalidRouterState(Exception): ...


class MaxTransitionError(Exception): ...


class CallbackError(Exception): ...


@dataclass(slots=True, frozen=True)
class TaskRequest:
    coro: Coroutine
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

    def submit[R](self, coro: Coroutine[Any, Any, R]) -> ConcurrentFuture[R]:
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
class BaseEngine:
    _state: State
    _runtime: EngineRuntime = EngineRuntime()

    def __init__(
        self,
        sm: SyncStateMachine | AsyncStateMachine,
        config: ConfigSpec,
        dispatcher: EventDispatcher,
        depth: int = 100,
    ):
        self._sm = sm
        self._config = config
        self._dispatcher = dispatcher
        self._info_pool = TransitionInfo(machine=sm)
        self._transition_depth = depth
        self._running: bool = False

    async def _start_on_runtime_loop(self) -> None:
        if not self._running:
            self._queue: asyncio.Queue[TaskRequest | Literal["STOP"]] = asyncio.Queue()
            self._worker_task = self._runtime.create_task(self._queue_manager())
            self._running = True

    def _start_queue_manager(self) -> None:
        if not self._runtime.is_running():
            self._runtime.start()

        self._runtime.submit(self._start_on_runtime_loop()).result()

    async def _stop_queue_manager(self) -> None:
        if self._running:
            self._drain_queue()
            await self._queue.put("STOP")
            await self._queue.join()
            await self._worker_task
            self._running = False

    async def _queue_task(self, coro: Coroutine[Any, Any, Any]) -> None:
        future = self._runtime.create_future()
        await self._queue.put(TaskRequest(coro=coro, future=future))
        await future

    async def _queue_manager(self) -> None:
        try:
            while True:
                request = await self._queue.get()
                if request == "STOP":
                    self._queue.task_done()
                    break

                try:
                    await request.coro
                    request.future.set_result(None)
                except Exception as e:
                    request.future.set_exception(e)
                    break
                finally:
                    self._queue.task_done()
        finally:
            self._drain_queue()

    def _drain_queue(self):
        while not self._queue.empty():
            try:
                item = self._queue.get_nowait()
                if item == "STOP":
                    self._queue.task_done()
                    break

                if not item.future.done():
                    item.future.cancel()

                try:
                    item.coro.close()
                except RuntimeError:
                    pass

                self._queue.task_done()
            except asyncio.QueueEmpty:
                break

    # async def _get_state(self) -> StateSpec:
    #     self._runtime.assert_runtime_thread()
    #     return self._state


# TODO: Implement a dedicated error handler
class AsyncEngine(BaseEngine):
    def start_engine(
        self, initial_state: StateSpec, context: Any, is_async: bool
    ) -> Coroutine | None:
        if self._running:
            return

        state = self._config.states.get(initial_state)
        if state is None:
            raise InvalidState

        self._context = context
        self._state = state
        self._start_queue_manager()

        event = None
        if state.type is StateType.CHOICE:
            event = EngineEvent.DYNAMIC_TRANSITION
        elif state.type is StateType.AUTOMATIC:
            event = EngineEvent.AUTOMATIC_TRANSITION

        if state.on_entry:
            info = self._info_pool
            info.source = state.state
            info.event = EngineEvent.MACHINE_START.name
            info.payload = None
            info.step = EngineStep.ON_ENTRY_EVALUATE.name

            coro = self._evaluate_on_entry(state)
            self._runtime.submit(coro=self._queue_task(coro)).result()

        if event is not None:
            return self.event_trigger(event=event, payload=None, is_async=is_async)

    def stop_engine(self, is_async: bool, force: bool = False) -> Coroutine | None:
        coro = self._stop_queue_manager()
        if is_async:
            return self._runtime.submit_async(coro=coro)

        self._runtime.submit(coro=coro).result()

    def event_trigger(
        self, event: EventSpec, payload: Any, is_async: bool
    ) -> Coroutine | None:
        if not self._running:
            raise UninitializedError(machine_name=self._config.name)

        if event not in self._config.events:
            raise InvalidEvent

        coro = self._processing_loop(event=event, payload=payload)
        if is_async:
            return self._runtime.submit_async(coro=self._queue_task(coro))

        self._runtime.submit(coro=self._queue_task(coro)).result()

    async def _processing_loop(
        self, event: EventSpec, payload: Any | None = None
    ) -> None:
        record = AuditRecord(event=event)
        token = active_audit_record.set(record)

        # TODO: If a source state has multiple automatic transitions decide
        #       on how to evaluate them - maybe based on a priority flag.
        try:
            transition_depth = 0
            while event is not EngineEvent.TRANSITION_COMPLETE and self._running:
                source_state = self._state
                info = self._info_pool
                info.source = source_state.state
                info.event = self._config.events.get(event)
                info.payload = payload

                transitions = self._resolve_transitions(state=self._state, event=event)

                if transitions is None:
                    break

                info.step = EngineStep.GUARD_EVALUATE.name
                transition = await self._evaluate_guards(transitions=transitions)

                if transition is None:
                    break

                target_state = transition.target

                if router := transition.router:
                    info.step = EngineStep.ROUTER_EVALUATE.name
                    target_state = await self._evaluate_choice_transition(router=router)

                info.target = target_state.state

                if source_state.on_exit:
                    info.step = EngineStep.ON_EXIT_EVALUATE.name
                    await self._evaluate_on_exit(state=source_state)

                if transition.actions:
                    info.step = EngineStep.ACTION_EVALUATE.name
                    await self._evaluate_transition_action(actions=transition.actions)

                self._apply_state_mutation(state=target_state)

                if target_state.on_entry:
                    info.step = EngineStep.ON_ENTRY_EVALUATE.name
                    await self._evaluate_on_entry(state=target_state)

                if target_state.type not in (StateType.AUTOMATIC, StateType.CHOICE):
                    event = EngineEvent.TRANSITION_COMPLETE

                transition_depth += 1
                if transition_depth >= self._transition_depth:
                    raise MaxTransitionError

                record.event = event
                record.success = True
        except Exception as e:
            record.event = EngineEvent.EXCEPTION
            record.exception = e
            record.success = False
            raise RuntimeError("Internal Engine Error in processing loop.") from e
        finally:
            active_audit_record.reset(token)
            self._dispatcher.emit(record)

    async def _evaluate_guards(
        self, transitions: list[Transition]
    ) -> Transition | None:
        for transition in transitions:
            guards = transition.guards
            if not guards or await self._execute_guards(guards=guards):
                return transition

    async def _execute_guards(self, guards: Iterable[CallbackSpec]) -> bool:
        for guard in guards:
            if not await self._execute_callback(callback=guard):
                return False

        return True

    async def _evaluate_on_entry(self, state: State) -> None:
        for on_entry in state.on_entry:
            await self._execute_callback(callback=on_entry)

    async def _evaluate_on_exit(self, state: State) -> None:
        for on_exit in state.on_exit:
            await self._execute_callback(callback=on_exit)

    async def _evaluate_transition_action(self, actions: list[CallbackSpec]) -> None:
        for action in actions:
            await self._execute_callback(callback=action)

    async def _evaluate_choice_transition(self, router: RouterSpec) -> State:
        router_state = await self._execute_callback(callback=router)
        if router_state in self._config.states:
            return self._config.states[router_state]

        raise InvalidRouterState

    async def _execute_callback(self, callback: CallbackSpec) -> Any:
        try:
            result = callback.invoke(self._context, self._info_pool)
            result = await result if callback.is_async else result
        except Exception as e:
            raise CallbackError from e

        return result

    def _apply_state_mutation(self, state: State) -> None:
        self._state = state

    def _resolve_transitions(
        self, state: State, event: EventSpec
    ) -> list[Transition] | None:
        return self._config.transitions.get((state.state, event))

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
