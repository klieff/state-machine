from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable, Coroutine, Iterable
from concurrent.futures import Future
from dataclasses import dataclass
from enum import Enum
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
from .utils import normalize_state_event

if TYPE_CHECKING:
    from .configuration import ConfigSpec
    from .definitions import EventSpec, State, StateMachineConfig, StateSpec, Transition
    from .dispatcher import EventDispatcher

    from .statemachine import SyncStateMachine, AsyncStateMachine


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
        self._queue: asyncio.Queue[TaskRequest | Literal["STOP"]] = asyncio.Queue()
        self._worker_task = self._runtime.create_task(self._queue_manager())

    def _start_queue_manager(self) -> None:
        self._runtime.start()
        self._runtime.submit(self._start_on_runtime_loop()).result()
        self._running = True

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

    # def _dispatch_internal_event(self, machine_event: EngineEvent) -> None:
    #     record = AuditRecord(machine_event=machine_event.name, source=self._state.state)
    #
    #     token = active_audit_record.set(record)
    #     active_audit_record.reset(token)
    #     self._dispatcher.emit(record=record)


# TODO: Implement a dedicated error handler
class AsyncEngine(BaseEngine):
    def start_engine(
        self, initial_state: StateSpec, context: Any, is_async: bool
    ) -> Coroutine | None:
        if self._running:
            return

        state_name = initial_state
        if state_name not in self._config.states:
            raise InvalidState

        state = self._config.states[state_name]
        event = (
            EngineEvent.DYNAMIC_TRANSITION
            if state.type is StateType.CHOICE
            else (
                EngineEvent.AUTOMATIC_TRANSITION
                if state.type is StateType.TRANSIENT
                else None
            )
        )

        self._context = context
        self._state = state
        self._start_queue_manager()

        # if (state.name, event) not in self._config.transitions:
        #     event = None

        if state.on_entry:
            record = AuditRecord(
                event=EngineEvent.MACHINE_START.name,
                source=self._state.name,
            )
            info = self._info_pool
            info.source = state.state
            info.event = EngineEvent.MACHINE_START.name
            info.payload = None
            info.step = EngineStep.ON_ENTRY_EVALUATE.name
            record.transitions.append(self._info_pool)

            async def start_machine() -> None:
                token = active_audit_record.set(record)
                await self._execute_actions(
                    actions=state.on_entry, action_type=EngineStep.ON_ENTRY_EVALUATE
                )
                active_audit_record.reset(token)
                record.success = True
                self._dispatcher.emit(record)

            if event is None and is_async:
                return self._runtime.submit_async(
                    coro=self._queue_task(start_machine())
                )
            else:
                self._runtime.submit(coro=self._queue_task(start_machine())).result()

        if event is not None:
            return self.event_trigger(event=event, payload=None, is_async=is_async)

        if is_async:

            async def coro():
                pass

            return coro()

    def stop_engine(self, is_async: bool, force: bool = False) -> Coroutine | None:
        if not self._running:
            return

        coro = self._stop_queue_manager()
        if is_async:
            return self._runtime.submit_async(coro=coro)

        self._runtime.submit(coro=coro).result()

    def event_trigger(
        self, event: EventSpec, payload: Any, is_async: bool
    ) -> Coroutine | None:
        if not self._running:
            raise UninitializedError(machine_name=self._config.name)

        # event_name = normalize_state_event(event)
        # if self._config.events.get(event_name) is None:
        if self._config.events.get(event) is None:
            raise InvalidEvent

        record = AuditRecord(event=event)
        coro = self._processing_loop(event=event, record=record, payload=payload)

        if is_async:
            return self._runtime.submit_async(coro=self._queue_task(coro))

        self._runtime.submit(coro=self._queue_task(coro)).result()

    async def _processing_loop(
        self, event: EventSpec, record: AuditRecord, payload: Any | None = None
    ) -> None:
        token = active_audit_record.set(record)

        # TODO: If a source state has multiple automatic transitions decide
        #       on how to evaluate them - maybe based on a priority flag.
        #       Currently, no audit record is logged if no event-triggered transition
        #       exists or if MAX_TRANSITION_DEPTH has been exceeded.
        try:
            transition_depth = 0
            while transition_depth < self._transition_depth and self._running:
                source_state = self._state
                info = self._info_pool
                info.source = source_state.state
                info.event = self._config.events.get(event)
                info.payload = payload

                transitions = self._resolve_transitions(state=self._state, event=event)

                if transitions is None:
                    break  # Break if transition map is empty

                info.step = EngineStep.GUARD_EVALUATE.name
                transition = await self._evaluate_guards(transitions=transitions)

                if transition is None:
                    break  # Break if no valid transition is found

                target_state = transition.target

                # TODO: Should fail-fast and throw an exception if target state is not registered
                if router := transition.router:
                    info.step = EngineStep.ROUTER_EVALUATE.name
                    target_state = await self._execute_choice_transition(router=router)

                if target_state is None:
                    break  # Break if dynamic router returns an invalid state

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

                record.success = True

                if target_state.final_state:
                    break  # Break if target state is a final state

                if target_state.type is StateType.TRANSIENT:
                    event = EngineEvent.AUTOMATIC_TRANSITION
                elif target_state.type is StateType.CHOICE:
                    event = EngineEvent.DYNAMIC_TRANSITION
                else:
                    break  # Break if no automatic/dynamic transitions

                transition_depth += 1
        except Exception as e:
            record.event = EngineEvent.EXCEPTION
            record.exception = e
            raise RuntimeError("Error in engine processing loop") from e
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

    async def _evaluate_on_entry(self, state: State) -> None:
        await self._execute_actions(
            actions=state.on_entry, action_type=EngineStep.ON_ENTRY_EVALUATE
        )

    async def _evaluate_on_exit(self, state: State) -> None:
        await self._execute_actions(
            actions=state.on_exit, action_type=EngineStep.ON_EXIT_EVALUATE
        )

    # async def evaluate_on_transition(self, source: S, target: S, info: Any) -> None:
    #     await self._execute_actions(
    #         info=info,
    #         actions=self._config.on_transition.get((source, target)),
    #         action_type=EngineStep.ON_TRANSITION,
    #     )

    async def _evaluate_transition_action(self, actions: list[CallbackSpec]) -> None:
        await self._execute_actions(
            actions=actions,
            action_type=EngineStep.ACTION_EVALUATE,
        )

    async def _execute_guards(self, guards: Iterable[CallbackSpec]) -> bool:
        for guard in guards:
            microstep = MicroStep(
                micro_step=EngineStep.GUARD_EVALUATE.name, target=str(self._state.name)
            )
            try:
                result = guard.invoke(self._context, self._info_pool)
                passed = await result if guard.is_async else result
                microstep.result = passed
            except Exception as e:
                microstep.micro_step = EngineEvent.EXCEPTION.name
                raise GuardError from e
            finally:
                self._dispatcher.log_micro_step(microstep)

            if not passed:
                return False

        return True

    async def _execute_actions(
        self,
        actions: Iterable[CallbackSpec],
        action_type: EngineStep,
    ) -> None:
        for action in actions:
            microstep = MicroStep(
                micro_step=action_type.name, target=action.callback.__name__
            )
            try:
                result = action.invoke(self._context, self._info_pool)
                result = await result if action.is_async else result
                microstep.result = result
            except Exception as e:
                microstep.micro_step = EngineEvent.EXCEPTION.name
                raise ActionError from e
            finally:
                self._dispatcher.log_micro_step(microstep)

    async def _execute_choice_transition(self, router: RouterSpec) -> State | None:
        router_state = await self._execute_callback(callback=router)
        # router_state_name = (
        #     router_state.name if isinstance(router_state, Enum) else router_state
        # )
        # if router_state_name in self._config.states:
        #     return self._config.states[router_state_name]
        if router_state in self._config.states:
            return self._config.states[router_state]

    async def _execute_callback(self, callback: CallbackSpec) -> Any:
        try:
            result = callback.invoke(self._context, self._info_pool)
            result = await result if callback.is_async else result
        except Exception as e:
            raise e

        return result

    def _apply_state_mutation(self, state: State) -> None:
        self._state = state
        self._dispatcher.log_micro_step(
            MicroStep(micro_step=EngineStep.STATE_CHANGE.name, result=True)
        )

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
