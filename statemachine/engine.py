from __future__ import annotations

import asyncio
import threading
from collections.abc import Awaitable, Callable, Coroutine, Iterable
from concurrent.futures import Future
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Literal

from .audit import AuditRecord, MicroStep
from .callbacks import CallbackSpec, invoke_callback
from .definitions import EngineEvent, EngineStep
from .dispatcher import active_audit_record
from .exceptions import ActionError, GuardError

if TYPE_CHECKING:
    from .definitions import StateMachineConfig, Transition, State, StateSpec, EventSpec
    from .dispatcher import EventDispatcher


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
class BaseEngine:
    _state: StateSpec
    _runtime: EngineRuntime = EngineRuntime()

    def __init__(
        self,
        config: StateMachineConfig,
        dispatcher: EventDispatcher,
        transition_depth: int = 100,
    ):
        self._config = config
        self._dispatcher = dispatcher
        self._transition_depth = transition_depth
        self._running: bool = False

    async def _start_on_runtime_loop(self) -> None:
        self._queue: asyncio.Queue[TaskRequest | Literal["STOP"]] = asyncio.Queue()
        self._worker_task = self._runtime.create_task(self._worker())

    def _start_worker(self) -> None:
        self._runtime.start()
        self._runtime.submit(self._start_on_runtime_loop()).result()
        self._running = True

        record = AuditRecord(
            machine_event=EngineEvent.MACHINE_START.name,
            source=self._state,
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
                source=self._state,
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

    async def _get_state(self) -> StateSpec:
        self._runtime.assert_runtime_thread()
        return self._state

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
    ) -> Awaitable | None:
        if self._running:
            return

        self._context = context
        self._state = initial_state  # self._config.states.get(initial_state)
        self._start_worker()

        return self.event_trigger(event=None, payload=None, is_async=is_async)

    def stop_engine(self, is_async: bool, force: bool = False) -> Awaitable | None:
        if not self._running:
            return

        coro = self._stop_worker()
        if is_async:
            return self._runtime.submit_async(coro=coro)

        self._runtime.submit(coro=coro).result()

    def event_trigger(
        self, event: EventSpec | None, payload: Any, is_async: bool
    ) -> Awaitable | None:
        coro = self.processing_loop(event=event, payload=payload)

        if is_async:
            return self._runtime.submit_async(coro=self._queue_task(coro))

        self._runtime.submit(coro=self._queue_task(coro)).result()

    async def processing_loop(
        self, event: EventSpec | None, payload: Any | None = None
    ) -> None:
        transitions = self.resolve_transitions(state=self._state, event=event)

        machine_event = (
            EngineEvent.EVENT_TRIGGER if event else EngineEvent.NULL_TRANSITION
        )
        info = AuditRecord(
            machine_event=machine_event.name,
            trigger_event=event,
            success=False,
            payload=payload,
        )
        token = active_audit_record.set(info)

        # TODO: If a source state has multiple automatic transitions decide
        #       on how to evaluate them - maybe based on a priority flag.
        #       Currently, no audit record is logged if no event-triggered transition
        #       exists or if MAX_TRANSITION_DEPTH has been exceeded.
        try:
            transition_depth = 0
            while transitions and transition_depth < self._transition_depth:
                info.source = self._config.states[self._state].state

                transition = await self.evaluate_guards(
                    transitions=transitions, info=info
                )

                if transition is None:
                    break

                target_state = transition.target

                if transition.router and transition.target is None:
                    result = invoke_callback(transition.router, self._context, info)
                    router_state: Any = (
                        await result if transition.router.is_async else result
                    )
                    router_state_name = (
                        router_state.name
                        if isinstance(router_state, Enum)
                        else str(router_state)
                    )
                    if self._config.states.get(router_state_name):
                        target_state = router_state_name

                if target_state is None:
                    break

                source_state = self._config.states[self._state]
                info.target = self._config.states[target_state].state

                if source_state.on_exit:
                    await self.evaluate_on_exit(state=source_state, info=info)

                if transition.actions:
                    await self.evaluate_transition_action(
                        actions=transition.actions, info=info
                    )

                self.apply_state_mutation(state=target_state)

                target_state = self._config.states[target_state]
                info.success = True

                if target_state.on_entry:
                    await self.evaluate_on_entry(state=target_state, info=info)

                # await self.evaluate_on_transition(
                #     source=source_state, target=target_state, info=info
                # )

                if target_state.final_state:
                    break

                event = None
                transitions = self.resolve_transitions(state=self._state, event=event)
                transition_depth += 1
        except Exception as e:
            raise RuntimeError("BIGLY error") from e
        finally:
            active_audit_record.reset(token)
            self._dispatcher.emit(info)

    def apply_state_mutation(self, state: StateSpec) -> None:
        self._state = state
        self._dispatcher.log_micro_step(
            MicroStep(micro_step=EngineStep.STATE_CHANGE.name, result=True)
        )

    async def evaluate_guards(
        self,
        transitions: list[Transition],
        info: Any,
    ) -> Transition | None:

        for transition in transitions:
            guards = transition.guards
            if not guards or await self.execute_guards(guards=guards, info=info):
                return transition

    async def evaluate_on_entry(self, state: State, info: Any) -> None:
        await self.execute_actions(
            info=info,
            actions=state.on_entry,
            action_type=EngineStep.ON_ENTRY,
        )

    async def evaluate_on_exit(self, state: State, info: Any) -> None:
        await self.execute_actions(
            info=info,
            actions=state.on_exit,
            action_type=EngineStep.ON_EXIT,
        )

    # async def evaluate_on_transition(self, source: S, target: S, info: Any) -> None:
    #     await self.execute_actions(
    #         info=info,
    #         actions=self._config.on_transition.get((source, target)),
    #         action_type=EngineStep.ON_TRANSITION,
    #     )

    async def evaluate_transition_action(
        self, actions: list[CallbackSpec | None], info: Any
    ) -> None:
        await self.execute_actions(
            info=info,
            actions=actions,
            action_type=EngineStep.TRANSITION_ACTION,
        )

    async def execute_guards(
        self, guards: Iterable[CallbackSpec | None], info: Any
    ) -> bool:
        for guard in guards:
            if guard is None:
                continue

            microstep = MicroStep(
                micro_step=EngineStep.GUARD_EVALUATE.name, target=str(self._state)
            )
            try:
                result = invoke_callback(guard, self._context, info)
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

    async def execute_actions(
        self,
        info: Any,
        actions: Iterable[CallbackSpec | None],
        action_type: EngineStep,
    ) -> None:
        if not actions:
            return

        for action in actions:
            if action is None:
                continue

            microstep = MicroStep(
                micro_step=action_type.name, target=action.callback.__name__
            )
            try:
                result = invoke_callback(action, self._context, info)
                result = await result if action.is_async else result
                microstep.result = result
            except Exception as e:
                microstep.micro_step = EngineEvent.EXCEPTION.name
                raise ActionError from e
            finally:
                self._dispatcher.log_micro_step(microstep)

    def resolve_transitions(
        self, state: StateSpec, event: EventSpec | None
    ) -> list[Transition] | None:
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
