The state machine owns a private event-loop thread and processes all events sequentially through an internal queue. Callers from any thread/event loop can safely call `await machine.trigger(...)`.

The key idea is:

```text
caller thread/event loop
    ↓
machine.trigger(event)
    ↓ thread-safe submission
owner event loop
    ↓
internal event queue
    ↓
one worker processes events sequentially
    ↓
atomic transition macro-step
```

So even if two threads interact with the same instance, only the machine’s owner loop mutates its state.

---

# 1. State machine implementation

```python
from __future__ import annotations

import asyncio
import inspect
import threading
from collections.abc import Awaitable, Callable
from concurrent.futures import Future
from dataclasses import dataclass
from typing import Any, Literal


State = str
Event = str

Action = Callable[["AsyncStateMachine"], None | Awaitable[None]]
Guard = Callable[["AsyncStateMachine"], bool | Awaitable[bool]]


@dataclass(frozen=True)
class Transition:
    source: State
    event: Event | None
    target: State
    guard: Guard | None = None
    action: Action | None = None


@dataclass
class TriggerRequest:
    event: Event
    result: asyncio.Future[None]


class AsyncStateMachine:
    def __init__(self, initial_state: State) -> None:
        self._state = initial_state

        # Transition definitions are not loop-bound.
        self._transitions: list[Transition] = []
        self._entry_actions: dict[State, list[Action]] = {}
        self._exit_actions: dict[State, list[Action]] = {}

        # Owner event loop infrastructure.
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._run_owner_loop,
            name="StateMachineLoop",
            daemon=True,
        )
        self._thread.start()

        # Loop-bound objects must be created inside the owner loop.
        setup_future = asyncio.run_coroutine_threadsafe(
            self._setup_on_owner_loop(),
            self._loop,
        )
        setup_future.result()

    # ------------------------------------------------------------------
    # Public configuration API
    # ------------------------------------------------------------------

    def add_transition(
        self,
        source: State,
        event: Event | None,
        target: State,
        *,
        guard: Guard | None = None,
        action: Action | None = None,
    ) -> None:
        """
        event=None means automatic transition.

        In a production library you may want to forbid configuration changes
        after the machine has started, or route these mutations through the
        owner loop too.
        """
        self._transitions.append(
            Transition(
                source=source,
                event=event,
                target=target,
                guard=guard,
                action=action,
            )
        )

    def add_entry_action(self, state: State, action: Action) -> None:
        self._entry_actions.setdefault(state, []).append(action)

    def add_exit_action(self, state: State, action: Action) -> None:
        self._exit_actions.setdefault(state, []).append(action)

    @property
    def state(self) -> State:
        """
        Simple read.

        For strict consistency, expose an async get_state() that runs on the
        owner loop. This property is okay for casual inspection but not for
        linearizable concurrent semantics.
        """
        return self._state

    async def get_state(self) -> State:
        return await self._submit_to_owner_loop(self._get_state_impl())

    # ------------------------------------------------------------------
    # Public runtime API
    # ------------------------------------------------------------------

    async def trigger(self, event: Event) -> None:
        """
        Async-friendly public API.

        This method may be awaited from any event loop in any thread.
        The actual state machine logic still runs only on the owner loop.
        """
        await self._submit_to_owner_loop(self._enqueue_event(event))

    def trigger_sync(self, event: Event) -> None:
        """
        Synchronous API for non-async callers.

        This can be called from any ordinary thread.
        """
        future = asyncio.run_coroutine_threadsafe(
            self._enqueue_event(event),
            self._loop,
        )
        future.result()

    async def close(self) -> None:
        """
        Shut down the owner loop cleanly.
        """
        await self._submit_to_owner_loop(self._close_impl())

        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join()

    # ------------------------------------------------------------------
    # Owner loop setup
    # ------------------------------------------------------------------

    def _run_owner_loop(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    async def _setup_on_owner_loop(self) -> None:
        self._queue: asyncio.Queue[TriggerRequest | Literal["STOP"]] = asyncio.Queue()
        self._worker_task = asyncio.create_task(self._worker())

    async def _submit_to_owner_loop(self, coro: Awaitable[Any]) -> Any:
        """
        Submit a coroutine to the machine's owner loop from any caller loop.

        asyncio.run_coroutine_threadsafe returns a concurrent.futures.Future.
        asyncio.wrap_future makes it awaitable from the caller's event loop.
        """
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return await asyncio.wrap_future(future)

    # ------------------------------------------------------------------
    # Owner-loop methods
    # ------------------------------------------------------------------

    async def _get_state_impl(self) -> State:
        self._assert_owner_thread()
        return self._state

    async def _enqueue_event(self, event: Event) -> None:
        """
        This runs on the owner loop.

        It enqueues the event and waits until the worker has processed it.
        """
        self._assert_owner_thread()

        result = self._loop.create_future()
        await self._queue.put(TriggerRequest(event=event, result=result))
        await result

    async def _worker(self) -> None:
        """
        Single consumer.

        Because there is only one worker, transition macro-steps cannot
        interleave.
        """
        while True:
            request = await self._queue.get()

            if request == "STOP":
                self._queue.task_done()
                break

            try:
                await self._process_external_event(request.event)
                request.result.set_result(None)
            except Exception as exc:
                request.result.set_exception(exc)
            finally:
                self._queue.task_done()

    async def _process_external_event(self, event: Event) -> None:
        """
        Atomic transition macro-step:

            external event transition
            + exit action(s)
            + transition action
            + state update
            + entry action(s)
            + automatic transition chain

        Since this is called only by the single worker task, no other transition
        can run at the same time.
        """
        self._assert_owner_thread()

        transition = await self._find_enabled_transition(event)

        if transition is None:
            raise RuntimeError(
                f"No enabled transition from state {self._state!r} "
                f"for event {event!r}"
            )

        await self._apply_transition(transition)

        # Exhaust automatic transitions before accepting the next external event.
        await self._run_automatic_transitions()

    async def _run_automatic_transitions(self) -> None:
        """
        Keep applying eventless transitions until the machine reaches a stable state.
        """
        while True:
            transition = await self._find_enabled_transition(event=None)

            if transition is None:
                break

            await self._apply_transition(transition)

    async def _find_enabled_transition(self, event: Event | None) -> Transition | None:
        for transition in self._transitions:
            if transition.source != self._state:
                continue

            if transition.event != event:
                continue

            if transition.guard is None:
                return transition

            guard_result = transition.guard(self)
            if inspect.isawaitable(guard_result):
                guard_result = await guard_result

            if guard_result:
                return transition

        return None

    async def _apply_transition(self, transition: Transition) -> None:
        old_state = self._state
        new_state = transition.target

        print(
            f"[{threading.current_thread().name}] "
            f"{old_state} --{transition.event}--> {new_state}"
        )

        for action in self._exit_actions.get(old_state, []):
            await self._run_action(action)

        if transition.action is not None:
            await self._run_action(transition.action)

        # The actual state mutation happens only on the owner thread.
        self._state = new_state

        for action in self._entry_actions.get(new_state, []):
            await self._run_action(action)

    async def _run_action(self, action: Action) -> None:
        result = action(self)
        if inspect.isawaitable(result):
            await result

    async def _close_impl(self) -> None:
        self._assert_owner_thread()

        await self._queue.put("STOP")
        await self._queue.join()
        await self._worker_task

    def _assert_owner_thread(self) -> None:
        if threading.current_thread() is not self._thread:
            raise RuntimeError(
                "State machine internals must only run on the owner thread."
            )
```

---

# 2. Example usage

A small machine with:

```text
idle --start--> running
running --finish--> finishing
finishing --automatic--> idle
```

The automatic transition is represented by `event=None`.

```python
import asyncio
import threading


async def async_transition_action(machine: AsyncStateMachine) -> None:
    print(
        f"    action running on thread: {threading.current_thread().name}, "
        f"state currently: {machine.state!r}"
    )
    await asyncio.sleep(0.2)


async def async_entry_action(machine: AsyncStateMachine) -> None:
    print(
        f"    entry action on thread: {threading.current_thread().name}, "
        f"entered: {machine.state!r}"
    )
    await asyncio.sleep(0.1)


def can_auto_reset(machine: AsyncStateMachine) -> bool:
    return machine.state == "finishing"


machine = AsyncStateMachine(initial_state="idle")

machine.add_transition(
    "idle",
    "start",
    "running",
    action=async_transition_action,
)

machine.add_transition(
    "running",
    "finish",
    "finishing",
    action=async_transition_action,
)

machine.add_transition(
    "finishing",
    None,
    "idle",
    guard=can_auto_reset,
    action=async_transition_action,
)

machine.add_entry_action("running", async_entry_action)
machine.add_entry_action("idle", async_entry_action)
```

---

# 3. Caller interaction from two different threads

Each caller thread has its own event loop. Both threads share the same state machine instance.

```python
async def caller_a(machine: AsyncStateMachine) -> None:
    print(f"[{threading.current_thread().name}] caller A triggering start")
    await machine.trigger("start")

    print(f"[{threading.current_thread().name}] caller A done")


async def caller_b(machine: AsyncStateMachine) -> None:
    await asyncio.sleep(0.05)

    print(f"[{threading.current_thread().name}] caller B triggering finish")
    await machine.trigger("finish")

    print(f"[{threading.current_thread().name}] caller B done")


def run_async_in_thread(name: str, coro_factory) -> threading.Thread:
    def runner() -> None:
        asyncio.run(coro_factory())

    thread = threading.Thread(target=runner, name=name)
    thread.start()
    return thread


thread_a = run_async_in_thread(
    "CallerThread-A",
    lambda: caller_a(machine),
)

thread_b = run_async_in_thread(
    "CallerThread-B",
    lambda: caller_b(machine),
)

thread_a.join()
thread_b.join()

print("Final state:", machine.state)

asyncio.run(machine.close())
```

The actual transition logic runs on:

```text
StateMachineLoop
```

not on:

```text
CallerThread-A
CallerThread-B
```

The caller threads merely submit events and await completion.

---

# 4. What this design guarantees

The design provides the important invariant:

```text
Only the owner loop mutates the state machine.
```

and because there is only one worker consuming the queue:

```text
Only one transition macro-step is active at a time.
```

Where a macro-step is:

```text
external event
→ guard evaluation
→ exit actions
→ transition action
→ state update
→ entry actions
→ automatic transition chain
→ stable state
```

So if caller A triggers `"start"` and caller B triggers `"finish"` at nearly the same time, they are serialized:

```text
start fully completes first
finish fully completes second
```

or vice versa, depending on queue order.

They cannot interleave like this:

```text
start begins
finish observes half-updated state
start resumes
finish mutates state
```

---

# 5. Important design choice: event queue vs direct lock

One could also use a cross-thread lock, but the event queue design is usually cleaner for a state machine.

Instead of thinking:

```text
many threads call methods, so protect methods with locks
```

The event queue idea is:

```text
many threads send events, one owner loop processes them
```

That is the actor model.

For a state machine, this maps very naturally to the domain.

---

# 6. One important caveat

In the example above, the configuration methods mutate lists/dicts directly:

```python
machine.add_transition(...)
machine.add_entry_action(...)
machine.add_exit_action(...)
```

That is fine if configuration happens before the machine is used.

For a serious library, use either document:

```text
The machine must be fully configured before concurrent use.
```

or make configuration also run on the owner loop.

For example:

```python
async def add_transition_async(...):
    await self._submit_to_owner_loop(self._add_transition_impl(...))
```

That way even runtime reconfiguration is serialized.

---

# 7. Cleaner public API idea

For a library, best is to probably expose two trigger methods:

```python
async def trigger(self, event: Event) -> None:
    ...
```

for async callers, and:

```python
def trigger_blocking(self, event: Event) -> None:
    ...
```

for sync/threaded callers.

Example:

```python
await machine.trigger("start")
```

from async code, and:

```python
machine.trigger_blocking("start")
```

from sync code.

Internally both submit to the same owner loop.

---

# 8. Core pattern in minimal form

The essence is this:

```python
async def trigger(self, event: Event) -> None:
    await asyncio.wrap_future(
        asyncio.run_coroutine_threadsafe(
            self._enqueue_event(event),
            self._loop,
        )
    )
```

where `_enqueue_event()` runs on the owner loop, and a single worker processes the queue:

```python
async def _worker(self) -> None:
    while True:
        request = await self._queue.get()
        await self._process_external_event(request.event)
```

That is the critical separation:

```text
public method may be called from anywhere
internal mutation happens in exactly one place
```


