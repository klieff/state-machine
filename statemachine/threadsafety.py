import asyncio
import concurrent.futures
import threading


class UnifiedStateMachineWrapper:
    def __init__(self, state_machine, loop: asyncio.AbstractEventLoop):
        self._sm = state_machine
        self._loop = loop
        self._queue = asyncio.Queue()

        # Start the background consumer task on the event loop
        self._loop.create_task(self._queue_consumer())

    async def _queue_consumer(self):
        """The single source of truth for execution.
        Processes one event at a time, strictly awaiting transitions."""
        while True:
            # 1. Wait for an event to arrive in the queue
            event, future = await self._queue.get()
            try:
                # 2. Execute the transition (handles any internal awaits/side effects)
                result = await self._sm.process_event(event)
                # 3. Pass the result back to the waiting caller
                future.set_result(result)
            except Exception as e:
                future.set_exception(e)
            finally:
                self._queue.task_done()

    # --- BASE 1 & 2: Asynchronous Callers (Single or Multi-threaded) ---
    async def send_event_async(self, event):
        """Call this if you are already inside an async function
        (handles single-threaded interleaving or multi-threaded async tasks)."""
        future = self._loop.create_future()
        # Enqueue the event and the future that will hold the result
        await self._queue.put((event, future))
        # Wait until the consumer processes it and returns the data
        return await future

    # --- BASE 3: Synchronous Callers (Multi-threaded) ---
    def send_event_sync(self, event):
        """Call this from a standard synchronous thread.
        Blocks the thread until the async loop processes the event and returns data."""
        # Submit the async scheduling function to the event loop safely from a thread
        future = asyncio.run_coroutine_threadsafe(
            self.send_event_async(event), self._loop
        )
        # Block the calling thread until the result is ready
        return future.result()
