import asyncio
import time
from statemachine import StateMachineBuilder
from statemachine.statemachine import SyncStateMachine, AsyncStateMachine


class async_test:
    async def wait(self):
        print("ASYNC SLEEP ENTERED")
        # a = 6 / 0
        await asyncio.sleep(2)
        # time.sleep(2)
        print("ASYNC SLEEP EXITED")

    async def trigger_event(self, context, info):
        print("TRIGGERING NEW ASYNC EVENT")
        await info.machine.trigger("check")

    async def on_entry(self):
        print("ASYNC ON ENTRY.")

    async def stop(self):
        print("ASYNC STOPPING")


class sync_test:
    def wait(self):
        time.sleep(2)

    def trigger_event(self, context, info):
        print("TRIGGERING NEW SYNC EVENT")
        info.machine.trigger("check")

    def on_entry(self):
        print("SYNC ON ENTRY.")


async def run_async(sm: AsyncStateMachine):
    await asyncio.gather(
        sm.start("off", None),
        sm.trigger("start"),
        sm.trigger("stop"),
        sm.stop(),
        return_exceptions=True,
    )
    # await sm.start("off", None)
    # await sm.trigger("start")
    # await sm.trigger("stop")
    # await sm.stop()
    print("ASYNC TESTS COMPLETED.")


def run_sync(sm: SyncStateMachine):
    sm.start("off", None)
    sm.trigger("start")
    sm.trigger("stop")
    sm.stop()


AT = async_test()
AS = sync_test()

sm_async = (
    StateMachineBuilder()
    .add_state("off", on_entry=AT.on_entry)
    .add_state("on")
    .add_state("wait")
    .add_transition("off", "start", "on", actions=AT.trigger_event)
    .add_transition("on", "check", "on", actions=AT.wait)
    .add_transition("on", "stop", "off", actions=AT.stop)
    .build_async()
)

sm_sync = (
    StateMachineBuilder()
    .add_state("off", on_entry=AS.on_entry)
    .add_state("on")
    .add_state("wait")
    .add_transition("off", "start", "on", actions=AS.trigger_event)
    .add_transition("on", "check", "on", actions=AS.wait)
    .add_transition("on", "stop", "off")
    .build()
)

asyncio.run(run_async(sm_async))
