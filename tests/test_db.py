import asyncio
import logging
import sqlite3
import time
from dataclasses import dataclass, field

from sm_core import (
    AsyncStateMachine,
    AsyncStateMachineRules,
    Enum,
    IntEnum,
    StateMachine,
    StateMachineModel,
    auto,
)

logging.basicConfig(level=logging.DEBUG, format="%(levelname)-8s:: %(message)s")
logger = logging.getLogger(__name__)  # name = module name


class DBStates(IntEnum):
    ONLINE = auto()
    OFFLINE = auto()
    RESTORING = auto()
    RECOVERING = auto()
    PENDING = auto()
    PROCESSED = auto()


class DBEvents(Enum):
    CONNECT = auto()
    DISCONNECT = auto()
    RECOVER = auto()
    RESTORE = auto()
    QUERY = auto()
    FETCH = auto()


test_events = [
    DBEvents.CONNECT,
    DBEvents.QUERY,
    DBEvents.DISCONNECT,
    DBEvents.FETCH,
    DBEvents.DISCONNECT,
]


@dataclass
class DBContext:
    queries: dict[str, str] = field(default_factory=dict)


def run_sync_tests():
    @dataclass
    class DBConnection:
        db_connection: sqlite3.Connection

        def connect(self, ctx) -> None:
            if not hasattr(self, "db_cursor"):
                self.db_cursor = self.db_connection.cursor()
            time.sleep(1)

        def disconnect(self, ctx) -> None:
            self.db_cursor.close()
            del self.db_cursor
            # self.db_connection.close()
            time.sleep(1)

        def query(self, ctx) -> None:
            self.current_query = "SELECT 123;"
            self.db_cursor.execute(self.current_query)
            time.sleep(3)

        def fetch(self, ctx) -> None:
            ctx.context.queries[self.current_query] = self.db_cursor.fetchone()
            time.sleep(2)

        def isvalid_cursor(self, ctx) -> None:
            logger.info(f"(Task {ctx.id}) Exit Action from State {ctx.state.name}")

        def on_disconnect(self, ctx) -> None:
            logger.info(f"(Task {ctx.id}) Entry Action on State {ctx.state.name}")
            # logger.info(f"(Task {ctx.id}) Transition Audit: {ctx.sm.audit}")

    @dataclass
    class SyncObject:
        id: int
        context: DBContext
        state: DBStates
        sm: StateMachine[DBStates, DBEvents, "SyncObject"]

        def trigger(self, event: DBEvents) -> None:
            self.state = self.sm.trigger(self.state, event, self)

    db_connection = sqlite3.connect(":memory:")
    db = DBConnection(db_connection=db_connection)

    sync_sm = (
        StateMachineModel[DBStates, DBEvents, SyncObject]()
        .add_transition(DBStates.OFFLINE, DBEvents.CONNECT, DBStates.ONLINE, db.connect)
        .add_transition(
            DBStates.ONLINE, DBEvents.DISCONNECT, DBStates.OFFLINE, db.disconnect
        )
        .add_transition(DBStates.ONLINE, DBEvents.QUERY, DBStates.PENDING, db.query)
        .add_transition(DBStates.PENDING, DBEvents.FETCH, DBStates.ONLINE, db.fetch)
        .add_exit(DBStates.OFFLINE, db.isvalid_cursor)
        .add_entry(DBStates.OFFLINE, db.on_disconnect)
    )

    sync_test_obj1 = SyncObject(
        id=1,
        context=DBContext(),
        state=DBStates.OFFLINE,
        sm=sync_sm.build(initial_state=DBStates.OFFLINE),
    )

    sync_test_obj2 = SyncObject(
        id=2,
        context=DBContext(),
        state=DBStates.OFFLINE,
        sm=sync_sm.build(initial_state=DBStates.OFFLINE),
    )

    def sync_tests(test_obj):
        for event in test_events:
            try:
                old_state = test_obj.state
                test_obj.trigger(event=event)
            except Exception as e:
                logger.error(f"(Task {test_obj.id}) Transition Action: {e}")
            else:
                new_state = test_obj.state.name
                logger.info(
                    f"(Task {test_obj.id}) Transition Action ({old_state.name}, {event.name}) -> {new_state}"
                )

    sync_tests(sync_test_obj1)
    sync_tests(sync_test_obj2)


def run_async_tests():
    @dataclass
    class AsyncDBConnection:
        db_connection: sqlite3.Connection

        async def connect(self, ctx: "DBContext") -> None:
            if not hasattr(self, "db_cursor"):
                self.db_cursor = self.db_connection.cursor()
            await asyncio.sleep(1)

        async def disconnect(self, ctx: "DBContext") -> None:
            self.db_cursor.close()
            # del self.db_cursor
            # self.db_connection.close()
            await asyncio.sleep(1)

        async def query(self, ctx: "DBContext") -> None:
            self.current_query = "SELECT 1;"
            self.db_cursor.execute(self.current_query)
            await asyncio.sleep(3)

        async def fetch(self, ctx: "DBContext") -> None:
            ctx.queries[self.current_query] = self.db_cursor.fetchone()
            await asyncio.sleep(2)

    @dataclass
    class AsyncObject:
        id: int
        context: DBContext
        state: DBStates
        sm: AsyncStateMachine[DBStates, DBEvents, DBContext]

        async def trigger(self, event: DBEvents) -> None:
            self.state = await self.sm.trigger(self.state, event, self.context)

    db_connection = sqlite3.connect(":memory:")
    db = AsyncDBConnection(db_connection=db_connection)

    async_rules = (
        AsyncStateMachineRules[DBStates, DBEvents, DBContext]()
        .add_transition(DBStates.OFFLINE, DBEvents.CONNECT, DBStates.ONLINE, db.connect)
        .add_transition(
            DBStates.ONLINE, DBEvents.DISCONNECT, DBStates.OFFLINE, db.disconnect
        )
        .add_transition(DBStates.ONLINE, DBEvents.QUERY, DBStates.PENDING, db.query)
        .add_transition(DBStates.PENDING, DBEvents.FETCH, DBStates.ONLINE, db.fetch)
        .build()
    )

    async_test_obj1 = AsyncObject(
        id=1,
        context=DBContext(),
        state=DBStates.OFFLINE,
        sm=AsyncStateMachine(state=DBStates.OFFLINE, transitions=async_rules),
    )

    async_test_obj2 = AsyncObject(
        id=2,
        context=DBContext(),
        state=DBStates.OFFLINE,
        sm=AsyncStateMachine(state=DBStates.OFFLINE, transitions=async_rules),
    )

    async def async_tests(test_obj):
        for event in test_events:
            try:
                old_state = test_obj.state
                await test_obj.trigger(event=event)
            except Exception as e:
                print(f"(Task {test_obj.id}) UNSUCCESSFUL TRANSITION: {e}")
            else:
                new_state = test_obj.state.name
                print(
                    f"(Task {test_obj.id}) SUCCESSFUL TRANSITION:   ({old_state.name}, {event.name})\t->\t{new_state}"
                )
        print(f"(Task {test_obj.id}) Database Queries: {test_obj.context.queries}")
        print(f"(Task {test_obj.id}) Transition Audit: {test_obj.sm.audit}")

    async def start_async_tests():
        await asyncio.gather(async_tests(async_test_obj1), async_tests(async_test_obj2))

    asyncio.run(start_async_tests())


def run_tests():
    print("############# SYNCHRONOUS TESTS #############")

    start_time = time.perf_counter()
    run_sync_tests()
    end_time = time.perf_counter()
    print(f"\nTotal runtime: {end_time - start_time:.2f} seconds")

    # print("\n############# ASYNCHRONOUS TESTS #############")
    #
    # start_time = time.perf_counter()
    # run_async_tests()
    # end_time = time.perf_counter()
    # print(f"\nTotal runtime: {end_time - start_time:.2f} seconds")


if __name__ == "__main__":
    run_tests()
