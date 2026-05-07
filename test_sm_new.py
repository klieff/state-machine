import asyncio
import time
from dataclasses import dataclass, field
from types import SimpleNamespace

from sm_core import (
    AsyncStateMachine,
    BlockedTransition,
    Enum,
    IntEnum,
    InvalidInitialState,
    InvalidTransition,
    StateMachine,
    StateMachineModel,
    TransitionMapError,
    auto,
)


class State(IntEnum):
    ONLINE = auto()
    OFFLINE = auto()
    PENDING = auto()
    PROCESSING = auto()
    COMPLETED = auto()
    FAILED = auto()


class Event(Enum):
    CONNECT = auto()
    DISCONNECT = auto()
    QUERY = auto()
    FETCH = auto()


@dataclass
class Action:
    results = []

    def sync_on_exit(self, _):
        self.results.append("exited action")

    def sync_on_enter(self, _):
        self.results.append("entered action")


@dataclass
class Context:
    output_buffer: list = field(default_factory=list)


@dataclass
class StateMachines[S: State, E: Event, C: Context]:
    model_0: StateMachineModel[S, E, C] = field(default_factory=StateMachineModel)
    model_1: StateMachineModel[S, E, C] = field(default_factory=StateMachineModel)
    model_2: StateMachineModel[S, E, C] = field(default_factory=StateMachineModel)


@dataclass
class Test[S: State, E: Event, C: Context]:
    model_0: StateMachineModel[S, E, C] | None = None
    sm_1: StateMachine[S, E, C] | None = None
    sm_2: StateMachine[S, E, C] | None = None
    context: Context = field(default_factory=Context)
    actions: Action = field(default_factory=Action)


@dataclass
class TestFunctions[S: State, E: Event, C: Context]:
    model_0: StateMachineModel[S, E, C]
    sm_1: StateMachine[S, E, C]
    sm_2: StateMachine[S, E, C]

    def test_valid_transition(self):
        print("TESTING VALID TRANSITION WITH ACTION...")
        # sm.trigger(Event.CONNECT, sm)
        # assert sm.state == State.ONLINE, f"Expected ONLINE, got {sm.state}"

    def empty_transition_map(self):
        print("TESTING EMPTY TRANSITION MAP...")

        try:
            self.model_0.build(initial_state=State.FAILED, name="SM-0", verbose=True)
        except TransitionMapError as e:
            return e
        except Exception as e:
            assert False, f"Caught wrong exception type: {type(e)}"
        else:
            assert False, f"Failed to raise TransitionMapError on state {State.ONLINE}"


def run_tests():
    tests = Test[State, Event, Context]()
    sm = StateMachines[State, Event, Context]()

    # sm.model_0.add_transition(State.OFFLINE, Event.CONNECT, State.ONLINE)
    (
        sm.model_1.add_transition(State.OFFLINE, Event.CONNECT, State.ONLINE)
        .add_transition(State.ONLINE, Event.QUERY, State.PENDING)
        .add_transition(State.PENDING, Event.FETCH, State.PROCESSING)
        .add_exit(State.OFFLINE, tests.actions.sync_on_exit)
        .add_entry(State.ONLINE, tests.actions.sync_on_enter)
    )

    tests.model_0 = sm.model_0  # .build(initial_state=State.OFFLINE)
    tests.sm_1 = sm.model_1.build(State.OFFLINE, name="SM-1", verbose=True)
    tests.sm_2 = (
        tests.sm_1
    )  # sm.model_2.build(State.OFFLINE, name="SM-1", verbose=True)

    test_funcs = TestFunctions[State, Event, Context](
        tests.model_0, tests.sm_1, tests.sm_2
    )
    test_queue = (
        test_funcs.empty_transition_map,
        # test_funcs.test_valid_transition,
        # test_funcs.test_invalid_transition_error,
        # test_funcs.test_enabled_guard,
        # test_funcs.test_blocking_guard,
        # test_funcs.test_entry_exit_actions,
        # test_funcs.test_empty_map_error,
        # test_funcs.test_state_machine_immutability,
        # test_funcs.test_async_exit_entry_actions,
        # test_funcs.test_async_transitions,
        # test_funcs.test_blocking_async,
    )

    passed = 0
    failed = 0

    print("------- Starting Module Tests -------")
    for test in test_queue:
        try:
            output = test()
            if output:
                print(f"[DEBUG] :: <Exception: {output}>")
            print(f"✅ {test.__name__} PASSED")
            passed += 1
        except AssertionError as e:
            print(f"❌ {test.__name__} FAILED: {e}")
            failed += 1
        except Exception as e:
            print(f"💥 {test.__name__} CRASHED: {type(e)} {e}")
            failed += 1
        finally:
            print()

    print("------- Summary -------")
    print(f"Passed: {passed:2d} | Failed: {failed:2d}")


if __name__ == "__main__":
    run_tests()
