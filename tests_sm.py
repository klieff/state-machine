import asyncio
import time
from dataclasses import dataclass, field
from types import SimpleNamespace

from sm_core import (
    BlockedTransition,
    Enum,
    IntEnum,
    InvalidState,
    InvalidTransition,
    StateMachine,
    StateMachineModel,
    TransitionMapError,
    auto,
)


class State(Enum):
    ONLINE = auto()
    OFFLINE = auto()
    RESTORING = auto()
    RECOVERING = auto()
    PENDING = auto()
    PROCESSED = auto()


class Event(Enum):
    CONNECT = auto()
    DISCONNECT = auto()
    RECOVER = auto()
    RESTORE = auto()
    QUERY = auto()
    FETCH = auto()


class Context:
    id: int


def test_initial_state():
    sm = StateMachineModel[State, Event, Context]()
    # .add_transition(State.OFFLINE, Event.CONNECT, State.ONLINE)

    try:
        sm.build(initial_state=State.PROCESSED, verbose=True)
    # except InvalidState as e:
    except TransitionMapError as e:
        return e
    except Exception as e:
        assert False, f"Caught wrong exception type: {type(e)} {e}"
    else:
        assert False, f"Failed to raise InvalidInitialState on state {State.ONLINE}"


def test_dead_state_exit():
    result = []

    def test_on_exit(ctx):
        result.append(True)

    sm = (
        StateMachineModel[State, Event, Context]()
        .add_transition(State.OFFLINE, Event.CONNECT, State.OFFLINE)
        .add_exit(State.OFFLINE, test_on_exit)
        .build(initial_state=State.OFFLINE, verbose=True)
    )

    sm.trigger(event=Event.CONNECT, context=Context())
    assert result == [True], f"Expected True, got {result}"


def test_valid_transition():
    def test_action(ctx):
        pass

    sm = (
        StateMachineModel[State, Event, StateMachine]()
        .add_transition(State.OFFLINE, Event.CONNECT, State.ONLINE, action=test_action)
        .build(initial_state=State.OFFLINE, verbose=True)
    )

    sm.trigger(Event.CONNECT, sm)
    assert sm._state == State.ONLINE, f"Expected ONLINE, got {sm._state}"


def test_invalid_transition_error():
    sm = (
        StateMachineModel[State, Event, Context]()
        .add_transition(State.OFFLINE, Event.CONNECT, State.ONLINE)
        .build(initial_state=State.OFFLINE, verbose=True)
    )
    try:
        sm.trigger(Event.DISCONNECT, Context())
    except InvalidTransition as e:
        return e
    except Exception as e:
        assert False, f"Caught wrong exception type: {type(e)} {e}"
    else:
        assert False, "Failed to raise InvalidTransition on invalid transition"


def test_transition_guards():
    def test_guard_pass(ctx):
        return True

    def test_guard_fail(ctx):
        return False

    sm = (
        StateMachineModel[State, Event, StateMachine]()
        .add_transition(
            State.OFFLINE, Event.CONNECT, State.ONLINE, guard=test_guard_pass
        )
        .add_transition(State.ONLINE, Event.FETCH, State.OFFLINE, guard=test_guard_fail)
        .build(initial_state=State.OFFLINE, verbose=True)
    )

    sm.trigger(Event.CONNECT, sm)
    assert sm._state == State.ONLINE, f"Expected ONLINE, got {sm._state}"

    try:
        sm.trigger(Event.FETCH, sm)
    except BlockedTransition as e:
        return e
    except Exception as e:
        assert False, f"Caught wrong exception type: {type(e)} {e}"
    else:
        assert False, "Failed to raise BlockedTransition on blocking guard"


def test_entry_exit_actions():
    results = []

    def test_exit_action(ctx):
        results.append("exited_offline")

    def test_enter_action(ctx):
        results.append("entered_online")

    sm = (
        StateMachineModel[State, Event, StateMachine]()
        .add_transition(State.OFFLINE, Event.CONNECT, State.ONLINE)
        .add_exit(State.OFFLINE, test_exit_action)
        .add_entry(State.ONLINE, test_enter_action)
        .build(initial_state=State.OFFLINE, verbose=True)
    )

    sm.trigger(Event.CONNECT, sm)
    assert results == ["exited_offline", "entered_online"], (
        "Actions fired in wrong order"
    )


def test_empty_map_error():
    sm = StateMachineModel[State, Event, Context]()

    try:
        sm.build(initial_state=State.OFFLINE, verbose=True)
    except TransitionMapError as e:
        return e
    except Exception as e:
        assert False, f"Caught wrong exception type: {type(e)} {e}"
    else:
        assert False, "Failed to raise TransitionMapError on empty map"


def test_state_machine_immutability():
    sm = (
        StateMachineModel[State, Event, Context]()
        .add_transition(State.OFFLINE, Event.CONNECT, State.ONLINE)
        .build(initial_state=State.OFFLINE, verbose=True)
    )

    try:
        sm._transitions["default"] = 123  # type: ignore
        sm._state = State.RECOVERING  # type: ignore
    except TypeError as e:
        return e
    except AttributeError as e:
        return e
    except Exception as e:
        assert False, f"Caught wrong exception type: {type(e)} {e}"
    else:
        assert False, "Failed to raise AttributeError - should NOT happen!"


def test_async_exit_entry_actions():
    results = []

    async def test_async_exit_action(ctx):
        await asyncio.sleep(ctx.time[0])
        results.append(ctx.name)

    async def test_async_enter_action(ctx):
        await asyncio.sleep(ctx.time[1])
        results.append(ctx.name)

    sm_rules = (
        StateMachineModel[State, Event, SimpleNamespace]()
        .add_transition(State.OFFLINE, Event.CONNECT, State.ONLINE)
        .add_exit(State.OFFLINE, test_async_exit_action)
        .add_entry(State.ONLINE, test_async_enter_action)
    )
    sm = sm_rules.build_async(initial_state=State.OFFLINE, name="SM_1", verbose=True)
    sm2 = sm_rules.build_async(initial_state=State.OFFLINE, name="SM_2", verbose=True)
    sm3 = sm_rules.build_async(initial_state=State.OFFLINE, name="SM_3", verbose=True)

    async def run_async_tests():
        ctx = SimpleNamespace(time=(1, 2), name=sm._name, state=sm._state.name)
        ctx2 = SimpleNamespace(time=(0.5, 1.1), name=sm2._name, state=sm2._state.name)
        ctx3 = SimpleNamespace(time=(1.5, 2.5), name=sm3._name, state=sm3._state.name)
        await asyncio.gather(
            sm.trigger(Event.CONNECT, ctx),
            sm2.trigger(Event.CONNECT, ctx2),
            sm3.trigger(Event.CONNECT, ctx3),
        )

    start_time = time.perf_counter()
    asyncio.run(run_async_tests())
    end_time = time.perf_counter()
    print(f"Total time: {end_time - start_time:.2f} seconds")

    results2 = (("ONLINE",) * 3, results)
    assert results2 == (
        ("ONLINE",) * 3,
        ["SM_2", "SM_1", "SM_3", "SM_2", "SM_1", "SM_3"],
    ), "Actions fired in wrong order"


def run_tests():
    tests = (
        (test_initial_state, "INITIAL STATE"),
        (test_dead_state_exit, "EXIT ACTION ON DEAD STATE"),
        (test_valid_transition, "VALID TRANSITION WITH ACTION"),
        (test_invalid_transition_error, "INVALID TRANSITION ERROR"),
        (test_transition_guards, "ENABLING & BLOCKING GUARDS"),
        (test_entry_exit_actions, "ENTRY & EXIT ACTIONS"),
        (test_empty_map_error, "EMPTY TRANSITION MAP ERROR"),
        (test_state_machine_immutability, "STATE MACHINE WEAK IMMUTABILITY"),
        (test_async_exit_entry_actions, "ASYNCHRONOUS EXIT & ENTRY ACTIONS"),
        # (test_async_transitions,
        # (test_blocking_async,
    )

    passed = 0
    failed = 0
    failed_tests = []

    print("------- Starting Module Tests -------\n")
    for i, (test, desc) in enumerate(tests):
        test_msg = f"TEST {i + 1}: {desc}..."
        print(test_msg)
        print("-" * len(test_msg))
        try:
            output = test()
            if output:
                print(f"[DEBUG] :: <{type(output).__name__}: {output}>")
            print(f"✅ {test.__name__} PASSED")
            passed += 1
        except AssertionError as e:
            print(f"❌ {test.__name__} FAILED: {e}")
            failed += 1
            failed_tests.append(test.__name__)
        except Exception as e:
            print(f"💥 {test.__name__} CRASHED: {type(e)} {e}")
            failed += 1
            failed_tests.append(test.__name__)
        finally:
            print()
            time.sleep(1.0)

    print("------- Summary -------")
    print(f"Passed: {passed:2d} | Failed: {failed:2d}")

    if failed_tests:
        print("Failed tests: ", *failed_tests)


if __name__ == "__main__":
    run_tests()
