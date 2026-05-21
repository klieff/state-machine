import asyncio
import time
from types import SimpleNamespace
from enum import Enum, auto
from graphviz import Digraph

from ..statemachine import StateMachineBuilder
from ..statemachine.definitions import TransitionMap
from ..statemachine.exceptions import (
    BlockedTransition,
    InvalidTransition,
    TransitionMapError,
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


def visualize_state_machine(
    transition_map: TransitionMap, filename="state_diagram.dot"
):
    dot = Digraph()
    dot.attr(rankdir="TB", nodesep="0.5", ranksep="1.0")
    dot.attr(
        "node", shape="circle", fontname="Arial", style="filled", fillcolor="white"
    )

    for (start_state, event), transitions in transition_map.items():
        event_name = event.name if event else "auto"

        for end_state, actions, guards in transitions:
            label_parts = [f"{event_name}"]
            if guards:
                for guard in guards:
                    label_parts.append(f"\n[{guard.__name__}]")
            if actions:
                for action in actions:
                    label_parts.append(f"\n{action.__name__}")

            edge_label = " ".join(label_parts)

            edge_style = "dashed" if event is None else "solid"

            dot.edge(
                str(start_state.name),
                str(end_state.name),
                label=edge_label,
                style=edge_style,
                fontsize="10",
            )
    dot.save(filename)
    # dot.render(filename, view=True)


def test_initial_state():
    sm = StateMachineBuilder[State, Event, Context]()
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

    def test_guard_fail(ctx):
        return True

    def test_on_exit(ctx):
        result.append(True)

    def test_on_action1(ctx):
        pass

    def test_on_action2(ctx):
        pass

    sm_model = (
        StateMachineBuilder[State, Event, Context]()
        # .add_transition(State.OFFLINE, Event.CONNECT, State.OFFLINE)
        .add_transition(
            State.OFFLINE, None, State.ONLINE, action=(test_on_action1, test_on_action2)
        )
        .add_transition(State.ONLINE, None, State.PENDING)
        .add_transition(State.PENDING, None, State.PROCESSED, guard=test_guard_fail)
        .add_transition(State.PENDING, None, State.OFFLINE, action=test_on_exit)
        .add_transition(
            State.PROCESSED, Event.RESTORE, State.RESTORING, action=test_on_exit
        )
        .on_exit(State.OFFLINE, test_on_exit)
        .on_entry(State.PROCESSED, test_on_exit)
    )

    # tm = sm_model.get_transition_map()
    # visualize_state_machine(tm)

    sm = sm_model.build(initial_state=State.OFFLINE, verbose=True)
    sm.start(context=Context())
    sm.stop()
    # sm.trigger(event=Event.CONNECT, context=Context())
    assert result == [True, True], f"Expected True, got {result}"


def test_valid_transition():
    def test_action(ctx):
        pass

    sm = (
        StateMachineBuilder[State, Event, Context]()
        .add_transition(State.OFFLINE, Event.CONNECT, State.ONLINE, action=test_action)
        .build(initial_state=State.OFFLINE, verbose=True)
    )

    sm.start(context=sm)
    sm.trigger(Event.CONNECT, context=sm)
    sm.stop()
    state = sm.get_state()
    assert state == State.ONLINE, f"Expected ONLINE, got {state}"


def test_invalid_transition_error():
    sm = (
        StateMachineBuilder[State, Event, Context]()
        .add_transition(State.OFFLINE, Event.CONNECT, State.ONLINE)
        .build(initial_state=State.OFFLINE, verbose=True)
    )
    try:
        sm.start(context=Context())
        sm.trigger(Event.DISCONNECT, Context())
        sm.stop()
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
        StateMachineBuilder[State, Event, Context]()
        .add_transition(
            State.OFFLINE, Event.CONNECT, State.ONLINE, guard=test_guard_pass
        )
        .add_transition(State.ONLINE, Event.FETCH, State.OFFLINE, guard=test_guard_fail)
        .build(initial_state=State.OFFLINE, verbose=True)
    )

    sm.start(context=sm)
    sm.trigger(Event.CONNECT, sm)
    state = sm.get_state()
    assert state == State.ONLINE, f"Expected ONLINE, got {state}"

    try:
        sm.trigger(Event.FETCH, sm)
        sm.stop()
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

    def test_on_transition(ctx):
        results.append("on_transition")

    sm = (
        StateMachineBuilder[State, Event, Context]()
        .add_transition(State.OFFLINE, Event.CONNECT, State.ONLINE)
        .on_exit(State.OFFLINE, test_exit_action)
        .on_entry(State.ONLINE, test_enter_action)
        .on_transition(State.OFFLINE, State.ONLINE, test_on_transition)
        .build(initial_state=State.OFFLINE, verbose=True)
    )

    sm.start(context=sm)
    sm.trigger(Event.CONNECT, sm)
    sm.stop()
    assert results == ["exited_offline", "entered_online", "on_transition"], (
        "Actions fired in wrong order"
    )


def test_empty_map_error():
    sm = StateMachineBuilder[State, Event, Context]()

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
        StateMachineBuilder[State, Event, Context]()
        .add_transition(State.OFFLINE, Event.CONNECT, State.ONLINE)
        .build(initial_state=State.OFFLINE, verbose=True)
    )
    try:
        sm._transitions["default"] = 123  # type: ignore
        sm._state = State.RECOVERING
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

    async def async_exit_action(ctx):
        await asyncio.sleep(ctx.time[0])
        results.append(ctx.name)

    async def async_enter_action(ctx):
        await asyncio.sleep(ctx.time[1])
        results.append(ctx.name)

    def test_guard_sync(ctx):
        print("Waiting for sync function...")
        time.sleep(2)
        results.append(ctx.name)
        return True

    async def test_guard_async(ctx):
        print("Waiting for async function...")
        await asyncio.sleep(ctx.time[1])
        return False

    sm_rules = (
        StateMachineBuilder[State, Event, SimpleNamespace]()
        .add_transition(State.OFFLINE, Event.CONNECT, State.ONLINE)
        .on_exit(State.OFFLINE, async_exit_action)
        .on_entry(State.ONLINE, async_enter_action)
    )
    sm = sm_rules.build_async(initial_state=State.OFFLINE, name="SM_1", verbose=True)
    sm2 = sm_rules.build_async(initial_state=State.OFFLINE, name="SM_2", verbose=True)
    sm3 = sm_rules.build_async(initial_state=State.OFFLINE, name="SM_3", verbose=True)

    async def run_async_tests():
        ctx = SimpleNamespace(time=(1, 2), name=sm._config.name)
        ctx2 = SimpleNamespace(time=(0.5, 1.25), name=sm2._config.name)
        ctx3 = SimpleNamespace(time=(1.5, 2.5), name=sm3._config.name)
        await asyncio.gather(
            sm.start(context=ctx),
            sm.trigger(Event.CONNECT, ctx),
            sm.stop(),
            sm2.start(context=ctx2),
            sm2.trigger(Event.CONNECT, ctx2),
            sm2.stop(),
            sm3.start(context=ctx3),
            sm3.trigger(Event.CONNECT, ctx3),
            sm3.stop(),
            return_exceptions=True,
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


def test_async_transitions():
    results = []

    async def async_action1(ctx):
        await asyncio.sleep(ctx.time[0])
        results.append(ctx.name)

    async def async_action2(ctx):
        await asyncio.sleep(ctx.time[1])
        results.append(ctx.name)

    def test_guard_sync(ctx):
        print("Waiting for sync function...")
        time.sleep(2)
        results.append(ctx.name)
        return True

    async def test_guard_async(ctx):
        print("Waiting for async function...")
        await asyncio.sleep(ctx.time[1])
        return False

    sm_rules = (
        StateMachineBuilder[State, Event, SimpleNamespace]()
        .add_transition(State.OFFLINE, Event.CONNECT, State.ONLINE, async_action1)
        .add_transition(State.ONLINE, Event.RESTORE, State.RESTORING, async_action2)
    )
    sm = sm_rules.build_async(initial_state=State.OFFLINE, name="SM_1", verbose=True)

    async def run_async_tests():
        ctx = SimpleNamespace(time=(2, 1), name=sm._config.name)
        await asyncio.gather(
            sm.start(context=ctx),
            sm.trigger(Event.CONNECT, ctx),
            sm.trigger(Event.RESTORE, ctx),
            sm.stop(),
            return_exceptions=True,
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
        (test_entry_exit_actions, "ON ENTRY/EXIT/TRANSITION ACTIONS"),
        (test_empty_map_error, "EMPTY TRANSITION MAP ERROR"),
        (test_state_machine_immutability, "STATE MACHINE WEAK IMMUTABILITY"),
        (test_async_exit_entry_actions, "ASYNCHRONOUS EXIT & ENTRY ACTIONS"),
        (test_async_transitions, "ASYNCHRONOUS RACE CONDITIONS"),
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
