import asyncio
import time
from types import SimpleNamespace
from enum import Enum
from graphviz import Digraph

from sm_core import (
    BlockedTransition,
    InvalidTransition,
    StateMachine,
    StateMachineBuilder,
    TransitionMapError,
    ProxyTransitionMap,
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


def visualize_state_machine(
    transition_map: ProxyTransitionMap, filename="state_diagram.dot"
):
    # dot = Digraph(format="png")
    dot = Digraph()
    dot.attr(rankdir="LR", nodesep="0.5", ranksep="1.0")

    # Configure default node styling
    dot.attr(
        "node", shape="circle", fontname="Arial", style="filled", fillcolor="white"
    )

    for (start_state, event), transitions in transition_map.items():
        # Handle Event or Automatic Transition label
        event_name = event.name if event else "auto"

        for end_state, action, guard in transitions:
            # Build a descriptive label: "Event [Guard] / Action"
            label_parts = [f"{event_name}"]
            if guard:
                label_parts.append(f"[{guard.__name__}]")
            if action:
                label_parts.append(f"/ {action.__name__}")

            edge_label = " ".join(label_parts)

            # Use different styling for automatic transitions
            edge_style = "dashed" if event is None else "solid"

            dot.edge(
                str(start_state.name),
                str(end_state.name),
                label=edge_label,
                style=edge_style,
                fontsize="10",
            )

    dot.save(filename)
    # with open(filename, "w") as f:
    #     f.write(dot.source)
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
        return False

    def test_on_exit(ctx):
        result.append(True)

    sm = (
        StateMachineBuilder[State, Event, Context]()
        # .add_transition(State.OFFLINE, Event.CONNECT, State.OFFLINE)
        .add_transition(State.OFFLINE, None, State.ONLINE)
        .add_transition(State.ONLINE, None, State.PENDING)
        .add_transition(State.PENDING, None, State.PROCESSED, guard=test_guard_fail)
        .add_transition(State.PENDING, None, State.OFFLINE, action=test_on_exit)
        .add_transition(
            State.PROCESSED, Event.RESTORE, State.RESTORING, action=test_on_exit
        )
        .on_exit(State.OFFLINE, test_on_exit)
        .on_entry(State.PROCESSED, test_on_exit)
        .build(initial_state=State.OFFLINE, verbose=True)
    )

    tm = sm.get_transition_map()
    visualize_state_machine(tm)

    a = sm.start(context=Context())
    print(a)
    # sm.trigger(event=Event.CONNECT, context=Context())
    assert result == [True], f"Expected True, got {result}"


def test_valid_transition():
    def test_action(ctx):
        pass

    sm = (
        StateMachineBuilder[State, Event, StateMachine]()
        .add_transition(State.OFFLINE, Event.CONNECT, State.ONLINE, action=test_action)
        .build(initial_state=State.OFFLINE, verbose=True)
    )

    sm.start(context=sm)
    sm.trigger(Event.CONNECT, context=sm)
    assert sm._state == State.ONLINE, f"Expected ONLINE, got {sm._state}"


def test_invalid_transition_error():
    sm = (
        StateMachineBuilder[State, Event, Context]()
        .add_transition(State.OFFLINE, Event.CONNECT, State.ONLINE)
        .build(initial_state=State.OFFLINE, verbose=True)
    )
    try:
        sm.start(context=Context())
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
        StateMachineBuilder[State, Event, StateMachine]()
        .add_transition(
            State.OFFLINE, Event.CONNECT, State.ONLINE, guard=test_guard_pass
        )
        .add_transition(State.ONLINE, Event.FETCH, State.OFFLINE, guard=test_guard_fail)
        .build(initial_state=State.OFFLINE, verbose=True)
    )

    sm.start(context=sm)
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

    def test_on_transition(ctx):
        results.append("on_transition")

    sm = (
        StateMachineBuilder[State, Event, StateMachine]()
        .add_transition(State.OFFLINE, Event.CONNECT, State.ONLINE)
        .on_exit(State.OFFLINE, test_exit_action)
        .on_entry(State.ONLINE, test_enter_action)
        .on_transition(State.OFFLINE, State.ONLINE, test_on_transition)
        .build(initial_state=State.OFFLINE, verbose=True)
    )

    sm.start(context=sm)
    sm.trigger(Event.CONNECT, sm)
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
        ctx = SimpleNamespace(time=(1, 2), name=sm._name)
        ctx2 = SimpleNamespace(time=(0.5, 1.25), name=sm2._name)
        ctx3 = SimpleNamespace(time=(1.5, 2.5), name=sm3._name)
        await asyncio.gather(
            sm.trigger(Event.CONNECT, ctx),
            sm2.trigger(Event.CONNECT, ctx2),
            sm3.trigger(Event.CONNECT, ctx3),
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
