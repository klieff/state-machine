# STATE MACHINE FIXES

1. No State Mutation (Critical)
    `StateMachine.trigger()` returns new_state but never updates the machine's current
    state. The caller must manually track state:
        `state = sm.trigger(state, event, ctx)  # Easy to forget`
    The machine should hold a `current_state: S` field and update it internally.
2. Action Cannot Be None
    The type `Action[C] = Callable[[C]`, None] forces every transition to have an action.
    No-op transitions require a dummy:
        `lambda ctx: None. Consider Action[C] | None`

3. No Guards/Predicates
    There's no support for conditional transitions (guards). Real-world state machines
    often need: "transition only if context.some_flag is true".
    Guards are predicates that must be true for a transition to fire. This enables
    conditional logic without polluting actions:
        `type GuardedTransition[S, E, C] = tuple[S, Action[C], Callable[[C], bool]]`
    Usage: transition only fires if guard(context) returns True.

4. `build()` Is Redundant
    `StateMachineRules.build()` just returns self.transitions — it's an identity function.
    The builder pattern adds no value here since transitions is already accessible.
    Consider either removing `build()` or making transitions private (`_transitions`).
    Add -> Remove or make `transistions` priave.

5. Fluent API Returns Self, But Mutates
    `add_transition()` returns self for chaining but mutates in place. This works but is
    inconsistent with typical builder semantics (which usually produce an immutable
    result on `build()`).

6. Duplicate Code (Sync vs Async)
    `StateMachine/AsyncStateMachine` and their Rules classes are nearly identical — only
    trigger differs (await vs direct). This could be consolidated with a base class or
    by accepting an async flag.

7. No `__slots__` on Dataclasses
    If these are instantiated frequently, adding `slots=True` to `@dataclass` would reduce
    memory overhead.

8. Type Constraint on C Is Missing
    C is unbounded while S and E are constrained to Enum. If context should also be
    constrained (e.g., dataclass), consider adding a bound or documenting expectations.

9. Audit Log Unbounded
    self.audit grows indefinitely. For long-running machines, this is a memory leak.
    Consider a max length or periodic flush mechanism.
    Add -> `python max_audit: int parameter`

10. Error Message Could Be Better
    `InvalidTransition` doesn't include the event type in its name for discoverability.
    Also, the message uses `.name` which assumes `Enum` — safe due to the type bound, but
    worth noting.

11. Entry/Exit Actions
    State machines often need actions when entering or leaving a state, not just on
    transitions. For example, entering Processing might start a timer; exiting it might
    cancel that timer.
        `@dataclass
        class StateConfig[S, C]:
            on_entry: Action[C] | None = None
            on_exit: Action[C] | None = None`

12. Visualization Support
    Generating a Graphviz/DOT representation enables automatic state diagram generation:
    `def to_dot(self) -> str:
        lines = ["digraph StateMachine {"]
        for (from_state, event), (to_state, _) in self.transitions.items():
            lines.append(f'  {from_state.name} -> {to_state.name} [label="{event.name}"];')
        lines.append("}")
        return "\n".join(lines)`
    This lets you pipe output to `dot -Tpng` and get a visual diagram.
