# STATE MACHINE TODOS

* Valid States/Events: Detect unreachable states, missing transitions, and ambiguous guards.
* Strict mode: Only one valid transition is allowed else raise exception (deterministic)
* Concurrency protection: Is used in async/multithreaded context there are no locks and
  state mutations are unsafe.
* Self-execution: An initial state that has no valid transitions (dead-end), should only
  be allowed by a user_flag=True and then ensure that it can run on_exit/on_entry actions.
* Transition hooks: Runs regardless of event on State A -> State B, could be useful?
* `InvalidTransition` could force the state machine into an "Internal Error State" e.g.
  an Enum ERROR or HALT rather than raising an exception or implement Policy-based
  handling `STRICT/IGNORE/FALLBACK` modes.
* Audit: `MAX_AUDIT` should be configurable. Offloading/flushing support?
* Exception context: add context when an external exception is caught
