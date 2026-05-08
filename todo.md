# STATE MACHINE TODOS

* Valid States/Events: Detect unreachable states, missing transitions, and ambiguous guards.
* Strict mode: Only one valid transition is allowed else raise exception (deterministic)
* Concurrency protection: Async/multithreaded context using locks -> state mutations are unsafe.
* Exception callbacks: Capture external exceptions and provide a callback support that rolls back an
  action/guard and forces the state machine into an "Internal Error State" e.g. an Enum ERROR or HALT
  rather than raising an exception or implement Policy-based handling `STRICT/IGNORE/FALLBACK` modes.
* Audit sink: Finalize...
