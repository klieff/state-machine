# Atomic State Machine

* Supports various `actions` and `guards` that can run before, during, or after a
  state transition
  * `Transition` actions run during a state change and connects State 1 and State 2
  * `On-exit` actions run before a state change and before transition actions
  * `On-entry` actions run after a state change and after transition actions
* `Actions` can be executed synchronously or asynchronously
  * `Asynchronous actions` are non-preemptive and **self-blocking** (run sequentially)
  * This is a safety precaution since actions running concurrently can lead to
    race conditions
  * As a consequence transitions are treated as transactional sequences
* `Guards` are executed synchronously and expected to return a **boolean**
* Non-deterministic transitions (multiple valid state transitions)
  * Apply a 'First-Match-Wins' strategy based on order of insertion
* Self-transitions
  * Internal transitions: not leaving the state
  * External self-transitions: exits and re-enters state
