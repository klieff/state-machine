# Event-Driven State Machine

>🚫 Work in progress...

## Overview

* Builder interface for quick and easy state machine configuration (see examples below).
* Supports event-triggered and eventless (automatic/transient) state transitions:
  * Transitions are treated transactionally on a per state-machine-instance basis to
  ensure atomicity. This means that transitions are serialized in a queue and processed
  sequentially.
  * The state machine execution logic is thread-safe running on a dedicated thread
  with its own event loop.
    * **NOTE:** By default all state machine instances share the same execution thread.
    Therefore, any blocking callbacks will block the entire thread and consequently
    all other state machine instances. To avoid this ensure that awaitable callbacks
    are passed. If required, a state machine instance can spawn its own dedicated
    execution thread and event loop. In this case one should be mindful of the
    resource overhead associated with spawning multiple threads.
* Support for user-defined `Actions` (callbacks) that can be executed synchronously or
asynchronously (see **NOTE** above). Actions are defined in terms of their type and run
before, during, or after a state transition:
  * `transition action`: runs during a state change connecting State A to State B.
  * `on-exit action`: runs immediately before leaving a state and before transition actions.
  * `on-entry action`: runs immediate on entering a new state and after transition actions.
* Supports `Guards` (predicates) which are expected to return a `boolean`.
* Supports non-deterministic transitions (multiple valid state transitions):
  * Applies an *First-Match-Wins* strategy based on order of insertion.
* Supports lifecycle self-transitions:
  * Internal self-transitions that do not leave the state.
  * External self-transitions that exits and re-enters the state.

## Internal Execution Flow

On an Event Trigger:
> Evaluate Guard Predicates → Execute On-Exit Actions → Execute Transition Actions →
State Change → Execute On-Entry Actions → Execute On-Transition Actions →
Execute Automatic Transitions

## Builder methods

* `.add_audit_sink(audit_callable)`: attach a user-defined audit listener
* `.add_transition(...)`: defines transition topology
  * Event-driven transitions: `.add_transition(source, event, target, action, guard)`
  * Automatic transitions: `.add_transition(source, None, target, action, guard)`
* `.on_transition(source, target, action)`: defines global transition observers
* `.on_exit(source, action)` and `.on_entry(source, action)`: define lifecycle callbacks

Other on_methods:

* `.on_failure(...)`
* `.on_invalid_transition(...)`
* `.on_guard_rejected(...)`
* `.on_before_transition(...)`
* `.on_after_transition(...)`
* `.on_error(..)`
* `.on_enter_any(..)`
* `.on_final_state(..)`

## State machine methods

* `.start(context)`: initializes the State Machine (must be called to start the machine)
* `.trigger(event, context)`: trigger an event
