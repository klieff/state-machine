# Event-Driven State Machine

>🚫 Work in progress...

## Overview

State machine with a builder interface for quick and easy state machine configuration.
Transitions are treated transactionally on a per state-machine-instance basis to
ensure atomicity.

### Transitions

State transitions are serialized in a transaction queue and processed sequentially.
The state machine execution logic is thread-safe running on a dedicated thread with its
own event loop.

> **NOTE:** By default all state machine instances share the same execution thread.
  Therefore, any blocking callbacks will block the entire thread and consequently
  all other state machine instances. To avoid this, ensure that awaitable callbacks
  are passed. If required, a state machine instance can spawn its own dedicated
  execution thread and event loop. In this case one should be mindful of the
  resource overhead associated with spawning multiple threads.

#### Transitions types

* Event-triggered transitions.
* Eventless (automatic/transient) transitions.
* Dynamic transitions (choice states):
  * Dynamic routing via a user-defined router function.
  * Useful for fully connected (dense) graphs such as Markov processes.
* Supports non-deterministic transitions (multiple valid state transitions):
  * Applies an *First-Match-Wins* strategy based on order of insertion.
* Supports lifecycle self-transitions:
  * Internal self-transitions that do not leave the state.
  * External self-transitions that exits and re-enters the state.

### Actions

Actions are user-defined callbacks that can be executed synchronously or asynchronously
(see **NOTE** above). Actions are defined in terms of their type and run before, during,
or after a state transition.

#### Action types

* `transition action`: runs during a state change connecting State A to State B.
* `on-exit action`: runs immediately before leaving a state and before transition actions.
* `on-entry action`: runs immediate on entering a new state and after transition actions.
* `on-transition action`: runs immediate after on-entry actions.

### Guards

Guards are conditional predicates that filter transition routing. Guards are expected
to return a `boolean`.

### Internal Execution Flow

On an Event Trigger:
> Evaluate Guard Predicates → Execute On-Exit Actions → Execute Transition Actions →
State Change → Execute On-Entry Actions → Execute On-Transition Actions →
Execute Automatic Transitions

### Builder methods

The `StateMachineBuilder` class is the centralized configuration interface for the
state machine.

* `.add_state(...)` main method for adding states
* `.add_choice_state(...)` method for adding transient choice states
* `.add_transition(...)`: defines transition topology
  * Event-driven transitions: `.add_transition(source, event, target, action, guard)`
  * Automatic transitions: `.add_transition(source, None, target, action, guard)`
* `.on_transition(source, target, action)`: defines global transition observers
* `.add_audit_sink(audit_callable)`: attach a user-defined audit listener

### State machine methods

* `.start(context)`: initializes the State Machine (must be called to start the machine)
* `.trigger(event, context)`: trigger an event

Other on_methods:

* `.on_failure(...)`
* `.on_invalid_transition(...)`
* `.on_guard_rejected(...)`
* `.on_before_transition(...)`
* `.on_after_transition(...)`
* `.on_error(..)`
* `.on_enter_any(..)`
* `.on_final_state(..)`
