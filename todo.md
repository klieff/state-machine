# STATE MACHINE TODOS

* Valid States/Events: Detect unreachable states, missing transitions, and ambiguous guards.
* Strict mode: Only one valid transition is allowed else raise exception (deterministic)
* Exception callbacks: Capture external exceptions and provide a callback support that rolls back an action/guard and forces the state machine into an "Internal Error State" e.g. an Enum ERROR or HALT rather than raising an exception or implement Policy-based handling `STRICT/IGNORE/FALLBACK` modes.

Your current architecture covers the essential building blocks of a robust state machine: event-driven transitions, conditional branching (choice states), transient states (automatic states), and a solid lifecycle hook system (entry/exit/action/guard).

To evolve this into an enterprise-grade engine, you should look toward the **UML Statecharts specification (Harel Statecharts)**. Here are the most relevant states, transitions, and features you should consider implementing next, categorized by the problems they solve.

---

## 1. Advanced State Types

As systems grow, flat state machines suffer from "state explosion." Adding hierarchical structures keeps the design clean and manageable.

### Composite (Hierarchical) States

Allows a state to contain substates. If the machine is in a substate, it is also implicitly in the parent state.

* **Why you need it:** Inheriting behavior. If a parent state has a transition for a `CANCEL` event, you don't need to duplicate that transition on every single substate.
* **Implementation note:** Your event dispatcher will need to bubble events up from the active leaf state to the root state until a guard passes and the event is handled.

### Orthogonal (Parallel) Regions

Allows the state machine to be in multiple states simultaneously, split into independent regions.

* **Why you need it:** Modeling concurrent, independent behaviors within the same entity. For example, a car can be simultaneously in `[Gear: Drive, CruiseControl: Active, Radio: On]`.
* **Implementation note:** This requires tracking a set of active states rather than a single `currentState` variable.

### History States (`H` and `H*`)

Remembers the last active substate of a composite state before it was exited.

* **Shallow History (`H`):** Restores the state to the top level of the composite state.
* **Deep History (`H*`):** Restores the state to the exact nested leaf substate that was active before exiting.
* **Why you need it:** Handling interruptions. If an incoming phone call interrupts a game, a history state allows the game to resume exactly where the player left off.

---

## 2. Advanced Transition Types

### Internal Transitions

A transition that executes an action but **does not change the state**, meaning it does *not* trigger the state's `on-exit` and `on-entry` callbacks.

* **Why you need it:** Optimizing performance and preventing side effects. Standard self-transitions (State A $\rightarrow$ State A) execute exit and entry hooks. If you just want to increment a counter or log data on an event without resetting the state, you need an internal transition.

### Deferred Events

Allows a state to temporarily postpone an event that it cannot handle immediately, storing it in an internal queue to be re-evaluated after the next state transition.

* **Why you need it:** Imagine an architecture loading data in a `Loading` state. If a user clicks `Submit`, you don't want to ignore it or fail; you defer the `Submit` event until the machine transitions to `Ready`.

---

## 3. Architecture & Operational Features

### Event Queuing (Run-to-Completion Guarantee)

If an `on-entry` callback or a transition `action` fires a new event into the state machine, it shouldn't be processed immediately if the current transition isn't finished.

* **Why you need it:** To prevent race conditions and stack overflows. Implement an internal event queue ensuring that the current transition fully completes (including all entry/exit hooks) before the next event is processed.

### Timeouts and Delayed Transitions

The ability to trigger a transition automatically after a specified duration (e.g., `after(5000ms) $\rightarrow$ TargetState`).

* **Why you need it:** Handling SLAs, token expirations, or retry loops.
* **Implementation note:** This usually requires integrating a scheduler or timer mechanism into your engine's event loop.

### State Machine Context (Extended State)

A centralized data object (often just called `context` or `machine data`) that persists across states.

* **Why you need it:** States should represent *qualitative* conditions (e.g., `LoggedOut`, `Processing`), while quantitative data (e.g., `retryCount`, `itemsInCart`) should live in the context. Your guards and choice router callbacks should read from this context, and your transition actions should mutate it.

---

## Summary of Execution Order

If you decide to implement these, keep in mind the standard **UML execution order** for a transition to ensure predictability:

$$\text{Evaluate Guards} \longrightarrow \text{Execute Exit Hooks (Source)} \longrightarrow \text{Execute Transition Action} \longrightarrow \text{Execute Entry Hooks (Target)}$$

If you introduce hierarchical states, the exit hooks run from the inner-most active substate up to the common ancestor, and entry hooks run from the common ancestor down to the target substate.

Which of these architectural patterns solves the immediate scaling pain points you are seeing in your current project?
