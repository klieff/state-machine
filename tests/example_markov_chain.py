from enum import Enum, auto
import random
from types import SimpleNamespace

from statemachine import StateMachineBuilder


State = Enum("Chain", ("PATROLLING", "CHASING", "RESTING"))
Event = Enum("Event", ("TICK"))


# Probability matrix: [Current State][Next State] = Probability
MARKOV_MATRIX = {
    "PATROLLING": {"PATROLLING": 0.60, "CHASING": 0.30, "RESTING": 0.10},
    "CHASING": {"PATROLLING": 0.20, "CHASING": 0.70, "RESTING": 0.10},
    "RESTING": {"PATROLLING": 0.70, "CHASING": 0.00, "RESTING": 0.30},
}


class MonsterContext:
    def __init__(self, name):
        self.name = name
        self.log = []


# Log the Markov transitions
def record_movement(context: MonsterContext, info) -> None:
    msg = f"[{context.name}] Moved from {info.source} to {info.target}!"
    context.log.append(msg)
    print(msg)


# A guard that rolls a random float and checks if it falls within the matrix slice
def evaluate_probability(context: MonsterContext, info) -> bool:
    # info.payload contains the random roll: e.g., 0.42
    roll = info.roll
    target_state = info.target
    source_state = info.source

    # Check what the matrix says the probability threshold is
    chance = MARKOV_MATRIX[source_state][target_state]

    return roll <= chance


builder = StateMachineBuilder[State, Event, MonsterContext]()

# Registering states and transitions based on the matrix...
builder.add_transition(
    source_state=State.PATROLLING,
    event=Event.TICK,
    target_state=State.CHASING,
    guard=evaluate_probability,
    action=record_movement,
)
engine = builder.build(initial_state=State.PATROLLING)

# Start with a dedicated context instance
monster_ctx = MonsterContext(name="Goblin")
engine.start(context=monster_ctx)

# Simulate 3 clock cycles on the event loop
for _ in range(3):
    # Pass a freshly generated random payload on every trigger
    payload = SimpleNamespace(roll=random.random())
    engine.trigger(Event.TICK, payload=payload)
