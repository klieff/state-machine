from enum import Enum, auto
import random
from typing import Any

from statemachine import StateMachineBuilder


State = Enum("Chain", ("PATROLLING", "CHASING", "RESTING", "SPOTTED", "EATEN"))
Event = Enum("Event", ("TICK"))


# Probability matrix: [Current State][Next State] = Probability
MARKOV_MATRIX = {
    State.RESTING: {
        State.PATROLLING: 0.50,
        State.SPOTTED: 0.80,
        State.RESTING: 0.00,
    },
    State.PATROLLING: {
        State.PATROLLING: 0.60,
        State.SPOTTED: 0.30,
        State.RESTING: 0.00,
    },
    State.SPOTTED: {
        State.PATROLLING: 0.70,
        State.CHASING: 0.20,
        State.RESTING: 0.00,
    },
    State.CHASING: {
        State.PATROLLING: 0.20,
        State.CHASING: 0.70,
        State.RESTING: 0.00,
    },
}


# Context to pass to callables
class MonsterContext:
    def __init__(self, name):
        self.name = name
        self.log = []


# Dynamic router function
def dynamic_router(context: MonsterContext, info) -> Any:
    for target in MARKOV_MATRIX.get(State[info.source], {}).keys():
        if evaluate_probability(State[info.source], target, info.payload):
            if info.source == State.CHASING.name and random.random() < 0.50:
                target = State.EATEN
            return target.name


# Log the Markov transitions
def record_movement(context: MonsterContext, info) -> None:
    msg = f"👺 {context.name} "
    if info.target == State.EATEN.name:
        msg += f"CAUGHT the chicken 🐔 and ate it 😭"
        msg = f"🐔 Chicken was {info.target} by the 👺 {context.name} 😭"
    elif info.target == State.RESTING.name:
        msg += f"is {info.target} 😴"
    elif info.target == State.PATROLLING.name:
        msg += f"is {info.target} ⚔️"
    elif info.target == State.CHASING.name:
        msg += f"is {info.target} the chicken 🐔"
    elif info.target == State.SPOTTED.name:
        msg += f"{info.target} a chicken 🐔"
    else:
        print("⛔ SHOULD NEVER END UP HERE!")
    context.log.append(msg)
    print(msg)


# A guard that rolls a random float and checks if it falls within the matrix slice
def evaluate_probability(source, target, payload) -> bool:
    roll = payload

    # Check what the matrix says the probability threshold is
    chance = MARKOV_MATRIX[source][target]
    return roll >= chance


# Registering states and transitions based on the matrix
builder = (
    StateMachineBuilder[State, Event, MonsterContext]()
    .add_transition(
        source=State.RESTING,
        event=Event.TICK,
        target=dynamic_router,
        action=record_movement,
    )
    .add_transition(
        source=State.PATROLLING,
        event=Event.TICK,
        target=dynamic_router,
        action=record_movement,
    )
    .add_transition(
        source=State.SPOTTED,
        event=Event.TICK,
        target=dynamic_router,
        action=record_movement,
    )
    .add_transition(
        source=State.CHASING,
        event=Event.TICK,
        target=dynamic_router,
        action=record_movement,
    )
    .add_transition(
        source=State.EATEN,
        event=None,
        target=None,
        action=record_movement,
    )
)

# Build state machine
engine = builder.build(initial_state=State.RESTING)

# Initialize a dedicated context instance
monster_ctx = MonsterContext(name="Goblin")
engine.start(context=monster_ctx)

# Simulate clock cycles on the event loop
num_cycles = 25
for i in range(num_cycles):
    payload = random.random()
    engine.trigger(Event.TICK, payload=payload)

    if engine.get_state().name == State.EATEN.name:
        break
    elif i == num_cycles - 1:
        print("🐔 Chicken SURVIVED ❤️")
