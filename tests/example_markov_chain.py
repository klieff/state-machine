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
    for target in MARKOV_MATRIX.get(info.source, {}).keys():
        if evaluate_probability(info.source, target, info.payload):
            if info.source == State.CHASING and random.random() < 0.50:
                target = State.EATEN
            return target


# Log the Markov transitions
def record_movement(context: MonsterContext, info) -> None:
    msg = f"{context.name} 👺 "
    if info.target == State.EATEN:
        msg = f"😭  Chicken 🐔 was {info.target.name} by the {context.name} 👺"
    elif info.target == State.RESTING:
        msg += f"is {info.target.name} 😴"
    elif info.target == State.PATROLLING:
        msg += f"is {info.target.name} ⚔️"
    elif info.target == State.CHASING:
        msg += f"is {info.target.name} the chicken 🐔"
    elif info.target == State.SPOTTED:
        msg += f"{info.target.name} a chicken 🐔"
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
    StateMachineBuilder[State, Event]()
    .add_state(State.RESTING)
    .add_state(State.PATROLLING)
    .add_state(State.CHASING)
    .add_state(State.SPOTTED)
    .add_state(State.EATEN)
    .add_transition(
        source=State.RESTING,
        event=Event.TICK,
        actions=record_movement,
        router=dynamic_router,
    )
    .add_transition(
        source=State.PATROLLING,
        event=Event.TICK,
        actions=record_movement,
        router=dynamic_router,
    )
    .add_transition(
        source=State.SPOTTED,
        event=Event.TICK,
        actions=record_movement,
        router=dynamic_router,
    )
    .add_transition(
        source=State.CHASING,
        event=Event.TICK,
        actions=record_movement,
        router=dynamic_router,
    )
    .add_transition(
        source=State.EATEN,
        event=None,
        # target=State.EATEN,
        actions=record_movement,
    )
)

# Build state machine
engine = builder.build()

# Initialize a dedicated context instance
monster_ctx = MonsterContext(name="Goblin")
engine.start(initial_state=State.RESTING, context=monster_ctx)

# Simulate clock cycles on the event loop
num_cycles = 25
for i in range(num_cycles):
    payload = random.random()
    engine.trigger(Event.TICK, payload=payload)

    # print(engine.get_state())
    if engine.get_state() == State.EATEN:
        break
    elif i == num_cycles - 1:
        print("❤️ Chicken 🐔 SURVIVED!")

engine.stop()
