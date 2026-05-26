from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum

from .callbacks import prepare_callbacks
from .definitions import (
    Callbacks,
    EngineEvent,
    EventSpec,
    State,
    StateSpec,
    StateType,
    Transition,
)


class ConfigurationError(Exception): ...


@dataclass(slots=True, frozen=True)
class ConfigSpec:
    name: str
    events: dict[EventSpec, EventSpec]
    states: dict[StateSpec, State]
    transitions: dict[tuple[StateSpec, EventSpec], list[Transition]]
    # on_transition: TransitionAction


@dataclass(slots=True)
class StateMachineConfigs:
    events: dict[EventSpec, EventSpec] = field(default_factory=dict)
    states: dict[StateSpec, State] = field(default_factory=dict)
    transitions: dict[tuple[StateSpec, EventSpec], list[Transition]] = field(
        default_factory=dict
    )

    def add_event(self, event: EventSpec):
        self.events[event] = event

    def add_state(
        self,
        type: StateType,
        state: StateSpec,
        on_entry: Callbacks | None = None,
        on_exit: Callbacks | None = None,
        final_state: bool = False,
    ) -> None:
        _state = self.states.get(state)
        if _state is not None:
            raise ConfigurationError(f"State '{state}' is already registered.")

        s = State(
            type=type,
            name=str(state),
            state=state,
            on_exit=prepare_callbacks(on_exit),
            on_entry=prepare_callbacks(on_entry),
            final_state=final_state,
        )

        self.states[state] = s

    def add_transition(
        self,
        source: StateSpec,
        event: EventSpec,
        target: StateSpec,
        actions: Callbacks | None,
        guards: Callbacks | None,
        router: Callable | None = None,
    ) -> None:
        source_state = self.states.get(source)
        target_state = self.states.get(target)
        self.events[event] = event

        if source_state is None:
            raise ConfigurationError(f"Source state '{source}' is not registered.")
        if target_state is None and event is not EngineEvent.DYNAMIC_TRANSITION:
            raise ConfigurationError(f"Target state '{target}' is not registered.")

        t = Transition(
            source=source_state,
            event=event,
            target=target_state,
            actions=prepare_callbacks(actions),
            guards=prepare_callbacks(guards),
            router=prepare_callbacks(router).pop(),
        )
        self.transitions.setdefault((source_state.state, event), []).append(t)

    def create_config(self, name: str) -> ConfigSpec:
        self.validate_config()
        return ConfigSpec(
            name=name,
            events=self.events,
            states=self.states,
            transitions=self.transitions,
        )

    def validate_config(self) -> None:
        if not self.transitions:
            raise ConfigurationError("State Machine configuration error")

        for state in self.states.keys():
            if not isinstance(state, (State, Enum, str)):
                raise ConfigurationError(
                    f"State '{state}' must be an Enum or str, not {type(state).__name__}."
                )

        for event in self.events:
            if not isinstance(event, (Enum, str)):
                raise ConfigurationError(
                    f"Event '{event}' must be an Enum or str, not {type(event).__name__}."
                )

        for transitions in self.transitions.values():
            for t in transitions:
                source = t.source.state
                target = t.target.state if t.target else t.target
                if source not in self.states:
                    raise ConfigurationError(
                        f"Source state '{source}' is not a valid state."
                    )

                if target not in self.states and target is not None:
                    raise ConfigurationError(
                        f"Target state '{target}' is not a valid state."
                    )
