import itertools
from collections import deque
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum, IntEnum, StrEnum, auto
from inspect import isawaitable
from types import MappingProxyType
from typing import TypedDict

# TODO: Valid States/Events: Detech unreacahable states, missing transitions, and ambiguous guards.
#       Strict mode: Only one valid transition is allowed else raise exception (deterministic)
#       Concurrency protection: Is used in async/multithreaded context there are no locks and
#       state mutations are unsafe.
#       Self-execution: An initial state that has no valid transtitions (dead-end), should only
#       be allowed by a user_flag=True and then ensure that it can run on_exit/on_entry actions.
#       Transition hooks: Runs regardless of event on State A -> State B, could be useful?
#       InvalidTransition could force the state machine into an "Internal Error State" e.g.
#       an Enum ERROR or HALT rather than raising an exception or implement Policy-based
#       handling STRICT/IGNORE/FALLBACK modes.
#       Audit: MAX_AUDIT should be configurable. Offloading/flushing support?

MAX_AUDIT = 200

type Action[C] = Callable[[C], Awaitable[None] | None]
type EntryExitAction[S, C] = dict[S, list[Action[C]]]
type Guard[C] = Callable[[C], Awaitable[bool] | bool]
type TransitionMap[S, E, C] = dict[
    tuple[S, E], list[tuple[S, Action[C] | None, Guard[C] | None]]
]
type ProxyEntryExitAction[S, C] = MappingProxyType[S, tuple[Action[C], ...]]
type ProxyTransitionMap[S, E, C] = MappingProxyType[
    tuple[S, E], tuple[tuple[S, Action[C] | None, Guard[C] | None], ...]
]


class StateMachineError[A: AuditRecord](Exception):
    def __init__(self, audit: A | None = None, **kwargs) -> None:
        if audit:
            formatted_msg = self.msg.format(**audit.details)
        else:
            formatted_msg = self.msg.format(**kwargs)

        super().__init__(formatted_msg)
        self.msg = formatted_msg


class BlockedTransition(StateMachineError):
    msg = "Transitions from state '{source}' on event '{event}' were blocked by guards."


class InvalidTransition(StateMachineError):
    msg = "No transition map registered for event '{event}' on state '{source}'"


class ActionError(StateMachineError):
    msg = "Critical failure in {action_type} action '{action}' (State: {source} Event: {event})"


class GuardError(StateMachineError):
    msg = "Critical failure in guard '{guard}' (State: {source} Event: {event})"


class InvalidState(StateMachineError):
    msg = "No transition map found for initial state '{initial_state}'"


class TransitionMapError(StateMachineError):
    msg = (
        "StateMachine '{machine_name}' cannot be built with an empty transition map. "
        "Ensure at least one transition is added before calling build()."
    )


class MachineEvent(IntEnum):
    EVENT_TRIGGER = auto()
    ACTION_EXECUTE = auto()
    GUARD_SKIP = auto()
    GUARD_EVALUATE = auto()
    STATE_CHANGE = auto()
    TRANSITION_COMPLETE = auto()
    EXCEPTION = auto()


class AuditDetails[S: Enum, E: Enum, C](TypedDict, total=False):
    source: S
    target: S
    event: E
    action: Action[C]
    action_type: str
    guard: Guard[C]
    passed: bool
    error_type: str
    error_message: str


@dataclass(slots=True)
class AuditRecord[S: Enum, E: Enum, C]:
    machine: str
    event: str
    details: AuditDetails[S, E, C] = field(default_factory=AuditDetails)
    timestamp: datetime = field(default_factory=datetime.now)


def _format_audit_log(record: AuditRecord) -> str:
    detail_str = ""
    details = record.details
    timestamp = record.timestamp.strftime("%H:%M:%S")

    line = f"[{timestamp}] <{record.machine}> {record.event:<20} | Source: {details.get('source')}"

    if MachineEvent.EVENT_TRIGGER.name in record.event:
        detail_str = f" Event: {details.get('event')}"
    elif "guard" in details:
        res = "PASS" if details.get("passed") else "FAIL"
        detail_str = f" Guard: {details.get('guard')} [{res}]"
    elif "action" in details:
        detail_str = f" Action [{details.get('action_type')}]: {details.get('action')}"
    elif "target" in details and record.event != "GUARD_SKIP":
        detail_str = f" -> Target: {details['target']}"
    elif "error_message" in details:
        detail_str = (
            f" Exception: <{details.get('error_type')}> {details.get('error_message')}"
        )

    return f"{line}{detail_str}"


class StateMachineMixin[S: Enum, E: Enum, C]:
    __slots__ = ()

    _name: str
    _state: S
    _transitions: ProxyTransitionMap[S, E, C]
    _on_entry: ProxyEntryExitAction[S, C]
    _on_exit: ProxyEntryExitAction[S, C]
    _audit: deque[AuditRecord]

    def get_state_events(self, state: S) -> list[E]:
        return [event for (s, event) in self._transitions.keys() if s == state]

    def _apply_transition(self, event: E, new_state: S) -> None:
        old_state = self._state
        self._state = new_state
        self._dispatch_audit(
            MachineEvent.STATE_CHANGE,
            source=old_state,
            target=new_state,
            event=event,
        )

    def _dispatch_audit(self, machine_event: MachineEvent, **kwargs) -> None:
        record = self._record_audit(machine_event, **kwargs)
        # if self._logger:
        print(_format_audit_log(record))

    def _get_name(self, obj) -> str:
        return getattr(obj, "__name__", type(obj).__name__)

    def _record_audit(self, machine_event: MachineEvent, **kwargs) -> AuditRecord:
        func_keys = frozenset(("action", "guard"))
        for key in func_keys:
            if key in kwargs:
                kwargs[key] = self._get_name(kwargs[key])

        record = AuditRecord(
            machine=self._name,
            event=machine_event.name,
            details=AuditDetails(**kwargs),
        )

        self._audit.append(record)
        return record

    def _resolve_transitions(
        self, event: E
    ) -> tuple[tuple[S, Action[C] | None, Guard[C] | None], ...]:
        if not (transitions := self._transitions.get((self._state, event))):
            self._dispatch_audit(
                MachineEvent.EXCEPTION,
                source=self._state,
                event=event,
                error_type=InvalidTransition.__name__,
                error_message=f"No transition map registered for {event}",
            )
            raise InvalidTransition(audit=self._audit[-1])

        return transitions


@dataclass(slots=True)
class StateMachine[S: Enum, E: Enum, C](StateMachineMixin[S, E, C]):
    _name: str
    _state: S
    _transitions: ProxyTransitionMap[S, E, C]
    _on_entry: ProxyEntryExitAction[S, C]
    _on_exit: ProxyEntryExitAction[S, C]
    _audit: deque[AuditRecord] = field(default_factory=lambda: deque(maxlen=MAX_AUDIT))

    def trigger(self, event: E, context: C) -> dict[str, S | E]:
        old_state = self._state
        self._dispatch_audit(
            MachineEvent.EVENT_TRIGGER, event=event, source=self._state
        )

        transitions = self._resolve_transitions(event)
        new_state, action = self._execute_guard_task(event, context, transitions)

        self._execute_exit_actions(event, context)
        self._execute_transition_action(event, action, context)
        self._apply_transition(event, new_state)
        self._execute_entry_actions(event, context)

        self._dispatch_audit(
            MachineEvent.TRANSITION_COMPLETE,
            source=old_state,
            target=new_state,
            event=event,
        )
        return dict(source=old_state, target=new_state, event=event)

    def _evaluate_guard(
        self, event: E, context: C, guard: Guard[C], target_state: S
    ) -> Awaitable[bool] | bool:
        try:
            passed = guard(context)
            self._dispatch_audit(
                MachineEvent.GUARD_EVALUATE,
                source=self._state,
                target=target_state,
                event=event,
                guard=guard,
                passed=passed,
            )
            return passed
        except Exception as e:
            self._dispatch_audit(
                MachineEvent.EXCEPTION,
                source=self._state,
                target=target_state,
                event=event,
                guard=guard,
                error_type=GuardError.__name__,
                error_message=f"{type(e).__name__}: {e}",
            )
            raise GuardError(audit=self._audit[-1]) from e

    def _execute_guard_task(
        self,
        event: E,
        context: C,
        transitions: tuple[tuple[S, Action[C] | None, Guard[C] | None], ...],
    ) -> tuple[S, Action[C] | None]:

        for target_state, action, guard in transitions:
            if guard is None:
                if len(transitions) > 1:
                    self._dispatch_audit(
                        MachineEvent.GUARD_SKIP,
                        source=self._state,
                        target=target_state,
                        event=event,
                    )
                return (target_state, action)

            if self._evaluate_guard(event, context, guard, target_state):
                return (target_state, action)

        self._dispatch_audit(
            MachineEvent.EXCEPTION,
            source=self._state,
            event=event,
            error_type=BlockedTransition.__name__,
            error_message=f"No guards passed for event {event}",
        )
        raise BlockedTransition(audit=self._audit[-1])

    def _execute_entry_actions(self, event: E, context: C) -> None:
        self._run_actions(
            event, context, self._on_entry.get(self._state), action_type="on_entry"
        )

    def _execute_exit_actions(self, event: E, context: C) -> None:
        self._run_actions(
            event, context, self._on_exit.get(self._state), action_type="on_exit"
        )

    def _execute_transition_action(
        self, event: E, action: Action[C] | None, context: C
    ) -> None:
        if action:
            self._run_action(event, context, action, action_type="transition")

    def _run_action(
        self, event: E, context: C, action: Action[C], action_type: str | None = None
    ) -> None:
        try:
            action(context)
            self._dispatch_audit(
                MachineEvent.ACTION_EXECUTE,
                source=self._state,
                event=event,
                action=action,
                action_type=action_type,
            )
        except Exception as e:
            self._dispatch_audit(
                MachineEvent.EXCEPTION,
                source=self._state,
                event=event,
                action=action,
                action_type=action_type,
                error_type=ActionError.__name__,
                error_message=f"{type(e).__name__}: {e}",
            )
            raise ActionError(audit=self._audit[-1]) from e

    def _run_actions(
        self,
        event: E,
        context: C,
        actions: Iterable[Action[C]] | None,
        action_type: str,
    ) -> None:
        if actions:
            for action in actions:
                self._run_action(event, context, action, action_type=action_type)


@dataclass(slots=True)
class AsyncStateMachine[S: Enum, E: Enum, C](StateMachineMixin[S, E, C]):
    _name: str
    _state: S
    _transitions: ProxyTransitionMap[S, E, C]
    _on_entry: ProxyEntryExitAction[S, C]
    _on_exit: ProxyEntryExitAction[S, C]
    _audit: deque[AuditRecord] = field(default_factory=lambda: deque(maxlen=MAX_AUDIT))

    async def trigger(self, event: E, context: C) -> dict[str, S | E]:
        old_state = self._state
        self._dispatch_audit(
            MachineEvent.EVENT_TRIGGER, event=event, source=self._state
        )

        transitions = self._resolve_transitions(event)
        new_state, action = await self._execute_guard_task(event, context, transitions)

        await self._execute_exit_actions(event, context)
        await self._execute_transition_action(event, action, context)
        self._apply_transition(event, new_state)
        await self._execute_entry_actions(event, context)

        self._dispatch_audit(
            MachineEvent.TRANSITION_COMPLETE,
            source=old_state,
            target=new_state,
            event=event,
        )
        return dict(source=old_state, target=new_state, event=event)

    async def _evaluate_guard(
        self, event: E, context: C, guard: Guard[C], target_state: S
    ) -> Awaitable[bool] | bool:
        try:
            result = guard(context)
            passed = await result if isawaitable(result) else result
            self._dispatch_audit(
                MachineEvent.GUARD_EVALUATE,
                source=self._state,
                target=target_state,
                event=event,
                guard=guard,
                passed=passed,
            )
            return passed
        except Exception as e:
            self._dispatch_audit(
                MachineEvent.EXCEPTION,
                source=self._state,
                target=target_state,
                event=event,
                guard=guard,
                error_type=GuardError.__name__,
                error_message=f"{type(e).__name__}: {e}",
            )
            raise GuardError(audit=self._audit[-1]) from e

    async def _execute_guard_task(
        self,
        event: E,
        context: C,
        transitions: tuple[tuple[S, Action[C] | None, Guard[C] | None], ...],
    ) -> tuple[S, Action[C] | None]:

        for target_state, action, guard in transitions:
            if guard is None:
                if len(transitions) > 1:
                    self._dispatch_audit(
                        MachineEvent.GUARD_SKIP,
                        source=self._state,
                        target=target_state,
                        event=event,
                    )
                return (target_state, action)

            if await self._evaluate_guard(event, context, guard, target_state):
                return (target_state, action)

        self._dispatch_audit(
            MachineEvent.EXCEPTION,
            source=self._state,
            event=event,
            error_type=BlockedTransition.__name__,
            error_message=f"No guards passed for event {event}",
        )
        raise BlockedTransition(audit=self._audit[-1])

    async def _execute_entry_actions(self, event: E, context: C) -> None:
        await self._run_actions(
            event, context, self._on_entry.get(self._state), action_type="on_entry"
        )

    async def _execute_exit_actions(self, event: E, context: C) -> None:
        await self._run_actions(
            event, context, self._on_exit.get(self._state), action_type="on_exit"
        )

    async def _execute_transition_action(
        self, event: E, action: Action[C] | None, context: C
    ) -> None:
        if action:
            await self._run_action(event, context, action, action_type="transition")

    async def _run_action(
        self, event: E, context: C, action: Action[C], action_type: str | None = None
    ) -> None:
        try:
            result = action(context)
            if isawaitable(result):
                await result

            self._dispatch_audit(
                MachineEvent.ACTION_EXECUTE,
                source=self._state,
                event=event,
                action=action,
                action_type=action_type,
            )
        except Exception as e:
            self._dispatch_audit(
                MachineEvent.EXCEPTION,
                source=self._state,
                event=event,
                action=action,
                action_type=action_type,
                error_type=ActionError.__name__,
                error_message=f"{type(e).__name__}: {e}",
            )
            raise ActionError(audit=self._audit[-1]) from e

    async def _run_actions(
        self,
        event: E,
        context: C,
        actions: Iterable[Action[C]] | None,
        action_type: str,
    ) -> None:
        if actions:
            for action in actions:
                await self._run_action(event, context, action, action_type=action_type)


@dataclass(slots=True)
class StateMachineModel[S: Enum, E: Enum, C]:
    events: set[E] = field(default_factory=set)
    states: set[S] = field(default_factory=set)
    transitions: TransitionMap[S, E, C] = field(default_factory=dict)
    on_entry: EntryExitAction[S, C] = field(default_factory=dict)
    on_exit: EntryExitAction[S, C] = field(default_factory=dict)
    counter = itertools.count(start=1)

    @classmethod
    def get_name(cls, base_name="SM"):
        return f"{base_name}_{next(cls.counter)}"

    def add_entry(self, state: S, action: Action[C]) -> "StateMachineModel[S, E, C]":
        self.states.add(state)
        self.on_entry.setdefault(state, []).append(action)
        return self

    def add_exit(self, state: S, action: Action[C]) -> "StateMachineModel[S, E, C]":
        self.states.add(state)
        self.on_exit.setdefault(state, []).append(action)
        return self

    def add_transition(
        self,
        source_state: S,
        event: E,
        target_state: S,
        action: Action[C] | None = None,
        guard: Guard[C] | None = None,
    ) -> "StateMachineModel[S, E, C]":
        self.events.add(event)
        self.states.add(source_state)
        self.states.add(target_state)
        self.transitions.setdefault((source_state, event), []).append(
            (target_state, action, guard)
        )
        return self

    def build(
        self, initial_state: S, name: str | None = None, verbose: bool = False
    ) -> StateMachine[S, E, C]:
        name = name if name else self.get_name()

        self.states.add(initial_state)
        self._validate_model(name, initial_state)
        sm_settings = self._prepare_immutables()

        return StateMachine(_name=name, _state=initial_state, **sm_settings)

    def build_async(
        self, initial_state: S, name: str | None = None, verbose: bool = False
    ) -> AsyncStateMachine[S, E, C]:
        name = name if name else self.get_name()

        self._validate_model(name, initial_state)
        sm_settings = self._prepare_immutables()

        return AsyncStateMachine(_name=name, _state=initial_state, **sm_settings)

    def _prepare_immutables(self) -> dict:
        template = dict(
            _transitions=self.transitions,
            _on_entry=self.on_entry,
            _on_exit=self.on_exit,
        )
        return {
            k: MappingProxyType({kk: tuple(vv) for kk, vv in v.items()})
            for k, v in template.items()
        }

    def _validate_model(self, name, initial_state) -> None:
        if not self.transitions:
            raise TransitionMapError(machine_name=name)

        if initial_state not in self.states:
            raise InvalidState(initial_state=initial_state)

        state_type = type(initial_state)
        for state in self.states:
            if not isinstance(state, Enum):
                raise TypeError(
                    f"State '{state}' must be an Enum, not {type(state).__name__}."
                )

            if not isinstance(state, state_type):
                raise TypeError(
                    f"Inconsistent Enum class: '{state}' is a {type(state).__name__}, "
                    f"but the machine expects {state_type.__name__}."
                )

        for event in self.events:
            if not isinstance(event, Enum):
                raise TypeError(
                    f"Event '{event}' must be an Enum, not {type(event).__name__}."
                )
