# class StateMachineError[A: AuditRecord](Exception):
class StateMachineError(Exception):
    def __init__(self, **kwargs) -> None:
        audit = kwargs.get("audit")
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
