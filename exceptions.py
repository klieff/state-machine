class StateMachineError(Exception):
    msg: str

    def __init__(self, **kwargs) -> None:
        format_values = kwargs.get("audit", {}).get("details", kwargs)

        try:
            formatted_msg = self.msg.format(**format_values)
        except (KeyError, AttributeError, IndexError) as e:
            keys = list(format_values.keys())
            formatted_msg = f"{type(e)}: {self.msg} | Missing context: {keys}"

        self.params = kwargs
        super().__init__(formatted_msg)


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
