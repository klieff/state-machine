class StateMachineException(Exception):
    msg: str

    def __init__(self, **kwargs) -> None:
        format_values = kwargs.get("event_record", {}).get("details", kwargs)

        try:
            formatted_msg = self.msg.format(**format_values)
        except (KeyError, AttributeError, IndexError) as e:
            keys = list(format_values.keys())
            formatted_msg = f"{type(e)}: {self.msg} | Missing context: {keys}"

        self.params = kwargs
        super().__init__(formatted_msg)


class BlockedTransition(StateMachineException):
    msg = "Transitions from state '{source}' on event '{event}' were blocked by guards."


class InvalidTransition(StateMachineException):
    msg = "No transition map registered for event '{event}' on state '{source}'"


class ActionError(StateMachineException):
    msg = "Critical failure in {action_type} action '{action}' (State: {source} Event: {event})"


class GuardError(StateMachineException):
    msg = "Critical failure in guard '{guard}' (State: {source} Event: {event})"


class InvalidEvent(StateMachineException):
    msg = "Event is not registered"


class InvalidState(StateMachineException):
    msg = "No transition map found for initial state '{initial_state}'"


class TransitionMapError(StateMachineException):
    msg = (
        "StateMachine '{machine_name}' cannot be built with an empty transition map. "
        "Ensure at least one transition is added before calling build()."
    )


class UninitializedError(StateMachineException):
    msg = (
        "StateMachine '{machine_name}' has not been initialized. "
        "Ensure start() is called before calling trigger()."
    )
