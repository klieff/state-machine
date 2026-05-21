# from typing import TYPE_CHECKING
# from .definitions import EngineEvent

# if TYPE_CHECKING:
#     from .definitions import EventRecord


# def format_event_log(record: EventRecord) -> str:
#     details = record.details
#     timestamp = record.timestamp.strftime("%H:%M:%S.%f")[:-3]
#     line = f"[{timestamp}] <{record.machine}> {record.machine_event:<20} | Source: {details.source}"
#
#     detail_str = ""
#     if EngineEvent.EVENT_TRIGGER.name in record.machine_event:
#         detail_str += f" Event: {details.event}"
#     if details.target and record.machine_event != EngineEvent.GUARD_SKIP.name:
#         detail_str += f" -> Target: {details.target}"
#     if details.action:
#         detail_str += f" Action: {details.action}"
#     if details.guard:
#         res = "PASS" if details.passed else "FAIL"
#         detail_str += f" Guard [{res}]: {details.guard}"
#     if details.error_message:
#         detail_str += f" Exception [{details.error_type}]: {details.error_message}"
#
#     return f"{line}{detail_str}"


# def get_obj_name(obj) -> str:
#     name = getattr(obj, "name", None)
#     return name or getattr(obj, "__name__", type(obj).__name__)


def ensure_tuple(obj) -> tuple:
    if callable(obj):
        return (obj,)
    elif isinstance(obj, (list, set, tuple)):
        return tuple(obj)
    return ()
