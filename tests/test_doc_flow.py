import asyncio
from itertools import dropwhile
from os import stat
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from types import SimpleNamespace

from graphviz import Digraph

from sm_core import (
    BlockedTransition,
    InvalidTransition,
    TransitionMap,
    ProxyTransitionMap,
    StateMachine,
    StateMachineBuilder,
    TransitionMapError,
)


class State(Enum):
    DRAFT = auto()
    REVIEW = auto()
    APPROVED = auto()
    REJECTED = auto()
    PUBLISHED = auto()
    ARCHIVED = auto()
    FLAGGED = auto()
    PREVIOUS = auto()


class Event(Enum):
    SUBMIT = auto()
    APPROVE = auto()
    REJECT = auto()
    REVISE = auto()
    PUBLISH = auto()
    ARCHIVE = auto()
    RESET = auto()


class UserID(Enum):
    IS_OWNER = auto()
    IS_REVIEWER = auto()
    IS_ADMIN = auto()
    IS_SUPER_ADMIN = auto()


@dataclass
class Context:
    _contents: str = ""
    has_content: bool = False
    has_pending: bool = False
    is_owner: bool = False
    is_reviewer: bool = False
    is_admin: bool = False
    is_super_admin: bool = False
    is_locked: bool = False

    @property
    def contents(self) -> str:
        return self._contents

    @contents.setter
    def contents(self, text) -> None:
        self.has_content = True if text else False
        self._contents = text


def visualize_state_machine(
    transition_map: TransitionMap, filename="state_diagram.dot"
):
    dot = Digraph()
    dot.attr(rankdir="TB", nodesep="0.25", ranksep="2.0")
    dot.attr(
        "node",
        width="1.4",
        shape="circle",
        fontname="Sans",
        style="filled",
        fillcolor="azure",
        fixedsize="true",
    )
    dot.attr("edge", fontname="Sans", fillcolor="aquamarine", fontcolor="black")

    for (start_state, event), transitions in transition_map.items():
        event_name = event.name if event else "auto"

        for end_state, actions, guards in transitions:
            label_parts = [f" <{event_name}> "]
            if guards:
                for guard in guards:
                    label_parts.append(f"\n[{guard.__name__}]")
            if actions:
                for action in actions:
                    label_parts.append(f"\n{action.__name__}")

            edge_label = " ".join(label_parts)
            edge_style = "dashed" if event is None else "solid"

            dot.edge(
                str(start_state.name),
                str(end_state.name),
                label=edge_label,
                style=edge_style,
                fontsize="10",
            )
    dot.node("DRAFT", fillcolor="green")
    # dot.save(filename)
    dot.render("doc_flow", format="svg", cleanup=True)
    # dot.render(filename, view=True)


def action_log_submit(ctx):
    pass


def action_log_archive(ctx):
    pass


def action_log_approve(ctx):
    pass


def action_log_reject(ctx):
    pass


def action_log_revise(ctx):
    pass


def action_log_publish(ctx):
    pass


def action_log_override(ctx):
    pass


def action_log_reset(ctx):
    pass


def action_log_unarchive(ctx):
    pass


def action_notify_author(ctx):
    pass


def action_notify_reviewer(ctx):
    pass


def action_notify_all(ctx):
    pass


def action_clear_draft(ctx):
    pass


def action_clear_reviewer(ctx):
    pass


def action_show_error(ctx):
    print("BIGLY Error!")


def guard_has_content(ctx):
    return ctx.has_content


def guard_no_content(ctx):
    return not ctx.has_content


def guard_has_pending(ctx):
    return ctx.has_pending


def guard_is_locked(ctx):
    return ctx.is_locked


def guard_is_owner(ctx):
    return ctx.is_owner


def guard_not_owner(ctx):
    return not guard_is_owner(ctx)


def guard_is_reviewer(ctx):
    return ctx.is_reviewer


def guard_not_reviewer(ctx):
    return not guard_is_reviewer(ctx)


def guard_is_admin(ctx):
    return ctx.is_admin


def guard_not_admin(ctx):
    return not guard_is_admin(ctx)


def guard_is_super_admin(ctx):
    return ctx.is_super_admin


sm_model = (
    StateMachineBuilder[State, Event, Context]()
    .add_transition(
        State.DRAFT,
        Event.SUBMIT,
        State.REVIEW,
        action=(action_log_submit, action_notify_reviewer),
        guard=(guard_has_content, guard_is_locked),
    )
    .add_transition(
        State.DRAFT,
        Event.ARCHIVE,
        State.ARCHIVED,
        action=action_log_archive,
        guard=guard_is_owner,
    )
    .add_transition(
        State.DRAFT,
        Event.SUBMIT,
        State.DRAFT,
        action=action_show_error,
        guard=guard_no_content,
    )
    .add_transition(State.DRAFT, Event.RESET, State.DRAFT, action=action_clear_draft)
    .add_transition(
        State.REVIEW,
        Event.APPROVE,
        State.APPROVED,
        action=(action_log_approve, action_notify_author),
        guard=(guard_is_reviewer, guard_has_pending),
    )
    .add_transition(
        State.REVIEW,
        Event.APPROVE,
        State.REVIEW,
        action=action_show_error,
        guard=guard_is_reviewer,
    )
    .add_transition(
        State.REVIEW,
        Event.REJECT,
        State.REJECTED,
        action=(action_log_reject, action_notify_author),
    )
    .add_transition(
        State.REVIEW,
        Event.REVISE,
        State.DRAFT,
        action=(action_log_revise, action_clear_reviewer),
    )
    .add_transition(
        State.REVIEW, Event.ARCHIVE, State.ARCHIVED, action=action_log_archive
    )
    .add_transition(
        State.APPROVED,
        Event.PUBLISH,
        State.PUBLISHED,
        action=(action_log_publish, action_notify_all),
        guard=(guard_is_owner, guard_is_admin),
    )
    .add_transition(
        State.APPROVED,
        Event.PUBLISH,
        State.APPROVED,
        action=action_show_error,
        guard=(guard_not_owner, guard_not_admin),
    )
    .add_transition(
        State.APPROVED,
        Event.REJECT,
        State.REJECTED,
        action=action_log_override,
        guard=guard_is_admin,
    )
    .add_transition(
        State.APPROVED,
        Event.ARCHIVE,
        State.ARCHIVED,
        action=action_log_archive,
        guard=guard_is_admin,
    )
    .add_transition(
        State.REJECTED,
        Event.REVISE,
        State.DRAFT,
        action=(action_log_revise, action_log_reset),
        guard=guard_is_owner,
    )
    .add_transition(
        State.REJECTED,
        Event.ARCHIVE,
        State.ARCHIVED,
        action=action_log_archive,
        guard=(guard_is_owner, guard_is_admin),
    )
    .add_transition(
        State.REJECTED,
        Event.RESET,
        State.DRAFT,
        action=action_log_reset,
        guard=guard_is_admin,
    )
    .add_transition(
        State.PUBLISHED, Event.ARCHIVE, State.ARCHIVED, action=action_log_archive
    )
    .add_transition(State.PUBLISHED, Event.APPROVE, State.PUBLISHED)
    .add_transition(State.PUBLISHED, Event.REJECT, State.PUBLISHED)
    .add_transition(
        State.ARCHIVED,
        Event.RESET,
        State.DRAFT,
        action=action_log_archive,
        guard=guard_is_super_admin,
    )
    .add_transition(State.ARCHIVED, Event.SUBMIT, State.ARCHIVED)
    .add_transition(State.ARCHIVED, Event.APPROVE, State.ARCHIVED)
)

tm = sm_model.get_transition_map()
visualize_state_machine(tm)


# Self-transtions with guards + action: first matching guard win, or do all evaluate?
def test_self_transitions():
    ctx = Context(is_owner=True)
    # ctx.contents = "hejsa"
    sm = sm_model.build(initial_state=State.DRAFT, verbose=True)
    sm.start(context=ctx)
    sm.trigger(event=Event.SUBMIT, context=ctx)


test_self_transitions()
exit()


#  Guard priority/ordering: which fires first?
def test_guard_priority():
    ctx = Context(is_owner=True, has_content=True)
    sm = sm_model.build(initial_state=State.REVIEW, verbose=True)
    sm.start(context=ctx)
    sm.trigger(event=Event.APPROVE, context=ctx)


# Multiple transitions on same event: guard disambiguation
def test_multiple_transitions():
    ctx = Context(is_owner=False, is_admin=False, has_content=True)
    sm = sm_model.build(initial_state=State.APPROVED, verbose=True)
    sm.start(context=ctx)
    sm.trigger(event=Event.PUBLISH, context=ctx)


# Multiple transitions on same event: guard disambiguation
def test_reverse_transition():
    ctx = Context(is_owner=False, is_admin=True, has_content=True)
    sm = sm_model.build(initial_state=State.APPROVED, verbose=True)
    sm.start(context=ctx)
    sm.trigger(event=Event.REJECT, context=ctx)


# Unhandled events: should silently drop or throw.
def test_error_handling():
    ctx = Context(is_owner=False, is_admin=True, has_content=True)
    sm = sm_model.build(initial_state=State.PUBLISHED, verbose=True)
    sm.start(context=ctx)
    sm.trigger(event=Event.APPROVE, context=ctx)


# Near-terminal state with single escape
def test_super_admin():
    ctx = Context(is_owner=False, is_admin=False, is_super_admin=True, has_content=True)
    sm = sm_model.build(initial_state=State.ARCHIVED, verbose=True)
    sm.start(context=ctx)
    sm.trigger(event=Event.RESET, context=ctx)


# Same target, different sources, different actions
def test_different_sources():
    ctx = Context(is_owner=False, is_admin=False, is_super_admin=True, has_content=True)
    sm = sm_model.build(initial_state=State.REVIEW, verbose=True)
    sm.start(context=ctx)
    sm.trigger(event=Event.REVISE, context=ctx)

    sm = sm_model.build(initial_state=State.REJECTED, verbose=True)
    sm.start(context=ctx)
    sm.trigger(event=Event.REVISE, context=ctx)


# Unconditional self-transition
def test_unconditional_transiton():
    ctx = Context(
        is_owner=False,
        is_admin=False,
        is_super_admin=True,
    )
    sm = sm_model.build(initial_state=State.DRAFT, verbose=True)
    sm.start(context=ctx)
    sm.trigger(event=Event.RESET, context=ctx)


test_unconditional_transiton()
