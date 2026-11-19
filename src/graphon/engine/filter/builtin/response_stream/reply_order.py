"""Filter-owned presentation order; execution continues while replies wait."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field

from graphon.engine_events.base import NodeEvent
from graphon.engine_events.frame import FrameStartedEvent
from graphon.engine_events.node import (
    NodeRunFailedEvent,
    NodeRunRetryEvent,
    NodeRunStreamChunkEvent,
)
from graphon.runtime.execution import ROOT_FRAME_ID


@dataclass
class _Reply:
    events: deque[NodeRunStreamChunkEvent] = field(default_factory=deque)
    finished: bool = False
    displayed: bool = False


@dataclass
class _FrameReplies:
    frame_id: str
    entries: deque[_Reply | _ContainerReplies] = field(default_factory=deque)
    finished: bool = False
    failed: bool = False
    waiting_after_failure: bool = False


@dataclass
class _ContainerReplies:
    container_key: tuple[str, str]
    invocation_id: str | None = None
    frames: dict[int, _FrameReplies] = field(default_factory=dict)
    next_index: int = 0
    unindexed_frame_count: int = 0
    finished: bool = False
    failed: bool = False
    waiting_after_failure: bool = False


class _ReplyOrder:
    def __init__(self) -> None:
        self._frames = {ROOT_FRAME_ID: _FrameReplies(ROOT_FRAME_ID)}
        self._containers: dict[tuple[str, str], _ContainerReplies] = {}

    def clear(self) -> None:
        self._frames = {ROOT_FRAME_ID: _FrameReplies(ROOT_FRAME_ID)}
        self._containers.clear()

    def _get_or_create_container(
        self, container_key: tuple[str, str]
    ) -> _ContainerReplies:
        container = self._containers.get(container_key)
        if container is None or container.finished:
            container = _ContainerReplies(container_key)
            self._containers[container_key] = container
            self._frames[container_key[0]].entries.append(container)
        return container

    def start_container(self, event: NodeEvent) -> None:
        self._frames[event.frame_id].waiting_after_failure = False
        self._get_or_create_container((event.frame_id, event.id))

    def finish_container(self, event: NodeEvent) -> None:
        if container := self._containers.get((event.frame_id, event.id)):
            container.finished = True
            container.failed = isinstance(event, NodeRunFailedEvent)
            if isinstance(event, (NodeRunFailedEvent, NodeRunRetryEvent)):
                for frame in container.frames.values():
                    frame.finished = True
                    self._discard_pending_replies(frame)

    def start_frame(self, event: FrameStartedEvent) -> None:
        if event.frame_id == ROOT_FRAME_ID:
            return
        parent_frame_id = event.parent_frame_id or ROOT_FRAME_ID
        container = self._get_or_create_container((
            parent_frame_id,
            event.container_execution_id or event.invocation_id or event.container_id,
        ))
        container.invocation_id = event.invocation_id
        # A newly scheduled frame establishes continuation after a failed item.
        container.waiting_after_failure = False
        index = event.index
        if index is None:
            index = container.unindexed_frame_count
            container.unindexed_frame_count += 1
        frame = _FrameReplies(event.frame_id)
        container.frames[index] = frame
        self._frames[event.frame_id] = frame

    def add_reply(self, frame_id: str) -> _Reply:
        reply = _Reply()
        self._frames[frame_id].entries.append(reply)
        return reply

    def finish_frame(self, frame_id: str, *, succeeded: bool) -> None:
        if frame := self._frames.get(frame_id):
            frame.finished = True
            frame.failed = not succeeded
            if not succeeded:
                self._discard_pending_replies(frame)

    def finish(self, *, succeeded: bool) -> None:
        for frame in self._frames.values():
            frame.finished = True
        for container in self._containers.values():
            container.finished = True
        if not succeeded:
            self._discard_pending_replies(self._frames[ROOT_FRAME_ID])

    def _discard_pending_replies(self, frame: _FrameReplies) -> None:
        for entry in frame.entries:
            entry.finished = True
            if isinstance(entry, _Reply):
                if not entry.displayed:
                    entry.events.clear()
            else:
                for child in entry.frames.values():
                    child.finished = True
                    self._discard_pending_replies(child)

    def drain(self) -> list[NodeRunStreamChunkEvent]:
        output: list[NodeRunStreamChunkEvent] = []
        self._drain_frame(self._frames[ROOT_FRAME_ID], output)
        return output

    def _drain_frame(
        self, frame: _FrameReplies, output: list[NodeRunStreamChunkEvent]
    ) -> bool:
        while frame.entries:
            if frame.waiting_after_failure and not frame.finished:
                return False
            entry = frame.entries[0]
            if isinstance(entry, _Reply):
                if entry.events:
                    entry.displayed = True
                    output.extend(entry.events)
                    entry.events.clear()
                if not entry.finished:
                    return False
            else:
                if not self._drain_container(entry, output):
                    return False
                frame.waiting_after_failure = entry.failed
            frame.entries.popleft()
        return frame.finished

    def _drain_container(
        self, container: _ContainerReplies, output: list[NodeRunStreamChunkEvent]
    ) -> bool:
        while container.frames:
            if container.waiting_after_failure and not container.finished:
                return False
            if container.next_index not in container.frames:
                if not container.finished:
                    return False
                container.next_index = min(container.frames)
            frame = container.frames[container.next_index]
            if not self._drain_frame(frame, output):
                return False
            del container.frames[container.next_index]
            self._frames.pop(frame.frame_id, None)
            container.next_index += 1
            container.waiting_after_failure = frame.failed
        if not container.finished:
            return False
        if self._containers.get(container.container_key) is container:
            del self._containers[container.container_key]
        return True
