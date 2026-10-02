"""App-owned inbox and observable events; no provider or tool execution here."""

import asyncio
import copy
import json
import time
import uuid
from dataclasses import dataclass, field, replace


@dataclass(frozen=True)
class Input:
    kind: str
    text: str = ""
    source: str = "user"
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    target: str | None = None
    attachments: tuple = ()
    call_id: str | None = None
    # Host-private capability.  It is never serialized into context or exposed
    # to providers; the loop binds it only while executing this input.
    activation: object | None = field(default=None, compare=False, repr=False)
    # Opt-in: this steering input may affect only the named generation.
    target_generation_id: str | None = None


class _Event(tuple):
    """Keep tuple-compatible inbox events tied to their producing task."""

    def __new__(cls, item, activation):
        event = super().__new__(cls, item)
        event.activation = activation
        return event


class _Inbox(asyncio.Queue):
    def __init__(self, runtime):
        super().__init__()
        self.runtime = runtime

    def put_nowait(self, item):
        capture = self.runtime.capture_activation
        super().put_nowait(_Event(item, capture() if capture else None))


class Runtime:
    """Single-process prototype. History is observation, never execution replay."""

    def __init__(self, session_id=None, observer=None, max_input_chars=16000):
        self.session_id = session_id or str(uuid.uuid4())
        # Bound external input separately so tool/provider completion cannot deadlock
        # behind a full command queue during shutdown.
        self.capture_activation = None
        self.inbox = _Inbox(self)
        self.queued_inputs = 0
        self.events = []
        self.sequence = 0
        self.dropped_events = 0
        self.accepted = {}
        self.observer = observer
        self.changed = asyncio.Condition()
        self.closed = False
        self.max_input_chars = max_input_chars
        self.generation = None
        self.anchored_steering_mode = None
        self.steering_outcomes = {}

    async def submit(self, command: Input):
        if self.closed:
            raise RuntimeError("Session is closed")
        if command.kind not in {"user", "steer", "service", "cancel_job", "stop"}:
            raise ValueError("Unknown command")
        if not command.id or len(command.id) > 128:
            raise ValueError("Invalid command identity")
        if len(command.text) > self.max_input_chars or len(command.source) > 128:
            raise ValueError("Input is too large")
        if command.kind in {"user", "steer", "service"} and not command.text.strip():
            raise ValueError("Text is required")
        if command.kind != "service" and command.source != "user":
            raise ValueError("Service observations cannot authorize commands")
        if command.kind == "service" and command.source in {"", "user", "system", "developer"}:
            raise ValueError("Service observations need a distinct source")
        if command.attachments and command.kind not in {"user", "steer"}:
            raise ValueError("Attachments require a user work message")
        previous = self.accepted.get(command.id)
        if previous:
            if previous != command:
                raise ValueError("Command identity reused with different content")
            return command.id
        if command.target_generation_id is not None:
            if command.kind != "steer" or not isinstance(command.target_generation_id, str) or not 1 <= len(command.target_generation_id) <= 128:
                raise ValueError("A bounded generation anchor is only valid for steering")
            if self.anchored_steering_mode != "request_boundary":
                raise ValueError("Anchored steering is unavailable for this execution mode")
            if not self.generation or self.generation["id"] != command.target_generation_id:
                raise ValueError("The anchored generation is no longer active")
        # Never silently evict identities and make an old command executable again.
        if len(self.accepted) >= 2000:
            raise RuntimeError("Session input limit reached; start a new session")
        if self.queued_inputs >= 128:
            raise asyncio.QueueFull()
        self.inbox.put_nowait(("input", command))
        self.queued_inputs += 1
        self.accepted[command.id] = command
        if command.target_generation_id is not None:
            self.steering_outcomes[command.id] = {"accepted": True, "inputId": command.id, "generationId": command.target_generation_id, "disposition": "queued"}
        await self.emit("input.accepted", input_id=command.id, kind=command.kind,
                        source=command.source, target=command.target)
        return command.id

    async def submit_steering(self, command: Input, generation_id: str):
        """Opt-in current-generation delivery; never starts a later turn.

        Acceptance only proves queue admission. Applied/held disposition is
        observed separately; the application owns durable command receipts.
        """
        if command.kind != "steer" or not isinstance(generation_id, str) or not 1 <= len(generation_id) <= 128:
            raise ValueError("submit_steering requires a steering input and bounded generation identity")
        if command.target_generation_id is not None and command.target_generation_id != generation_id:
            raise ValueError("Conflicting generation anchors")
        await self.submit(replace(command, target_generation_id=generation_id))
        return copy.deepcopy(self.steering_outcomes[command.id])

    async def emit(self, event_type, **data):
        # Optional portable generation correlation. A generation is one finite
        # manager turn; it may finish while delegated jobs are still running.
        if event_type == "generation.started":
            self.generation = {"id": data["generation_id"], "initial_input_id": data.get("initial_input_id"), "input_ids": [], "accepted_input_ids": []}
        generation = self.generation
        if generation is not None:
            if event_type in {"input.delivered", "steering.applied"}:
                identity = data.get("input_id")
                if identity and identity not in generation["input_ids"]:
                    generation["input_ids"].append(identity)
            elif event_type == "steering.accepted":
                identity = data.get("input_id")
                if identity and identity not in generation["accepted_input_ids"]:
                    generation["accepted_input_ids"].append(identity)
            if event_type in {"assistant.message", "generation.finished", "generation.failed", "generation.detached"}:
                data["generation_id"] = generation["id"]
                data["input_ids"] = list(generation["input_ids"])
                data["accepted_input_ids"] = [identity for identity in generation["accepted_input_ids"] if identity not in generation["input_ids"]]
        identity = data.get("input_id")
        outcome = self.steering_outcomes.get(identity)
        if outcome is not None and data.get("target_generation_id") == outcome["generationId"]:
            if event_type in {"input.delivered", "steering.applied"}:
                self.steering_outcomes[identity] = {**outcome, "disposition": "applied"}
            elif event_type == "steering.held" and outcome["disposition"] != "applied":
                self.steering_outcomes[identity] = {**outcome, "disposition": "held", "reason": data.get("reason", "generation-ended")}
            elif event_type == "steering.unknown" and outcome["disposition"] == "queued":
                self.steering_outcomes[identity] = {**outcome, "disposition": "unknown", "reason": data.get("reason", "injection-unconfirmed")}
        self.sequence += 1
        event = {"version": 1, "sequence": self.sequence,
                 "session_id": self.session_id, "time": time.time(),
                 "type": event_type, **copy.deepcopy(data)}
        self.events.append(event)
        if len(self.events)>10000:
            self.events.pop(0);self.dropped_events+=1
        if self.observer:
            self.observer(copy.deepcopy(event))
        async with self.changed:
            self.changed.notify_all()
        if event_type in {"generation.finished", "generation.failed", "generation.detached"}:
            self.generation = None
        return event

    async def wait_for(self, predicate, timeout=30):
        async def wait():
            async with self.changed:
                while True:
                    for event in self.events:
                        if predicate(event):
                            return copy.deepcopy(event)
                    await self.changed.wait()
        return await asyncio.wait_for(wait(), timeout)


def observation(source, text, **metadata):
    # The provider's instruction message explicitly treats this envelope as data.
    return json.dumps({"observation": {"source": source, "text": text, **metadata}})
