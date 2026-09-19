"""Portable host ownership: no application, lock store, or provider SDK imports."""

import asyncio
from contextlib import asynccontextmanager
from contextvars import ContextVar
from types import SimpleNamespace

import pytest

from amplifier_core import AmplifierSession
from amplifier_core.message_models import ToolCall
from amplifier_module_loop_live.runtime import Input, Runtime

from test_loop import Delegate, Provider


class Ownership:
    """Fixture implementation of the optional host activation capability."""

    def __init__(self):
        self.active = None
        self.bound = ContextVar("fixture_activation", default=None)

    def activate(self):
        self.active = object()
        self.bound.set(self.active)
        return self.active

    def current(self):
        return self.bound.get()

    def check(self, activation):
        if activation is None or activation is not self.active:
            raise RuntimeError("released or superseded activation")

    def bind(self, activation):
        self.check(activation)
        return self.bound.set(activation)

    def reset(self, token):
        self.bound.reset(token)

    def release(self, activation):
        self.check(activation)
        self.active = None

    @property
    def current_valid(self):
        return self.active is not None and self.current() is self.active


@asynccontextmanager
async def manager(ownership, park):
    runtime = Runtime()
    runtime.capture_activation = ownership.current
    session = AmplifierSession({"session": {
        "orchestrator": {"module": "loop-live", "config": {
            "configured_bundle": True, "background_delegate": True,
            "min_delay_between_calls_ms": 0}},
        "context": {"module": "context-simple"}}, "providers": []},
        session_id=runtime.session_id)
    await session.initialize()
    provider, tool = Provider(), Delegate()
    await session.coordinator.mount("providers", provider, name="fixture")
    await session.coordinator.mount("tools", tool, name="delegate")
    checkpoints = []

    async def checkpoint(status=None):
        ownership.check(ownership.current())
        checkpoints.append((ownership.current(), status))

    for name, capability in {"live.runtime": runtime, "live.activation": ownership,
                             "live.park": park, "live.checkpoint": checkpoint}.items():
        session.coordinator.register_capability(name, capability)
    task = asyncio.create_task(session.execute(""))
    try:
        await runtime.wait_for(lambda event: event["type"] == "session.ready", 3)
        yield SimpleNamespace(runtime=runtime, session=session, provider=provider,
                              tool=tool, task=task, checkpoints=checkpoints)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await session.cleanup()


@pytest.mark.asyncio
async def test_queue_captures_producing_task_not_latest_owner():
    ownership, runtime = Ownership(), Runtime()
    runtime.capture_activation = ownership.current
    first = ownership.activate()
    release = asyncio.Event()

    async def delayed_producer():
        await release.wait()
        await runtime.inbox.put(("child_report", {"report": "old"}))

    producer = asyncio.create_task(delayed_producer())
    ownership.release(first)
    second = ownership.activate()
    release.set()
    await producer
    runtime.inbox.put_nowait(("child_report", {"report": "new"}))
    old, new = runtime.inbox.get_nowait(), runtime.inbox.get_nowait()
    assert tuple(old) == ("child_report", {"report": "old"})
    assert old.activation is first
    assert new.activation is second
    with pytest.raises(RuntimeError, match="released or superseded"):
        ownership.bind(old.activation)
    ownership.bind(new.activation)
    assert ownership.current_valid


@pytest.mark.asyncio
async def test_duplicate_receipt_survives_host_reacquisition_without_requeue():
    ownership, runtime = Ownership(), Runtime()
    runtime.capture_activation = ownership.current
    first = ownership.activate()
    command = Input("user", "one task", id="same", activation=first)
    assert await runtime.submit(command) == "same"
    ownership.release(first)
    second = ownership.activate()
    assert await runtime.submit(Input("user", "one task", id="same", activation=second)) == "same"
    assert runtime.queued_inputs == 1
    assert runtime.inbox.qsize() == 1
    assert runtime.accepted["same"] is command
    assert len(runtime.events) == 1
    with pytest.raises(ValueError, match="different content"):
        await runtime.submit(Input("user", "changed task", id="same", activation=second))


@pytest.mark.asyncio
@pytest.mark.parametrize("command_activation", [None, object()])
async def test_bad_command_activation_fails_explicitly_without_hanging(command_activation):
    ownership = Ownership()
    ownership.activate()

    async def park(*, activation):
        pass

    async with manager(ownership, park) as live:
        # Inbox production is admitted, but a malformed host supplied the
        # wrong turn token. This must become an explicit terminal failure.
        await live.runtime.submit(Input("user", "bad admission", activation=command_activation))
        with pytest.raises(RuntimeError, match="Manager turn failed"):
            await asyncio.wait_for(live.task, 3)
        assert live.runtime.closed
        assert live.provider.requests.empty()
        assert any(event["type"] == "generation.failed" for event in live.runtime.events)
        assert not any(event["type"] == "generation.finished" for event in live.runtime.events)
        assert all(status is not None for token, status in live.checkpoints)


@pytest.mark.asyncio
async def test_parking_is_awaited_and_next_turn_binds_fresh_owner():
    ownership = Ownership()
    first = ownership.activate()
    entered, finish_parking = asyncio.Event(), asyncio.Event()
    parked = []

    async def park(*, activation):
        if activation is None:
            return  # Initial idle has no completed turn to relinquish.
        ownership.release(activation)
        parked.append(activation)
        entered.set()
        await finish_parking.wait()

    async with manager(ownership, park) as live:
        await live.runtime.submit(Input("user", "first", id="one", activation=first))
        await live.provider.request()
        await live.provider.reply("first answer")
        await asyncio.wait_for(entered.wait(), 3)
        second = ownership.activate()
        await live.runtime.submit(Input("user", "second", id="two", activation=second))
        # A turn must not cross the host's pending relinquishment barrier.
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(live.provider.requests.get(), 0.03)
        finish_parking.set()
        request = await live.provider.request()
        assert "second" in str(request.messages)
        await live.provider.reply("second answer")
        await live.runtime.wait_for(lambda event: event["type"] == "generation.finished"
                                   and event["input_ids"] == ["two"], 3)
        assert [token for token, status in live.checkpoints] == [first, second]
        assert parked == [first, second]
        await live.runtime.submit(Input("stop"))
        await asyncio.wait_for(live.task, 3)
        # Stopping a parked manager does not reacquire or save stale context.
        assert [token for token, status in live.checkpoints] == [first, second]


@pytest.mark.asyncio
@pytest.mark.parametrize("kind,payload", [
    ("child_report", {"report": "late report"}),
    ("child_event", {"event": "session.closed"}),
    ("bundle_turn", ("late completion", None)),
])
async def test_callback_after_release_cannot_start_work_or_checkpoint(kind, payload):
    ownership = Ownership()
    first = ownership.activate()
    parked = asyncio.Event()

    async def park(*, activation):
        if activation is not None:
            ownership.release(activation)
            parked.set()

    async with manager(ownership, park) as live:
        await live.runtime.submit(Input("user", "first", activation=first))
        await live.provider.request()
        await live.provider.reply("complete")
        await asyncio.wait_for(parked.wait(), 3)
        assert live.checkpoints == [(first, None)]
        # This producer still has the retired token in its task context.
        await live.runtime.inbox.put((kind, payload))
        with pytest.raises(RuntimeError, match="released or superseded"):
            await asyncio.wait_for(live.task, 3)
        assert live.provider.requests.empty()
        assert live.checkpoints == [(first, None)]
        assert live.runtime.closed
        assert not any(event["type"] == "child.updated" for event in live.runtime.events)
        assert len([event for event in live.runtime.events
                    if event["type"] == "generation.started"]) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("kind,payload", [
    ("child_report", {"report": "child result"}),
    ("child_event", {"event": "session.closed"}),
])
async def test_admitted_child_followup_inherits_ownership(kind, payload):
    ownership = Ownership()
    first = ownership.activate()

    async def park(*, activation):
        pass  # Hosts may retain ownership at idle.

    async with manager(ownership, park) as live:
        await live.runtime.inbox.put((kind, payload))
        request = await live.provider.request()
        assert "External observation" in str(request.messages)
        await live.provider.reply("child acknowledged")
        await live.runtime.wait_for(lambda event: event["type"] == "generation.finished", 3)
        assert live.checkpoints == [(first, None)]
        # Opaque host authority is never included in public event data.
        assert all("activation" not in event for event in live.runtime.events)


@pytest.mark.asyncio
async def test_background_delegate_followup_keeps_its_producing_owner():
    ownership = Ownership()
    first = ownership.activate()

    async def park(*, activation):
        pass

    async with manager(ownership, park) as live:
        await live.runtime.submit(Input("user", "delegate this", activation=first))
        await live.provider.request()
        await live.provider.reply(calls=[ToolCall(id="work", name="delegate", arguments={})])
        await live.provider.request()
        await live.provider.reply("worker pending")
        await live.runtime.wait_for(lambda event: event["type"] == "generation.finished", 3)
        live.tool.release.set()
        request = await live.provider.request()
        assert "CHILD-RESULT" in str(request.messages)
        await live.provider.reply("worker reported")
        await live.runtime.wait_for(lambda event: event["type"] == "generation.finished"
                                   and event["text"] == "worker reported", 3)
        assert live.tool.calls == 1
        assert live.checkpoints == [(first, None), (first, None)]
