"""Real Core sessions suspend/release/remount without replaying user input."""

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from amplifier_core import AmplifierSession, ToolResult
from amplifier_core.message_models import ChatResponse, TextBlock, ToolCall

from amplifier_module_loop_live.runtime import Input, Runtime


class Provider:
    name = "fixture"
    config = {}

    def __init__(self):
        self.requests, self.responses = asyncio.Queue(), asyncio.Queue()

    def get_info(self):
        return SimpleNamespace(default_model="fixture")

    async def complete(self, request, **kwargs):
        await self.requests.put(request)
        return await self.responses.get()

    def parse_tool_calls(self, response):
        return response.tool_calls or []

    async def request(self):
        return await asyncio.wait_for(self.requests.get(), 3)

    async def reply(self, text="", calls=None):
        await self.responses.put(ChatResponse(content=[TextBlock(text=text)], tool_calls=calls))


class Effect:
    name = "effect"
    description = "A counted deterministic effect"
    input_schema = {"type": "object", "properties": {}}

    def __init__(self):
        self.count = 0

    async def execute(self, arguments):
        self.count += 1
        return ToolResult(success=True, output="effect completed")


class Owner:
    def __init__(self, root, runtime, context):
        self.root, self.runtime, self.context = Path(root), runtime, context
        self.root.mkdir(exist_ok=True)

    def requested(self):
        return self.runtime.pause_epoch is not None

    async def checkpoint(self, frame):
        (self.root / "boundary.json").write_text(json.dumps({
            "continuation": frame, "messages": await self.context.get_messages()
        }))
        return True

    async def live_checkpoint(self, envelope):
        (self.root / "live.json").write_text(json.dumps(envelope))
        return True

    async def claim(self, frame):
        claimed = self.root / ("claimed-" + frame["id"])
        if claimed.exists() or self.requested() or self.runtime.stop_requested:
            return False
        claimed.write_text("consumed")
        return True

    async def admit(self, epoch):
        return self.runtime.pause_epoch == epoch and not self.runtime.stop_requested


async def start(root, provider, effect, *, restored=False):
    runtime = Runtime(session_id="maintenance-root")
    session = AmplifierSession({"session": {
        "orchestrator": {"module": "loop-live", "config": {"min_delay_between_calls_ms": 0}},
        "context": {"module": "context-simple"}}, "providers": []},
        session_id=runtime.session_id)
    await session.initialize()
    coordinator = session.coordinator
    await coordinator.mount("providers", provider, name="fixture")
    await coordinator.mount("tools", effect, name="effect")
    context = coordinator.get("context")
    owner = Owner(root, runtime, context)
    coordinator.register_capability("live.runtime", runtime)
    coordinator.register_capability("execution.suspension", owner)
    coordinator.register_capability("execution.resume", owner.claim)
    coordinator.register_capability("live.suspend", owner.live_checkpoint)
    coordinator.register_capability("live.maintenance_admit", owner.admit)
    if restored:
        boundary = json.loads((Path(root) / "boundary.json").read_text())
        await context.set_messages(boundary["messages"])
        coordinator.register_capability("live.suspended_execution",
                                        json.loads((Path(root) / "live.json").read_text()))
    task = asyncio.create_task(session.execute(""))
    await runtime.wait_for(lambda e: e["type"] == "session.ready", 3)
    return session, runtime, task


@pytest.mark.asyncio
async def test_pause_release_fresh_core_remount_explicit_resume_same_generation(tmp_path):
    provider, effect = Provider(), Effect()
    session, runtime, task = await start(tmp_path, provider, effect)
    try:
        await runtime.submit(Input("user", "original request", id="original-input"))
        await provider.request()
        generation = runtime.generation["id"]
        await runtime.request_pause("maintenance-epoch")
        await provider.reply(calls=[ToolCall(id="effect-call", name="effect", arguments={})])
        await runtime.wait_for(lambda e: e["type"] == "session.suspended", 3)
        assert effect.count == 0 and provider.requests.empty()
        assert runtime.generation["id"] == generation
        with pytest.raises(RuntimeError, match="held"):
            await runtime.submit(Input("user", "old browser new send"))
        await runtime.release_paused("maintenance-epoch")
        await asyncio.wait_for(task, 3)
        assert runtime.events[-1]["type"] == "session.closed"
        assert runtime.events[-1]["status"] == "paused"
        assert not any(e["type"] in {"generation.finished", "generation.failed", "generation.detached"}
                       for e in runtime.events)
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await session.cleanup()

    session, runtime, task = await start(tmp_path, provider, effect, restored=True)
    try:
        await runtime.wait_for(lambda e: e["type"] == "session.suspended", 3)
        assert effect.count == 0 and provider.requests.empty()
        await runtime.resume_paused("maintenance-epoch")
        request = await provider.request()
        assert effect.count == 1
        assert str(request.messages).count("original request") == 1
        await provider.reply("finished")
        finished = await runtime.wait_for(lambda e: e["type"] == "generation.finished", 3)
        assert finished["generation_id"] == generation
        assert finished["input_ids"] == ["original-input"]
        assert not any(e["type"] == "generation.started" for e in runtime.events)
        with pytest.raises(RuntimeError, match="changed"):
            await runtime.resume_paused("maintenance-epoch")
        assert effect.count == 1
    finally:
        await runtime.submit(Input("stop"))
        await asyncio.wait_for(task, 3)
        await session.cleanup()
