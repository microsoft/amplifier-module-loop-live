"""Provider-owned native frames retain the core loop's durable call identity."""

import asyncio
import json
from pathlib import Path
import sys
import socket
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

pytest.importorskip("amplifier_module_provider_openai.native")
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "adapters/cli"))
from amplifier_core import AmplifierSession, ToolResult
from amplifier_core.message_models import (
    ChatRequest,
    ChatResponse,
    Message,
    TextBlock,
    ToolSpec,
)
from amplifier_loop_live_cli.bundle_astra import BundleAstraProvider, SafeOpenAIProvider
from amplifier_loop_live_cli.selection import select_root
from amplifier_module_loop_live.job_store import JobStore
from amplifier_module_loop_live.runtime import Runtime
from amplifier_module_loop_live.scope import LIVE_OWNER, NATIVE_REQUEST as LOOP_REQUEST
from amplifier_module_provider_openai import OpenAIProvider
from amplifier_module_provider_openai.native import NATIVE_REQUEST as PROVIDER_REQUEST


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    attempts = []

    def reject(*args, **kwargs):
        attempts.append(True)
        raise AssertionError("Native attribution fixtures cannot use the network")

    monkeypatch.setattr(socket.socket, "connect", reject)
    monkeypatch.setattr(socket.socket, "connect_ex", reject)
    yield
    assert not attempts


class Socket:
    def __init__(self, events):
        self.events = list(events)
        self.sent = []
        self.closed = False

    async def send(self, value):
        self.sent.append(json.loads(value))

    async def recv(self):
        event = self.events.pop(0)
        if isinstance(event, BaseException):
            raise event
        return json.dumps(event)

    async def close(self):
        self.closed = True


@pytest.mark.asyncio
async def test_real_native_dispatch_retains_original_wire_call_through_job_recovery(
    tmp_path, monkeypatch
):
    session = AmplifierSession(
        {
            "session": {
                "orchestrator": {
                    "module": "loop-live",
                    "config": {"background_delegate": True},
                },
                "context": {"module": "context-simple"},
            },
            "providers": [],
        }
    )
    await session.initialize()
    loop = session.coordinator.get("orchestrator")
    loop.runtime = Runtime()
    loop.coordinator = session.coordinator
    loop.context = session.coordinator.get("context")
    loop.hooks = session.coordinator.hooks
    await loop.context.add_message({"role": "user", "content": "synthetic fixture"})
    ledger = JobStore(tmp_path / "jobs")
    session.coordinator.register_capability("live.jobs", ledger)
    provider = BundleAstraProvider.wrap(
        OpenAIProvider(api_key="fixture", config={"default_model": "gpt-6-astra"})
    )
    provider._guard_assembled_params_with_provider_count = AsyncMock(return_value=None)
    wire = {
        "id": "fc_fixture",
        "type": "function_call",
        "call_id": "call_fixture",
        "name": "delegate",
        "arguments": "{}",
        "async": True,
    }
    provider.socket = Socket(
        [
            {"type": "response.created", "response": {"id": "response_fixture"}},
            {"type": "response.output_item.done", "item": wire},
            {
                "type": "response.completed",
                "response": {
                    "id": "response_fixture",
                    "model": "gpt-6-astra",
                    "status": "completed",
                    "output": [wire],
                    "usage": {"input_tokens": 1, "output_tokens": 1},
                },
            },
        ]
    )
    original_complete = OpenAIProvider.complete
    utility_scopes = []

    async def ordinary_complete(self, request, **kwargs):
        if (request.metadata or {}).get("stream") is False and request.metadata.get(
            "fixture_utility"
        ):
            utility_scopes.append((LOOP_REQUEST.get(), PROVIDER_REQUEST.get()))
            return ChatResponse(content=[TextBlock(text="utility")])
        return await original_complete(self, request, **kwargs)

    monkeypatch.setattr(OpenAIProvider, "complete", ordinary_complete)

    class Tool:
        name = "delegate"
        description = "Synthetic fixture"
        input_schema = {"type": "object", "properties": {}}
        calls = 0

        async def execute(self, input):
            self.calls += 1
            assert LOOP_REQUEST.get() is provider and PROVIDER_REQUEST.get() is provider
            await provider.complete(
                ChatRequest(
                    messages=[Message(role="user", content="utility")],
                    metadata={"stream": False, "fixture_utility": True},
                )
            )
            assert LOOP_REQUEST.get() is provider and PROVIDER_REQUEST.get() is provider
            return ToolResult(success=True, output="saved-result")

    tool = Tool()
    await session.coordinator.mount("tools", tool, name=tool.name)
    loop.tools = session.coordinator.get("tools")
    previous = object()
    token = LOOP_REQUEST.set(previous)
    owner_token = LIVE_OWNER.set(loop)
    try:
        await provider.complete(
            ChatRequest(
                messages=[Message(role="user", content="synthetic fixture")],
                tools=[
                    ToolSpec(
                        name="delegate",
                        description="fixture",
                        parameters=tool.input_schema,
                    )
                ],
            )
        )
        await asyncio.gather(*(job["task"] for job in loop.jobs.values()))
        assert LOOP_REQUEST.get() is previous and PROVIDER_REQUEST.get() is None
        assert tool.calls == 1 and utility_scopes == [(None, None)]
        assert ledger.rows[wire["call_id"]]["native_call"] == wire
        assert ledger.rows[wire["call_id"]]["status"] == "returned"
        ledger.close()
        recovered_store = JobStore(tmp_path / "jobs")
        try:
            messages, recovered = recovered_store.recover([])
            assert messages[0]["metadata"]["converge_native_async_calls"] == [wire]
            assert recovered[0]["call_id"] == wire["call_id"]
            assert tool.calls == 1
        finally:
            recovered_store.close()
        assert len(provider.socket.sent) == 1
    finally:
        LIVE_OWNER.reset(owner_token)
        LOOP_REQUEST.reset(token)
        ledger.close()
        await provider.close_live()
        await session.cleanup()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["no_owner", "utility", "ordinary_model", "cancel"])
async def test_provider_scopes_reset_for_ordinary_requests_and_native_cancellation(
    kind, monkeypatch
):
    provider = BundleAstraProvider.wrap(
        OpenAIProvider(api_key="fixture", config={"default_model": "gpt-6-astra"})
    )
    owner = SimpleNamespace(
        context=SimpleNamespace(get_messages=AsyncMock(return_value=[])),
        runtime=SimpleNamespace(emit=AsyncMock()),
        config={},
    )
    original_complete = OpenAIProvider.complete
    scopes = []

    async def ordinary_complete(self, request, **kwargs):
        if kind != "cancel":
            scopes.append((LOOP_REQUEST.get(), PROVIDER_REQUEST.get()))
            return ChatResponse(content=[TextBlock(text="ordinary")])
        return await original_complete(self, request, **kwargs)

    monkeypatch.setattr(OpenAIProvider, "complete", ordinary_complete)
    provider._guard_assembled_params_with_provider_count = AsyncMock(return_value=None)
    provider.socket = Socket([asyncio.CancelledError()])
    previous_loop, previous_provider = object(), object()
    loop_token = LOOP_REQUEST.set(previous_loop)
    provider_token = PROVIDER_REQUEST.set(previous_provider)
    owner_token = LIVE_OWNER.set(None if kind == "no_owner" else owner)
    try:
        request = ChatRequest(
            messages=[Message(role="user", content="fixture")],
            metadata={"stream": False} if kind == "utility" else None,
        )
        kwargs = {"model": "gpt-5.6-terra"} if kind == "ordinary_model" else {}
        if kind == "cancel":
            with pytest.raises(asyncio.CancelledError):
                await provider.complete(request, **kwargs)
            assert provider.request_uncertain and provider.owner is None
        else:
            await provider.complete(request, **kwargs)
            assert scopes == [(None, None)]
        assert LOOP_REQUEST.get() is previous_loop
        assert PROVIDER_REQUEST.get() is previous_provider
    finally:
        LIVE_OWNER.reset(owner_token)
        PROVIDER_REQUEST.reset(provider_token)
        LOOP_REQUEST.reset(loop_token)
        await provider.close_live()


@pytest.mark.asyncio
async def test_selection_preserves_existing_instance_wrapper_and_ordinary_cleanup():
    original = OpenAIProvider(
        api_key="fixture", config={"default_model": "gpt-6-astra"}
    )
    wrapped_complete = AsyncMock(return_value="wrapped")
    original.complete = wrapped_complete
    mounted = SafeOpenAIProvider.wrap(original)
    loop, cleanups = SimpleNamespace(), []
    coordinator = SimpleNamespace(
        get=lambda name: {"fixture": mounted} if name == "providers" else loop,
        get_capability=lambda name: None,
        register_cleanup=cleanups.append,
    )
    await select_root(
        SimpleNamespace(coordinator=coordinator),
        {"providers": [{"id": "fixture", "module": "provider-openai"}]},
        {"instance": "fixture", "model": "gpt-6-astra", "effort": "low"},
    )
    assert not loop.root_provider.native_bundle_live
    assert loop.root_provider.provider.complete is wrapped_complete
    assert await loop.root_provider.provider.complete(None) == "wrapped"
    assert len(cleanups) == 1 and cleanups[0].__name__ == "close"
    await cleanups[0]()
