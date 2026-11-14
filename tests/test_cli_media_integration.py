"""Candidate-graph offline integration, not inference or accepted-graph proof.

Requires the CLI late factory, Foundation F1, and optional guarded bridge.
The bridge negative fixture must not be reported as forward tool-media success.
"""
import asyncio
import base64
import copy
import hashlib
import io
import json
import os
from types import SimpleNamespace

import pytest

pytest.importorskip("amplifier_foundation.session.media_provider")
pytest.importorskip("amplifier_app_cli.session_media")
Image = pytest.importorskip("PIL.Image")
from amplifier_app_cli.session_media import install_session_media, mount_session_media
from amplifier_app_cli.session_store import SessionStore
from amplifier_app_cli.session_checkpoint import install_session_checkpoint
from amplifier_core import AmplifierSession, HookResult, ToolResult
from amplifier_core.message_models import ChatRequest, ImageBlock, Message, ToolCall
from amplifier_loop_live_cli.host_adapter import CLIHostAdapter
from amplifier_module_loop_live.job_store import JobStore
from amplifier_module_loop_live.runtime import Runtime
from test_cli_media_provider import Provider, no_network  # noqa: F401

pytestmark = pytest.mark.skipif(os.name != "posix", reason="Candidate durable media store is POSIX-only")
POLICY = {"enabled": True, "max_bytes": 1024 * 1024, "max_pixels": 10000, "max_images": 10}


def pixels():
    stream = io.BytesIO()
    Image.new("RGB", (3, 2), "orange").save(stream, format="PNG")
    return stream.getvalue()


class Owner:
    def __init__(self, rows):
        self.providers, self.caps, self.cleanups, self.config = {}, {}, [], {}
        self.session_id = "root"
        async def messages():
            return rows
        self.context = SimpleNamespace(get_messages=messages)
    def get(self, kind):
        return self.providers if kind == "providers" else self.context if kind == "context" else {}
    def get_capability(self, name):
        return self.caps.get(name)
    def register_capability(self, name, value):
        self.caps[name] = value
    def register_cleanup(self, callback):
        self.cleanups.append(callback)
    async def mount(self, kind, value, *, name):
        self.providers[name] = value
    async def cleanup(self):
        for cleanup in reversed(self.cleanups):
            await cleanup()


@pytest.mark.parametrize("enabled", [False, True])
@pytest.mark.parametrize("support", [False, True])
async def test_actual_cli_late_factory_uses_f1_private_projection_and_checkpoint(tmp_path, enabled, support):
    # This is an already-published upload/provider boundary, not a fabricated
    # tool-post acceptance. The guarded tool bridge is tested separately below.
    rows, observed = [], []
    owner = Owner(rows)
    config = {"session": {"orchestrator": "loop-live"}}
    if enabled:
        config["session_media"] = POLICY
    session = SimpleNamespace(session_id="root", coordinator=owner, config=config)
    native = SessionStore(tmp_path / "sessions")
    install_session_checkpoint(session, native, session_id="root")
    host = install_session_media(session, native)
    class Selected(Provider):
        def get_media_request_support(self, **kwargs):
            return {"v": 1, "positions": ["user"] if support else []}
        def observe(self, request, purpose):
            host.capability.validate(request, purpose=purpose, request_options={})
            images = [hashlib.sha256(base64.b64decode(block.source["data"])).hexdigest()
                      for message in request.messages
                      for block in message.content if isinstance(message.content, list)
                      if isinstance(block, ImageBlock)]
            observed.append((purpose, id(request), images, copy.deepcopy(request.model_dump())))
        async def request_budget(self, request, **kwargs):
            self.observe(request, "count")
            return {"estimated_input_tokens": 10, "input_limit_tokens": 100, "context_token_budget": 100,
                    "measurement": {"kind": "provider_count", "source": "offline fixture", "input_tokens": 10}}
        async def complete(self, request, **kwargs):
            self.observe(request, "transport")
            return "done"
    driver = Selected()
    owner.providers["selected-account"] = driver
    try:
        if enabled:
            origin = {"kind": "upload", "session_id": "root", "id": "upload"}
            ref = (await host.capability.ingest(pixels(), source_format="image-bytes", origin=origin, policy=None))["value"]
            await host.capability.publish([ref], origin=origin, disposition="retain")
            rows.append({"role": "user", "content": "Observation", "metadata": {"experimental.media.v1": {"refs": [ref]}}})
            await owner.get_capability("session.durable_checkpoint")()
        else:
            # Neutral policy accepts text but never ingests or creates storage.
            rows.append({"role": "user", "content": "ordinary text"})
        await mount_session_media(session)
        assert owner.providers["selected-account"] is driver  # Live intentionally delayed.
        adapter = CLIHostAdapter()
        provider = adapter.prepare_provider(driver, owner)
        assert adapter.prepare_provider(provider, owner) is provider
        assert provider is not driver and provider.__wrapped__ is driver
        assert not hasattr(provider, "stream") and not hasattr(provider, "refit_request")
        request = ChatRequest(messages=[Message(**row) for row in rows])
        before = request.model_dump()
        await provider.request_budget(request)
        assert await provider.complete(request) == "done"
        count, send = observed
        assert count[1] == send[1]
        assert count[2] == send[2] == ([ref["sha256"]] if enabled and support else [])
        if enabled and not support:
            assert "Image unavailable" in str(count[3])
        assert request.model_dump() == before
        if enabled:
            assert "base64" not in str(native.load("root")[0])
        else:
            assert not (native.base_dir / "root").exists()
    finally:
        await owner.cleanup()
    assert host.closed


@pytest.mark.parametrize("mode", ["keep", "null", "throw"])
async def test_actual_guarded_bridge_never_admits_live_background_media_or_replays(tmp_path, mode):
    bridge_module = pytest.importorskip("amplifier_foundation.session.media_hook_bridge")
    from amplifier_foundation.session.media import SessionMediaStore, MediaPolicy, bind_session_media
    session = AmplifierSession({"session": {"orchestrator": {"module": "loop-live"},
                                "context": {"module": "context-simple"}}, "providers": []})
    owner = session.coordinator
    bridge = bridge_module.install_media_hook_bridge(owner)
    await session.initialize()
    store = SessionMediaStore(tmp_path / "media", owner.session_id, policy=MediaPolicy(1024 * 1024, 10000, 10))
    cap = bind_session_media(owner, store=store, enabled=True)
    loop = owner.get("orchestrator")
    loop.runtime, loop.coordinator = Runtime(), owner
    loop.config.update(background_delegate=True)
    ledger = JobStore(tmp_path / "jobs")
    owner.register_capability("live.jobs", ledger)
    counts = {"tool": 0, "post": 0, "process": 0}
    original_process = owner.process_hook_result
    async def process(result, event, *args, **kwargs):
        if event == "tool:post":
            counts["process"] += 1
        return await original_process(result, event, *args, **kwargs)
    owner.process_hook_result = process
    encoded = base64.b64encode(pixels()).decode()
    async def post(event, data):
        counts["post"] += 1
        assert encoded not in json.dumps(data)
        if mode == "throw":
            raise ValueError("fixture callback failure")
        return HookResult(action="modify", data={"result": None}) if mode == "null" else HookResult()
    unregister = bridge.hooks.register("tool:post", post, name="fixture-post")
    class Tool:
        name, description, input_schema = "delegate", "offline image fixture", {"type": "object"}
        async def execute(self, arguments):
            counts["tool"] += 1
            return ToolResult(success=True, output="caption", content=[{"type": "image", "source": {
                "type": "base64", "media_type": "image/png", "data": encoded}}])
    tool = Tool()
    await owner.mount("tools", tool, name=tool.name)
    call = ToolCall(id="call", name="delegate", arguments={"async": True})
    try:
        with loop.approved_dispatch.run_scope(owner):
            await loop._execute_tool_only(call, owner.get("tools"), owner.hooks, None, owner)
            job_id, job = next(iter(loop.jobs.items()))
            await job["task"]
        assert counts == {"tool": 1, "post": 1, "process": 1}
        # Streaming's existing safety net returns an explicit error carrier;
        # "returned" means a report arrived, not that media/tool work succeeded.
        assert ledger.rows["call"]["status"] == "returned"
        record = ledger.rows["call"]["settled_result"]
        assert "Media tool post boundary failed; no media was accepted" in record["content"]
        assert record["metadata"] == {} and record["success"] is not True
        kind, args = loop.runtime.inbox.get_nowait()
        assert kind == "bundle_job"
        await loop._job_returned(*args, deliver=True)
        assert loop.pending[0].job_result["metadata"] == {}
        await loop._execute_tool_only(call, {}, owner.hooks, None, owner)
        assert counts == {"tool": 1, "post": 1, "process": 1}
        assert not bridge.readiness()["authoritative"]
        for path in (tmp_path / "jobs").glob("*.json"):
            assert encoded not in path.read_text()
        assert "experimental.media.v1" not in json.dumps(ledger.rows["call"])
    finally:
        unregister()
        ledger.close()
        store.close()
        await session.cleanup()
