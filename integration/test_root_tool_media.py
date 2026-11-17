"""Real Core/F1/root LIVE composition; offline transport, not Unified or vendor proof.

Run explicitly with the exact Foundation media candidate on PYTHONPATH.
No CLI adapter, user history, network inference or child-session path is used.
"""

import asyncio
import base64
import copy
import hashlib
import inspect
import io
import json
import sys
from contextlib import contextmanager

import pytest
from PIL import Image
from amplifier_core import AmplifierSession, HookResult, ToolResult
from amplifier_core.message_models import ChatResponse, ImageBlock, TextBlock, ToolCall
from amplifier_foundation.session.checkpoint import acknowledge_legacy_checkpoint, verified_checkpoint_refs
from amplifier_foundation.session.media import MediaPolicy, SessionMediaStore, bind_session_media
from amplifier_foundation.session.media_hook_bridge import install_media_hook_bridge, participating_module_ids
from amplifier_foundation.session.media_provider import MediaPreparedProvider
from amplifier_module_loop_live.host import HostAdapter
from amplifier_module_loop_live.runtime import Input, Runtime

MEDIA = "experimental.media.v1"
pytestmark = pytest.mark.asyncio


class ReadImage:
    name, description, input_schema = "read_file", "offline authorized image fixture", {"type": "object"}

    def __init__(self, path, mime):
        self.path, self.mime, self.calls = path, mime, 0

    async def execute(self, arguments):
        self.calls += 1
        assert arguments == {"file_path": str(self.path)}
        return ToolResult(success=True, output="caption", content=[{
            "type": "image", "source": {
                "type": "base64", "media_type": self.mime,
                "data": base64.b64encode(self.path.read_bytes()).decode(),
            },
        }])


class Driver:
    name, config = "fixture", {}

    def __init__(self, cap, tool, *, restored):
        self.cap, self.tool, self.restored = cap, tool, restored
        self.counted, self.sent, self.views = [], [], []

    def get_info(self):
        from amplifier_core import ProviderInfo
        return ProviderInfo(id="fixture", display_name="Offline",
                            defaults={"model": "fixture"}, credential_env_vars=[])

    def get_media_request_support(self, **kwargs):
        return {"v": 1, "positions": ["tool"]}

    def parse_tool_calls(self, response):
        return response.tool_calls or []

    def request_budget(self, request, *, context_estimate=None, request_options=None):
        self.cap.validate(request, purpose="count", request_options=request_options)
        self.counted.append(request)
        self.counted_view = copy.deepcopy(request.model_dump())
        return {"estimated_input_tokens": 10, "input_limit_tokens": 10000,
                "context_token_budget": 9000, "measurement": {
                    "kind": "provider_count", "source": "offline.fixture", "input_tokens": 10}}

    async def complete(self, request, **kwargs):
        self.cap.validate(request, purpose="transport", request_options=kwargs)
        assert request is self.counted[-1]
        assert request.model_dump() == self.counted_view
        self.sent.append(request)
        self.views.append(copy.deepcopy(request.model_dump()))
        if len(self.sent) == 1 and not self.restored:
            return ChatResponse(content=[], tool_calls=[ToolCall(
                id="image-call", name="read_file", arguments={"file_path": str(self.tool.path)})])
        return ChatResponse(content=[TextBlock(text="done")])


class Host(HostAdapter):
    def __init__(self, graph, bridge, provider, raw):
        self.graph, self.bridge, self.provider, self.raw = graph, bridge, provider, raw
        self.receipt = None

    @contextmanager
    def execution_registration(self, coordinator, hooks):
        view = self.graph.participant("host.execution")
        yield view, view.hooks
        self.receipt = self.graph.seal()
        assert self.bridge.validate_receipt(self.receipt)

    def prepare_provider(self, provider, coordinator):
        assert provider is self.raw
        return self.provider


def images(view):
    return [block for row in view["messages"] if isinstance(row["content"], list)
            for block in row["content"] if block["type"] == "image"]


async def run_root(tmp_path, tool, *, live, action, restored=None):
    config = {"session": {
        "orchestrator": {"module": "loop-live", "config": {
            "min_delay_between_calls_ms": 0, "background_delegate": True}},
        "context": {"module": "context-simple"}}, "providers": []}
    session = AmplifierSession(config, session_id="root-image-fixture")
    owner = session.coordinator
    bridge = install_media_hook_bridge(owner)
    graph = bridge.begin_participating_graph(
        session, owner="offline.root.live.fixture",
        participants=(*participating_module_ids(session), "host.setup", "host.execution"))
    store = SessionMediaStore(tmp_path / "media", session.session_id,
                              policy=MediaPolicy(1024 * 1024, 10000, 8),
                              checkpoint_verifier=verified_checkpoint_refs)
    cap = bind_session_media(owner, store=store, enabled=True)
    task = None
    prepared = None
    native_count = {"post": 0, "process": 0}
    previous = sys.getprofile()
    try:
        await session.initialize()
        view = graph.participant("host.setup")
        await view.mount("tools", tool, name="read_file")
        context = owner.get("context")
        checkpoint_dir = tmp_path / "session"
        checkpoint_dir.mkdir(exist_ok=True)

        async def checkpoint():
            rows = await context.get_messages()
            (checkpoint_dir / "transcript.jsonl").write_text(
                "".join(json.dumps(row) + "\n" for row in rows))
            receipt = acknowledge_legacy_checkpoint(
                checkpoint_dir, session_id=session.session_id, expected_messages=rows)
            return store.acknowledge_checkpoint(receipt)
        owner.register_capability("session.durable_checkpoint", checkpoint)
        if restored is not None:
            await context.set_messages(copy.deepcopy(restored))
            await checkpoint()

        async def policy(event, data):
            native_count["post"] += 1
            assert "base64" not in json.dumps(data)
            if action in {"null", "null-inject"}:
                return HookResult(action="modify", data={**data, "result": None})
            if action == "redact":
                return HookResult(action="modify", data={**data, "result": "redacted"})
            if action == "filter":
                data["result"]["output"].pop(MEDIA)
            return HookResult()

        async def inject(event, data):
            if action == "null-inject":
                assert data["result"] is None
                return HookResult(action="inject_context", ephemeral=True, context_injection="reminder")
            return HookResult()
        view.hooks.register("tool:post", policy, priority=10, name="actual-policy")
        view.hooks.register("tool:post", inject, priority=20, name="actual-inject")
        driver = Driver(cap, tool, restored=restored is not None)
        await view.mount("providers", driver, name="fixture")
        prepared = MediaPreparedProvider(
            driver, cap, provider_identity={"provider": "fixture", "account": "offline", "generation": 1},
            model="fixture", execution_scope="root", request_scope=bridge.request_scope)
        host = Host(graph, bridge, prepared, driver)
        owner.register_capability("live.host", host)
        runtime = Runtime(session_id=session.session_id)
        if live:
            owner.register_capability("live.runtime", runtime)
        loop = owner.get("orchestrator")
        native_process = owner.process_hook_result
        assert inspect.isbuiltin(native_process)
        def profile(frame, event, callee):
            if (event == "c_call" and callee == native_process
                    and frame.f_locals.get("event") == "tool:post"):
                native_count["process"] += 1
            if previous is not None:
                previous(frame, event, callee)
        sys.setprofile(profile)
        task = asyncio.create_task(session.execute("" if live else "read the fixture"))
        if live:
            await runtime.wait_for(lambda event: event["type"] == "session.ready", 3)
            assert host.receipt is not None and bridge.validate_receipt(host.receipt)
            await runtime.submit(Input("user", "read fixture", id="initial"))
            await runtime.wait_for(lambda event: event["type"] == "generation.finished", 3)
            # Warm same-owner turn must use saved refs without rereading.
            await runtime.submit(Input("user", "inspect saved fixture", id="warm"))
            await runtime.wait_for(lambda event: event["type"] == "generation.finished"
                                   and "warm" in event["input_ids"], 3)
            assert bridge.validate_receipt(host.receipt)
            await runtime.submit(Input("stop"))
        await asyncio.wait_for(task, 3)
        if not live:
            assert bridge.validate_receipt(host.receipt)
        else:
            # Normal live shutdown unregisters its observed callbacks; the
            # old construction receipt is no longer authority for new work.
            assert not bridge.validate_receipt(host.receipt)
        rows = await context.get_messages()
        saved = [json.loads(line) for line in (checkpoint_dir / "transcript.jsonl").read_text().splitlines()]
        toolrow = next(row for row in saved if row.get("tool_call_id") == "image-call")
        pair = next(row for row in saved if row.get("tool_calls"))
        assert pair["tool_calls"][0]["id"] == toolrow["tool_call_id"]
        assert native_count == ({"post": 0, "process": 0} if restored is not None else {"post": 1, "process": 1})
        expected = [hashlib.sha256(tool.path.read_bytes()).hexdigest()] if action == "retain" else []
        sent_images = images(driver.views[-1])
        assert [hashlib.sha256(base64.b64decode(block["source"]["data"])).hexdigest()
                for block in sent_images] == expected
        assert bool(toolrow.get("metadata", {}).get(MEDIA)) is (action == "retain")
        if action == "null" or action == "null-inject":
            assert toolrow["content"] == "null"
        encoded = base64.b64encode(tool.path.read_bytes()).decode()
        assert encoded not in json.dumps(rows) + json.dumps(saved)
        assert all(request.messages == [] for request in driver.sent)  # released projections
        return rows
    finally:
        sys.setprofile(previous)
        if task is not None and not task.done():
            task.cancel()
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)
        if prepared is not None:
            prepared.close()
        await session.cleanup()
        store.close()


@pytest.mark.parametrize("mime", ["image/png", "image/jpeg"])
@pytest.mark.parametrize("live", [False, True])
@pytest.mark.parametrize("action", ["retain", "null", "null-inject", "redact", "filter"])
async def test_actual_root_post_witness_checkpoint_private_count_send_and_reload(tmp_path, mime, live, action):
    buffer = io.BytesIO()
    Image.new("RGB", (3, 2), "orange").save(
        buffer, format="PNG" if mime == "image/png" else "JPEG")
    path = tmp_path / "image"
    path.write_bytes(buffer.getvalue())
    tool = ReadImage(path, mime)
    rows = await run_root(tmp_path, tool, live=live, action=action)
    assert tool.calls == 1
    await run_root(tmp_path, tool, live=live, action=action, restored=rows)
    assert tool.calls == 1