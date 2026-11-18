"""First image after real loop initialization, without fixture readiness reports."""

import asyncio
import base64
import hashlib
import io
import json
from contextlib import contextmanager

import pytest
from PIL import Image
from amplifier_core import AmplifierSession, HookResult
from amplifier_foundation.session.checkpoint import acknowledge_legacy_checkpoint, verified_checkpoint_refs
from amplifier_foundation.session.media import MediaPolicy, SessionMediaStore, bind_session_media
from amplifier_foundation.session.media_hook_bridge import install_media_hook_bridge, participating_module_ids
from amplifier_foundation.session.media_provider import MediaPreparedProvider
from amplifier_module_tool_filesystem.read import ReadTool
from amplifier_module_loop_live.host import HostAdapter
from amplifier_module_loop_live.runtime import Input, Runtime
from amplifier_module_loop_streaming.media import report_consumer_ready

from test_root_tool_media import Driver, images

pytestmark = pytest.mark.asyncio
MEDIA = "experimental.media.v1"


class CountedRead(ReadTool):
    """Count calls without changing the actual descriptor-bound implementation."""
    def __init__(self, root, path, mime, coordinator):
        super().__init__({"working_dir": str(root), "allowed_read_paths": [str(root)]}, coordinator)
        self.path, self.mime, self.calls = path, mime, 0

    async def execute(self, arguments):
        self.calls += 1
        return await super().execute(arguments)


class StartupHost(HostAdapter):
    def __init__(self, graph, bridge, cap, raw, prepared):
        self.graph, self.bridge, self.cap = graph, bridge, cap
        self.raw, self.prepared, self.receipt = raw, prepared, None

    def seal(self, coordinator):
        self.receipt = self.graph.seal()
        rows = self.cap.report("test-host", event="readiness", outcome="inspect")["consumers"]
        # This is the host's independent admission, not a loop-ready injection.
        ready = any(row.get("ready") is True and row.get("role") == "loop"
                    and type(row.get("contract")) is int and row["contract"] == 1
                    for row in rows.values())
        self.cap.enabled = bool(ready and self.bridge.validate_receipt(self.receipt)
                                and callable(coordinator.get_capability("session.durable_checkpoint")))
        assert self.cap.enabled, rows

    @contextmanager
    def execution_registration(self, coordinator, hooks):
        view = self.graph.participant("host.execution")
        yield view, view.hooks
        self.seal(coordinator)

    def prepare_provider(self, provider, coordinator):
        assert provider is self.raw
        return self.prepared


@pytest.mark.parametrize("loop_mode", ["streaming", "live-runtime", "live-finite"])
@pytest.mark.parametrize("mime", ["image/png", "image/jpeg"])
@pytest.mark.parametrize("late_checkpoint", [False, True])
async def test_first_descriptor_image_after_consumer_owned_startup(
    tmp_path, loop_mode, mime, late_checkpoint,
):
    loop_name = "loop-streaming" if loop_mode == "streaming" else "loop-live"
    buffer = io.BytesIO()
    Image.new("RGB", (3, 2), "purple").save(
        buffer, format="PNG" if mime == "image/png" else "JPEG")
    path = tmp_path / "image"
    path.write_bytes(buffer.getvalue())
    session = AmplifierSession({"session": {
        "orchestrator": {"module": loop_name, "config": {"min_delay_between_calls_ms": 0}},
        "context": {"module": "context-simple"}}, "providers": []},
        session_id="startup-root")
    owner = session.coordinator
    bridge = install_media_hook_bridge(owner)
    graph = bridge.begin_participating_graph(
        session, owner="offline.startup.fixture",
        participants=(*participating_module_ids(session), "host.setup",
                      *(("host.execution",) if loop_name == "loop-live" else ())))
    store = SessionMediaStore(tmp_path / "media", session.session_id,
                              policy=MediaPolicy(1024 * 1024, 10000, 8),
                              checkpoint_verifier=verified_checkpoint_refs)
    cap = bind_session_media(owner, store=store, enabled=False)
    directory = tmp_path / "session"
    directory.mkdir()

    async def checkpoint():
        rows = await owner.get("context").get_messages()
        (directory / "transcript.jsonl").write_text("".join(json.dumps(row) + "\n" for row in rows))
        receipt = acknowledge_legacy_checkpoint(
            directory, session_id=session.session_id, expected_messages=rows)
        return store.acknowledge_checkpoint(receipt)
    if not late_checkpoint:
        owner.register_capability("session.durable_checkpoint", checkpoint)
    prepared, task = None, None
    trace = []
    try:
        await session.initialize()
        assert cap.enabled is False
        ready = cap.report("test-host", event="readiness", outcome="inspect")["consumers"]
        assert ready.get(loop_name, {}).get("ready") is (not late_checkpoint), ready
        assert not list((tmp_path / "media" / "assets").iterdir())
        if late_checkpoint:
            owner.register_capability("session.durable_checkpoint", checkpoint)
        view = graph.participant("host.setup")
        tool = CountedRead(tmp_path, path, mime, view)
        await view.mount("tools", tool, name="read_file")
        raw = Driver(cap, tool, restored=False)
        prepared = MediaPreparedProvider(
            raw, cap, provider_identity={"provider": "fixture", "account": "offline", "generation": 1},
            model="fixture", execution_scope="root", request_scope=bridge.request_scope)
        host = StartupHost(graph, bridge, cap, raw, prepared)
        async def post(event, data):
            trace.append(data["tool_call_id"])
            assert cap.enabled is True and bridge.validate_receipt(bridge.graph_receipt)
            assert data["result"]["output"][MEDIA]["refs"]
            return HookResult()
        view.hooks.register("tool:post", post, name="real-first-post")
        if loop_name == "loop-live":
            await view.mount("providers", raw, name="fixture")
            owner.register_capability("live.host", host)
            if loop_mode == "live-runtime":
                runtime = Runtime(session_id=session.session_id)
                owner.register_capability("live.runtime", runtime)
                task = asyncio.create_task(session.execute(""))
                await runtime.wait_for(lambda event: event["type"] == "session.ready", 3)
                await runtime.submit(Input("user", "read image", id="initial"))
                await runtime.wait_for(lambda event: event["type"] == "generation.finished", 3)
                await runtime.submit(Input("stop"))
            else:
                task = asyncio.create_task(session.execute("read image"))
        else:
            await view.mount("providers", prepared, name="fixture")
            if late_checkpoint:
                async def seal_before_prompt(event, data):
                    host.seal(owner)
                    return HookResult()
                view.hooks.register("prompt:submit", seal_before_prompt, name="host-seal")
            else:
                host.seal(owner)
            task = asyncio.create_task(session.execute("read image"))
        await asyncio.wait_for(task, 3)
        assert tool.calls == 1
        assert trace == ["image-call"]
        sent = images(raw.views[-1])
        assert [hashlib.sha256(base64.b64decode(block["source"]["data"])).hexdigest()
                for block in sent] == [hashlib.sha256(path.read_bytes()).hexdigest()]
        saved = [json.loads(line) for line in (directory / "transcript.jsonl").read_text().splitlines()]
        row = next(row for row in saved if row.get("tool_call_id") == "image-call")
        assert row["metadata"][MEDIA]["refs"][0]["mime"] == mime
        assert base64.b64encode(path.read_bytes()).decode() not in json.dumps(saved)
        assert all(request.messages == [] for request in raw.sent)
    finally:
        if task is not None and not task.done():
            task.cancel()
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)
        if prepared is not None:
            prepared.close()
        await session.cleanup()
        ready = cap.report("test-host", event="readiness", outcome="inspect")["consumers"]
        assert ready.get(loop_name, {}).get("ready") is False
        store.close()


@pytest.mark.parametrize("loop_name", ["loop-streaming", "loop-live"])
@pytest.mark.parametrize("failure", ["missing-boundary", "no-participating-graph", "replacement", "cleanup"])
async def test_incompatible_or_retired_consumer_never_reports_ready(tmp_path, loop_name, failure):
    session = AmplifierSession({"session": {
        "orchestrator": {"module": loop_name}, "context": {"module": "context-simple"}},
        "providers": []})
    owner = session.coordinator
    bridge = install_media_hook_bridge(owner)
    graph = None
    if failure != "no-participating-graph":
        graph = bridge.begin_participating_graph(
            session, owner="startup-negative", participants=participating_module_ids(session))
    store = SessionMediaStore(tmp_path / "media", session.session_id,
                              policy=MediaPolicy(1024 * 1024, 10000, 8))
    cap = bind_session_media(owner, store=store, enabled=False)
    owner.register_capability("session.durable_checkpoint", lambda: None)
    if failure == "missing-boundary":
        owner.register_capability("session.media.tool_post", None)
    cleaned = False
    try:
        await session.initialize()
        loop = owner.get("orchestrator")
        registration = loop._media_consumer
        if failure == "replacement":
            # Remount the real singleton through the actual participant API.
            await graph.participant(loop_name).mount("orchestrator", type(loop)({}))
            await registration.refresh()
        elif failure == "cleanup":
            await session.cleanup()
            cleaned = True
            # A stale completed-post observation cannot recreate ready=True.
            await report_consumer_ready(owner, post_complete=True)
            await registration.refresh()
        ready = cap.report("test-host", event="readiness", outcome="inspect")["consumers"]
        assert ready.get(loop_name, {}).get("ready") is False
        assert cap.enabled is False
        # Ordinary text reading does not need or turn on media.
        text = tmp_path / "text"
        text.write_text("ordinary text")
        result = await ReadTool({"working_dir": str(tmp_path)}, owner).execute({"file_path": str(text)})
        assert result.success and "ordinary text" in result.output["content"]
        picture = tmp_path / "picture.png"
        Image.new("RGB", (1, 1), "purple").save(picture)
        unavailable = await ReadTool({"working_dir": str(tmp_path)}, owner).execute(
            {"file_path": str(picture)})
        assert unavailable.success is False and "not enabled" in unavailable.error["message"]
        assert MEDIA not in (unavailable.output or {})
        assert not list((tmp_path / "media" / "assets").iterdir())
    finally:
        if not cleaned:
            await session.cleanup()
        store.close()