"""Public cooperative-graph qualification with actual storage, never inference.

This is an explicitly named offline composition, not qualification of a CLI
entrypoint, arbitrary plugins, or a production provider's media promises.
"""
import asyncio
import base64
import copy
import hashlib
import inspect
from importlib.machinery import ModuleSpec
import io
import json
import os
import sys
from types import ModuleType, SimpleNamespace

import pytest

bridge_module = pytest.importorskip("amplifier_foundation.session.media_hook_bridge")
if not hasattr(bridge_module.MediaHookBridge, "begin_participating_graph"):
    pytest.skip("Requires the public participating-graph candidate", allow_module_level=True)
Image = pytest.importorskip("PIL.Image")
from amplifier_app_cli.session_checkpoint import install_session_checkpoint
from amplifier_app_cli.session_media import install_session_media
from amplifier_app_cli.session_store import SessionStore
from amplifier_core import AmplifierSession, HookResult, ToolResult
from amplifier_core.message_models import ChatRequest, ImageBlock, Message, ToolCall
from amplifier_loop_live_cli.host_adapter import CLIHostAdapter
from amplifier_module_loop_live.job_store import JobStore
from amplifier_module_loop_live.runtime import Runtime
from test_cli_media_provider import Provider, no_network  # noqa: F401

MEDIA = "experimental.media.v1"
POLICY = {"enabled": True, "max_bytes": 1024 * 1024, "max_pixels": 10000, "max_images": 10}
pytestmark = pytest.mark.skipif(os.name != "posix", reason="Actual media-store fixture is POSIX-only")


def image_bytes(format):
    stream = io.BytesIO()
    Image.new("RGB", (3, 2), "orange").save(stream, format=format)
    return stream.getvalue()


@pytest.mark.parametrize("format", ["PNG", "JPEG"])
@pytest.mark.parametrize("mode", ["keep", "null", "redact", "error", "continue-null-echo", "cancel"])
async def test_public_positive_background_result_checkpoint_and_private_send(tmp_path, format, mode):
    """No private observer and no dictionary masquerading as a bridge receipt."""
    config = {"session": {"orchestrator": {"module": "loop-live"},
                           "context": {"module": "context-simple"}},
              "providers": [], "session_media": POLICY}
    session = AmplifierSession(config)
    owner = session.coordinator
    bridge = bridge_module.install_media_hook_bridge(owner)
    graph = bridge.begin_participating_graph(
        session, owner="offline.live.background.cli-components",
        participants=("loop-live", "context-simple", "host.media", "host.fixtures", "host.policy"))
    native = SessionStore(tmp_path / "sessions")
    # Core's session.coordinator is read-only. This explicit host-registration
    # facade calls the real CLI component with the supplied trusted view. It is
    # deliberately not evidence that a CLI entrypoint constructs this facade.
    registration_session = SimpleNamespace(session_id=session.session_id, config=session.config,
                                           coordinator=graph.participant("host.media"))
    host = install_session_media(registration_session, native)
    install_session_checkpoint(session, native, session_id=session.session_id)
    await session.initialize()
    context, loop = owner.get("context"), owner.get("orchestrator")
    view = graph.participant("host.fixtures")
    loop.runtime, loop.coordinator = Runtime(), view
    loop.config.update(background_delegate=True)
    ledger = JobStore(tmp_path / "jobs")
    owner.register_capability("live.jobs", ledger)
    counts = {"tool": 0, "post": 0, "process": 0}
    effective_refs, observed = [], []
    pixels = image_bytes(format)
    encoded = base64.b64encode(pixels).decode()
    post_entered = asyncio.Event()

    async def post(event, data):
        counts["post"] += 1
        assert encoded not in json.dumps(data)
        if mode == "cancel":
            post_entered.set()
            await asyncio.Event().wait()
        if mode == "error":
            raise ValueError("declared fixture callback failed")
        if mode == "null":
            return HookResult(action="modify", data={"result": None})
        if mode == "continue-null-echo":
            return HookResult(action="continue", data={"result": None})
        if mode == "redact":
            return HookResult(action="modify", data={"result": {"success": True, "output": "redacted"}})
        effective_refs.extend(copy.deepcopy(data["result"]["output"][MEDIA]["refs"]))
        return HookResult()

    unregister = graph.participant("host.policy").hooks.register(
        "tool:post", post, name="actual-effective-policy")
    unregister_echo = None
    if mode == "continue-null-echo":
        # Continue.data is Core-visible, so a later normal echo must not restore
        # a staged result that the earlier participant explicitly removed.
        unregister_echo = graph.participant("host.policy").hooks.register(
            "tool:post", lambda event, data: HookResult(action="modify", data=data),
            priority=10, name="later-echo-current-effective-value")

    class Tool:
        name, description, input_schema = "delegate", "Offline exact image owner", {"type": "object"}
        async def execute(self, arguments):
            counts["tool"] += 1
            return ToolResult(success=True, output="caption", content=[{"type": "image", "source": {
                "type": "base64", "media_type": "image/" + format.lower(), "data": encoded}}])

    class Selected(Provider):
        def parse_tool_calls(self, response):
            return response.tool_calls or []
        def get_media_request_support(self, **kwargs):
            return {"v": 1, "positions": ["tool"]}
        def observe(self, request, purpose):
            host.capability.validate(request, purpose=purpose, request_options={})
            hashes = [hashlib.sha256(base64.b64decode(block.source["data"])).hexdigest()
                      for message in request.messages
                      for block in message.content if isinstance(message.content, list)
                      if isinstance(block, ImageBlock)]
            observed.append((purpose, id(request), hashes))
        async def request_budget(self, request, *, context_estimate=None, request_options=None):
            self.observe(request, "count")
            return {"estimated_input_tokens": 10, "input_limit_tokens": 100,
                    "context_token_budget": 100, "measurement": {
                        "kind": "provider_count", "source": "offline synthetic owner", "input_tokens": 10}}
        async def complete(self, request, **kwargs):
            self.observe(request, "transport")
            return "done"

    driver = Selected()
    await view.mount("providers", driver, name="account")
    await view.mount("tools", Tool(), name="delegate")
    prepared = CLIHostAdapter().prepare_provider(driver, view)
    receipt = graph.seal()  # All actual declared mounts and late callbacks precede this.
    assert bridge.validate_receipt(receipt)
    call = ToolCall(id="image-call", name="delegate", arguments={"async": True})
    native_process = owner.process_hook_result
    assert inspect.isbuiltin(native_process)
    previous = sys.getprofile()
    assert previous is None
    def observe_native(frame, event, callee):
        if event == "c_call" and callee == native_process and frame.f_locals.get("event") == "tool:post":
            counts["process"] += 1
    sys.setprofile(observe_native)
    try:
        with loop.approved_dispatch.run_scope(view):
            pending = await loop._execute_tool_only(call, owner.get("tools"), view.hooks, None, view)
            job_id, job = next(iter(loop.jobs.items()))
            if mode == "cancel":
                await asyncio.wait_for(post_entered.wait(), 3)
                loop.cancel_job(job)
            await job["task"]
        if mode == "cancel":
            assert counts == {"tool": 1, "post": 1, "process": 0}
            assert ledger.rows[call.id]["status"] == "cancelled"
            assert MEDIA not in json.dumps(ledger.rows[call.id]) and observed == []
            await loop._execute_tool_only(call, {}, view.hooks, None, view)
            assert counts == {"tool": 1, "post": 1, "process": 0}
            assert bridge.validate_receipt(receipt)
            return
        assert counts == {"tool": 1, "post": 1, "process": 1}
        saved = ledger.rows[call.id]["settled_result"]
        assert ledger.rows[call.id]["status"] == "returned"
        kind, args = loop.runtime.inbox.get_nowait()
        assert kind == "bundle_job"
        await loop._job_returned(*args, deliver=True)
        # The actual engine appends the returned pending carrier at this original
        # tool row. Exercise that same context API and live synchronization here.
        await context.add_message({"role": "tool", "tool_call_id": call.id, "name": call.name,
                                   "content": pending.content})
        await loop._synchronize_job_results(context)
        rows = await context.get_messages()
        canonical = copy.deepcopy(rows)
        if mode != "keep":
            # No media barrier is required after redaction. The ordinary final
            # checkpoint is a separate host operation, not an invented media ACK.
            await owner.get_capability("session.durable_checkpoint")()
        checkpointed, _ = native.load(session.session_id)
        row_refs = (rows[0].get("metadata") or {}).get(MEDIA, {}).get("refs", [])
        ack_refs = (checkpointed[0].get("metadata") or {}).get(MEDIA, {}).get("refs", [])
        assert row_refs == ack_refs == saved["metadata"].get(MEDIA, {}).get("refs", [])
        if mode == "keep":
            assert row_refs == effective_refs and len(row_refs) == 1
            assert row_refs[0]["sha256"] == hashlib.sha256(pixels).hexdigest()
        else:
            assert row_refs == [] and saved["metadata"] == {}
        request = ChatRequest(messages=[Message(**row) for row in rows])
        before = copy.deepcopy(request.model_dump())
        await prepared.request_budget(request)
        assert await prepared.complete(request) == "done"
        assert observed[0][1] == observed[1][1]
        assert observed[0][2] == observed[1][2] == [ref["sha256"] for ref in row_refs]
        assert request.model_dump() == before and await context.get_messages() == canonical
        if mode == "keep":
            changed = ChatRequest(messages=[Message(**row) for row in rows])
            await prepared.request_budget(changed)
            changed.messages[0].content = "changed after fitting"
            with pytest.raises(ValueError, match="changed before dispatch"):
                await prepared.complete(changed)
            assert sum(purpose == "transport" for purpose, *_ in observed) == 1
            assert await context.get_messages() == canonical
        assert encoded not in json.dumps(native.load(session.session_id)[0])
        assert encoded not in json.dumps(ledger.rows[call.id])
        await loop._execute_tool_only(call, {}, view.hooks, None, view)
        assert counts == {"tool": 1, "post": 1, "process": 1}
        assert bridge.validate_receipt(receipt)
    finally:
        sys.setprofile(previous)
        unregister()
        if unregister_echo:
            unregister_echo()
        ledger.close()
        await session.cleanup()


@pytest.mark.parametrize("format", ["PNG", "JPEG"])
@pytest.mark.parametrize("scope", ["root", "child"])
@pytest.mark.parametrize("construction", ["declared-components", "actual-cli-installer"])
async def test_controlled_live_manager_late_seal_actual_engine_media_send(tmp_path, monkeypatch, format, scope, construction):
    """Actual live engine path, with explicitly owned late execution callbacks."""
    from amplifier_core.message_models import ChatResponse, TextBlock
    from amplifier_module_loop_live.runtime import Input
    config = {"session": {"orchestrator": {"module": "loop-live", "config": {
        "background_delegate": True, "min_delay_between_calls_ms": 0}},
        "context": {"module": "context-simple"}}, "providers": [],
        "tools": [{"module": "tool-offline-live-positive"}], "session_media": POLICY}
    parent_id = "offline-parent" if scope == "child" else None
    session = AmplifierSession(config, parent_id=parent_id)
    owner = session.coordinator
    bridge = bridge_module.install_media_hook_bridge(owner)
    cli_graph = None
    if construction == "actual-cli-installer":
        from amplifier_app_cli.session_hooks import install_session_hook_graph
        cli_graph = install_session_hook_graph(session, owner="offline.actual-cli.live-installer",
                                               host_participants=("host.media",))
        if not callable(owner.get_capability("cli.media.execution_registration")):
            pytest.skip("Requires current actual CLI late-registration capability")
        graph = cli_graph.construction
        view = cli_graph.coordinator("host.loop-live")
    else:
        graph = bridge.begin_participating_graph(session, owner="offline.live.manager.cli-components",
            participants=("loop-live", "context-simple", "tool-offline-live-positive",
                          "host.media", "host.execution"))
        view = graph.participant("host.execution")
    native = SessionStore(tmp_path / "sessions")
    host = install_session_media(session if cli_graph else SimpleNamespace(
        session_id=session.session_id, config=session.config, coordinator=graph.participant("host.media")), native)
    install_session_checkpoint(session, native, session_id=session.session_id)
    ledger = JobStore(tmp_path / "jobs")
    pixels = image_bytes(format)
    encoded = base64.b64encode(pixels).decode()
    counts, sends, receipts = {"tool": 0}, [], []
    image_sent = asyncio.Event()
    class Selected(Provider):
        def parse_tool_calls(self, response):
            return response.tool_calls or []
        def get_media_request_support(self, **kwargs):
            return {"v": 1, "positions": ["tool"]}
        async def request_budget(self, request, *, context_estimate=None, request_options=None):
            host.capability.validate(request, purpose="count", request_options=request_options)
            if cli_graph:
                # Exercise the actual issuer fence while the CLI-installed F1
                # wrapper holds the private count/send candidate, not a flag.
                with pytest.raises(ValueError, match="active leases"):
                    with cli_graph.transition():
                        raise AssertionError("counted candidate must fence graph changes")
            self.counted = request
            return {"estimated_input_tokens": 10, "input_limit_tokens": 10000,
                    "context_token_budget": 10000, "measurement": {
                        "kind": "provider_count", "source": "offline synthetic owner", "input_tokens": 10}}
        async def complete(self, request, **kwargs):
            host.capability.validate(request, purpose="transport", request_options=kwargs)
            assert request is self.counted
            hashes = [hashlib.sha256(base64.b64decode(block.source["data"])).hexdigest()
                      for message in request.messages
                      for block in message.content if isinstance(message.content, list)
                      if isinstance(block, ImageBlock)]
            sends.append(hashes)
            if len(sends) == 1:
                return ChatResponse(content=[], tool_calls=[
                    ToolCall(id="manager-image", name="delegate", arguments={"async": True})])
            if hashes:
                image_sent.set()
            return ChatResponse(content=[TextBlock(text="image received" if hashes else "pending")])
    class Tool:
        name, description, input_schema = "delegate", "Offline exact image owner", {"type": "object"}
        async def execute(self, arguments):
            counts["tool"] += 1
            return ToolResult(success=True, output="caption", content=[{"type": "image", "source": {
                "type": "base64", "media_type": "image/" + format.lower(), "data": encoded}}])
    # Normal Core filesystem-import discovery, not a replacement loader. The
    # actual source-bound mount is needed to qualify the AsyncTool adapter edge.
    module_name = "amplifier_module_tool_offline_live_positive"
    module = ModuleType(module_name)
    module.__file__, module.__spec__ = __file__, ModuleSpec(module_name, loader=None, origin=__file__)
    async def mount_tool(current, config=None):
        await current.mount("tools", Tool(), name="delegate")
    module.mount = mount_tool
    monkeypatch.setitem(sys.modules, module_name, module)
    await session.initialize()
    loop, context = owner.get("orchestrator"), owner.get("context")
    def status(event):
        if event["type"] == "session.ready":
            # The late CLI diagnostics and actual live-manager observers have
            # registered. A host-owned observed phase seals before work input.
            receipts.append(bridge.graph_receipt if cli_graph else graph.seal())
    runtime = Runtime(observer=status)
    owner.register_capability("live.runtime", runtime)
    owner.register_capability("live.host", CLIHostAdapter())
    owner.register_capability("live.jobs", ledger)
    await view.mount("providers", Selected(), name="account")
    task = asyncio.create_task(session.execute("") if cli_graph else loop.execute(
        "", context, owner.get("providers"), owner.get("tools"), view.hooks, view))
    try:
        await runtime.wait_for(lambda event: event["type"] == "session.ready", 3)
        assert len(receipts) == 1 and bridge.validate_receipt(receipts[0]), (
            bridge.receipt_status(bridge.graph_receipt), graph.participants_evidence(),
            task.exception() if task.done() else "execute pending")
        await runtime.submit(Input("user", "inspect the fixture", id="manager-input"))
        try:
            await asyncio.wait_for(image_sent.wait(), 5)
        except TimeoutError:
            pytest.fail(repr({"sends": sends, "events": runtime.events, "ledger": ledger.rows,
                              "receipt": bridge.receipt_status(receipts[0]),
                              "failure": repr(task.exception()) if task.done() else "pending"}))
        assert counts["tool"] == 1
        saved_rows, _ = native.load(session.session_id)
        row = next(row for row in saved_rows if row.get("tool_call_id") == "manager-image")
        refs = row["metadata"][MEDIA]["refs"]
        assert sends[-1] == [ref["sha256"] for ref in refs] == [hashlib.sha256(pixels).hexdigest()]
        assert refs == ledger.rows["manager-image"]["settled_result"]["metadata"][MEDIA]["refs"]
        assert encoded not in json.dumps(saved_rows) and bridge.validate_receipt(receipts[0])
        # A second authoritative turn stays with this exact live owner and
        # receipt. Idle does not park, retire or release it implicitly.
        await runtime.wait_for(lambda event: event["type"] == "session.idle" and event["sequence"] > 2, 3)
        generation_count = sum(event["type"] == "generation.finished" for event in runtime.events)
        image_sent.clear()
        await runtime.submit(Input("user", "inspect the saved image again", id="second-input"))
        await asyncio.wait_for(image_sent.wait(), 3)
        await runtime.wait_for(lambda event: event["type"] == "generation.finished" and
            sum(row["type"] == "generation.finished" for row in runtime.events) > generation_count, 3)
        assert counts["tool"] == 1 and len(receipts) == 1 and bridge.validate_receipt(receipts[0])
        await runtime.submit(Input("stop"))
        await asyncio.wait_for(task, 3)
        saved_rows, _ = native.load(session.session_id)
        old_receipt, old_host = receipts[0], host
        ledger.close()
        await session.cleanup()
        assert old_host.closed
        # Fresh real Core session restores saved rows and reopens the ledger,
        # never reusing the prior private candidate or receipt.
        session = AmplifierSession(config, session_id=session.session_id, parent_id=parent_id)
        owner = session.coordinator
        if cli_graph:
            cli_graph = install_session_hook_graph(session, owner="offline.actual-cli.live-installer",
                                                   host_participants=("host.media",))
            graph = cli_graph.construction
            bridge = owner.get_capability("session.media.tool_post")
        else:
            graph = graph.reconstruct(session, participants=graph.participants)
            bridge = owner.get_capability("session.media.tool_post")
        assert not bridge.validate_receipt(old_receipt)
        view = cli_graph.coordinator("host.loop-live") if cli_graph else graph.participant("host.execution")
        host = install_session_media(session if cli_graph else SimpleNamespace(
            session_id=session.session_id, config=session.config, coordinator=graph.participant("host.media")),
            native, restored_messages=saved_rows)
        install_session_checkpoint(session, native, session_id=session.session_id)
        await session.initialize()
        loop, context = owner.get("orchestrator"), owner.get("context")
        await context.set_messages(saved_rows)
        ledger = JobStore(tmp_path / "jobs")
        receipts.clear()
        runtime = Runtime(observer=status)
        owner.register_capability("live.runtime", runtime)
        owner.register_capability("live.host", CLIHostAdapter())
        owner.register_capability("live.jobs", ledger)
        await view.mount("providers", Selected(), name="account")
        image_sent.clear()
        task = asyncio.create_task(session.execute("") if cli_graph else loop.execute(
            "", context, owner.get("providers"), owner.get("tools"), view.hooks, view))
        await runtime.wait_for(lambda event: event["type"] == "session.ready", 3)
        await runtime.submit(Input("user", "resume saved image", id="resumed-input"))
        await asyncio.wait_for(image_sent.wait(), 3)
        assert counts["tool"] == 1 and bridge.validate_receipt(receipts[0])
        assert sends[-1] == [hashlib.sha256(pixels).hexdigest()]
        await runtime.submit(Input("stop"))
        await asyncio.wait_for(task, 3)
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        ledger.close()
        await session.cleanup()


@pytest.mark.parametrize("format", ["PNG", "JPEG"])
async def test_actual_cli_raw_finite_child_and_cold_resume_use_fresh_media_owner(tmp_path, monkeypatch, format):
    """Real spawner functions and Core loader, synthetic provider, selected history."""
    from unittest.mock import MagicMock
    from amplifier_core import ProviderInfo
    from amplifier_core.message_models import ChatResponse, TextBlock
    from amplifier_app_cli import session_spawner, session_store, paths
    from amplifier_app_cli.lib import settings as settings_module
    from amplifier_app_cli.session_hooks import install_session_hook_graph
    from amplifier_module_loop_live.scope import HOST_ADAPTER
    native = SessionStore(tmp_path / "sessions")
    observed, mounts, receipts = [], [], []
    payload = image_bytes(format)
    expected = hashlib.sha256(payload).hexdigest()
    class Selected:
        name, default_model, config = "fixture", "fixture", {}
        def get_info(self):
            return ProviderInfo(id="fixture", display_name="Offline", capabilities=[],
                                defaults={"model": "fixture"})
        def get_media_request_support(self, **kwargs):
            return {"v": 1, "positions": ["user"]}
        async def request_budget(self, request, **kwargs):
            self.counted = request
            return {"estimated_input_tokens": 10, "input_limit_tokens": 10000,
                    "context_token_budget": 10000, "measurement": {
                        "kind": "provider_count", "source": "offline synthetic owner", "input_tokens": 10}}
        async def complete(self, request, **kwargs):
            assert request is self.counted
            bridge = self.owner.get_capability("session.media.tool_post")
            assert bridge.validate_receipt(bridge.graph_receipt)
            receipts.append(bridge.graph_receipt)
            hashes = [hashlib.sha256(base64.b64decode(block.source["data"])).hexdigest()
                      for message in request.messages
                      for block in message.content if isinstance(message.content, list)
                      if isinstance(block, ImageBlock)]
            observed.append((self.owner.session_id, hashes, request))
            return ChatResponse(content=[TextBlock(text="child done")])
        def parse_tool_calls(self, response):
            return []
    module_name = "amplifier_module_provider_live_child_fixture"
    module = ModuleType(module_name)
    module.__file__, module.__spec__ = __file__, ModuleSpec(module_name, loader=None, origin=__file__)
    async def mount(current, config=None):
        provider = Selected()
        provider.owner = current
        mounts.append((current, provider))
        await current.mount("providers", provider, name="fixture")
    module.mount = mount
    monkeypatch.setitem(sys.modules, module_name, module)
    plan = {"session": {"orchestrator": {"module": "loop-live", "config": {
        "min_delay_between_calls_ms": 0}}, "context": {"module": "context-simple"}},
        "providers": [{"module": "provider-live-child-fixture"}], "session_media": POLICY}
    parent = AmplifierSession(plan, session_id="parent")
    install_session_hook_graph(parent, owner="offline.parent.selected-history",
                               host_participants=("host.media",))
    host = install_session_media(parent, native)
    install_session_checkpoint(parent, native, session_id="parent")
    await parent.initialize()
    origin = {"kind": "upload", "session_id": "parent", "id": "selected-upload"}
    ref = (await host.capability.ingest(payload, source_format="image-bytes", origin=origin, policy=None))["value"]
    await host.capability.publish([ref], origin=origin, disposition="retain")
    selected = [{"role": "user", "content": "selected observation", "metadata": {MEDIA: {"refs": [ref]}}}]
    await parent.coordinator.get("context").set_messages(selected)
    await parent.coordinator.get_capability("session.durable_checkpoint")()
    settings = MagicMock()
    settings.get_config_overrides.return_value = {}
    settings.get_provider_overrides.return_value = []
    settings.get_notification_hook_overrides.return_value = []
    monkeypatch.setattr(settings_module, "AppSettings", lambda: settings)
    monkeypatch.setattr(session_store, "SessionStore", lambda: native)
    monkeypatch.setattr(paths, "create_foundation_resolver", lambda: None)
    monkeypatch.setattr(session_spawner, "_expand_system_instruction", __import__("unittest.mock").mock.AsyncMock(
        side_effect=lambda text, _: text))
    monkeypatch.setenv("AMPLIFIER_SPAWN_CHECKPOINT_INTERVAL_S", "-1")
    token = HOST_ADAPTER.set(CLIHostAdapter())
    try:
        result = await session_spawner.spawn_sub_session(agent_name="named", instruction="inspect selected",
            parent_session=parent, agent_configs={"named": {}}, sub_session_id="child", parent_messages=selected)
        assert result["output"] == "child done"
        first_host = mounts[-1][0].get_capability("cli.session_media")
        assert first_host.closed
        await parent.cleanup()
        resumed = await session_spawner.resume_sub_session("child", "follow-up", parent_session=None)
        assert resumed["output"] == "child done"
        assert len(observed) == 2 and all(row[0] == "child" and row[1] == [expected] for row in observed)
        assert receipts[0] is not receipts[1] and observed[0][2] is not observed[1][2]
        child_rows = native.load("child")[0]
        child_ref = next(row for row in child_rows if (row.get("metadata") or {}).get(MEDIA))["metadata"][MEDIA]["refs"][0]
        assert child_ref["session_id"] == "child" and child_ref["sha256"] == ref["sha256"]
        assert selected[0]["metadata"][MEDIA]["refs"][0]["session_id"] == "parent"
        assert base64.b64encode(payload).decode() not in json.dumps(child_rows)
    finally:
        HOST_ADAPTER.reset(token)
        await parent.cleanup()