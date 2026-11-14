"""Candidate-graph offline integration, not inference or accepted-graph proof.

Requires the CLI late factory, Foundation F1, and optional guarded bridge.
The bridge negative fixture must not be reported as forward tool-media success.
"""
import asyncio
import base64
import copy
import hashlib
import io
import inspect
import json
import os
import sys
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
from amplifier_module_loop_streaming.media import UNAVAILABLE
from test_cli_media_provider import Provider, no_network  # noqa: F401
from amplifier_loop_live_cli.selection import RootProvider
from amplifier_loop_live_cli.worker_settings import ObservedWorkerProvider

pytestmark = pytest.mark.skipif(os.name != "posix", reason="Candidate durable media store is POSIX-only")
POLICY = {"enabled": True, "max_bytes": 1024 * 1024, "max_pixels": 10000, "max_images": 10}


def pixels():
    stream = io.BytesIO()
    Image.new("RGB", (3, 2), "orange").save(stream, format="PNG")
    return stream.getvalue()


@pytest.mark.parametrize("wrapper", ["mounted", "root", "worker"])
@pytest.mark.parametrize("streaming", [False, True])
async def test_actual_engine_counts_and_dispatches_through_cli_neutral_factory(tmp_path, wrapper, streaming):
    """Exercise the actual engine call shape, including stream's tools option."""
    from amplifier_core import ProviderInfo
    from amplifier_core.message_models import ChatResponse, TextBlock
    session = AmplifierSession({"session": {"orchestrator": {"module": "loop-live",
                                "config": {"min_delay_between_calls_ms": 0}},
                                "context": {"module": "context-simple"}}, "providers": []})
    native = SessionStore(tmp_path / "sessions")
    host = install_session_media(session, native)
    await session.initialize()
    owner = session.coordinator
    observed, records = [], []
    class Selected(Provider):
        def get_info(self):
            return ProviderInfo(id="fixture", display_name="Offline fixture",
                                capabilities=["tools"], defaults={"model": "default"})
        async def request_budget(self, request, *, context_estimate, request_options=None):
            host.capability.validate(request, purpose="count", request_options=request_options)
            observed.append(("count", id(request)))
            return None  # Ordering/identity only, never a measured fit claim.
        async def complete(self, request, **kwargs):
            host.capability.validate(request, purpose="transport", request_options=kwargs)
            observed.append(("send", id(request)))
            return ChatResponse(content=[TextBlock(text="done")])
    driver = Selected()
    if streaming:
        # This local fixture is the actual owner of its public stream method.
        # Reviewed production vendors have complete(), not this optional route.
        driver.get_stream_request_support = lambda **kwargs: {"v": 1, "tools": "chat_request"}
        async def stream(request, **kwargs):
            assert "tools" not in kwargs
            host.capability.validate(request, purpose="transport", request_options=kwargs)
            observed.append(("send", id(request)))
            yield {"content": "done"}
        driver.stream = stream
    await owner.mount("providers", driver, name="account")
    loop = owner.get("orchestrator")
    if wrapper == "root":
        loop.root_provider = RootProvider(driver, {"instance": "account", "model": "default",
                                                  "effort": None, "thinkingBudget": None}, "fixture")
    elif wrapper == "worker":
        registry = SimpleNamespace(configuration=lambda *args: records.append(args))
        loop.root_provider = ObservedWorkerProvider(driver, {"account": driver}, registry, "child")
    owner.register_capability("live.host", CLIHostAdapter())
    try:
        result = await loop.execute("ordinary text", owner.get("context"), owner.get("providers"),
                                    owner.get("tools"), owner.hooks, owner)
        assert result == "done"
        assert observed[-1][0] == "send" and any(kind == "count" for kind, _ in observed)
        assert observed[-2] == ("count", observed[-1][1])
        if wrapper == "worker":
            assert records[-1][1]["instance"] == "account"
        assert not (native.base_dir / session.session_id).exists()
    finally:
        await session.cleanup()


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


@pytest.mark.parametrize("wrapper", ["root", "worker-foreign-tools", "worker-ordinary-tools"])
@pytest.mark.parametrize("boundary", ["complete", "stream"])
@pytest.mark.parametrize("preparation", ["fallback", "count"])
async def test_actual_f1_text_fallback_uses_same_selected_view_as_dispatch(tmp_path, wrapper, boundary, preparation):
    """Fallback is not a provider count; bind its selected warning-only candidate."""
    from amplifier_core.message_models import ToolSpec
    owner, observed = Owner([]), []
    native = SessionStore(tmp_path / "sessions")
    session = SimpleNamespace(session_id="root", coordinator=owner,
                              config={"session": {"orchestrator": "loop-live"}})
    host = install_session_media(session, native)
    class Selected(Provider):
        async def request_budget(self, request, *, context_estimate=None, request_options=None):
            return {"estimated_input_tokens": 10, "input_limit_tokens": 100,
                    "context_token_budget": 100, "measurement": {
                        "kind": "provider_count", "source": "offline synthetic owner", "input_tokens": 10}}
        def observe(self, request, kwargs):
            host.capability.validate(request, purpose="transport", request_options=kwargs)
            assert all(not isinstance(block, ImageBlock) for message in request.messages
                       for block in message.content if isinstance(message.content, list))
            observed.append((id(request), request.model, request.tools, copy.deepcopy(kwargs)))
        async def complete(self, request, **kwargs):
            self.observe(request, kwargs)
            return "done"
        async def stream(self, request, **kwargs):
            self.observe(request, kwargs)
            yield "done"
    driver = Selected()
    owner.providers["selected-account"] = driver
    tools = [ToolSpec(name="computer", parameters={}, description="offline fixture")]
    if preparation == "fallback":
        tools = []  # A messages-only local estimate must not omit tool schemas.
    elif wrapper == "worker-foreign-tools":
        tools[0] = tools[0].model_copy(update={"type": "computer"})
    canonical = ChatRequest(messages=[Message(role="user", content=[{
        "type": "image", "source": {"type": "base64", "media_type": "image/png",
                                    "data": base64.b64encode(pixels()).decode()}}])], tools=tools)
    before = canonical.model_dump()
    if wrapper == "root":
        raw = RootProvider(driver, {"instance": "selected-account", "model": "selected-model",
                                    "effort": "high", "thinkingBudget": None}, "fixture")
    else:
        raw = ObservedWorkerProvider(driver, {"selected-account": driver},
                                     SimpleNamespace(configuration=lambda *a: None), "child")
    view = CLIHostAdapter().prepare_provider(raw, owner)
    options = {"temperature": 0.3}
    try:
        if preparation == "fallback":
            witness = await view.media_text_fallback_witness(canonical, request_options=options)
            assert witness["validated"] is True
            assert "Image unavailable" in str(witness["messages"])
        else:
            # Full request counting retains all converted tool schemas. New
            # Foundation correctly refuses a messages-only fallback with tools.
            await view.request_budget(canonical, request_options=options)
        prepared = host.wrappers[id(driver)][1]
        private_id = id(prepared._candidate.prepared.request)
        if boundary == "complete":
            assert await view.complete(canonical, **options) == "done"
        else:
            assert [chunk async for chunk in view.stream(canonical, **options)] == ["done"]
        assert len(observed) == 1 and observed[0][0] == private_id
        assert observed[0][1] == ("selected-model" if wrapper == "root" else None)
        assert all(getattr(tool, "type", "function") == "function" for tool in observed[0][2] or [])
        assert observed[0][3] == ({**options, "model": "selected-model", "reasoning_effort": "high"}
                                  if wrapper == "root" else options)
        assert canonical.model_dump() == before
        assert prepared._candidate is None
    finally:
        await owner.cleanup()


@pytest.mark.parametrize("wrapper", ["root", "worker-foreign-tools"])
@pytest.mark.parametrize("mutation", ["canonical", "selection", "copy-then-valid", "stale-then-current"])
async def test_actual_f1_release_preserves_preparation_time_selected_binding(tmp_path, wrapper, mutation):
    from amplifier_core.message_models import ToolSpec
    from amplifier_foundation.session.media_provider import MediaPreparedProvider
    if not hasattr(MediaPreparedProvider, "release_media_request"):
        pytest.skip("Requires the exact candidate-release Foundation API")
    class Counted(Provider):
        async def request_budget(self, request, *, context_estimate=None, request_options=None):
            return {"estimated_input_tokens": 10, "input_limit_tokens": 100,
                    "context_token_budget": 100, "measurement": {
                        "kind": "provider_count", "source": "offline synthetic owner", "input_tokens": 10}}
    owner, driver = Owner([]), Counted()
    session = SimpleNamespace(session_id="root", coordinator=owner,
                              config={"session": {"orchestrator": "loop-live"}})
    host = install_session_media(session, SessionStore(tmp_path / "sessions"))
    owner.providers["account"] = driver
    if wrapper == "root":
        raw = RootProvider(driver, {"instance": "account", "model": "selected-model",
                                   "effort": "high", "thinkingBudget": None}, "fixture")
    else:
        raw = ObservedWorkerProvider(driver, {"account": driver},
                                     SimpleNamespace(configuration=lambda *a: None), "child")
    view = CLIHostAdapter().prepare_provider(raw, owner)
    prepared = host.wrappers[id(driver)][1]
    def request():
        return ChatRequest(messages=[Message(role="user", content=[{
            "type": "image", "source": {"type": "base64", "media_type": "image/png",
                                      "data": base64.b64encode(pixels()).decode()}}])],
            tools=[ToolSpec(name="computer", parameters={}, description="fixture").model_copy(update={"type": "computer"})])
    canonical, options = request(), {"temperature": 0.3}
    try:
        await view.request_budget(canonical, request_options=options)
        candidate = prepared._candidate
        if mutation == "canonical":
            canonical.messages[0].content = "mutated after preparation"
        elif mutation == "selection" and wrapper == "root":
            view.selection = {**view.selection, "model": "later-model", "effort": "low"}
        elif mutation in {"copy-then-valid", "selection"}:
            assert view.release_media_request(canonical.model_copy(deep=True), request_options=options) is False
            assert prepared._candidate is candidate
        else:
            old = canonical
            canonical = request()
            await view.request_budget(canonical, request_options=options)
            candidate = prepared._candidate
            assert view.release_media_request(old, request_options=options) is False
            assert prepared._candidate is candidate
        assert view.release_media_request(canonical, request_options={"temperature": 0.5}) is False
        assert prepared._candidate is candidate
        assert view.release_media_request(canonical, request_options=options) is True
        assert prepared._candidate is None and prepared._active == [] and prepared._lifetimes == []
        assert candidate.prepared.request.messages == []
        assert view.release_media_request(canonical, request_options=options) is False
        assert driver.requests == []
    finally:
        await owner.cleanup()


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
    native_process = owner.process_hook_result
    assert inspect.isbuiltin(native_process) and native_process.__self__ is owner
    # Observe the captured Rust endpoint without replacing it: newer bridges
    # intentionally refuse Python endpoint facades. This exact-candidate probe
    # counts native invocations, while awaited settlement below proves completion.
    def observe_native(frame, event, callee):
        if (event == "c_call" and callee == native_process
                and frame.f_locals.get("event") == "tool:post"):
            counts["process"] += 1
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
    previous_profile = sys.getprofile()
    assert previous_profile is None, "Native-count fixture requires an unprofiled thread"
    sys.setprofile(observe_native)
    try:
        with loop.approved_dispatch.run_scope(owner):
            await loop._execute_tool_only(call, owner.get("tools"), owner.hooks, None, owner)
            job_id, job = next(iter(loop.jobs.items()))
            await job["task"]
        assert counts == {"tool": 1, "post": 1, "process": 1}
        # A returned report is not bridge/media acceptance. The guarded current
        # route preserves explicit null, otherwise freezes unavailable text.
        assert ledger.rows["call"]["status"] == "returned"
        record = ledger.rows["call"]["settled_result"]
        if mode == "null":
            assert record["content"] == "null" and record["result"] is None
        else:
            assert UNAVAILABLE in record["content"]
        assert record["metadata"] == {}
        kind, args = loop.runtime.inbox.get_nowait()
        assert kind == "bundle_job"
        await loop._job_returned(*args, deliver=True)
        assert loop.pending[0].job_result["metadata"] == {}
        await loop._execute_tool_only(call, {}, owner.hooks, None, owner)
        assert counts == {"tool": 1, "post": 1, "process": 1}
        assert owner.process_hook_result == native_process
        assert not bridge.readiness()["authoritative"]
        for path in (tmp_path / "jobs").glob("*.json"):
            assert encoded not in path.read_text()
        assert "experimental.media.v1" not in json.dumps(ledger.rows["call"])
    finally:
        sys.setprofile(previous_profile)
        unregister()
        ledger.close()
        store.close()
        await session.cleanup()
