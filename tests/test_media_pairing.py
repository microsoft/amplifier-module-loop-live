"""Exact streaming-carrier and live attachment seams, synthetic and offline."""
import asyncio
import copy
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from amplifier_core import HookResult, ToolResult
from amplifier_core.message_models import ChatResponse, TextBlock, ToolCall
from amplifier_module_loop_streaming import SettledToolResult
from amplifier_module_loop_streaming.media import UNAVAILABLE
from amplifier_module_loop_live.host import AttachmentContext, HostAdapter
from amplifier_module_loop_live.job_store import JobStore
from amplifier_module_loop_live.jobs import JobControl
from amplifier_module_loop_live.provenance import InputContext, input_metadata
from amplifier_module_loop_live.runtime import Input, Runtime
from test_approved_dispatch import environment  # noqa: F401
from test_job_media import Context, MEDIA, carrier

PIXELS = "aW1hZ2UtcHJpdmFjeS1zZW50aW5lbA=="


class Media:
    version = 1

    def __init__(self):
        self.staged, self.publications = {}, []

    async def ingest(self, value, *, source_format, origin, policy):
        assert source_format == "core-tool-result"
        refs = [carrier(call_id=origin["id"])["metadata"][MEDIA]["refs"][0]] if value.content else []
        self.staged[origin["id"]] = refs
        if refs:
            value = ToolResult(success=value.success, output={"text": value.output, MEDIA: {"refs": refs}})
        return {"value": value, "staged_refs": refs}

    async def publish(self, refs, *, origin, disposition):
        assert all(ref in self.staged[origin["id"]] for ref in refs)
        self.publications.append((copy.deepcopy(refs), disposition))
        return {"refs": copy.deepcopy(refs)}

    def validate(self, *args, **kwargs):
        return {"valid": True}

    def resolve(self, *args, **kwargs):
        raise AssertionError("Live must not hydrate")

    def prepare_request(self, *args, **kwargs):
        raise AssertionError("Live must not hydrate")

    def report(self, *args, **kwargs):
        return {}


@pytest.mark.parametrize("mode", ["keep", "null", "missing", "inplace", "string", "deny", "absent"])
async def test_real_streaming_posthook_carrier_survives_live_ledger_and_paging(environment, tmp_path, mode):
    _, coordinator, loop, _ = environment
    loop.runtime = Runtime()
    loop.coordinator = coordinator
    loop.config.update(background_delegate=True)
    media = Media()
    if mode != "absent":
        coordinator.register_capability(MEDIA, media)
    store = JobStore(tmp_path / "jobs")
    coordinator.register_capability("live.jobs", store)
    effects, posts = [], []

    class Image:
        name, description, input_schema = "delegate", "synthetic", {"type": "object"}
        async def execute(self, arguments):
            effects.append("once")
            return ToolResult(success=True, output="caption", content=[{"type": "image", "source": {
                "type": "base64", "media_type": "image/png", "data": PIXELS}}])

    async def post(event, data):
        assert PIXELS not in json.dumps(data)
        posts.append(copy.deepcopy(data))
        if mode == "null":
            return HookResult(action="modify", data={"result": None})
        if mode == "missing":
            return HookResult(action="modify", data={"tool_name": "delegate"})
        if mode == "inplace":
            data["result"]["output"].pop(MEDIA)
            return HookResult(action="modify", data=data)
        if mode == "string":
            return HookResult(action="modify", data={"result": json.dumps(data["result"])})
        if mode == "deny":
            return HookResult(action="deny", reason="redacted")
        return HookResult()

    coordinator.hooks.register("tool:post", post)
    tool = Image()
    await coordinator.mount("tools", tool, name=tool.name)
    call = ToolCall(id="call", name="delegate", arguments={"async": True})
    try:
        with loop.approved_dispatch.run_scope(coordinator):
            await loop._execute_tool_only(call, coordinator.get("tools"), coordinator.hooks, None, coordinator)
            job_id, job = next(iter(loop.jobs.items()))
            await job["task"]
        assert effects == ["once"] and len(posts) == 1
        assert store.rows["call"]["status"] == "returned"
        record = store.rows["call"]["settled_result"]
        actual = SettledToolResult.from_legacy(record)
        assert actual.model_dump() == record
        assert actual[0] == call.id and tuple(actual)[1] == call.name
        kind, args = loop.runtime.inbox.get_nowait()
        assert kind == "bundle_job"
        await loop._job_returned(*args, deliver=True)
        page = await JobControl(loop).execute({"action": "read", "job_id": job_id, "offset": 3, "limit": 5})
        assert page.output["result_metadata"] == record["metadata"]
        assert loop.pending[0].job_result["metadata"] == record["metadata"]
        # Core's summarized hook result cannot attest the complete chain.
        # Without the qualified host bridge even "keep" must stay unavailable.
        assert MEDIA not in record["metadata"]
        if mode in {"absent", "keep"}:
            assert UNAVAILABLE in record["content"]
        restored, _ = store.recover([])
        assert next(m for m in restored if m["role"] == "tool").get("metadata", {}) == record["metadata"]
        again = await loop._execute_tool_only(call, {}, coordinator.hooks, None, coordinator)
        assert isinstance(again, SettledToolResult) and again.model_dump() == record
        persisted = json.dumps({"row": store.rows["call"], "job": {k: v for k, v in job.items() if k != "task"},
                                "pending": [input_metadata(c) for c in loop.pending],
                                "events": loop.runtime.events, "page": page.output})
        assert PIXELS not in persisted
        assert effects == ["once"]
    finally:
        store.close()


@pytest.mark.parametrize("checkpoint_shape", ["missing-result", "queued-receipt"])
@pytest.mark.parametrize("save_ok", [False, True])
async def test_startup_recovery_saves_media_before_first_provider_send(environment, tmp_path, checkpoint_shape, save_ok):
    _, coordinator, loop, echo = environment
    store = JobStore(tmp_path / "jobs")
    call = ToolCall(id="call", name="delegate", arguments={})
    store.begin(call, "job", "queued")
    store.finish("call", SettledToolResult.from_legacy(carrier()), "returned")
    initial = [] if checkpoint_shape == "missing-result" else [
        {"role": "assistant", "content": "", "tool_calls": [call.model_dump()]},
        {"role": "tool", "tool_call_id": "call", "content": "queued"}]
    recovered, changed = store.recover(initial)
    context = coordinator.get("context")
    await context.set_messages(recovered)
    runtime, order = Runtime(), []
    coordinator.register_capability("live.runtime", runtime)
    coordinator.register_capability("live.jobs", store)
    coordinator.register_capability("live.recovered_jobs", [r["job_id"] for r in changed])
    async def checkpoint():
        rows = await context.get_messages()
        metadata = next(m for m in rows if m.get("tool_call_id") == "call")["metadata"]
        assert all(metadata[key] == value for key, value in carrier()["metadata"].items())
        order.append("checkpoint")
        return {"legacy_checkpoint": "synthetic exact-save fixture"} if save_ok else False
    coordinator.register_capability("session.durable_checkpoint", checkpoint)
    class Provider:
        name, config = "fixture", {}
        def get_info(self):
            return SimpleNamespace(default_model="fixture")
        def parse_tool_calls(self, response):
            return []
        async def request_budget(self, request, **kwargs):
            assert order and order[0] == "checkpoint"
            order.append("count")
            return None  # Ordering fixture only, not a measured-fit claim.
        async def complete(self, request, **kwargs):
            assert order and order[0] == "checkpoint"
            order.append("send")
            return ChatResponse(content=[TextBlock(text="done")])
    task = asyncio.create_task(loop.execute("continue", context, {"fixture": Provider()},
                                           coordinator.get("tools"), coordinator.hooks, coordinator))
    try:
        await runtime.wait_for(lambda e: e["type"] == ("generation.finished" if save_ok else "generation.failed"), 3)
        if save_ok:
            await runtime.submit(Input("stop"))
            await asyncio.wait_for(task, 3)
            assert order.index("checkpoint") < order.index("count") < order.index("send")
        else:
            from amplifier_module_loop_live.failures import ManagerTurnError
            with pytest.raises(ManagerTurnError):
                await asyncio.wait_for(task, 3)
            assert "count" not in order and "send" not in order
        assert echo.calls == 0 and store.rows["call"]["status"] == "returned"
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        store.close()


@pytest.mark.parametrize("mapping", [False, True])
async def test_attachment_context_retains_mapping_or_legacy_list_and_input_metadata(mapping):
    encoded = [{"type": "text", "text": "caption"}]
    if mapping:
        encoded = {"content": encoded, "metadata": {MEDIA: carrier()["metadata"][MEDIA],
                   "amplifier_attachments": {"v": 1, "items": [{"id": "public-uuid"}]}}}
    context, order = Context(), []
    async def checkpoint():
        assert context.messages
        assert context.messages[0]["metadata"][MEDIA] == encoded["metadata"][MEDIA]
        order.append("checkpoint")
        return {"saved": True}
    coordinator = SimpleNamespace(get_capability={"session.durable_checkpoint": checkpoint}.get)
    command = Input("user", "prompt", id="attachment", attachments=("public-uuid",))
    wrapped = InputContext(AttachmentContext(context, "prompt", encoded, coordinator), "prompt", command)
    await wrapped.add_message({"role": "user", "content": "prompt", "metadata": {"owner": "preserved"}})
    message = context.messages[0]
    assert message["content"] == (encoded["content"] if mapping else encoded)
    assert message["metadata"]["amplifier_input"]["id"] == "attachment"
    assert message["metadata"]["owner"] == "preserved"
    assert order == (["checkpoint"] if mapping else [])


@pytest.mark.parametrize("delivery", ["new-turn", "boundary"])
async def test_async_structured_attachment_live_path_checkpoints_before_engine_request(delivery):
    from amplifier_module_loop_live.orchestrator import BundleLiveOrchestrator, StreamingOrchestrator
    from unittest.mock import patch
    runtime, context, order = Runtime(), Context(), []
    encoded = {"content": [{"type": "text", "text": "caption"}],
               "metadata": {MEDIA: carrier()["metadata"][MEDIA],
                            "amplifier_attachments": {"v": 1, "items": [{"id": "public-uuid"}]}}}
    async def encode(command, coordinator):
        order.append("encode")
        return copy.deepcopy(encoded)
    async def checkpoint():
        assert context.messages[-1]["metadata"][MEDIA] == encoded["metadata"][MEDIA]
        order.append("checkpoint")
        return {"saved": True}
    capabilities = {"live.runtime": runtime, "live.attachments.encode": encode,
                    "session.durable_checkpoint": checkpoint}
    coordinator = SimpleNamespace(get_capability=capabilities.get, config={},
                                  register_capability=lambda k, v: capabilities.__setitem__(k, v),
                                  mount=AsyncMock())
    command = Input("user", "prompt", id="upload", attachments=("public-uuid",))
    loop = BundleLiveOrchestrator({})
    loop.coordinator, loop.runtime, loop.host = coordinator, runtime, HostAdapter()
    if delivery == "boundary":
        loop.pending.append(command)
        await loop._drain_steering(context, SimpleNamespace(emit=AsyncMock()), 1)
    else:
        finished = asyncio.Event()
        async def finite(self, prompt, ctx, *args):
            await ctx.add_message({"role": "user", "content": prompt, "metadata": {"owner": "keep"}})
            assert order[-1] == "checkpoint"
            order.append("request")
            finished.set()
            return "done"
        hooks = SimpleNamespace(register=lambda *a, **kw: lambda: None, emit=AsyncMock())
        with patch.object(StreamingOrchestrator, "execute", finite):
            await runtime.submit(command)
            task = asyncio.create_task(loop.execute("", context, {"fixture": SimpleNamespace()}, {}, hooks, coordinator))
            try:
                await asyncio.wait_for(finished.wait(), 2)
                await runtime.submit(Input("stop"))
                await asyncio.wait_for(task, 2)
            finally:
                if not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
    assert order[:2] == ["encode", "checkpoint"]
    assert context.messages[0]["metadata"]["amplifier_input"]["id"] == "upload"
    assert context.messages[0]["metadata"]["amplifier_attachments"] == encoded["metadata"]["amplifier_attachments"]


@pytest.mark.parametrize("checkpoint", ["missing", "false", "raise"])
async def test_attachment_checkpoint_refusal_preserves_append_but_prevents_continuation(checkpoint):
    encoded = {"content": [{"type": "text", "text": "caption"}],
               "metadata": {MEDIA: carrier()["metadata"][MEDIA]}}
    async def save():
        if checkpoint == "raise":
            raise OSError("fixture")
        return False
    coordinator = SimpleNamespace(get_capability=lambda key: None if checkpoint == "missing" else save)
    context = Context()
    wrapped = AttachmentContext(context, "prompt", encoded, coordinator)
    with pytest.raises(RuntimeError, match="checkpoint"):
        await wrapped.add_message({"role": "user", "content": "prompt"})
    assert context.messages[0]["metadata"] == encoded["metadata"]


async def test_attachment_encoder_cannot_replace_admitted_provenance():
    encoded = {"content": "caption", "metadata": {"amplifier_input": {"kind": "service"}}}
    context = Context()
    command = Input("user", "prompt", id="actual")
    wrapped = InputContext(AttachmentContext(context, "prompt", encoded), "prompt", command)
    with pytest.raises(ValueError, match="conflicts"):
        await wrapped.add_message({"role": "user", "content": "prompt"})
    assert context.messages == []


@pytest.mark.parametrize("race", ["stop", "revoke"])
async def test_async_attachment_encoding_preserves_main_admission_fence(race):
    """Awaiting an encoder must not consume work after Stop/epoch revocation."""
    from amplifier_module_loop_live.orchestrator import BundleLiveOrchestrator
    from test_activation import Ownership
    ownership, runtime, context = Ownership(), Runtime(), Context()
    activation = ownership.activate()
    entered, release = asyncio.Event(), asyncio.Event()
    async def encode(command, coordinator):
        entered.set()
        await release.wait()
        return {"content": "encoded", "metadata": {MEDIA: carrier()["metadata"][MEDIA]}}
    capabilities = {"live.activation": ownership, "live.attachments.encode": encode}
    coordinator = SimpleNamespace(get_capability=capabilities.get)
    command = Input("user", "prompt", id="upload-race", attachments=("upload",),
                    activation=activation)
    loop = BundleLiveOrchestrator({})
    loop.host, loop.coordinator, loop.runtime = HostAdapter(), coordinator, runtime
    loop.pending.append(command)
    task = asyncio.create_task(loop._drain_steering(context, SimpleNamespace(emit=AsyncMock()), 1))
    await asyncio.wait_for(entered.wait(), 2)
    if race == "stop":
        runtime.stop_requested = True
    else:
        ownership.release(activation)
    release.set()
    with pytest.raises(asyncio.CancelledError if race == "stop" else RuntimeError):
        await task
    assert list(loop.pending) == [command]  # Never consumed, no automatic replay.
    assert context.messages == []
    assert loop._boundary_pending
    assert not any(event["type"] in {"input.delivered", "input.unknown"} for event in runtime.events)


async def test_async_attachment_checkpoint_lost_ack_is_unknown_not_redelivered():
    from amplifier_module_loop_live.orchestrator import BundleLiveOrchestrator
    runtime, context, entered = Runtime(), Context(), asyncio.Event()
    encoded = {"content": "encoded", "metadata": {MEDIA: carrier()["metadata"][MEDIA]}}
    async def encode(*args):
        return copy.deepcopy(encoded)
    async def checkpoint():
        assert context.messages[0]["metadata"][MEDIA] == encoded["metadata"][MEDIA]
        entered.set()
        raise OSError("saved append, acknowledgment lost")
    capabilities = {"live.attachments.encode": encode, "session.durable_checkpoint": checkpoint}
    coordinator = SimpleNamespace(get_capability=capabilities.get)
    command = Input("user", "prompt", id="upload-lost", attachments=("upload",))
    loop = BundleLiveOrchestrator({})
    loop.host, loop.coordinator, loop.runtime = HostAdapter(), coordinator, runtime
    loop.pending.append(command)
    with pytest.raises(RuntimeError, match="checkpoint failed") as error:
        await loop._drain_steering(context, SimpleNamespace(emit=AsyncMock()), 1)
    assert isinstance(error.value.__cause__, OSError)
    assert entered.is_set() and not loop.pending
    assert len(context.messages) == 1 and loop._boundary_pending
    assert [event["type"] for event in runtime.events] == ["input.unknown"]
    assert runtime.events[0]["input_id"] == command.id
    await loop._drain_steering(context, SimpleNamespace(emit=AsyncMock()), 2)
    assert len(context.messages) == 1


@pytest.mark.parametrize("race", ["stop", "revoke"])
async def test_initial_async_attachment_does_not_enter_engine_after_admission_loss(monkeypatch, race):
    from amplifier_module_loop_live.orchestrator import BundleLiveOrchestrator, StreamingOrchestrator
    from test_activation import Ownership
    ownership, runtime, context = Ownership(), Runtime(), Context()
    activation = ownership.activate()
    runtime.capture_activation = ownership.current
    entered, release = asyncio.Event(), asyncio.Event()
    async def encode(*args):
        entered.set()
        await release.wait()
        return {"content": "not admitted", "metadata": {MEDIA: carrier()["metadata"][MEDIA]}}
    engine = AsyncMock(return_value="must not run")
    monkeypatch.setattr(StreamingOrchestrator, "execute", engine)
    capabilities = {"live.runtime": runtime, "live.activation": ownership,
                    "live.attachments.encode": encode}
    coordinator = SimpleNamespace(get_capability=capabilities.get, config={},
        register_capability=lambda key, value: capabilities.__setitem__(key, value), mount=AsyncMock())
    hooks = SimpleNamespace(register=lambda *a, **kw: lambda: None, emit=AsyncMock())
    loop = BundleLiveOrchestrator({})
    command = Input("user", "prompt", id="initial-race", attachments=("upload",),
                    activation=activation)
    task = asyncio.create_task(loop.execute("", context, {"fixture": SimpleNamespace()},
                                           {}, hooks, coordinator))
    try:
        await runtime.submit(command)
        await asyncio.wait_for(entered.wait(), 2)
        if race == "stop":
            await runtime.submit(Input("stop"))
        else:
            ownership.release(activation)
        release.set()
        await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), 2)
        engine.assert_not_awaited()
        assert context.messages == []
        assert not any(e["type"] == "input.delivered" and e.get("input_id") == command.id
                       for e in runtime.events)
        held = [e for e in runtime.events if e["type"] == "input.held" and e["input_id"] == command.id]
        assert len(held) == 1 and held[0]["reason"] == "attachment-preparation-not-admitted"
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_cancel_during_attachment_checkpoint_preserves_append_and_unknown_disposition():
    from amplifier_module_loop_live.orchestrator import BundleLiveOrchestrator
    runtime, context = Runtime(), Context()
    entered, release = asyncio.Event(), asyncio.Event()
    encoded = {"content": "canonical attachment", "metadata": {MEDIA: carrier()["metadata"][MEDIA]}}
    async def encode(*args):
        return copy.deepcopy(encoded)
    async def checkpoint():
        entered.set()
        await release.wait()
        return {"saved": True}
    capabilities = {"live.attachments.encode": encode, "session.durable_checkpoint": checkpoint}
    coordinator = SimpleNamespace(get_capability=capabilities.get)
    loop = BundleLiveOrchestrator({})
    loop.host, loop.coordinator, loop.runtime = HostAdapter(), coordinator, runtime
    command = Input("user", "prompt", id="cancel-checkpoint", attachments=("upload",))
    loop.pending.append(command)
    task = asyncio.create_task(loop._drain_steering(context, SimpleNamespace(emit=AsyncMock()), 1))
    await asyncio.wait_for(entered.wait(), 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not loop.pending and len(context.messages) == 1
    assert context.messages[0]["metadata"][MEDIA] == encoded["metadata"][MEDIA]
    assert [event["type"] for event in runtime.events] == ["input.unknown"]
    assert loop._boundary_pending  # No request may dispatch from this failed drain.
    await loop._drain_steering(context, SimpleNamespace(emit=AsyncMock()), 2)
    assert len(context.messages) == 1


@pytest.mark.parametrize("checkpoint_outcome", ["cancel", "lost-ack", "ack"])
async def test_initial_attachment_delivery_requires_append_checkpoint_ack(checkpoint_outcome):
    """Actual engine new-turn append; a native provider is not needed for this race."""
    from amplifier_core import AmplifierSession
    from test_loop import Provider
    runtime, provider = Runtime(), Provider()
    entered, release = asyncio.Event(), asyncio.Event()
    session = AmplifierSession({"session": {
        "orchestrator": {"module": "loop-live", "config": {"min_delay_between_calls_ms": 0}},
        "context": {"module": "context-simple"}}, "providers": []})
    await session.initialize()
    owner, checkpoints = session.coordinator, []
    context = owner.get("context")
    encoded = {"content": "initial canonical attachment",
               "metadata": {MEDIA: carrier()["metadata"][MEDIA]}}
    async def encode(*args):
        return copy.deepcopy(encoded)
    async def checkpoint():
        rows = await context.get_messages()
        checkpoints.append(copy.deepcopy(rows))
        if len(checkpoints) == 1:
            entered.set()
            await release.wait()
            if checkpoint_outcome == "lost-ack":
                raise OSError("fixture lost durable acknowledgment")
        return {"saved": True}
    owner.register_capability("live.runtime", runtime)
    owner.register_capability("live.attachments.encode", encode)
    owner.register_capability("session.durable_checkpoint", checkpoint)
    await owner.mount("providers", provider, name="fixture")
    command = Input("user", "initial prompt", id="initial-checkpoint", attachments=("upload",))
    task = asyncio.create_task(session.execute(""))
    try:
        await runtime.wait_for(lambda e: e["type"] == "session.ready", 2)
        await runtime.submit(command)
        await asyncio.wait_for(entered.wait(), 2)
        assert provider.requests.empty()
        assert not any(e["type"] == "input.delivered" and e.get("input_id") == command.id
                       for e in runtime.events)
        if checkpoint_outcome == "cancel":
            await runtime.submit(Input("stop"))
            await asyncio.wait_for(task, 2)
        else:
            release.set()
            if checkpoint_outcome == "lost-ack":
                # The actual Rust session wraps the engine failure, preserving
                # no-replay status rather than the inner checkpoint exception.
                with pytest.raises(RuntimeError, match="not automatically replayed"):
                    await asyncio.wait_for(task, 2)
            else:
                await provider.request()
                await provider.reply("done")
                await runtime.wait_for(lambda e: e["type"] == "generation.finished", 2)
        rows = await context.get_messages()
        matching = [m for m in rows if (m.get("metadata") or {}).get("amplifier_input", {}).get("id") == command.id]
        assert len(matching) == 1 and matching[0]["metadata"][MEDIA] == encoded["metadata"][MEDIA]
        dispositions = [e["type"] for e in runtime.events if e.get("input_id") == command.id
                        and e["type"] in {"input.delivered", "input.unknown", "input.held"}]
        assert dispositions == ["input.delivered" if checkpoint_outcome == "ack" else "input.unknown"]
        if checkpoint_outcome != "ack":
            assert provider.requests.empty()
            terminal = next(e for e in runtime.events if e["type"] in {"generation.failed", "generation.detached"})
            assert command.id not in terminal["input_ids"]
    finally:
        release.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await session.cleanup()


@pytest.mark.parametrize("mount_name", ["delegate", "live_job"])
@pytest.mark.parametrize("failure", ["cancel", "lost-ack"])
async def test_live_mount_interruption_closes_owner_without_rolling_back_effect(mount_name, failure):
    from amplifier_core import AmplifierSession
    from amplifier_module_loop_live.jobs import AsyncTool, JobControl
    from test_loop import NativeProvider, Delegate
    runtime = Runtime()
    session = AmplifierSession({"session": {"orchestrator": {"module": "loop-live"},
                                           "context": {"module": "context-simple"}}, "providers": []})
    await session.initialize()
    owner = session.coordinator
    driver = NativeProvider(runtime)
    driver.close_live = AsyncMock()
    await owner.mount("providers", driver, name="fixture")
    await owner.mount("tools", Delegate(), name="delegate")
    owner.register_capability("live.runtime", runtime)
    entered = asyncio.Event()
    attempted = []
    class MountView:
        def __getattr__(self, name):
            return getattr(owner, name)
        async def mount(self, kind, module, *, name):
            await owner.mount(kind, module, name=name)
            attempted.append(name)
            if name == mount_name:
                entered.set()
                if failure == "lost-ack":
                    raise RuntimeError("mount acknowledgment lost")
                await asyncio.Event().wait()
    view = MountView()
    loop = owner.get("orchestrator")
    task = asyncio.create_task(loop.execute("", owner.get("context"), owner.get("providers"),
                                           owner.get("tools"), owner.hooks, view))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        if failure == "cancel":
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            with pytest.raises(RuntimeError, match="mount acknowledgment lost"):
                await task
        assert not loop.running and loop.runtime is None and runtime.closed
        assert runtime.steering_wake is None and runtime.anchored_steering_mode is None
        driver.close_live.assert_awaited_once()
        assert isinstance(owner.get("tools", mount_name), AsyncTool if mount_name == "delegate" else JobControl)
        assert attempted == (["delegate"] if mount_name == "delegate" else ["delegate", "live_job"])
        assert not driver.requests.qsize()
        assert not any(event["type"] == "session.ready" for event in runtime.events)
        assert len([event for event in runtime.events if event["type"] == "session.closed"]) == 1
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await session.cleanup()