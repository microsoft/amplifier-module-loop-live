"""Recovery preconditions at the real Live loop boundary, without model calls."""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from amplifier_core import HookRegistry, HookResult, ToolResult
from amplifier_core.message_models import ToolCall
from amplifier_module_loop_live.orchestrator import BundleLiveOrchestrator
from amplifier_module_loop_live.runtime import Input, Runtime
from amplifier_module_loop_live.job_store import JobStore


def setup(restored=None, ledger=None):
    runtime = Runtime(session_id="root")
    capabilities = {"live.runtime": runtime, "live.suspend": AsyncMock(return_value=True),
                    "live.maintenance_admit": AsyncMock(return_value=True)}
    if restored is not None:
        capabilities["live.suspended_execution"] = restored
    if ledger is not None:
        capabilities["live.jobs"] = ledger
    coordinator = SimpleNamespace(
        session_id="root", config={}, session_state={}, hooks=HookRegistry(),
        cancellation=SimpleNamespace(is_cancelled=False, register_tool_start=Mock(),
                                     register_tool_complete=Mock()),
        get_capability=capabilities.get,
        register_capability=lambda name, value: capabilities.__setitem__(name, value),
        process_hook_result=AsyncMock(return_value=HookResult()),
    )
    context = SimpleNamespace(get_messages=AsyncMock(return_value=[]), set_messages=AsyncMock())
    provider = SimpleNamespace(name="fixture", get_info=lambda: SimpleNamespace(default_model="fixture"))
    loop = BundleLiveOrchestrator({"background_tools": ["effect"]})
    # Result-observation turns are not the subject of these recovery tests.
    loop._execute_guarded_goal = AsyncMock(return_value="observed")
    return loop, runtime, coordinator, context, provider, capabilities


async def start_or_fail(parts, tools=None):
    loop, runtime, coordinator, context, provider, _ = parts
    task = asyncio.create_task(loop.execute("", context, {"fixture": provider}, tools or {},
                                            coordinator.hooks, coordinator))
    notice = asyncio.create_task(runtime.wait_for(
        lambda e: e["type"] in {"session.suspended", "session.idle"}, 2))
    try:
        done, _ = await asyncio.wait((task, notice), timeout=3, return_when=asyncio.FIRST_COMPLETED)
        assert done, "loop neither suspended nor refused"
        if task in done:
            await task
        await notice
        return task
    except BaseException:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        raise
    finally:
        notice.cancel()
        await asyncio.gather(notice, return_exceptions=True)


async def finish(task, runtime):
    if not task.done():
        await runtime.submit(Input("stop"))
    await asyncio.wait_for(task, 2)


@pytest.mark.asyncio
@pytest.mark.parametrize("version", [None, 2, True])
async def test_unsupported_envelope_refuses_before_restore_or_ack(version):
    envelope = {"version": version, "epoch": "held", "pending": []}
    parts = setup(envelope)
    task = None
    try:
        with pytest.raises(ValueError, match="version"):
            task = await start_or_fail(parts)
        assert parts[1].pause_epoch is None
        parts[5]["live.suspend"].assert_not_awaited()
    finally:
        if task:
            await finish(task, parts[1])


@pytest.mark.asyncio
async def test_no_ledger_cannot_acknowledge_paused_background_job():
    parts = setup()
    loop, runtime, _, _, _, capabilities = parts
    done = asyncio.get_running_loop().create_future()
    done.set_result(None)
    loop.jobs["job"] = {"task": done, "call_id": "call", "status": "paused",
                        "continuation": {"version": 1, "child": "child"}}
    await runtime.request_pause("held")
    task = None
    try:
        with pytest.raises(RuntimeError, match="ledger"):
            task = await start_or_fail(parts)
        capabilities["live.suspend"].assert_not_awaited()
        assert runtime.closed and runtime.events[-1]["status"] == "error"
        assert loop.jobs["job"]["continuation"] == {"version": 1, "child": "child"}
        assert not any(e["type"] == "session.suspended" for e in runtime.events)
    finally:
        if task:
            await finish(task, runtime)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["missing", "continuation", "extra", "no-envelope", "generation"])
async def test_restored_jobs_must_match_durable_ledger(tmp_path, change):
    store = JobStore(tmp_path / "jobs")
    call = ToolCall(id="call", name="effect", arguments={})
    continuation = {"version": 1, "child": "child"}
    store.begin(call, "job", "receipt")
    store.suspend(call.id, continuation)
    row = {"job_id": "job", "call_id": "call", "status": "paused", "continuation": continuation}
    envelope = {"version": 1, "epoch": "held", "jobs": [row]}
    if change == "missing":
        ledger = None
    else:
        ledger = store
        if change == "continuation":
            row["continuation"] = {"version": 1, "child": "changed"}
        elif change == "extra":
            envelope["jobs"] = []
        elif change == "generation":
            row["generation"] = {"id": "different-generation"}
        else:
            envelope = None
    parts = setup(envelope, ledger)
    task = None
    try:
        with pytest.raises(RuntimeError, match="ledger"):
            task = await start_or_fail(parts)
        parts[5]["live.suspend"].assert_not_awaited()
        assert store.rows[call.id]["status"] == "paused"
        assert store.rows[call.id]["continuation"] == continuation
    finally:
        if task:
            await finish(task, parts[1])
        store.close()


@pytest.mark.asyncio
async def test_two_paused_jobs_remount_once_with_original_group(tmp_path):
    store = JobStore(tmp_path / "jobs")
    calls = [ToolCall(id=f"call-{i}", name="effect", arguments={}) for i in range(2)]
    envelope = {"version": 1, "epoch": "held", "jobs": []}
    for i, call in enumerate(calls):
        continuation = {"version": 1, "child": f"child-{i}"}
        store.begin(call, f"job-{i}", "receipt", parallel_group_id="original-group")
        store.suspend(call.id, continuation)
        envelope["jobs"].append({"job_id": f"job-{i}", "call_id": call.id,
                                  "status": "paused", "continuation": continuation})
    store.close()
    store = JobStore(tmp_path / "jobs")
    parts = setup(envelope, store)
    _, runtime, coordinator, _, _, _ = parts
    resumed = []

    class Effect:
        name = "effect"
        input_schema = {"type": "object", "properties": {}}
        async def execute(self, arguments):
            pytest.fail("suspended operation must not be redispatched")
        async def resume_execution(self, continuation):
            dispatch = coordinator._tool_dispatch_contexts[asyncio.current_task()]
            resumed.append((continuation["child"], dict(dispatch)))
            return ToolResult(success=True, output=continuation["child"])

    task = await start_or_fail(parts, {"effect": Effect()})
    try:
        assert resumed == []
        await runtime.resume_paused("held")
        await runtime.wait_for(lambda e: e["type"] == "job.returned" and e["job_id"] == "job-1", 2)
        assert sorted(child for child, _ in resumed) == ["child-0", "child-1"]
        assert {d["tool_call_id"] for _, d in resumed} == {"call-0", "call-1"}
        assert {d["parallel_group_id"] for _, d in resumed} == {"original-group"}
        assert all(row["status"] == "returned" for row in store.rows.values())
        with pytest.raises(RuntimeError, match="changed"):
            await runtime.resume_paused("held")
        assert len(resumed) == 2
    finally:
        await finish(task, runtime)
        store.close()


@pytest.mark.parametrize("epoch", [None, 7, "", "a" * 129])
def test_invalid_epoch_refuses(epoch):
    with pytest.raises(ValueError, match="epoch"):
        BundleLiveOrchestrator._validate_suspended_envelope(
            {"version": 1, "epoch": epoch}, None)


@pytest.mark.parametrize("jobs", [None, {}, ["not-a-row"]])
def test_invalid_job_collection_refuses(jobs):
    with pytest.raises(ValueError, match="jobs"):
        BundleLiveOrchestrator._validate_suspended_envelope(
            {"version": 1, "epoch": "held", "jobs": jobs}, None)


@pytest.mark.parametrize("change", ["duplicate", "call", "status", "null-continuation"])
def test_invalid_job_reference_refuses(tmp_path, change):
    store = JobStore(tmp_path)
    call = ToolCall(id="call", name="effect", arguments={})
    store.begin(call, "job", "receipt")
    store.suspend(call.id, {"child": "held"})
    row = {"job_id": "job", "call_id": "call", "status": "paused",
           "continuation": {"child": "held"}}
    envelope = {"version": 1, "epoch": "held", "jobs": [row]}
    if change == "duplicate":
        envelope["jobs"].append(dict(row))
    elif change == "call":
        row["call_id"] = "other"
    elif change == "status":
        row["status"] = "pending"
    else:
        row["continuation"] = None
    try:
        with pytest.raises(RuntimeError, match="ledger"):
            BundleLiveOrchestrator._validate_suspended_envelope(envelope, store)
        assert store.rows[call.id]["status"] == "paused"
    finally:
        store.close()
