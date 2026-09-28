"""Recovery compares checkpoint evidence, not the presence of display notices."""

import copy
import json

import pytest
from amplifier_core.message_models import ToolCall

from amplifier_module_loop_live.job_store import JobStore


@pytest.fixture
def ledger(tmp_path):
    store = JobStore(tmp_path / "jobs")
    yield store
    store.close()


def finish_job(ledger, status="returned"):
    call = ToolCall(id="call", name="delegate", arguments={"instruction": "fixture"})
    ledger.begin(call, "job", "queued receipt")
    ledger.finish(call.id, "saved report", status)
    return ledger.rows[call.id]


def checkpoint(row, *, content=None):
    return [
        {"role": "assistant", "content": "", "tool_calls": [row["tool_call"]]},
        {"role": "tool", "tool_call_id": row["call_id"], "name": "delegate",
         "content": row["result"] if content is None else content},
    ]


def notices(messages):
    return [m for m in messages if (m.get("metadata") or {}).get("live_recovery_job")]


def without_notices(messages):
    return [m for m in messages if not (m.get("metadata") or {}).get("live_recovery_job")]


def reopen(ledger):
    directory = ledger.directory
    ledger.close()
    return JobStore(directory)


def test_complete_checkpoint_is_silent_without_marker_or_ledger_write(ledger):
    row = finish_job(ledger)
    original = checkpoint(row)
    expected = copy.deepcopy(original)
    before = ledger._path(row["call_id"]).read_bytes()
    restored, recovered = ledger.recover(original)
    assert restored == original == expected
    assert recovered == []
    assert ledger._path(row["call_id"]).read_bytes() == before
    assert "recovery_notice" not in ledger.rows["call"]


def test_compaction_dropping_only_returned_notice_is_quiet_across_process_resume(ledger):
    finish_job(ledger)
    repaired, recovered = ledger.recover([])
    assert len(recovered) == len(notices(repaired)) == 1
    compacted = without_notices(repaired)
    resumed = reopen(ledger)
    try:
        restored, recovered = resumed.recover(compacted)
        assert restored == compacted
        assert recovered == []
        assert resumed.recover(restored) == (restored, [])
    finally:
        resumed.close()


@pytest.mark.parametrize("status", ["cancelled", "failed", "interrupted"])
def test_uncertainty_is_reported_once_even_after_notice_compaction(ledger, status):
    row = finish_job(ledger, status)
    original = checkpoint(row)
    restored, recovered = ledger.recover(original)
    notice = notices(restored)[0]
    assert notice["metadata"]["recovery"] == {
        "version": 1, "job_id": "job", "call_id": "call", "status": status,
        "outcome": "unconfirmed", "reason": "uncertain_outcome",
    }
    assert notice["metadata"]["amplifier_input"]["kind"] == "service"
    assert "not instructions or approval" in notice["content"]
    assert "Do not automatically repeat" in notice["content"]
    assert len(recovered) == 1
    resumed = reopen(ledger)
    try:
        assert resumed.recover(without_notices(restored)) == (original, [])
        assert resumed.recover(restored) == (restored, [])
    finally:
        resumed.close()


@pytest.mark.parametrize("gap", ["call", "result", "both", "receipt"])
def test_old_checkpoint_is_repaired_despite_durable_notice_fingerprint(ledger, gap):
    row = finish_job(ledger)
    latest, _ = ledger.recover([])
    older = checkpoint(row)
    if gap == "call":
        older = older[1:]
    elif gap == "result":
        older = older[:1]
    elif gap == "both":
        older = [{"role": "user", "content": "compacted summary"}]
    else:
        older[1]["content"] = row["receipt"]
    expected = copy.deepcopy(older)
    resumed = reopen(ledger)
    try:
        restored, recovered = resumed.recover(older)
        assert older == expected
        assert len(recovered) == 1
        assert [m["content"] for m in restored if m["role"] == "tool"] == [row["result"]]
        assert notices(restored)[0]["metadata"]["recovery"]["reason"] == (
            "changed_evidence" if gap == "receipt" else "restored_evidence")
        assert resumed.recover(restored) == (restored, [])
        # The ledger cannot be used to redispatch an already saved call.
        with pytest.raises(RuntimeError, match="cannot be dispatched"):
            resumed.begin(ToolCall(**row["tool_call"]), "another-job", "receipt")
    finally:
        resumed.close()


def test_retained_notice_never_suppresses_actual_restoration(ledger):
    row = finish_job(ledger)
    messages, _ = ledger.recover([])
    compacted = notices(messages)
    repaired, recovered = ledger.recover(compacted)
    assert len(recovered) == 1
    assert len(notices(repaired)) == 1
    assert notices(repaired) == compacted
    assert next(m for m in repaired if m["role"] == "tool")["content"] == row["result"]
    assert ledger.recover(repaired) == (repaired, [])


def test_legacy_checkpoint_notice_is_adopted_without_duplicate(ledger):
    row = finish_job(ledger, "cancelled")
    original = checkpoint(row) + [{"role": "user", "content": "legacy recovery observation",
                                  "metadata": {"live_recovery_job": "job"}}]
    assert ledger.recover(original) == (original, [])
    assert ledger.rows["call"].get("recovery_notice")
    assert ledger.recover(without_notices(original)) == (without_notices(original), [])


def test_conflict_is_rejected_even_with_notice_fingerprint_and_matching_duplicate(ledger):
    row = finish_job(ledger)
    ledger.recover([])
    conflict = checkpoint(row) + [{"role": "tool", "tool_call_id": "call", "content": "conflict"}]
    expected = copy.deepcopy(conflict)
    before = ledger._path("call").read_bytes()
    with pytest.raises(RuntimeError, match="conflicts"):
        ledger.recover(conflict)
    assert conflict == expected
    assert ledger._path("call").read_bytes() == before


def test_pending_checkpoint_retains_uncertain_effects_and_never_requeues(ledger):
    call = ToolCall(id="call", name="delegate", arguments={})
    ledger.begin(call, "job", "queued receipt")
    original = [{"role": "assistant", "content": [{"type": "tool_call", "id": "call",
                                                      "name": "delegate", "input": {}}]},
                {"role": "tool", "tool_call_id": "call", "content": "queued receipt"}]
    repaired, recovered = ledger.recover(original)
    result = json.loads(next(m for m in repaired if m["role"] == "tool")["content"])
    assert result["status"] == recovered[0]["status"] == "interrupted"
    assert result["effects"] == "not_rolled_back"
    assert result["outcome"] == "unconfirmed"
    assert len(notices(repaired)) == 1
    assert ledger.recover(without_notices(repaired)) == (without_notices(repaired), [])


@pytest.mark.asyncio
@pytest.mark.parametrize("missing_evidence", [False, True])
async def test_mounted_live_runtime_announces_only_reconciliation_without_execution(ledger, missing_evidence):
    import asyncio
    from types import SimpleNamespace
    from amplifier_core import AmplifierSession
    from amplifier_module_loop_live.runtime import Runtime

    row = finish_job(ledger)
    messages, recovered = ledger.recover([] if missing_evidence else checkpoint(row))
    runtime = Runtime()
    session = AmplifierSession({"session": {
        "orchestrator": {"module": "loop-live"}, "context": {"module": "context-simple"}},
        "providers": []}, session_id=runtime.session_id)

    class Provider:
        name = "fixture"
        config = {}
        calls = 0
        def get_info(self):
            return SimpleNamespace(default_model="fixture")
        async def complete(self, *args, **kwargs):
            self.calls += 1
            raise AssertionError("Recovery must not start a provider call")

    task = None
    provider = Provider()
    try:
        await session.initialize()
        coordinator = session.coordinator
        await coordinator.get("context").set_messages(messages)
        stored_messages = await coordinator.get("context").get_messages()
        await coordinator.mount("providers", provider, name="fixture")
        coordinator.register_capability("live.runtime", runtime)
        coordinator.register_capability("live.jobs", ledger)
        coordinator.register_capability("live.recovered_jobs", [r["job_id"] for r in recovered])
        task = asyncio.create_task(session.execute(""))
        await runtime.wait_for(lambda event: event["type"] == "session.idle", 3)
        events = [event for event in runtime.events if event["type"] == "job.recovered"]
        assert len(events) == int(missing_evidence)
        if events:
            assert events[0]["job_id"] == "job"
            assert events[0]["call_id"] == "call"
        saved_job = coordinator.get("orchestrator").jobs["job"]
        assert saved_job["restored"] and saved_job["task"].done()
        assert provider.calls == 0
        assert await coordinator.get("context").get_messages() == stored_messages
    finally:
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await session.cleanup()
