"""Synthetic live-owned carrier boundaries; no image capture or model calls."""
import asyncio
import copy
import json
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from amplifier_core.message_models import ToolCall
from amplifier_module_loop_live.job_store import JobStore, result_fields
from amplifier_module_loop_live.jobs import JobControl
from amplifier_module_loop_live.orchestrator import BundleLiveOrchestrator, StreamingOrchestrator
from amplifier_module_loop_live.provenance import InputContext, input_metadata
from amplifier_module_loop_live.runtime import Input, Runtime


MEDIA = "experimental.media.v1"


def carrier(*, call_id="call", content="post-hook report", digest="a"):
    ref = {"v": 1, "session_id": "fixture", "sha256": digest * 64, "mime": "image/png",
           "bytes": 29, "width": 1, "height": 1,
           "provenance": {"kind": "tool", "session_id": "fixture", "id": call_id,
                          "created_at": "2026-01-01T00:00:00+00:00", "expires_at": None}}
    metadata = {MEDIA: {"refs": [ref, copy.deepcopy(ref)]}, "owner": {"keep": True}}
    return {"version": 1, "tool_call_id": call_id, "tool_name": "delegate",
            "content": content, "metadata": metadata,
            "result": {"success": True, "output": {"report": content, MEDIA: metadata[MEDIA]}},
            "success": True}


class Context:
    def __init__(self, messages=()):
        self.messages = copy.deepcopy(list(messages))

    async def get_messages(self):
        return copy.deepcopy(self.messages)

    async def set_messages(self, messages):
        self.messages = copy.deepcopy(messages)

    async def add_message(self, message):
        self.messages.append(copy.deepcopy(message))


class CarrierLedgerTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.store = JobStore(self.directory.name)
        self.addCleanup(lambda: self.store.close())
        self.call = ToolCall(id="call", name="delegate", arguments={"async": True})
        self.store.begin(self.call, "job", "queued", {"call_id": "call", "async": True})

    def finish(self, value=None):
        self.store.finish("call", carrier() if value is None else value, "returned")
        return self.store.rows["call"]

    def test_v2_carrier_roundtrip_and_missing_checkpoint_restore_exact_refs(self):
        value = carrier()
        row = self.finish(value)
        self.assertEqual(row["version"], 2)
        value["metadata"][MEDIA]["refs"].clear()
        self.store.close()
        self.store = JobStore(self.directory.name)
        messages, recovered = self.store.recover([])
        tool = next(m for m in messages if m["role"] == "tool")
        self.assertEqual(tool["metadata"], carrier()["metadata"])
        self.assertEqual(tool["content"], carrier()["content"])
        self.assertEqual(recovered[0]["settled_result"], carrier())
        self.assertEqual(messages[0]["metadata"]["converge_native_async_calls"][0]["call_id"], "call")
        self.assertEqual(self.store.recover(messages), (messages, []))
        notice = next(m for m in messages if m["role"] == "user")
        self.assertNotIn(MEDIA, notice["metadata"])
        self.assertEqual(notice["metadata"]["amplifier_input"]["kind"], "service")

    def test_queued_replacement_preserves_unrelated_metadata_and_original_order(self):
        self.finish()
        original = [{"role": "tool", "tool_call_id": "call", "content": "queued",
                     "metadata": {"opaque": {"signature": "unchanged"}}}]
        messages, _ = self.store.recover(original)
        tool = next(m for m in messages if m["role"] == "tool")
        self.assertEqual(tool["metadata"][MEDIA], carrier()["metadata"][MEDIA])
        self.assertEqual(tool["metadata"]["opaque"], original[0]["metadata"]["opaque"])
        self.assertEqual(original[0]["content"], "queued")
        self.assertEqual(messages[0]["role"], "assistant")

    def test_equal_text_different_refs_or_missing_refs_is_conflict(self):
        row = self.finish()
        for metadata in ({}, carrier(digest="b")["metadata"]):
            with self.subTest(metadata=metadata):
                with self.assertRaisesRegex(RuntimeError, "metadata conflicts"):
                    self.store.recover([{"role": "tool", "tool_call_id": "call",
                                         "content": row["result"], "metadata": metadata}])

    def test_ref_order_and_multiplicity_are_part_of_evidence(self):
        value = carrier()
        refs = value["metadata"][MEDIA]["refs"]
        refs.append(copy.deepcopy(carrier(digest="b")["metadata"][MEDIA]["refs"][0]))
        row = self.finish(value)
        for replacement in (refs[:1], list(reversed(refs))):
            metadata = {**value["metadata"], MEDIA: {"refs": replacement}}
            with self.subTest(refs=replacement), self.assertRaisesRegex(RuntimeError, "metadata conflicts"):
                self.store.recover([{"role": "tool", "tool_call_id": "call",
                                     "content": row["result"], "metadata": metadata}])

    def test_checkpoint_cannot_change_tool_name_even_with_matching_refs(self):
        row = self.finish()
        with self.assertRaisesRegex(RuntimeError, "conflicts"):
            self.store.recover([{"role": "tool", "tool_call_id": "call", "name": "other-tool",
                                 "content": row["result"], "metadata": carrier()["metadata"]}])

    def test_duplicate_output_conflict_is_not_hidden_by_matching_first_output(self):
        row = self.finish()
        outputs = [{"role": "tool", "tool_call_id": "call", "content": row["result"],
                    "metadata": carrier()["metadata"]},
                   {"role": "tool", "tool_call_id": "call", "content": row["result"],
                    "metadata": carrier(digest="b")["metadata"]}]
        with self.assertRaisesRegex(RuntimeError, "metadata conflicts"):
            self.store.recover(outputs)

    def test_queued_checkpoint_cannot_smuggle_refs(self):
        self.finish()
        with self.assertRaisesRegex(RuntimeError, "metadata conflicts"):
            self.store.recover([{"role": "tool", "tool_call_id": "call", "content": "queued",
                                 "metadata": carrier()["metadata"]}])

    def test_identity_or_version_conflict_does_not_publish(self):
        before = self.store._path("call").read_bytes()
        for value in (carrier(call_id="other"), {**carrier(), "version": 2}):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    self.finish(value)
                self.assertEqual(self.store._path("call").read_bytes(), before)
                self.assertEqual(self.store.rows["call"]["status"], "pending")

    def test_changed_structured_result_fingerprint_refuses_reload(self):
        row = copy.deepcopy(self.finish())
        row["settled_result"]["metadata"] = carrier(digest="b")["metadata"]
        self.store._write(row)  # Model an untrusted/corrupt saved file, not a valid finish.
        self.store.close()
        with self.assertRaisesRegex(ValueError, "fingerprint conflicts"):
            JobStore(self.directory.name)

    def test_v2_cannot_lose_carrier_and_downgrade_silently_to_text(self):
        row = copy.deepcopy(self.finish())
        del row["settled_result"]
        self.store._write(row)
        self.store.close()
        with self.assertRaisesRegex(ValueError, "missing its settled result"):
            JobStore(self.directory.name)

    def test_legacy_json_looking_report_never_promotes_refs(self):
        report = json.dumps(carrier())
        row = self.finish(report)
        self.assertNotIn("settled_result", row)
        messages, _ = self.store.recover([])
        tool = next(m for m in messages if m["role"] == "tool")
        self.assertEqual(tool["content"], report)
        self.assertNotIn("metadata", tool)
        self.assertEqual(row["version"], 1)

    def test_pending_process_loss_is_unknown_without_refs_or_redispatch(self):
        messages, recovered = self.store.recover([])
        self.assertEqual(recovered[0]["status"], "interrupted")
        tool = next(m for m in messages if m["role"] == "tool")
        self.assertEqual(json.loads(tool["content"])["outcome"], "unconfirmed")
        self.assertNotIn("metadata", tool)
        with self.assertRaisesRegex(RuntimeError, "cannot be dispatched"):
            self.store.begin(self.call, "new-job", "new receipt")

    def test_failure_and_cancel_do_not_claim_returned_media(self):
        for outcome in ("failed", "cancelled"):
            fields = result_fields(None, outcome)
            self.assertNotIn("settled_result", fields)
            self.assertEqual(json.loads(fields["result"])["status"], outcome)

    def test_real_process_death_before_and_after_finish_never_reexecutes(self):
        from test_job_store import _python_command
        for finish in (False, True):
            with self.subTest(finish=finish), tempfile.TemporaryDirectory() as directory:
                code = """
import os,sys
from pathlib import Path
from amplifier_core.message_models import ToolCall
from amplifier_module_loop_live.job_store import JobStore
root=Path(sys.argv[1]); store=JobStore(root/'jobs')
store.begin(ToolCall(id='call',name='delegate',arguments={}), 'job', 'queued')
(root/'effect-count').write_text('1')
""" + (f"store.finish('call', {carrier()!r}, 'returned')\n" if finish else "") + "os._exit(91)\n"
                process = subprocess.run(_python_command(code, directory), capture_output=True, timeout=15)
                self.assertEqual(process.returncode, 91, process.stderr)
                store = JobStore(Path(directory) / "jobs")
                try:
                    messages, recovered = store.recover([])
                    self.assertEqual((Path(directory) / "effect-count").read_text(), "1")
                    self.assertEqual(recovered[0]["status"], "returned" if finish else "interrupted")
                    tool = next(m for m in messages if m["role"] == "tool")
                    if finish:
                        self.assertEqual(tool["metadata"], carrier()["metadata"])
                    else:
                        self.assertNotIn("metadata", tool)
                finally:
                    store.close()


class LiveCarrierTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.loop = BundleLiveOrchestrator({"background_delegate": True})
        self.loop.runtime = Runtime("fixture")
        self.capabilities = {}
        self.loop.coordinator = SimpleNamespace(get_capability=self.capabilities.get)
        self.call = ToolCall(id="call", name="delegate", arguments={"async": True})

    async def launch(self, value=None, failure=None):
        async def execute(*args, **kwargs):
            if failure is not None:
                raise failure
            return carrier() if value is None else value
        with patch.object(StreamingOrchestrator, "_execute_tool_only", execute):
            await self.loop._execute_tool_only(self.call, {}, None, None, self.loop.coordinator)
            job_id, job = next(iter(self.loop.jobs.items()))
            await job["task"]
        return job_id, job

    async def returned(self, value=None, deliver=True):
        job_id, job = await self.launch(value)
        kind, args = self.loop.runtime.inbox.get_nowait()
        self.assertEqual(kind, "bundle_job")
        await self.loop._job_returned(*args, deliver=deliver)
        return job_id, job

    async def test_commit_is_before_inbox_publication_and_service_keeps_nested_metadata(self):
        with tempfile.TemporaryDirectory() as directory:
            store = JobStore(directory)
            self.addCleanup(store.close)
            self.capabilities["live.jobs"] = store
            job_id, job = await self.launch()
            self.assertEqual(store.rows["call"]["settled_result"], carrier())
            self.assertNotIn("result", job)  # Inbox was not applied yet.
            _, args = self.loop.runtime.inbox.get_nowait()
            await self.loop._job_returned(*args, deliver=True)
            command = self.loop.pending[0]
            metadata = input_metadata(command)
            self.assertEqual(metadata["live_job_result"]["metadata"], carrier()["metadata"])
            self.assertNotIn(MEDIA, metadata)  # Do not relocate tool pixels to a fabricated user role.
            self.assertEqual(metadata["amplifier_input"]["call_id"], "call")
            self.assertEqual(metadata["amplifier_input"]["kind"], "service")
            self.assertEqual(command.job_result["job_id"], job_id)
            self.assertEqual(job["result"], carrier()["content"])

    async def test_service_metadata_survives_initial_and_boundary_delivery_without_alias(self):
        await self.returned()
        command = self.loop.pending[0]
        context = Context()
        tagged = InputContext(context, self.loop._text(command), command)
        await tagged.add_message({"role": "user", "content": self.loop._text(command)})
        await self.loop._drain_steering(context, SimpleNamespace(emit=AsyncMock()), 1)
        self.assertEqual(len(context.messages), 2)
        self.assertEqual(context.messages[0]["metadata"], context.messages[1]["metadata"])
        context.messages[0]["metadata"]["live_job_result"]["metadata"][MEDIA]["refs"].clear()
        self.assertEqual(len(command.job_result["metadata"][MEDIA]["refs"]), 2)

    async def test_read_and_wait_page_text_but_keep_explicit_refs_independent_of_offset(self):
        value = carrier(content="{" + "clipped report" * 1000)
        job_id, job = await self.returned(value)
        control = JobControl(self.loop)
        for action, offset in (("read", 1), ("wait", 9000), ("read", 999999)):
            result = await control.execute({"action": action, "job_id": job_id, "offset": offset, "limit": 7})
            self.assertTrue(result.success)
            self.assertEqual(result.output["report"], value["content"][offset:offset + 7])
            self.assertEqual(result.output["result_metadata"][MEDIA], value["metadata"][MEDIA])
            self.assertNotIn(MEDIA, result.output)  # Original call origin, not live_job's new call.
            self.assertEqual(result.output["result_fingerprint"], job["result_fingerprint"])
            result.output["result_metadata"][MEDIA]["refs"].clear()
            self.assertEqual(len(job["settled_result"]["metadata"][MEDIA]["refs"]), 2)
        listing = await control.execute({"action": "list"})
        self.assertNotIn(MEDIA, listing.output["jobs"][0])
        self.assertNotIn("result_metadata", listing.output["jobs"][0])

    async def test_json_looking_legacy_page_never_promotes_refs(self):
        job_id, _ = await self.returned(("call", "delegate", json.dumps(carrier())))
        page = await JobControl(self.loop).execute({"action": "read", "job_id": job_id, "limit": 3})
        self.assertNotIn(MEDIA, page.output)
        self.assertNotIn("result_metadata", page.output)

    async def test_pending_wait_wakes_with_explicit_metadata_after_inbox_settlement(self):
        job_id, _ = await self.launch()
        waiting = asyncio.Event()
        original = self.loop.runtime.wait_for
        async def wait_for(*args, **kwargs):
            waiting.set()
            return await original(*args, **kwargs)
        self.loop.runtime.wait_for = wait_for
        waiter = asyncio.create_task(JobControl(self.loop).execute(
            {"action": "wait", "job_id": job_id, "limit": 3, "timeout": 2}))
        await asyncio.wait_for(waiting.wait(), 2)
        _, args = self.loop.runtime.inbox.get_nowait()
        await self.loop._job_returned(*args, deliver=True)
        result = await asyncio.wait_for(waiter, 2)
        self.assertEqual(result.output["wake_reason"], "settled")
        self.assertEqual(result.output["report"], carrier()["content"][:3])
        self.assertEqual(result.output["result_metadata"], carrier()["metadata"])

    async def test_synchronize_at_original_call_retains_owner_metadata(self):
        _, job = await self.returned()
        self.capabilities["session.durable_checkpoint"] = AsyncMock(return_value={"saved": True})
        context = Context([{"role": "tool", "tool_call_id": "call", "content": job["receipt"],
                            "metadata": {"signature": "preserved"}}])
        await self.loop._synchronize_job_results(context)
        self.assertEqual(context.messages[0]["metadata"][MEDIA], carrier()["metadata"][MEDIA])
        self.assertEqual(context.messages[0]["metadata"]["signature"], "preserved")
        self.assertEqual(context.messages[0]["content"], carrier()["content"])

    async def test_sync_checkpoint_failure_keeps_settled_evidence_and_refuses_until_reconciled(self):
        _, job = await self.returned()
        context = Context([{"role": "tool", "tool_call_id": "call", "content": job["receipt"]}])
        with self.assertRaisesRegex(RuntimeError, "durable session checkpoint"):
            await self.loop._synchronize_job_results(context)
        self.assertEqual(context.messages[0]["metadata"][MEDIA], carrier()["metadata"][MEDIA])
        checkpoint = AsyncMock(return_value=False)
        self.capabilities["session.durable_checkpoint"] = checkpoint
        with self.assertRaisesRegex(RuntimeError, "checkpoint failed"):
            await self.loop._synchronize_job_results(context)
        checkpoint.return_value = {"saved": True}
        await self.loop._synchronize_job_results(context)
        self.assertEqual(checkpoint.await_count, 2)
        await self.loop._synchronize_job_results(context)
        self.assertEqual(checkpoint.await_count, 2)

    async def test_persistence_failure_publishes_no_return_or_service(self):
        store = SimpleNamespace(begin=lambda *a: None, finish=lambda *a: (_ for _ in ()).throw(OSError("fixture")))
        self.capabilities["live.jobs"] = store
        await self.launch()
        kind, _ = self.loop.runtime.inbox.get_nowait()
        self.assertEqual(kind, "bundle_persistence_error")
        self.assertFalse(self.loop.pending)
        self.assertFalse(any(e["type"] == "job.returned" for e in self.loop.runtime.events))

    async def test_recovered_exact_metadata_requires_fresh_host_checkpoint(self):
        _, job = await self.returned()
        context = Context([{"role": "tool", "tool_call_id": "call", "content": job["result"],
                            "metadata": carrier()["metadata"]}])
        with self.assertRaisesRegex(RuntimeError, "durable session checkpoint"):
            await self.loop._synchronize_job_results(context)
        checkpoint = AsyncMock(return_value={"saved": True})
        self.capabilities["session.durable_checkpoint"] = checkpoint
        await self.loop._synchronize_job_results(context)
        checkpoint.assert_awaited_once()

    async def test_context_install_lost_ack_keeps_checkpoint_requirement(self):
        _, job = await self.returned()
        context = Context([{"role": "tool", "tool_call_id": "call", "content": job["receipt"]}])
        install = context.set_messages
        async def lost_ack(messages):
            await install(messages)
            raise OSError("installed, acknowledgment lost")
        context.set_messages = lost_ack
        with self.assertRaisesRegex(OSError, "acknowledgment lost"):
            await self.loop._synchronize_job_results(context)
        self.assertEqual(context.messages[0]["content"], job["result"])
        with self.assertRaisesRegex(RuntimeError, "durable session checkpoint"):
            await self.loop._synchronize_job_results(context)
        checkpoint = AsyncMock(return_value={"saved": True})
        self.capabilities["session.durable_checkpoint"] = checkpoint
        await self.loop._synchronize_job_results(context)
        checkpoint.assert_awaited_once()

    async def test_unrelated_media_does_not_gate_legacy_text_job(self):
        _, job = await self.returned(("call", "delegate", "legacy text"))
        context = Context([{"role": "tool", "tool_call_id": "call", "content": job["receipt"]},
                           {"role": "user", "content": "other", "metadata": carrier()["metadata"]}])
        await self.loop._synchronize_job_results(context)
        self.assertEqual(context.messages[0]["content"], "legacy text")
        self.assertFalse(self.loop._job_media_checkpoint_pending)

    async def test_inbox_mutation_cannot_replace_already_saved_carrier(self):
        with tempfile.TemporaryDirectory() as directory:
            store = JobStore(directory)
            self.addCleanup(store.close)
            self.capabilities["live.jobs"] = store
            _, job = await self.launch()
            _, args = self.loop.runtime.inbox.get_nowait()
            args[1]["metadata"] = carrier(digest="b")["metadata"]
            with self.assertRaisesRegex(RuntimeError, "conflicts with the saved ledger"):
                await self.loop._job_returned(*args, deliver=True)
            self.assertNotIn("result", job)
            self.assertEqual(store.rows["call"]["settled_result"], carrier())
            self.assertFalse(self.loop.pending)

    async def test_cancellation_never_creates_refs_or_returned_claim(self):
        await self.launch(failure=asyncio.CancelledError())
        _, args = self.loop.runtime.inbox.get_nowait()
        await self.loop._job_returned(*args, deliver=True)
        job = next(iter(self.loop.jobs.values()))
        self.assertEqual(job["status"], "cancelled")
        self.assertNotIn("settled_result", job)
        self.assertEqual(self.loop.pending[0].job_result["metadata"], {})
        self.assertFalse(any(e["type"] == "job.returned" for e in self.loop.runtime.events))

    async def test_job_service_uses_boundary_provenance_not_native_steering_shortcut(self):
        runtime, context, saved = Runtime(), Context(), asyncio.Event()
        command = Input("service", "worker report", source="amplifier-delegate", call_id="call",
                        job_result={"version": 1, "metadata": carrier()["metadata"]})
        async def finite(self, prompt, ctx, *args):
            await ctx.add_message({"role": "user", "content": prompt})
            saved.set()
            return "done"
        native = SimpleNamespace(native_bundle_live=True, steer_live=AsyncMock(return_value=True),
                                 close_live=AsyncMock())
        capabilities = {"live.runtime": runtime}
        coordinator = SimpleNamespace(get_capability=capabilities.get,
                                      register_capability=lambda k, v: capabilities.__setitem__(k, v), config={})
        hooks = SimpleNamespace(register=lambda *a, **kw: lambda: None, emit=AsyncMock())
        loop = BundleLiveOrchestrator({})
        loop.root_provider = native
        await runtime.submit(command)
        with patch.object(StreamingOrchestrator, "execute", finite):
            task = asyncio.create_task(loop.execute("", context, {"fixture": native}, {}, hooks, coordinator))
            try:
                await asyncio.wait_for(saved.wait(), 2)
                await runtime.submit(Input("stop"))
                await asyncio.wait_for(task, 2)
            finally:
                if not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
        native.steer_live.assert_not_awaited()
        self.assertEqual(context.messages[0]["metadata"]["amplifier_input"]["kind"], "service")
        self.assertEqual(context.messages[0]["metadata"]["live_job_result"], command.job_result)
        native.close_live.assert_awaited_once()

    async def test_refs_only_carrier_is_exact_in_owned_state_and_ledger(self):
        # This verifies no new payload field at the live boundary; real pixel
        # removal belongs to streaming's independently tested ingest/post seam.
        with tempfile.TemporaryDirectory() as directory:
            store = JobStore(directory)
            self.addCleanup(store.close)
            self.capabilities["live.jobs"] = store
            _, job = await self.returned()
            self.assertEqual(job["settled_result"], carrier())
            self.assertEqual(self.loop.pending[0].job_result["metadata"], carrier()["metadata"])
            for path in Path(directory).glob("job-*.json"):
                saved = json.loads(path.read_bytes())
                self.assertEqual(saved["settled_result"], carrier())
                self.assertEqual(set(saved["settled_result"]), set(carrier()))