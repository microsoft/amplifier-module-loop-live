import asyncio

import copy

import json

import unittest

import tempfile

from unittest.mock import patch

from types import SimpleNamespace

from amplifier_core import AmplifierSession, HookResult, ToolResult, ApprovalRequest

from amplifier_core.message_models import ChatResponse, TextBlock, ToolCall

from amplifier_module_loop_live.runtime import Runtime, Input

class Provider:
    name = "fixture"
    config = {}
    def __init__(self):
        self.requests, self.responses = asyncio.Queue(), asyncio.Queue()
    async def complete(self, request, **kwargs):
        await self.requests.put(request)
        return await self.responses.get()
    def parse_tool_calls(self, response):
        return response.tool_calls or []
    def get_info(self):
        return SimpleNamespace(default_model="fixture")
    async def request(self):
        return await asyncio.wait_for(self.requests.get(), 3)
    async def reply(self, text="", calls=None):
        await self.responses.put(ChatResponse(content=[TextBlock(text=text)], tool_calls=calls))

class Delegate:
    name = "delegate"
    description = "Controlled delegate"
    input_schema = {"type": "object", "properties": {}}
    def __init__(self):
        self.release = asyncio.Event()
        self.calls = 0
    async def execute(self, input):
        self.calls += 1
        await self.release.wait()
        return ToolResult(success=True, output={"report": "CHILD-RESULT"})

class BundleTests(unittest.IsolatedAsyncioTestCase):
    async def test_compaction_accepts_input_and_continues_same_task(self):
        try:
            from amplifier_module_context_managed.boundary import mount_boundary
        except ImportError:
            self.skipTest("Install the matching context-managed development module for this integration test")
        await self.start(manager=False)
        coordinator = self.session.coordinator
        coordinator.register_capability("live.runtime", self.runtime)
        await mount_boundary(coordinator, {"max_tokens": 6000, "summarize_trigger": 0.2})
        context = coordinator.get("context")
        original = [{"role": "user", "content": "ORIGINAL TASK"},
            {"role": "assistant", "content": "Evidence " * 2400},
            {"role": "user", "content": "Keep working"}, {"role": "assistant", "content": "In progress"}]
        await context.set_messages(original)
        self.task = asyncio.create_task(self.session.execute(""))
        await self.runtime.wait_for(lambda e: e["type"] == "session.ready", 3)
        await self.runtime.submit(Input("user", "Continue"))
        summary_request = await self.provider.request()
        self.assertEqual(summary_request.metadata["purpose"], "context-compaction")
        await self.runtime.submit(Input("steer", "CORRECTION DURING COMPACTION", id="during-compact"))
        await self.runtime.wait_for(lambda e: e["type"] == "input.queued" and e["input_id"] == "during-compact", 3)
        await self.provider.reply("Original task remains active; research gathered; work remains.")
        await self.provider.request()
        await self.provider.reply("Continuing the task")
        request = await self.provider.request()
        self.assertIn("CORRECTION DURING COMPACTION", str(request.messages))
        self.assertIn("ORIGINAL TASK", str(await context.get_messages()))
        await self.provider.reply("Updated result")

    async def test_explicit_async_opt_in_and_wait_wakes_on_user_input(self):
        await self.start()
        self.loop.config["background_delegate"] = False
        await self.runtime.submit(Input("user", "background task"))
        await self.provider.request()
        await self.provider.reply(calls=[ToolCall(id="opt-in", name="delegate", arguments={"async": True})])
        request = await self.provider.request()
        self.assertIn("job_id", str(request.messages))
        job_id = next(iter(self.loop.jobs))
        await self.provider.reply(calls=[ToolCall(id="wait-job", name="live_job", arguments={"action": "wait", "job_id": job_id, "timeout": 60})])
        await self.runtime.wait_for(lambda e: e["type"] == "tool.pre" and e.get("tool") == "live_job", 3)
        await self.runtime.submit(Input("steer", "NEW REQUIREMENT", id="correction"))
        request = await self.provider.request()
        self.assertIn("NEW REQUIREMENT", str(request.messages))
        self.assertIn("input_pending", str(request.messages))
        self.assertEqual(self.tool.calls, 1)
        await self.provider.reply("accepted correction")

    async def test_explicit_sync_overrides_legacy_background_default(self):
        await self.start()
        await self.runtime.submit(Input("user", "finite delegate"))
        await self.provider.request()
        await self.provider.reply(calls=[ToolCall(id="sync", name="delegate", arguments={"async": False})])
        await self.runtime.wait_for(lambda e: e["type"] == "tool.pre", 3)
        self.assertEqual(self.loop.jobs, {})
        self.tool.release.set()
        request = await self.provider.request()
        self.assertIn("CHILD-RESULT", str(request.messages))
        await self.provider.reply("done")

    async def test_only_public_text_streams_and_compaction_stream_is_suppressed(self):
        await self.start()
        hooks = self.session.coordinator.hooks
        for kind in ("text", "thinking", "tool_use"):
            await hooks.emit("llm:stream_block_delta", {"block_type": kind, "text": kind})
        self.session.coordinator.register_capability("context.compacting", lambda: True)
        await hooks.emit("llm:stream_block_delta", {"block_type": "text", "text": "private summary"})
        events = [e for e in self.runtime.events if e["type"] == "assistant.delta"]
        self.assertEqual([e["text"] for e in events], ["text"])

    async def start(self, *, manager=True):
        self.runtime = Runtime()
        self.session = AmplifierSession({"session": {
            "orchestrator": {"module": "loop-live", "config": {"configured_bundle": True,
                "background_delegate": True, "min_delay_between_calls_ms": 0}},
            "context": {"module": "context-simple"}}, "providers": []}, session_id=self.runtime.session_id)
        await self.session.initialize()
        self.provider, self.tool = Provider(), Delegate()
        await self.session.coordinator.mount("providers", self.provider, name="fixture")
        await self.session.coordinator.mount("tools", self.tool, name="delegate")
        self.loop = self.session.coordinator.get("orchestrator")
        if manager:
            self.session.coordinator.register_capability("live.runtime", self.runtime)
            self.task = asyncio.create_task(self.session.execute(""))
            await self.runtime.wait_for(lambda e: e["type"] == "session.ready", 3)

    async def asyncTearDown(self):
        if hasattr(self, "task"):
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
        if hasattr(self, "session"):
            await self.session.cleanup()

    async def test_worker_is_finite_and_uses_prompt_factory(self):
        await self.start(manager=False)
        context = self.session.coordinator.get("context")
        async def factory():
            return "DYNAMIC-BUNDLE-INSTRUCTIONS"
        await context.set_system_prompt_factory(factory)
        self.task = asyncio.create_task(self.session.execute("worker task"))
        request = await self.provider.request()
        self.assertIn("DYNAMIC-BUNDLE-INSTRUCTIONS", str(request.messages))
        await self.provider.reply("worker finished")
        self.assertEqual(await asyncio.wait_for(self.task, 3), "worker finished")

    async def test_manager_keeps_dynamic_bundle_factory_and_adds_request_instructions(self):
        await self.start()
        context = self.session.coordinator.get("context")
        version = 1
        async def factory():
            return f"BUNDLE-PROMPT-{version}"
        await context.set_system_prompt_factory(factory)
        await self.runtime.submit(Input("user", "first"))
        request = await self.provider.request()
        self.assertIn("BUNDLE-PROMPT-1", str(request.messages))
        self.assertIn("Live manager operation", str(request.messages))
        await self.provider.reply("first answer")
        await self.runtime.wait_for(lambda e: e["type"] == "generation.finished", 3)
        version = 2
        await self.runtime.submit(Input("user", "second"))
        request = await self.provider.request()
        self.assertIn("BUNDLE-PROMPT-2", str(request.messages))
        self.assertNotIn("BUNDLE-PROMPT-1", str(request.messages))
        self.assertIn("Live manager operation", str(request.messages))
        await self.provider.reply("second answer")

    async def test_inputs_at_turn_start_and_during_request_not_lost_or_duplicated(self):
        await self.start()
        await self.runtime.submit(Input("user", "FIRST", id="one"))
        await self.runtime.submit(Input("steer", "EARLY", id="two"))
        request = await self.provider.request()
        self.assertIn("EARLY", str(request.messages))
        await self.runtime.submit(Input("steer", "DURING", id="three"))
        await self.provider.reply("first answer")
        request = await self.provider.request()
        self.assertEqual(str(request.messages).count("DURING"), 1)
        await self.provider.reply("corrected answer")
        await self.runtime.wait_for(lambda e: e["type"] == "generation.finished", 3)
        delivered = [e["input_id"] for e in self.runtime.events if e["type"] == "input.delivered"]
        self.assertEqual(sorted(delivered), ["one", "three", "two"])

    async def test_background_delegate_keeps_hooks_and_answers_side_question(self):
        await self.start()
        seen = []
        async def hook(event, data):
            seen.append((event, data.get("tool_call_id")))
            if event == "tool:post":
                return HookResult(action="modify", data={**data, "result": {"success": True, "output": "MODIFIED-REPORT"}})
            return HookResult()
        for event in ("tool:pre", "tool:post"):
            self.session.coordinator.hooks.register(event, hook, name=event)
        await self.runtime.submit(Input("user", "delegate task"))
        await self.provider.request()
        await self.provider.reply("starting worker", [ToolCall(id="worker-call", name="delegate", arguments={})])
        request = await self.provider.request()
        self.assertIn("job_id", str(request.messages))
        await self.provider.reply("worker pending")
        await self.runtime.wait_for(lambda e: e["type"] == "generation.finished", 3)
        await self.runtime.submit(Input("user", "side question"))
        await self.provider.request()
        await self.provider.reply("side answer")
        await self.runtime.wait_for(lambda e: e["type"] == "assistant.message" and e["text"] == "side answer", 3)
        self.assertEqual(self.tool.calls, 1)
        self.tool.release.set()
        request = await self.provider.request()
        self.assertIn("MODIFIED-REPORT", str(request.messages))
        self.assertIn(("tool:pre", "worker-call"), seen)
        self.assertIn(("tool:post", "worker-call"), seen)
        await self.provider.reply("worker reported")

    async def test_durable_result_is_post_hook_output_before_public_completion(self):
        from amplifier_module_loop_live.job_store import JobStore
        with tempfile.TemporaryDirectory() as directory:
            ledger = JobStore(directory)
            await self.start()
            self.session.coordinator.register_capability("live.jobs", ledger)
            async def modify(event, data):
                return HookResult(action="modify", data={**data, "result": {"success": True, "output": "SAVED-POST-HOOK"}})
            self.session.coordinator.hooks.register("tool:post", modify)
            await self.runtime.submit(Input("user", "delegate"))
            await self.provider.request()
            await self.provider.reply(calls=[ToolCall(id="saved-call", name="delegate", arguments={})])
            await self.runtime.wait_for(lambda e: e["type"] == "job.queued", 3)
            self.assertEqual(ledger.rows["saved-call"]["status"], "pending")
            self.tool.release.set()
            await self.runtime.wait_for(lambda e: e["type"] == "job.returned", 3)
            self.assertIn("SAVED-POST-HOOK", ledger.rows["saved-call"]["result"])
            self.assertEqual(self.tool.calls, 1)
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
            ledger.close()

    async def test_dispatch_does_not_run_when_intent_cannot_be_saved(self):
        await self.start()
        ledger = SimpleNamespace(begin=lambda *args: (_ for _ in ()).throw(OSError("disk unavailable")))
        self.session.coordinator.register_capability("live.jobs", ledger)
        with self.assertRaises(OSError):
            await self.loop._execute_tool_only(ToolCall(id="no-save", name="delegate", arguments={}),
                self.loop.tools, self.loop.hooks, None, self.session.coordinator)
        self.assertEqual(self.tool.calls, 0)
        self.assertEqual(self.loop.jobs, {})

    async def test_background_delegate_denial_never_executes_tool(self):
        await self.start()
        async def deny(event, data):
            return HookResult(action="deny", reason="test policy")
        self.session.coordinator.hooks.register("tool:pre", deny)
        await self.runtime.submit(Input("user", "delegate task"))
        await self.provider.request()
        await self.provider.reply(calls=[ToolCall(id="denied", name="delegate", arguments={})])
        request = await self.provider.request()
        await self.provider.reply("pending")
        if "Denied by hook" not in str(request.messages):
            request = await self.provider.request()
        self.assertIn("Denied by hook", str(request.messages))
        self.assertEqual(self.tool.calls, 0)
        await self.provider.reply("denied")


    async def test_input_after_last_request_is_delivered_in_next_turn(self):
        await self.start()
        sent = False
        async def late_input(event, data):
            nonlocal sent
            if not sent:
                sent = True
                await self.runtime.submit(Input("user", "LATE-CORRECTION", id="late"))
            return HookResult()
        self.session.coordinator.hooks.register("orchestrator:complete", late_input)
        await self.runtime.submit(Input("user", "first"))
        await self.provider.request()
        await self.provider.reply("first done")
        request = await self.provider.request()
        self.assertEqual(str(request.messages).count("LATE-CORRECTION"), 1)
        await self.provider.reply("late correction done")
        await self.runtime.wait_for(lambda e: e["type"] == "input.delivered" and e["input_id"] == "late", 3)

    async def test_background_cancel_reports_identity_and_does_not_claim_success(self):
        await self.start()
        await self.runtime.submit(Input("user", "delegate"))
        await self.provider.request()
        await self.provider.reply(calls=[ToolCall(id="cancel-me", name="delegate", arguments={})])
        await self.provider.request()
        await self.provider.reply("waiting")
        job = await self.runtime.wait_for(lambda e: e["type"] == "job.queued", 3)
        await self.runtime.submit(Input("cancel_job", target=job["job_id"]))
        cancelled = await self.runtime.wait_for(lambda e: e["type"] == "job.cancelled", 3)
        self.assertEqual(cancelled["call_id"], "cancel-me")
        self.assertEqual(cancelled["effects"], "not_rolled_back")
        self.assertFalse(any(e["type"] == "job.returned" for e in self.runtime.events))

    async def test_generation_completion_correlates_delivered_input_after_tools(self):
        await self.start()
        await self.runtime.submit(Input("user", "start", id="first"))
        await self.provider.request()
        await self.provider.reply("starting worker", [ToolCall(id="background", name="delegate", arguments={})])
        await self.provider.request()
        interim = [event for event in self.runtime.events if event["type"] == "assistant.message"]
        self.assertTrue(interim)
        self.assertFalse(any(event["type"] == "generation.finished" for event in self.runtime.events))
        await self.runtime.submit(Input("steer", "new direction", id="correction"))
        await self.provider.reply("pending")
        await self.provider.request()
        await self.provider.reply("Worker continues with the new direction.")
        finished = await self.runtime.wait_for(lambda event: event["type"] == "generation.finished", 3)
        self.assertEqual(finished["input_ids"], ["first", "correction"])
        self.assertEqual(finished["text"], "Worker continues with the new direction.")
        self.assertEqual(finished["active_job_ids"], [job_id for job_id, job in self.loop.jobs.items() if job["call_id"] == "background"])
        self.assertEqual(finished["disposition"], "manager_turn_finished")
        self.assertEqual(interim[0]["generation_id"], finished["generation_id"])

    async def test_generation_failure_has_input_identity_without_success(self):
        await self.start()
        await self.runtime.submit(Input("user", "start", id="first"))
        await self.provider.request()
        await self.provider.responses.put(RuntimeError("fixture failure"))
        failed = await self.runtime.wait_for(lambda event: event["type"] == "generation.failed", 3)
        self.assertEqual(failed["input_ids"], ["first"])
        self.assertFalse(any(event["type"] == "generation.finished" for event in self.runtime.events))
