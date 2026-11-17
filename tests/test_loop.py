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

class NativeProvider(Provider):
    """Native callback fixture, not a remote acceptance simulator."""
    native_bundle_live = True
    def __init__(self, runtime):
        super().__init__()
        self.runtime, self.steered = runtime, []
        self.native_result = True
    async def steer_live(self, command):
        if self.runtime.generation is None:
            return False
        self.steered.append(command)
        if isinstance(self.native_result, BaseException):
            raise self.native_result
        if self.native_result:
            await self.runtime.emit("steering.accepted", input_id=command.id,
                target_generation_id=self.runtime.generation["id"], delivery="native")
        return self.native_result
    async def close_live(self):
        pass

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
    async def test_busy_native_job_service_keeps_structured_evidence_on_boundary(self):
        from test_job_media import carrier, MEDIA
        await self.start(native=True)
        await self.runtime.submit(Input("user", "ORIGINAL", id="original"))
        await self.provider.request()  # A genuinely active native request.
        evidence = {"version": 1, "metadata": carrier()["metadata"]}
        command = Input("service", "WORKER REPORT", id="structured-job",
                        source="amplifier-delegate", call_id="call", job_result=evidence)
        await self.runtime.submit(command)
        await self.runtime.wait_for(lambda e: e["type"] == "input.queued"
                                   and e["input_id"] == command.id, 3)
        self.assertEqual(self.provider.steered, [])
        self.tool.release.set()
        await self.provider.reply(calls=[ToolCall(id="boundary", name="delegate", arguments={"async": False})])
        request = await self.provider.request()
        message = next(m for m in request.messages if (m.metadata or {}).get("amplifier_input", {}).get("id") == command.id)
        self.assertEqual(message.metadata["live_job_result"], evidence)
        self.assertNotIn(MEDIA, message.metadata)
        self.assertEqual(message.metadata["amplifier_input"]["kind"], "service")
        self.assertEqual(self.provider.steered, [])

    async def test_async_attachment_encoder_keeps_fifo_native_barrier(self):
        await self.start(native=True)
        entered, release = asyncio.Event(), asyncio.Event()
        async def encode(command, coordinator):
            entered.set()
            await release.wait()
            return command.text + "-ENCODED"
        self.session.coordinator.register_capability("live.attachments.encode", encode)
        await self.runtime.submit(Input("user", "ORIGINAL", id="original"))
        await self.provider.request()
        await self.runtime.submit(Input("user", "FIRST", id="async-upload", attachments=("fixture",)))
        await self.runtime.wait_for(lambda e: e["type"] == "input.queued"
                                   and e["input_id"] == "async-upload", 3)
        self.tool.release.set()
        await self.provider.reply(calls=[ToolCall(id="boundary", name="delegate", arguments={"async": False})])
        await asyncio.wait_for(entered.wait(), 3)
        await self.runtime.submit(Input("user", "SECOND", id="second"))
        await self.runtime.wait_for(lambda e: e["type"] == "input.queued"
                                   and e["input_id"] == "second", 3)
        self.assertEqual(self.provider.steered, [])
        self.assertEqual(self.loop.pending[0].id, "async-upload")
        release.set()
        first = await self.provider.request()
        self.assertIn("FIRST-ENCODED", str(first.messages))
        await self.provider.reply("first answer")
        second = await self.provider.request()
        self.assertLess(str(second.messages).index("FIRST-ENCODED"), str(second.messages).index("SECOND"))
        self.assertEqual(self.provider.steered, [])

    async def test_ordinary_native_midstep_keeps_identity_and_provider_ownership(self):
        await self.start(native=True)
        await self.runtime.submit(Input("user", "ORIGINAL", id="original"))
        await self.provider.request()
        generation = self.runtime.generation["id"]
        command = Input("user", "MIDSTEP", id="midstep")
        await self.runtime.submit(command)
        event = await self.runtime.wait_for(lambda e: e["type"] == "steering.accepted", 3)
        self.assertEqual(event["target_generation_id"], generation)
        self.assertEqual(self.provider.steered, [command])
        self.assertIs(self.runtime.accepted["midstep"], command)
        await self.runtime.submit(command)
        with self.assertRaises(ValueError):
            await self.runtime.submit(Input("user", "DIFFERENT", id="midstep"))
        await self.provider.reply("native attempt pending")
        finished = await self.runtime.wait_for(lambda e: e["type"] == "generation.finished", 3)
        self.assertEqual(finished["accepted_input_ids"], ["midstep"])
        self.assertNotIn("midstep", finished["input_ids"])
        self.assertEqual(len(self.provider.steered), 1)
        self.assertFalse(any(e["type"] == "input.queued" and e["input_id"] == "midstep"
                             for e in self.runtime.events))
        self.assertTrue(self.provider.requests.empty())

    async def test_ordinary_boundary_head_blocks_native_overtaking(self):
        await self.start(native=True)
        await self.runtime.submit(Input("user", "ORIGINAL", id="original"))
        await self.provider.request()
        self.provider.native_result = False
        await self.runtime.submit(Input("user", "FIRST", id="first"))
        await self.runtime.wait_for(lambda e: e["type"] == "input.queued" and e["input_id"] == "first", 3)
        self.provider.native_result = True
        await self.runtime.submit(Input("user", "SECOND", id="second"))
        await self.runtime.wait_for(lambda e: e["type"] == "input.queued" and e["input_id"] == "second", 3)
        self.assertEqual([c.id for c in self.provider.steered], ["first"])
        self.tool.release.set()
        await self.provider.reply(calls=[ToolCall(id="boundary", name="delegate", arguments={"async": False})])
        request = await self.provider.request()
        text = str(request.messages)
        self.assertLess(text.index("FIRST"), text.index("SECOND"))
        self.assertEqual(text.count("FIRST"), 1)
        self.assertEqual(text.count("SECOND"), 1)
        await self.runtime.submit(Input("user", "THIRD", id="third"))
        await self.runtime.wait_for(lambda e: e["type"] == "steering.accepted" and e["input_id"] == "third", 3)
        self.assertEqual([c.id for c in self.provider.steered], ["first", "third"])

    async def test_ordinary_attachment_drain_blocks_native_until_request_prepared(self):
        await self.start(native=True)
        coordinator = self.session.coordinator
        coordinator.register_capability("live.attachments.encode", lambda c, _: c.text + "-ATTACHMENT")
        await self.runtime.submit(Input("user", "ORIGINAL"))
        await self.provider.request()
        context = coordinator.get("context")
        original = context.add_message
        entered, release = asyncio.Event(), asyncio.Event()
        async def add(message):
            if message.get("metadata", {}).get("amplifier_input", {}).get("id") == "attachment":
                entered.set()
                await release.wait()
            return await original(message)
        context.add_message = add
        await self.runtime.submit(Input("user", "FIRST", id="attachment", attachments=("fixture",)))
        await self.runtime.wait_for(lambda e: e["type"] == "input.queued" and e["input_id"] == "attachment", 3)
        self.tool.release.set()
        await self.provider.reply(calls=[ToolCall(id="boundary", name="delegate", arguments={"async": False})])
        await asyncio.wait_for(entered.wait(), 3)
        await self.runtime.submit(Input("user", "SECOND", id="second"))
        await self.runtime.wait_for(lambda e: e["type"] == "input.queued" and e["input_id"] == "second", 3)
        self.assertEqual(self.provider.steered, [])
        release.set()
        first = await self.provider.request()
        self.assertIn("FIRST-ATTACHMENT", str(first.messages))
        await self.provider.reply("first boundary answer")
        second = await self.provider.request()
        self.assertLess(str(second.messages).index("FIRST-ATTACHMENT"), str(second.messages).index("SECOND"))
        self.assertEqual(self.provider.steered, [])

    async def test_ordinary_checkpoint_inbox_continues_same_generation(self):
        await self.start()
        await self.runtime.submit(Input("user", "ORIGINAL", id="original"))
        await self.provider.request()
        generation = self.runtime.generation["id"]
        entered, release = asyncio.Event(), asyncio.Event()
        async def checkpoint(*_args):
            entered.set()
            await release.wait()
        self.session.coordinator.register_capability("live.checkpoint", checkpoint)
        await self.provider.reply("first answer")
        await asyncio.wait_for(entered.wait(), 3)
        for i in range(3):
            await self.runtime.submit(Input("user", f"LATE-{i}", id=f"late-{i}"))
        release.set()
        request = await self.provider.request()
        self.assertEqual(self.runtime.generation["id"], generation)
        text = str(request.messages)
        self.assertEqual(text.count("ORIGINAL"), 1)
        self.assertEqual([text.index(f"LATE-{i}") for i in range(3)],
                         sorted(text.index(f"LATE-{i}") for i in range(3)))
        self.assertTrue(all(text.count(f"LATE-{i}") == 1 for i in range(3)))
        await self.provider.reply("updated")
        finished = await self.runtime.wait_for(lambda e: e["type"] == "generation.finished", 3)
        self.assertEqual(finished["input_ids"], ["original", "late-0", "late-1", "late-2"])
        for event in self.runtime.events:
            if event["type"] in {"input.queued", "input.delivered"} and event.get("input_id", "").startswith("late-"):
                self.assertEqual((event["delivery"], event["generation_id"], event["source"]),
                                 ("request_boundary", generation, "user"))

    async def test_ordinary_native_exception_never_falls_back(self):
        await self.start(native=True)
        await self.runtime.submit(Input("user", "ORIGINAL"))
        await self.provider.request()
        self.provider.native_result = RuntimeError("uncertain native effect")
        await self.runtime.submit(Input("user", "NO REPLAY", id="uncertain"))
        with self.assertRaisesRegex(Exception, "uncertain native effect"):
            await asyncio.wait_for(self.task, 3)
        self.assertFalse(any(e["type"] in {"input.queued", "input.delivered", "input.held"}
                             and e.get("input_id") == "uncertain" for e in self.runtime.events))
        unknown = [e for e in self.runtime.events if e["type"] == "input.unknown" and e["input_id"] == "uncertain"]
        self.assertEqual(len(unknown), 1)
        self.assertEqual(unknown[0]["delivery"], "native")
        self.assertTrue(self.provider.requests.empty())

    async def test_ordinary_context_inserted_but_unprepared_still_blocks_native(self):
        await self.start(native=True)
        await self.runtime.submit(Input("user", "ORIGINAL"))
        await self.provider.request()
        context = self.session.coordinator.get("context")
        original = context.get_messages_for_request
        entered, release = asyncio.Event(), asyncio.Event()
        async def messages(*args, **kwargs):
            if self.loop._boundary_pending:
                entered.set()
                await release.wait()
            return await original(*args, **kwargs)
        context.get_messages_for_request = messages
        self.provider.native_result = False
        await self.runtime.submit(Input("user", "FIRST", id="first"))
        await self.runtime.wait_for(lambda e: e["type"] == "input.queued" and e["input_id"] == "first", 3)
        self.tool.release.set()
        await self.provider.reply(calls=[ToolCall(id="boundary", name="delegate", arguments={"async": False})])
        await asyncio.wait_for(entered.wait(), 3)
        self.assertFalse(self.loop.pending)
        self.provider.native_result = True
        await self.runtime.submit(Input("user", "SECOND", id="second"))
        await self.runtime.wait_for(lambda e: e["type"] == "input.queued" and e["input_id"] == "second", 3)
        self.assertEqual([c.id for c in self.provider.steered], ["first"])
        release.set()
        await self.provider.request()

    async def test_ordinary_after_closed_admission_starts_new_generation(self):
        await self.start(native=True)
        await self.runtime.submit(Input("user", "ORIGINAL"))
        await self.provider.request()
        generation = self.runtime.generation["id"]
        context = self.session.coordinator.get("context")
        original = context.get_messages
        entered, release = asyncio.Event(), asyncio.Event()
        async def messages():
            if self.runtime.steering_closed:
                entered.set()
                await release.wait()
            return await original()
        context.get_messages = messages
        await self.provider.reply("done")
        await asyncio.wait_for(entered.wait(), 3)
        await self.runtime.submit(Input("user", "NEW TURN", id="new"))
        release.set()
        request = await self.provider.request()
        self.assertEqual(str(request.messages).count("NEW TURN"), 1)
        self.assertNotEqual(self.runtime.generation["id"], generation)
        self.assertEqual(self.provider.steered, [])
        delivered = next(e for e in self.runtime.events if e["type"] == "input.delivered" and e["input_id"] == "new")
        self.assertEqual(delivered["delivery"], "new_turn")
        self.assertEqual(delivered["generation_id"], self.runtime.generation["id"])

    async def test_stop_during_ordinary_partial_context_write_is_unknown(self):
        await self.start()
        await self.runtime.submit(Input("user", "ORIGINAL"))
        await self.provider.request()
        context = self.session.coordinator.get("context")
        original = context.add_message
        entered, release = asyncio.Event(), asyncio.Event()
        async def add(message):
            await original(message)
            if message.get("metadata", {}).get("amplifier_input", {}).get("id") == "partial":
                entered.set()
                await release.wait()
        context.add_message = add
        await self.runtime.submit(Input("user", "PARTIAL", id="partial"))
        await self.runtime.wait_for(lambda e: e["type"] == "input.queued" and e["input_id"] == "partial", 3)
        self.tool.release.set()
        await self.provider.reply(calls=[ToolCall(id="boundary", name="delegate", arguments={"async": False})])
        await asyncio.wait_for(entered.wait(), 3)
        await self.runtime.submit(Input("stop", target="cancel"))
        await asyncio.wait_for(self.task, 3)
        unknown = [e for e in self.runtime.events if e["type"] == "input.unknown" and e["input_id"] == "partial"]
        self.assertEqual(len(unknown), 1)
        self.assertEqual(unknown[0]["delivery"], "request_boundary")
        self.assertFalse(any(e["type"] == "input.held" and e["input_id"] == "partial" for e in self.runtime.events))
        self.assertIn("PARTIAL", str(await context.get_messages()))
        self.assertTrue(self.provider.requests.empty())

    async def test_native_provider_owned_boundary_fallback_is_not_enqueued_twice(self):
        await self.start(native=True)
        await self.runtime.submit(Input("user", "ORIGINAL"))
        await self.provider.request()
        async def steer(command):
            # Mirrors a definitive preaccept refusal handled by the provider,
            # which still returns True because it owns the fallback.
            self.loop.pending.append(command)
            self.loop.steer("[loop-live inbox wake]")
            return True
        self.provider.steer_live = steer
        command = Input("user", "OWNED FALLBACK", id="owned")
        await self.runtime.submit(command)
        await self.runtime.wait_for(lambda e: e["type"] == "input.accepted" and e["input_id"] == "owned", 3)
        self.tool.release.set()
        await self.provider.reply(calls=[ToolCall(id="boundary", name="delegate", arguments={"async": False})])
        request = await self.provider.request()
        self.assertEqual(str(request.messages).count("OWNED FALLBACK"), 1)
        self.assertEqual(len([e for e in self.runtime.events if e["type"] == "input.delivered" and e["input_id"] == "owned"]), 1)
        self.assertIs(self.runtime.accepted["owned"], command)

    async def test_stop_during_native_attempt_keeps_owned_input_out_of_fallback(self):
        await self.start(native=True)
        await self.runtime.submit(Input("user", "ORIGINAL"))
        await self.provider.request()
        entered, release = asyncio.Event(), asyncio.Event()
        async def steer(command):
            entered.set()
            await release.wait()
            return True
        self.provider.steer_live = steer
        await self.runtime.submit(Input("user", "NATIVE PENDING", id="native"))
        await asyncio.wait_for(entered.wait(), 3)
        await self.runtime.submit(Input("user", "LOCAL INBOX", id="local"))
        await self.runtime.submit(Input("stop", target="cancel"))
        release.set()
        await asyncio.wait_for(self.task, 3)
        self.assertFalse(any(e["type"] in {"input.queued", "input.delivered", "input.held"}
                             and e.get("input_id") == "native" for e in self.runtime.events))
        self.assertEqual([e["input_id"] for e in self.runtime.events if e["type"] == "input.held"], ["local"])
        self.assertTrue(self.provider.requests.empty())

    async def test_stop_at_checkpoint_holds_ordinary_inbox_in_original_generation(self):
        await self.start()
        await self.runtime.submit(Input("user", "ORIGINAL"))
        await self.provider.request()
        generation = self.runtime.generation["id"]
        entered, release = asyncio.Event(), asyncio.Event()
        async def checkpoint(*_args):
            entered.set()
            await release.wait()
        self.session.coordinator.register_capability("live.checkpoint", checkpoint)
        await self.provider.reply("done")
        await asyncio.wait_for(entered.wait(), 3)
        await self.runtime.submit(Input("user", "NOT DELIVERED", id="late"))
        await self.runtime.submit(Input("stop", target="cancel"))
        release.set()
        await asyncio.wait_for(self.task, 3)
        held = [e for e in self.runtime.events if e["type"] == "input.held" and e["input_id"] == "late"]
        self.assertEqual(len(held), 1)
        self.assertEqual(held[0]["generation_id"], generation)
        self.assertTrue(self.provider.requests.empty())

    async def test_completion_marker_yields_once_not_forever_to_service_callbacks(self):
        await self.start()
        await self.runtime.submit(Input("user", "ORIGINAL"))
        await self.provider.request()
        generation = self.runtime.generation["id"]
        checkpoints = []
        async def checkpoint(*_args):
            checkpoints.append(generation)
            await self.runtime.submit(Input("service", "CALLBACK", source="fixture",
                                            id=f"callback-{len(checkpoints)}"))
        self.session.coordinator.register_capability("live.checkpoint", checkpoint)
        await self.provider.reply("done")
        finished = await self.runtime.wait_for(lambda e: e["type"] == "generation.finished", 3)
        self.assertEqual(finished["generation_id"], generation)
        self.assertEqual(checkpoints, [generation])
        await self.provider.request()
        self.assertNotEqual(self.runtime.generation["id"], generation)

    async def test_indeterminate_native_return_is_unknown_not_fallback(self):
        await self.start(native=True)
        await self.runtime.submit(Input("user", "ORIGINAL"))
        await self.provider.request()
        self.provider.native_result = None
        await self.runtime.submit(Input("user", "UNCERTAIN", id="unknown"))
        with self.assertRaisesRegex(Exception, "definitive ownership"):
            await asyncio.wait_for(self.task, 3)
        self.assertEqual(len([e for e in self.runtime.events if e["type"] == "input.unknown" and e["input_id"] == "unknown"]), 1)
        self.assertFalse(any(e["type"] == "input.queued" and e["input_id"] == "unknown" for e in self.runtime.events))

    async def test_failure_holds_ordinary_local_pending_without_falling_forward(self):
        await self.start()
        await self.runtime.submit(Input("user", "ORIGINAL"))
        await self.provider.request()
        generation = self.runtime.generation["id"]
        await self.runtime.submit(Input("user", "NOT DELIVERED", id="pending"))
        await self.runtime.wait_for(lambda e: e["type"] == "input.queued" and e["input_id"] == "pending", 3)
        await self.provider.responses.put(RuntimeError("fixture failure"))
        with self.assertRaisesRegex(Exception, "Manager turn failed"):
            await asyncio.wait_for(self.task, 3)
        held = [e for e in self.runtime.events if e["type"] == "input.held" and e["input_id"] == "pending"]
        self.assertEqual(len(held), 1)
        self.assertEqual((held[0]["generation_id"], held[0]["delivery"]), (generation, "request_boundary"))
        self.assertTrue(self.provider.requests.empty())
        self.assertFalse(self.loop.pending)

    async def test_stop_holds_ordinary_pending_and_inbox_without_replay(self):
        await self.start(native=True)
        await self.runtime.submit(Input("user", "ORIGINAL"))
        await self.provider.request()
        generation = self.runtime.generation["id"]
        self.provider.native_result = False
        await self.runtime.submit(Input("user", "PENDING", id="pending"))
        await self.runtime.wait_for(lambda e: e["type"] == "input.queued" and e["input_id"] == "pending", 3)
        await self.runtime.submit(Input("user", "INBOX", id="inbox"))
        await self.runtime.submit(Input("stop", target="cancel"))
        await asyncio.wait_for(self.task, 3)
        held = [e for e in self.runtime.events if e["type"] == "input.held"]
        self.assertEqual({e["input_id"] for e in held}, {"pending", "inbox"})
        self.assertTrue(all(e["generation_id"] == generation for e in held))
        self.assertFalse(self.loop.pending)
        self.assertTrue(self.provider.requests.empty())

    async def test_anchored_steering_applies_only_to_its_current_generation(self):
        await self.start()
        await self.runtime.submit(Input("user", "ORIGINAL", id="original"))
        await self.provider.request()
        generation = self.runtime.generation["id"]
        capability = self.session.coordinator.get_capability("live.steering")
        self.assertEqual((capability["version"], capability["mode"], capability["cancellable"]), (1, "request_boundary", False))
        result = await capability["submit"](Input("steer", "ANCHORED", id="anchored"), generation)
        self.assertEqual(result["disposition"], "queued")
        await self.runtime.wait_for(lambda e: e["type"] == "input.queued" and e["input_id"] == "anchored", 3)
        self.tool.release.set()
        await self.provider.reply(calls=[ToolCall(id="boundary", name="delegate", arguments={"async": False})])
        request = await self.provider.request()
        self.assertIn("ANCHORED", str(request.messages))
        applied = await self.runtime.wait_for(lambda e: e["type"] == "steering.applied" and e["input_id"] == "anchored", 3)
        self.assertEqual(applied["target_generation_id"], generation)
        duplicate = await capability["submit"](Input("steer", "ANCHORED", id="anchored"), generation)
        self.assertEqual(duplicate["disposition"], "applied")
        rows = await self.session.coordinator.get("context").get_messages()
        matching = [row for row in rows if row.get("content") == "ANCHORED"]
        self.assertEqual(len(matching), 1)
        self.assertEqual(matching[0]["metadata"]["amplifier_input"]["target_generation_id"], generation)
        await self.provider.reply("done")
        finished = await self.runtime.wait_for(lambda e: e["type"] == "generation.finished" and e["generation_id"] == generation, 3)
        self.assertEqual(finished["input_ids"], ["original", "anchored"])

    async def test_accepted_checkpoint_steering_continues_the_same_generation(self):
        await self.start()
        await self.runtime.submit(Input("user", "ORIGINAL", id="original"))
        await self.provider.request()
        generation = self.runtime.generation["id"]
        checkpoint_entered, release_checkpoint = asyncio.Event(), asyncio.Event()
        async def checkpoint(*_args):
            checkpoint_entered.set()
            await release_checkpoint.wait()
        self.session.coordinator.register_capability("live.checkpoint", checkpoint)
        await self.provider.reply("first answer")
        await asyncio.wait_for(checkpoint_entered.wait(), 3)
        for i in range(3):
            outcome = await self.runtime.submit_steering(Input("steer", f"LATE-{i}", id=f"late-{i}"), generation)
            self.assertEqual(outcome["disposition"], "queued")
        release_checkpoint.set()
        request = await self.provider.request()
        text = str(request.messages)
        self.assertEqual(text.count("ORIGINAL"), 1)
        positions = [text.index(f"LATE-{i}") for i in range(3)]
        self.assertEqual(positions, sorted(positions))
        self.assertTrue(all(text.count(f"LATE-{i}") == 1 for i in range(3)))
        self.assertEqual(self.runtime.generation["id"], generation)
        self.assertFalse(any(e["type"] == "generation.finished" for e in self.runtime.events))
        await self.provider.reply("all questions answered")
        finished = await self.runtime.wait_for(lambda e: e["type"] == "generation.finished", 3)
        self.assertEqual(finished["input_ids"], ["original", "late-0", "late-1", "late-2"])
        self.assertEqual(len([e for e in self.runtime.events if e["type"] == "generation.started"]), 1)
        self.assertEqual((await self.runtime.submit_steering(Input("steer", "LATE-1", id="late-1"), generation))["disposition"], "applied")
        with self.assertRaisesRegex(ValueError, "no longer active"):
            await self.runtime.submit_steering(Input("steer", "STALE", id="stale"), generation)
        rows = await self.session.coordinator.get("context").get_messages()
        self.assertFalse(any(m.get("role") == "user" and m.get("content") == "" for m in rows))

    async def test_burst_during_model_and_tools_reaches_next_request_once(self):
        await self.start()
        await self.runtime.submit(Input("user", "ORIGINAL", id="original"))
        await self.provider.request()
        generation = self.runtime.generation["id"]
        await self.runtime.submit_steering(Input("steer", "MODEL-UPDATE", id="model"), generation)
        await self.provider.reply(calls=[ToolCall(id="step", name="delegate", arguments={"async": False})])
        await self.runtime.wait_for(lambda e: e["type"] == "tool.pre", 3)
        for i in range(3):
            await self.runtime.submit_steering(Input("steer", f"TOOL-UPDATE-{i}", id=f"tool-{i}"), generation)
        self.tool.release.set()
        request = await self.provider.request()
        text = str(request.messages)
        updates = ["MODEL-UPDATE", *[f"TOOL-UPDATE-{i}" for i in range(3)]]
        positions = [text.index(value) for value in updates]
        self.assertEqual(positions, sorted(positions))
        self.assertTrue(all(text.count(value) == 1 for value in updates))
        self.assertEqual(self.tool.calls, 1)
        await self.provider.reply("updated answer")
        finished = await self.runtime.wait_for(lambda e: e["type"] == "generation.finished", 3)
        self.assertEqual(finished["input_ids"], ["original", "model", "tool-0", "tool-1", "tool-2"])

    async def test_stop_during_final_checkpoint_does_not_resume_pending_steer(self):
        await self.start()
        await self.runtime.submit(Input("user", "ORIGINAL"))
        await self.provider.request()
        entered, release = asyncio.Event(), asyncio.Event()
        async def checkpoint():
            entered.set()
            await release.wait()
        self.session.coordinator.register_capability("live.checkpoint", checkpoint)
        await self.provider.reply("answer")
        await asyncio.wait_for(entered.wait(), 3)
        await self.runtime.submit_steering(Input("steer", "NO REPLAY", id="late"), self.runtime.generation["id"])
        await self.runtime.submit(Input("stop", target="cancel"))
        release.set()
        await asyncio.wait_for(self.task, 3)
        self.assertTrue(self.provider.requests.empty())
        self.assertEqual(self.runtime.steering_outcomes["late"]["disposition"], "held")

    async def test_admission_closes_before_terminal_publication_awaits(self):
        await self.start()
        await self.runtime.submit(Input("user", "ORIGINAL"))
        await self.provider.request()
        generation = self.runtime.generation["id"]
        context = self.session.coordinator.get("context")
        original = context.get_messages
        entered, release = asyncio.Event(), asyncio.Event()
        async def messages():
            if self.runtime.steering_closed:
                entered.set()
                await release.wait()
            return await original()
        context.get_messages = messages
        await self.provider.reply("done")
        await asyncio.wait_for(entered.wait(), 3)
        with self.assertRaisesRegex(ValueError, "finishing"):
            await self.runtime.submit_steering(Input("steer", "TOO LATE", id="late"), generation)
        self.assertNotIn("late", self.runtime.accepted)
        release.set()
        await self.runtime.wait_for(lambda e: e["type"] == "generation.finished", 3)
        self.assertTrue(self.provider.requests.empty())

    async def test_exhausted_budget_does_not_resume_checkpoint_steer(self):
        await self.start()
        await self.runtime.submit(Input("user", "ORIGINAL"))
        await self.provider.request()
        async def checkpoint():
            self.loop._budget_exhausted = True
            await self.runtime.submit_steering(Input("steer", "WAIT", id="late"), self.runtime.generation["id"])
        self.session.coordinator.register_capability("live.checkpoint", checkpoint)
        await self.provider.reply("budget summary")
        await self.runtime.wait_for(lambda e: e["type"] == "generation.finished", 3)
        self.assertEqual(self.runtime.steering_outcomes["late"]["disposition"], "held")
        self.assertTrue(self.provider.requests.empty())

    async def test_cancellation_holds_only_not_yet_injected_anchored_work(self):
        await self.start()
        await self.runtime.submit(Input("user", "ORIGINAL", id="original"))
        await self.provider.request()
        generation = self.runtime.generation["id"]
        await self.runtime.submit_steering(Input("steer", "HELD ON CANCEL", id="held"), generation)
        await self.runtime.wait_for(lambda e: e["type"] == "input.queued" and e["input_id"] == "held", 3)
        await self.runtime.submit(Input("stop", target="cancel"))
        await asyncio.wait_for(self.task, 3)
        self.assertEqual(self.runtime.steering_outcomes["held"]["disposition"], "held")
        self.assertNotIn("HELD ON CANCEL", str(await self.session.coordinator.get("context").get_messages()))
        self.assertTrue(self.provider.requests.empty())

    async def test_interrupted_context_injection_is_unknown_not_false_held_evidence(self):
        await self.start()
        context = self.session.coordinator.get("context")
        original = context.add_message
        injected, release = asyncio.Event(), asyncio.Event()
        async def add(message):
            await original(message)
            if message.get("metadata", {}).get("amplifier_input", {}).get("id") == "uncertain":
                injected.set()
                await release.wait()
        context.add_message = add
        await self.runtime.submit(Input("user", "ORIGINAL", id="original"))
        await self.provider.request()
        await self.runtime.submit_steering(Input("steer", "PARTIAL EFFECT", id="uncertain"), self.runtime.generation["id"])
        await self.runtime.wait_for(lambda e: e["type"] == "input.queued" and e["input_id"] == "uncertain", 3)
        self.tool.release.set()
        await self.provider.reply(calls=[ToolCall(id="boundary", name="delegate", arguments={"async": False})])
        await asyncio.wait_for(injected.wait(), 3)
        await self.runtime.submit(Input("stop", target="cancel"))
        await asyncio.wait_for(self.task, 3)
        context.add_message = original
        self.assertEqual(self.runtime.steering_outcomes["uncertain"]["disposition"], "unknown")
        self.assertIn("PARTIAL EFFECT", str(await context.get_messages()))
        self.assertFalse(any(e["type"] == "steering.held" and e.get("input_id") == "uncertain" for e in self.runtime.events))

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

    async def test_anchored_input_wakes_job_wait_without_stopping_child(self):
        await self.start()
        await self.runtime.submit(Input("user", "background task"))
        await self.provider.request()
        generation = self.runtime.generation['id']
        await self.provider.reply(calls=[ToolCall(id="background", name="delegate", arguments={"async": True})])
        await self.provider.request()
        job_id = next(iter(self.loop.jobs))
        await self.provider.reply(calls=[ToolCall(id="wait", name="live_job", arguments={"action": "wait", "job_id": job_id, "timeout": 60})])
        await self.runtime.wait_for(lambda e: e['type'] == 'tool.pre' and e.get('tool') == 'live_job', 3)
        await self.runtime.submit_steering(Input('steer', 'NEW GUIDANCE', id='steer'), generation)
        request = await self.provider.request()
        self.assertIn('NEW GUIDANCE', str(request.messages))
        self.assertIn('input_pending', str(request.messages))
        self.assertFalse(self.loop.jobs[job_id]['task'].done())
        self.assertEqual(self.tool.calls, 1)
        await self.provider.reply('guidance received while child runs')

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

    async def start(self, *, manager=True, native=False):
        self.runtime = Runtime()
        self.session = AmplifierSession({"session": {
            "orchestrator": {"module": "loop-live", "config": {"configured_bundle": True,
                "background_delegate": True, "min_delay_between_calls_ms": 0}},
            "context": {"module": "context-simple"}}, "providers": []}, session_id=self.runtime.session_id)
        await self.session.initialize()
        self.provider, self.tool = NativeProvider(self.runtime) if native else Provider(), Delegate()
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
        returned = [event for event in self.runtime.events if event["type"] == "input.delivered"
                    and event.get("source") == "amplifier-delegate"]
        self.assertEqual(len(returned), 1)
        self.assertEqual(returned[0]["call_id"], "worker-call")
        messages = await self.session.coordinator.get("context").get_messages()
        retained = next(message["metadata"]["amplifier_input"] for message in messages
                        if message.get("metadata", {}).get("amplifier_input", {}).get("id") == returned[0]["input_id"])
        self.assertEqual(retained["call_id"], "worker-call")
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


    async def test_ordinary_input_after_last_request_continues_current_generation(self):
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
        generation = self.runtime.generation["id"]
        await self.provider.reply("first done")
        request = await self.provider.request()
        self.assertEqual(str(request.messages).count("LATE-CORRECTION"), 1)
        self.assertEqual(self.runtime.generation["id"], generation)
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
