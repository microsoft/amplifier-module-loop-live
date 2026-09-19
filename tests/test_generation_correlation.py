"""Portable lifecycle semantics; no provider/host SDK is required."""
import unittest
from amplifier_module_loop_live.runtime import Runtime, Input

class GenerationTests(unittest.IsolatedAsyncioTestCase):
    async def test_only_delivered_and_applied_steering_belong_to_generation(self):
        runtime = Runtime()
        await runtime.submit(Input("user", "first", id="first"))
        await runtime.emit("generation.started", generation_id="turn-1")
        await runtime.emit("input.delivered", input_id="first")
        await runtime.emit("input.queued", input_id="queued")
        await runtime.emit("steering.sent", input_id="failed")
        await runtime.emit("steering.failed", input_id="failed")
        await runtime.emit("steering.sent", input_id="native")
        await runtime.emit("steering.accepted", input_id="native")
        await runtime.emit("steering.accepted", input_id="native")
        pending = await runtime.emit("assistant.message", text="earlier answer")
        self.assertEqual(pending["input_ids"], ["first"])
        self.assertEqual(pending["accepted_input_ids"], ["native"])
        await runtime.emit("steering.applied", input_id="native")
        message = await runtime.emit("assistant.message", text="answer")
        self.assertEqual(message["input_ids"], ["first", "native"])
        finished = await runtime.emit("generation.finished", text="answer", active_job_ids=[])
        self.assertEqual(finished["generation_id"], "turn-1")
        self.assertEqual(finished["input_ids"], ["first", "native"])
        await runtime.emit("generation.started", generation_id="turn-2")
        await runtime.emit("input.delivered", input_id="queued")
        second = await runtime.emit("generation.finished", text="next", active_job_ids=[])
        self.assertEqual(second["input_ids"], ["queued"])
        self.assertEqual(finished["input_ids"], ["first", "native"])

    async def test_cancellation_is_correlated_without_success(self):
        runtime = Runtime()
        await runtime.emit("generation.started", generation_id="turn")
        await runtime.emit("input.delivered", input_id="request")
        detached = await runtime.emit("generation.detached", cancellation="unconfirmed")
        self.assertEqual(detached["input_ids"], ["request"])
        self.assertIsNone(runtime.generation)
        unrelated = await runtime.emit("assistant.message", text="outside")
        self.assertNotIn("generation_id", unrelated)
