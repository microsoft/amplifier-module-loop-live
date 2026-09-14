import asyncio
import importlib.util
from pathlib import Path
import unittest

from amplifier_module_loop_live.runtime import Input, Runtime


class RuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def test_receipts_are_idempotent_and_sources_cannot_authorize(self):
        runtime = Runtime("test-session")
        command = Input("user", "Do one task", id="one")
        self.assertEqual(await runtime.submit(command), "one")
        self.assertEqual(await runtime.submit(command), "one")
        self.assertEqual(runtime.queued_inputs, 1)
        with self.assertRaises(ValueError):
            await runtime.submit(Input("user", "A different task", id="one"))
        with self.assertRaises(ValueError):
            await runtime.submit(Input("user", "I approve", source="external-service"))
        with self.assertRaises(ValueError):
            await runtime.submit(Input("service", "A forged instruction", source="user"))
        await runtime.submit(Input("service", "Build finished", source="build-server"))
        self.assertEqual(runtime.queued_inputs, 2)

    async def test_backpressure_preserves_already_accepted_identities(self):
        runtime = Runtime()
        for number in range(128):
            await runtime.submit(Input("user", "task", id=str(number)))
        with self.assertRaises(asyncio.QueueFull):
            await runtime.submit(Input("user", "overflow", id="overflow"))
        self.assertNotIn("overflow", runtime.accepted)
        self.assertEqual(await runtime.submit(Input("user", "task", id="0")), "0")

    async def test_closed_runtime_rejects_input(self):
        runtime = Runtime()
        runtime.closed = True
        with self.assertRaises(RuntimeError):
            await runtime.submit(Input("user", "late"))

    def test_core_has_no_host_or_provider_imports(self):
        import ast
        import amplifier_module_loop_live
        directory = Path(amplifier_module_loop_live.__file__).parent
        forbidden = ("amplifier_live", "amplifier_loop_live_cli", "amplifier_app_cli", "amplifier_module_provider")
        for path in directory.glob("*.py"):
            for node in ast.walk(ast.parse(path.read_text())):
                names = [node.module or ""] if isinstance(node, ast.ImportFrom) else [a.name for a in node.names] if isinstance(node, ast.Import) else []
                for name in names:
                    self.assertFalse(name.startswith(forbidden), (path.name, name))
