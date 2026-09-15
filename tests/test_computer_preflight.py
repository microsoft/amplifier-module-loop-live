import importlib.util
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from amplifier_core.message_models import ChatRequest, Message

path = Path(__file__).resolve().parents[1] / 'adapters/cli/amplifier_loop_live_cli/computer_results.py'
spec = importlib.util.spec_from_file_location('computer_results_under_test', path)
computer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(computer)


class ComputerPreflightTests(unittest.TestCase):
    def test_budget_uses_hook_view_without_mutating_request_or_wrapping_twice(self):
        original = ChatRequest(messages=[Message(role='tool', tool_call_id='capture', content='saved screenshot reference')])
        calls = []
        provider = SimpleNamespace(request_budget=lambda request, **kwargs: calls.append((request, kwargs)) or {'estimated_input_tokens': 42})
        expanded = [Message(role='tool', tool_call_id='capture', content='expanded screenshot blocks')]
        limits = []
        def expand(messages, limit):
            self.assertIs(messages, original.messages)
            limits.append(limit)
            return expanded
        hook = SimpleNamespace(_wrap_provider=lambda p, c, n: setattr(p, '_amplifier_computer_use_wrapped', True), _expand_tool_results=expand)
        coordinator = SimpleNamespace(get=lambda name: {'computer': object()}, config={'hooks': [{'module': 'hook-computer-use', 'config': {'max_inline_screenshots': 2}}]})
        with patch.dict(sys.modules, {'amplifier_module_hook_computer_use': hook}):
            computer.prepare_computer_provider(provider, coordinator)
            computer.prepare_computer_provider(provider, coordinator)
            result = provider.request_budget(original, context_estimate=100)
        self.assertEqual(result, {'estimated_input_tokens': 42})
        self.assertEqual(limits, [2])
        self.assertIsNot(calls[0][0], original)
        self.assertIs(calls[0][0].messages, expanded)
        self.assertEqual(calls[0][1], {'context_estimate': 100})
        self.assertEqual(original.messages[0].content, 'saved screenshot reference')

    def test_unsupported_provider_is_not_given_screenshot_preflight(self):
        budget = lambda *args, **kwargs: {}
        provider = SimpleNamespace(request_budget=budget)
        hook = SimpleNamespace(_wrap_provider=lambda *args: False)
        coordinator = SimpleNamespace(get=lambda name: {'computer': object()}, config={})
        with patch.dict(sys.modules, {'amplifier_module_hook_computer_use': hook}):
            computer.prepare_computer_provider(provider, coordinator)
        self.assertIs(provider.request_budget, budget)
