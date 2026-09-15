import base64
import copy
import importlib.util
import json
from pathlib import Path
import struct
import unittest

path = Path(__file__).resolve().parents[1] / 'adapters/cli/amplifier_loop_live_cli/image_budget.py'
spec = importlib.util.spec_from_file_location('image_budget_under_test', path)
images = importlib.util.module_from_spec(spec)
spec.loader.exec_module(images)


class OriginalBudget:
    def _serialized_input_bytes(self, params):
        return len(json.dumps(params, ensure_ascii=False).encode())
    def _estimated_input_tokens(self, params):
        size = self._serialized_input_bytes(params)
        return size, size
    def _record_budget_calibration(self, params, response):
        self.calibrated = True


class Provider(images.ImageBudgetMixin, OriginalBudget):
    pass


class ImageBudgetTests(unittest.TestCase):
    def params(self, width=1280, height=826):
        data = b'\x89PNG\r\n\x1a\n' + struct.pack('>I', 13) + b'IHDR' + struct.pack('>II', width, height) + b'x' * 1000000
        return {'model': 'gpt-6-astra', 'input': [{'type': 'computer_call_output', 'call_id': 'capture',
            'output': {'type': 'computer_screenshot', 'image_url': 'data:image/png;base64,' + base64.b64encode(data).decode()}}]}

    def test_large_png_counts_pixels_and_preserves_wire_bytes_and_identity(self):
        params = self.params();before = copy.deepcopy(params);provider = Provider()
        estimate, raw = provider._estimated_input_tokens(params)
        self.assertLess(estimate, 2000)
        self.assertGreater(raw, 1000000)
        self.assertEqual(params, before)
        self.assertEqual(images.png_budget_view(params)[1], 1248)
        provider._record_budget_calibration(params, object())
        self.assertFalse(getattr(provider, 'calibrated', False))
        provider._record_budget_calibration({'input': 'plain text'}, object())
        self.assertTrue(provider.calibrated)

    def test_text_and_unknown_or_invalid_images_keep_original_conservative_budget(self):
        fixtures = [self.params(width=0), self.params(width=65536), self.params(width=65535, height=65535),
            {'model': 'gpt-6-astra', 'input': 'data:image/png;base64,' + 'x' * 500},
            {**self.params(), 'model': 'future-model'}]
        bad = self.params();bad['input'][0]['output']['image_url'] = 'data:image/png;base64,broken';fixtures.append(bad)
        for params in fixtures:
            self.assertEqual(Provider()._estimated_input_tokens(params), OriginalBudget()._estimated_input_tokens(params))

    def test_non_image_fields_are_never_discounted(self):
        params = self.params();params['tools'] = [{'description': params['input'][0]['output']['image_url']}]
        estimate, _ = Provider()._estimated_input_tokens(params)
        self.assertGreater(estimate, 1000000)
