"""Conservative PNG budgeting for the tested patch-based OpenAI models.

Image pixels become tokens; base64 characters do not. The configured provider's
byte fallback otherwise rejects ordinary saved screenshots as million-token
text. This changes estimates only, never the request or image sent to OpenAI.
Guidance checked 2026-09-15: https://developers.openai.com/api/docs/guides/images-vision
"""
import base64
import binascii
import math
import struct


MODELS = ('gpt-6-astra', 'gpt-5.6-sol', 'gpt-5.6-terra', 'gpt-5.6-luna')


def png_budget_view(params):
    model = params.get('model', '')
    if not isinstance(model, str) or not any(model == name or model.startswith(name + '-') for name in MODELS):
        return None
    image_tokens = 0

    def visit(value):
        nonlocal image_tokens
        if isinstance(value, list):
            return [visit(item) for item in value]
        if not isinstance(value, dict):
            return value
        result = {key: visit(item) for key, item in value.items()}
        url = value.get('image_url')
        if value.get('type') not in ('input_image', 'computer_screenshot') or not isinstance(url, str) or not url.startswith('data:image/png;base64,'):
            return result
        try:
            data = base64.b64decode(url.split(',', 1)[1], validate=True)
            if data[:8] != b'\x89PNG\r\n\x1a\n' or data[12:16] != b'IHDR':
                return result
            width, height = struct.unpack('>II', data[16:24])
        except (ValueError, binascii.Error, struct.error):
            return result
        # Count original pixels even for low/high detail: resizing only reduces
        # their cost. Unsupported dimensions retain the provider's byte fallback.
        if not (0 < width <= 65535 and 0 < height <= 65535):
            return result
        patches = math.ceil(width / 32) * math.ceil(height / 32)
        if patches > 30000:
            return result
        image_tokens += math.ceil(patches * 1.2)
        result['image_url'] = f'[PNG image {width}x{height}; pixels budgeted separately]'
        return result

    view = {**params, 'input': visit(params.get('input'))}
    return (view, image_tokens) if image_tokens else None


class ImageBudgetMixin:
    def _estimated_input_tokens(self, params, *, serialized_bytes=None):
        # Current providers own typed-media budgeting and supply measured bytes.
        # Preserve their decision; retain the PNG fallback for older drivers.
        if serialized_bytes is not None:
            return super()._estimated_input_tokens(params, serialized_bytes=serialized_bytes)
        adjusted = png_budget_view(params)
        if adjusted is None:
            return super()._estimated_input_tokens(params)
        view, image_tokens = adjusted
        # Preserve the conservative one-token-per-UTF-8-byte ceiling for text,
        # tools and instructions, plus headroom on documented image patch costs.
        estimated = self._serialized_input_bytes(view) + math.ceil(image_tokens * 1.10)
        return estimated, self._serialized_input_bytes(params)

    def _record_budget_calibration(self, params, response):
        if png_budget_view(params) is None:
            return super()._record_budget_calibration(params, response)
        # Raw image bytes would contaminate subsequent text-only byte calibration.
