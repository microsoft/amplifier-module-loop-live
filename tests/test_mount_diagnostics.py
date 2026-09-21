"""A module-load diagnostic never retains exception or source details."""
import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from amplifier_module_loop_live.orchestrator import mount


@pytest.mark.asyncio
@pytest.mark.parametrize('reason,expected', [
    ('invalid_package_layout','invalid_package_layout'),
    ('missing_source','missing_source'),
    ('invalid_entry_point','invalid_entry_point'),
    ('invalid_module_metadata','invalid_module_metadata'),
    ('validation_failed','validation_failed'),
    (None,'unknown'),
    ({'secret':'private-value'},'unknown'),
    ('https://private-value','unknown'),
])
async def test_mount_diagnostics_keep_only_safe_reason(reason,expected):
    coordinator=MagicMock()
    coordinator.mount=AsyncMock()
    await mount(coordinator,{})
    loop=coordinator.mount.call_args.args[1]
    handler=next(call.args[1] for call in coordinator.hooks.register.call_args_list
                 if call.args[0]=='module:load_failed')
    await handler('module:load_failed',{'module_id':'tool-fixture','module_type':'tool',
        'reason_code':reason,'error':'private-value','source':'/private-value','api_key':'private-value'})
    assert loop.load_failures==[{'module':'tool-fixture','type':'tool','reason_code':expected}]
    assert 'private-value' not in json.dumps(loop.load_failures)
