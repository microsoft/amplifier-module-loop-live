import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from amplifier_core import HookResult
from amplifier_core.llm_errors import AuthenticationError, ContextLengthError, LLMTimeoutError
from amplifier_module_loop_live.failures import ManagerTurnError, turn_failure
from amplifier_module_loop_live.orchestrator import BundleLiveOrchestrator
from amplifier_module_loop_live.runtime import Runtime


@pytest.mark.parametrize('error,category,stage', [
    (ContextLengthError('secret prompt and base64 data'), 'context_limit', 'manager_turn'),
    (ContextLengthError('private request', provider='fixture'), 'context_limit', 'provider_request'),
    (AuthenticationError('Authorization: Bearer secret-key', provider='fixture'), 'authentication', 'provider_request'),
    (LLMTimeoutError('https://secret:password@example.com', provider='fixture'), 'provider_timeout', 'provider_request'),
    (ValueError('private local file and tool arguments'), 'unknown', 'manager_turn'),
])
def test_failure_uses_safe_taxonomy_not_exception_payload(error, category, stage):
    failure = turn_failure(error)
    assert failure['error_category'] == category
    assert failure['error_stage'] == stage
    assert failure['effects'] == 'not_rolled_back' and failure['replayed'] is False
    assert str(error) not in json.dumps(failure)


async def test_local_failure_keeps_cause_and_identity_without_provider_error_or_replay():
    runtime = Runtime()
    loop = BundleLiveOrchestrator({})
    from amplifier_module_context_simple import SimpleContextManager
    budget_context = SimpleContextManager({})
    budget_context._request_hard_fit = True
    try:
        budget_context._check_retained_budget([{'role': 'user', 'content': 'secret-key ' * 100}], 1)
    except ContextLengthError as error:
        cause = error
    loop._execute_guarded_goal = AsyncMock(side_effect=cause)
    context = SimpleNamespace(get_messages=AsyncMock(return_value=[]))
    capabilities = {'live.runtime': runtime}
    coordinator = SimpleNamespace(get_capability=capabilities.get,
        register_capability=capabilities.__setitem__, config={})
    hooks = SimpleNamespace(register=lambda *a, **kw: None,
        emit=AsyncMock(return_value=HookResult()))
    provider = SimpleNamespace(name='fixture', config={})
    with pytest.raises(ManagerTurnError) as captured:
        await asyncio.wait_for(loop.execute('user request', context, {'fixture': provider}, {}, hooks, coordinator), 3)
    assert captured.value.__cause__ is cause
    assert 'secret-key' not in str(captured.value)
    failed = next(e for e in runtime.events if e['type'] == 'generation.failed')
    delivered = next(e for e in runtime.events if e['type'] == 'input.delivered')
    assert failed['input_ids'] == [delivered['input_id']]
    assert failed['error_type'] == 'ContextLengthError'
    assert failed['error_stage'] == 'context_preparation'
    assert 'provider.error' not in [e['type'] for e in runtime.events]
    assert 'generation.finished' not in [e['type'] for e in runtime.events]
    assert 'secret-key' not in json.dumps(runtime.events)
    loop._execute_guarded_goal.assert_awaited_once()


@pytest.mark.parametrize('code', ['native_input_oversized', 'private payload'])
def test_compaction_failure_preserves_bounded_code_without_optional_dependency(code):
    error_class = type('CompactionError', (RuntimeError,), {
        '__module__': 'amplifier_module_context_managed.errors'})
    error = error_class('secret transcript and SDK response')
    error.code = code
    failure = turn_failure(error)
    assert failure['error_category'] == 'context_compaction'
    assert failure['error_stage'] == 'context_preparation'
    assert failure['error_type'] == 'CompactionError'
    assert failure.get('error_code') == ('native_input_oversized' if code != 'private payload' else None)
    assert 'secret' not in json.dumps(failure)
    assert 'private payload' not in json.dumps(failure)
    assert failure['retryable'] is False


def test_token_count_failure_has_safe_diagnostic_without_generation_retry():
    from amplifier_core.llm_errors import LLMError
    cls = type('TokenCountError', (LLMError,), {'__module__': 'amplifier_module_provider_openai._token_count'})
    error = cls('private SDK body', provider='openai', retryable=False)
    error.count_failure = {'category': 'service', 'retryable': True, 'httpStatus': 503,
                           'attempts': 3, 'requestId': 'req_123', 'body': 'SECRET'}
    result = turn_failure(error)
    assert result['error_category'] == 'context_measurement'
    assert result['error_stage'] == 'context_preparation'
    assert result['retryable'] is True and error.retryable is False
    assert result['count_failure'] == {k: v for k, v in error.count_failure.items() if k != 'body'}
    assert 'SECRET' not in json.dumps(result) and 'private' not in json.dumps(result)


def test_token_count_diagnostic_rejects_unknown_values():
    cls = type('TokenCountError', (RuntimeError,), {'__module__': 'amplifier_module_provider_openai._token_count'})
    error = cls('secret')
    error.count_failure = {'category': 'secret', 'retryable': True, 'httpStatus': True,
                           'attempts': 999, 'requestId': 'https://secret@example.com'}
    result = turn_failure(error)
    assert result['count_failure'] == {'retryable': False}
    assert not result['retryable']
    assert 'secret' not in json.dumps(result)
