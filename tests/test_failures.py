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


@pytest.mark.parametrize('code', ['native_input_oversized', 'native_checkpoint_invalid',
    'native_no_reduction', 'native_measurement_unavailable', 'native_compaction_failed',
    'disabled', 'request_context_unavailable', 'invalid_native_contract',
    'authoritative_measurement_unavailable', 'summary_output_limit', 'summary_empty',
    'native_recovery_required_context_oversized', 'checkpoint_persist_failed',
    'previous_failure', 'summary_failed', 'native_failed', 'private payload', {'secret': 'value'}])
def test_compaction_failure_preserves_bounded_code_without_optional_dependency(code):
    error_class = type('CompactionError', (RuntimeError,), {
        '__module__': 'amplifier_module_context_managed.errors'})
    error = error_class('secret transcript and SDK response')
    error.code = code
    failure = turn_failure(error)
    assert failure['error_category'] == 'context_compaction'
    assert failure['error_stage'] == 'context_preparation'
    assert failure['error_type'] == 'CompactionError'
    assert failure.get('error_code') == (code if isinstance(code, str) and code != 'private payload' else None)
    assert 'secret' not in json.dumps(failure)
    assert 'private payload' not in json.dumps(failure)
    assert failure['retryable'] is False


SUMMARY_PAUSE_MESSAGES = [
    ('summary_work_paused',
     'Context preparation used its local work allowance. Completed checkpoint progress is retained. '
     'Further preparation requires separate admission; work was not automatically retried.'),
    ('summary_progress_incompatible',
     'Saved paid summary progress is incompatible with the current source or policy. '
     'No replacement work was started; the checkpoint is preserved. An explicit decision is required.'),
]


def compaction_pause(code):
    def forbidden_str(self):
        raise AssertionError('Exception text must remain private')

    error_class = type('CompactionError', (RuntimeError,), {
        '__module__': 'amplifier_module_context_managed.errors', '__str__': forbidden_str})
    error = error_class('SECRET transcript, credential, path and SDK response')
    error.code = code
    error.retryable = True  # Arbitrary exception attributes cannot authorize retries.
    error.details = {'body': 'SECRET', 'source': 'SECRET', 'policy': 'SECRET'}
    error.count_failure = {'category': 'service', 'retryable': True, 'body': 'SECRET'}
    error.error_message = 'SECRET'
    return error


@pytest.mark.parametrize('code,message', SUMMARY_PAUSE_MESSAGES)
def test_summary_pause_has_static_safe_nonretryable_failure(code, message):
    failure = turn_failure(compaction_pause(code), stage='provider_request')
    assert failure == {
        'error_code': code, 'error_type': 'CompactionError',
        'error_category': 'context_compaction', 'error_stage': 'context_preparation',
        'error_message': message, 'effects': 'not_rolled_back',
        'replayed': False, 'retryable': False,
    }
    assert str(ManagerTurnError(failure)) == (
        message + ' Work was not automatically replayed; earlier effects were not rolled back.')
    assert 'SECRET' not in json.dumps(failure)


@pytest.mark.parametrize('code', ['summary_work_paused', 'summary_progress_incompatible'])
@pytest.mark.parametrize('module,name', [
    ('untrusted.errors', 'CompactionError'),
    ('amplifier_module_context_managed.errors', 'OtherError'),
])
def test_summary_pause_rejects_wrong_exception_identity(code, module, name):
    error = type(name, (RuntimeError,), {'__module__': module})('SECRET')
    error.code = code
    failure = turn_failure(error)
    assert failure['error_type'] == 'RuntimeError'
    assert failure['error_category'] == 'unknown'
    assert failure['error_stage'] == 'manager_turn'
    assert 'error_code' not in failure
    assert 'SECRET' not in json.dumps(failure)
    assert failure['retryable'] is False


@pytest.mark.parametrize('code', ['summary_work_paused', 'summary_progress_incompatible'])
def test_summary_pause_does_not_accept_compaction_subclass(code):
    parent = type(compaction_pause(code))
    error = type('CompactionPause', (parent,), {'__module__': parent.__module__})('SECRET')
    error.code = code
    failure = turn_failure(error)
    assert failure['error_category'] == 'unknown'
    assert 'error_code' not in failure
    assert 'SECRET' not in json.dumps(failure)


@pytest.mark.parametrize('code', ['summary_work_paused:SECRET', 'unknown', None, [], {'SECRET': True}])
def test_summary_pause_rejects_unknown_or_malformed_code(code):
    failure = turn_failure(compaction_pause(code))
    assert 'error_code' not in failure
    assert failure['error_message'] == (
        'Context compaction failed. Original history is preserved; repair context preparation before continuing.')
    assert failure['error_category'] == 'context_compaction'
    assert failure['error_stage'] == 'context_preparation'
    assert failure['retryable'] is False
    assert 'SECRET' not in json.dumps(failure)


@pytest.mark.parametrize('code,message', SUMMARY_PAUSE_MESSAGES)
async def test_summary_pause_keeps_cause_and_input_identity_without_replay(code, message):
    runtime = Runtime()
    loop = BundleLiveOrchestrator({})
    cause = compaction_pause(code)
    loop._execute_guarded_goal = AsyncMock(side_effect=cause)
    context = SimpleNamespace(get_messages=AsyncMock(return_value=[]))
    capabilities = {'live.runtime': runtime}
    coordinator = SimpleNamespace(get_capability=capabilities.get,
        register_capability=capabilities.__setitem__, config={})
    hooks = SimpleNamespace(register=lambda *a, **kw: None,
        emit=AsyncMock(return_value=HookResult()))
    provider = SimpleNamespace(name='fixture', config={}, complete=AsyncMock())
    with pytest.raises(ManagerTurnError) as captured:
        await asyncio.wait_for(loop.execute('user request', context, {'fixture': provider}, {}, hooks, coordinator), 3)
    assert captured.value.__cause__ is cause
    failed = [e for e in runtime.events if e['type'] == 'generation.failed']
    assert len(failed) == 1
    delivered = next(e for e in runtime.events if e['type'] == 'input.delivered')
    assert failed[0]['input_ids'] == [delivered['input_id']]
    assert failed[0]['generation_id'] == delivered['generation_id']
    assert failed[0]['error_code'] == code
    assert failed[0]['error_message'] == message
    assert failed[0]['error_category'] == 'context_compaction'
    assert failed[0]['error_stage'] == 'context_preparation'
    assert failed[0]['retryable'] is False and failed[0]['replayed'] is False
    assert 'SECRET' not in str(captured.value) + json.dumps(runtime.events)
    assert not {'provider.error', 'generation.finished'} & {e['type'] for e in runtime.events}
    loop._execute_guarded_goal.assert_awaited_once()
    provider.complete.assert_not_awaited()


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
