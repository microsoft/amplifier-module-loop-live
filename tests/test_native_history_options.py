"""Provider-owned ordinary-tool classification survives the optional CLI adapter."""
import copy
import json
import sys
from pathlib import Path

import pytest

pytest.importorskip('amplifier_module_provider_openai.native')
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'adapters/cli'))
from amplifier_loop_live_cli.bundle_astra import BundleAstraProvider, SafeOpenAIProvider
from amplifier_loop_live_cli.computer_results import ComputerResultError, failed_computer_history
from amplifier_module_provider_openai import OpenAIProvider


def history(*, block, later_user):
    call = {'id': 'ordinary-call', 'name': 'computer', 'arguments': {'actions': []}}
    assistant = {'role': 'assistant', 'content': '', 'tool_calls': [call]}
    if block:
        assistant = {'role': 'assistant', 'content': [
            {'type': 'tool_call', 'id': call['id'], 'name': call['name'], 'input': call['arguments']}]}
    messages = [{'role': 'user', 'content': 'Use the ordinary tool'}, assistant,
                {'role': 'tool', 'tool_call_id': call['id'],
                 'content': json.dumps({'success': False, 'error': 'ordinary tool failure'})}]
    if later_user:
        messages.append({'role': 'user', 'content': 'Explain that result'})
    return messages


@pytest.mark.parametrize('provider_type', [BundleAstraProvider, SafeOpenAIProvider])
@pytest.mark.parametrize('block', [False, True])
@pytest.mark.parametrize('later_user', [False, True])
def test_actual_provider_serializer_preserves_ordinary_computer_result(provider_type, block, later_user):
    messages = history(block=block, later_user=later_user)
    original = copy.deepcopy(messages)
    provider = provider_type.wrap(OpenAIProvider(api_key='fixture', config={'default_model': 'gpt-6-astra'}))
    converted = provider._convert_messages(messages, ordinary_computer_names=frozenset({'computer'}))
    output = [item for item in converted if item.get('type') == 'function_call_output']
    assert len(output) == 1 and output[0]['call_id'] == 'ordinary-call'
    assert 'ordinary tool failure' in str(output[0]['output'])
    assert not any(item.get('type') == 'computer_call_output' for item in converted)
    assert messages == original


@pytest.mark.parametrize('block', [False, True])
def test_native_failure_still_fails_closed_in_current_turn(block):
    with pytest.raises(ComputerResultError, match='ordinary tool failure'):
        failed_computer_history(history(block=block, later_user=False))


@pytest.mark.parametrize('block', [False, True])
def test_later_native_failure_is_reference_only_and_does_not_edit_transcript(block):
    messages = history(block=block, later_user=True)
    original = copy.deepcopy(messages)
    projected, changed = failed_computer_history(messages)
    assert changed and messages == original
    assert any('Do not replay the prior action automatically' in str(m.get('content')) for m in projected)
    assert not any(m.get('role') == 'tool' for m in projected)

@pytest.mark.parametrize('block', [False, True])
@pytest.mark.parametrize('later_user', [False, True])
def test_native_projection_accepts_provider_owned_ordinary_names(block, later_user):
    messages = history(block=block, later_user=later_user)
    provider = BundleAstraProvider.wrap(OpenAIProvider(api_key='fixture', config={'default_model': 'gpt-6-astra'}))
    assert provider._prepare_native_messages(messages, ordinary_names=frozenset({'computer'})) is messages
