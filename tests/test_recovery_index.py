"""Recovery scales with evidence while preserving ordinary reconciliation rules."""
import asyncio
import copy
import sys

import pytest
from amplifier_core.message_models import ToolCall

from amplifier_module_loop_live import job_store
from amplifier_module_loop_live.job_store import JobStore


@pytest.fixture
def ledger(tmp_path):
    store = JobStore(tmp_path / 'jobs')
    try:
        yield store
    finally:
        store.close()


def add_job(store, identity, *, job_id=None, status='returned'):
    call = ToolCall(id=identity, name='read_file', arguments={'path': identity + '.txt'})
    store.begin(call, job_id or 'job-' + identity, 'receipt-' + identity)
    if status != 'pending':
        store.finish(identity, 'result-' + identity, status)
    return store.rows[identity]


def call_message(row, *, blocks=False):
    call = row['tool_call']
    return ({'role': 'assistant', 'content': [{'type': 'tool_call', 'id': call['id'],
              'name': call['name'], 'input': call['arguments']}]}
            if blocks else {'role': 'assistant', 'content': '', 'tool_calls': [call]})


def output(row, content=None):
    return {'role': 'tool', 'tool_call_id': row['call_id'],
            'content': row['result'] if content is None else content}


def _lookup_count(store, messages):
    """Count actual builtin mapping reads in the recovery owner, not wall time."""
    count = 0
    previous = sys.getprofile()
    def profile(frame, event, arg):
        nonlocal count
        if (event == 'c_call' and frame.f_code.co_filename == job_store.__file__
                and getattr(arg, '__name__', None) == 'get'
                and isinstance(getattr(arg, '__self__', None), dict)):
            count += 1
    try:
        sys.setprofile(profile)
        result = store.recover(messages)
    finally:
        sys.setprofile(previous)
    return count, result


def test_recovery_evidence_work_grows_linearly_with_jobs_and_history(tmp_path):
    observations = []
    for size in (32, 128):
        store = JobStore(tmp_path / str(size))
        try:
            messages = []
            for index in range(size):
                row = add_job(store, str(index))
                messages.extend([call_message(row, blocks=index % 2 == 0), output(row),
                                 {'role': 'user', 'content': 'unrelated history'},
                                 {'role': 'assistant', 'content': 'ordinary response'}])
            count, (restored, recovered) = _lookup_count(store, messages)
            assert restored == messages and recovered == []
            assert count <= 16 * len(messages) + 40 * size
            observations.append(count)
        finally:
            store.close()
    assert observations[1] <= observations[0] * 5


def test_many_missing_calls_use_the_same_linear_evidence_pass(tmp_path):
    observations = []
    for size in (16, 64):
        store = JobStore(tmp_path / str(size))
        try:
            rows = [add_job(store, str(index)) for index in range(size)]
            messages = [output(row) for row in reversed(rows)]
            count, (restored, recovered) = _lookup_count(store, messages)
            assert len(recovered) == size
            assert [m['tool_call_id'] for m in restored if m['role'] == 'tool'] == [r['call_id'] for r in reversed(rows)]
            for index in range(0, size * 2, 2):
                assert restored[index]['content'][0]['id'] == restored[index + 1]['tool_call_id']
            assert count <= 16 * len(messages) + 40 * size
            observations.append(count)
        finally:
            store.close()
    assert observations[1] <= observations[0] * 5


def test_missing_calls_keep_original_output_order_and_append_order(ledger):
    first, second, third = [add_job(ledger, identity) for identity in ('first', 'second', 'third')]
    messages = [{'role': 'user', 'content': 'start'}, output(second),
                {'role': 'assistant', 'content': 'between'}, output(first)]
    original = copy.deepcopy(messages)
    restored, recovered = ledger.recover(messages)
    assert messages == original
    assert [r['call_id'] for r in recovered] == ['first', 'second', 'third']
    assert [(m['role'], m.get('tool_call_id') or (m.get('content')[0]['id']
            if isinstance(m.get('content'), list) else m.get('metadata', {}).get('live_recovery_job')))
            for m in restored] == [('user', None), ('assistant', 'second'), ('tool', 'second'),
            ('assistant', None), ('assistant', 'first'), ('tool', 'first'),
            ('user', 'job-first'), ('user', 'job-second'), ('assistant', 'third'),
            ('tool', 'third'), ('user', 'job-third')]
    assert ledger.recover(restored) == (restored, [])


def test_duplicate_outputs_are_all_reconciled_and_aliases_are_isolated(ledger):
    row = add_job(ledger, 'one')
    shared = output(row, row['receipt'])
    shared['metadata'] = {'nested': ['keep']}
    messages = [call_message(row), shared, shared]
    restored, recovered = ledger.recover(messages)
    assert len(recovered) == 1
    assert restored[1] is restored[2] and restored[1] is not shared
    assert restored[1]['content'] == row['result']
    assert shared['content'] == row['receipt']
    restored[1]['metadata']['nested'].append('detached')
    assert shared['metadata']['nested'] == ['keep']
    # Every attempt uses current evidence, including edits after a successful one.
    restored[2]['content'] = 'conflicting result'
    with pytest.raises(RuntimeError, match='conflicts'):
        ledger.recover(restored)


def test_two_jobs_with_one_notice_identity_keep_single_notice(ledger):
    first = add_job(ledger, 'first', job_id='shared-job', status='cancelled')
    second = add_job(ledger, 'second', job_id='shared-job', status='cancelled')
    messages = [call_message(first), output(first), call_message(second), output(second)]
    restored, recovered = ledger.recover(messages)
    assert len(recovered) == 1
    assert len([m for m in restored if m.get('metadata', {}).get('live_recovery_job') == 'shared-job']) == 1
    assert all(row.get('recovery_notice') for row in ledger.rows.values())
    assert ledger.recover(restored) == (restored, [])


def test_malformed_later_calls_keep_original_short_circuit_behavior(ledger):
    row = add_job(ledger, 'one')
    messages = [call_message(row), output(row), {'role': 'assistant', 'tool_calls': [None]}]
    assert ledger.recover(messages) == (messages, [])
    with pytest.raises(AttributeError):
        ledger.recover(messages[1:])


@pytest.mark.parametrize('failure', [asyncio.CancelledError('cancel'), OSError('write failed')])
def test_pending_write_failure_preserves_input_and_exact_failure(ledger, monkeypatch, failure):
    row = add_job(ledger, 'one', status='pending')
    messages = [call_message(row), {'role': 'tool', 'tool_call_id': 'one', 'content': row['receipt']}]
    original = copy.deepcopy(messages)
    def failed(_):
        raise failure
    monkeypatch.setattr(ledger, '_write', failed)
    with pytest.raises(type(failure)) as caught:
        ledger.recover(messages)
    assert caught.value is failure
    assert messages == original and ledger.rows['one']['status'] == 'pending'


def test_restored_call_record_is_visible_to_later_ledger_identity(ledger):
    first = add_job(ledger, 'first')
    second = add_job(ledger, 'second')
    # The existing loader validates the ledger identity, not nested call.id.
    ledger._write({**first, 'tool_call': {**first['tool_call'], 'id': 'second'}})
    messages = [output(second), output(first)]
    restored, recovered = ledger.recover(messages)
    calls = [m for m in restored if m['role'] == 'assistant']
    assert len(calls) == 1
    assert calls[0]['content'][0]['id'] == 'first'
    assert calls[0]['tool_calls'][0]['id'] == 'second'
    assert [row['call_id'] for row in recovered] == ['first']
    assert restored[0] == messages[0]
    assert ledger.recover(restored) == (restored, [])


def test_no_jobs_does_not_scan_message_fields(ledger):
    messages = [{'role': 'user', 'content': 'saved'}] * 100
    count, (restored, recovered) = _lookup_count(ledger, messages)
    assert count == 0 and recovered == [] and restored == messages
    assert restored is not messages and restored[0] is not messages[0]
