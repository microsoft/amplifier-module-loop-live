"""Actual finite/live loop cancellation boundaries without a model service."""
import asyncio
import json
from types import SimpleNamespace

import pytest
from amplifier_core import AmplifierSession, ToolResult
from amplifier_core.message_models import ChatResponse, TextBlock, ToolCall
from amplifier_module_loop_live.runtime import Input, Runtime


class Provider:
    name = 'fixture'
    config = {}

    def __init__(self, *, completed_tool=False):
        self.completed_tool = completed_tool
        self.calls = 0
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.turn_done = asyncio.Event()

    async def complete(self, request, **kwargs):
        self.calls += 1
        if self.completed_tool and self.calls == 1:
            return ChatResponse(content=[], tool_calls=[
                ToolCall(id='completed-effect', name='record', arguments={})])
        self.entered.set()
        await self.release.wait()
        # The inner task really terminates; a timeout in the assertion below
        # must not cancel the outer owner and manufacture terminal evidence.
        asyncio.current_task().add_done_callback(lambda _task: self.turn_done.set())
        raise asyncio.CancelledError('private admission detail')

    def parse_tool_calls(self, response):
        return response.tool_calls or []

    def get_info(self):
        return SimpleNamespace(default_model='fixture')


class Record:
    name = 'record'
    description = 'Record one completed effect.'
    input_schema = {'type': 'object', 'properties': {}}

    def __init__(self):
        self.calls = 0

    async def execute(self, _input):
        self.calls += 1
        return ToolResult(success=True, output={'saved': 'completed-effect'})


async def open_session(*, live, completed_tool):
    runtime = Runtime()
    session = AmplifierSession({'session': {
        'orchestrator': {'module': 'loop-live', 'config': {
            'configured_bundle': True, 'min_delay_between_calls_ms': 0}},
        'context': {'module': 'context-simple'}}, 'providers': []},
        session_id=runtime.session_id)
    await session.initialize()
    provider, tool = Provider(completed_tool=completed_tool), Record()
    await session.coordinator.mount('providers', provider, name='fixture')
    await session.coordinator.mount('tools', tool, name='record')
    if live:
        session.coordinator.register_capability('live.runtime', runtime)
    return session, runtime, provider, tool


@pytest.mark.parametrize('completed_tool', [False, True])
async def test_inner_cancel_settles_live_owner_without_success_or_replay(completed_tool):
    session, runtime, provider, tool = await open_session(live=True, completed_tool=completed_tool)
    task = asyncio.create_task(session.execute(''))
    try:
        await runtime.wait_for(lambda e: e['type'] == 'session.ready', 2)
        await runtime.submit(Input('user', 'Do this once', id='original'))
        await asyncio.wait_for(provider.entered.wait(), 2)
        generation = runtime.generation['id']
        await runtime.submit_steering(Input('steer', 'Do not lose my guidance', id='held'), generation)
        provider.release.set()
        await asyncio.wait_for(provider.turn_done.wait(), 2)
        done, _ = await asyncio.wait({task}, timeout=0.2)
        assert task in done, 'The inner turn ended but its live owner is still waiting on an empty inbox'
        with pytest.raises((asyncio.CancelledError, RuntimeError), match='private admission detail'):
            await task
        terminal = [e for e in runtime.events if e['type'] == 'generation.detached']
        assert len(terminal) == 1
        assert terminal[0]['generation_id'] == generation
        assert terminal[0]['input_ids'] == ['original']
        assert terminal[0]['cancellation'] == 'unconfirmed'
        closed = [e for e in runtime.events if e['type'] == 'session.closed']
        assert len(closed) == 1 and closed[0]['status'] == 'cancelled'
        assert not any(e['type'] in {'generation.finished', 'generation.failed'} for e in runtime.events)
        assert runtime.generation is None and runtime.closed
        assert runtime.steering_outcomes['held']['disposition'] == 'held'
        assert tool.calls == int(completed_tool)
        assert provider.calls == 1 + int(completed_tool)
        messages = await session.coordinator.get('context').get_messages()
        if completed_tool:
            assert any(m.get('role') == 'tool' and 'completed-effect' in str(m.get('content')) for m in messages)
        assert 'private admission detail' not in json.dumps(runtime.events)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await session.cleanup()


async def test_explicit_stop_wins_cancelled_turn_race_without_another_turn():
    session, runtime, provider, tool = await open_session(live=True, completed_tool=True)
    task = asyncio.create_task(session.execute(''))
    try:
        await runtime.wait_for(lambda e: e['type'] == 'session.ready', 2)
        await runtime.submit(Input('user', 'Do this once', id='original'))
        await asyncio.wait_for(provider.entered.wait(), 2)
        await runtime.submit(Input('stop', target='cancel'))
        provider.release.set()
        await asyncio.wait_for(task, 2)
        assert tool.calls == 1 and provider.calls == 2
        assert len([e for e in runtime.events if e['type'] == 'generation.detached']) == 1
        assert len([e for e in runtime.events if e['type'] == 'session.closed']) == 1
        assert runtime.events[-1]['status'] == 'cancelled'
        assert not any(e['type'] == 'generation.finished' for e in runtime.events)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await session.cleanup()


async def test_finite_turn_still_propagates_cancellation_once():
    session, runtime, provider, tool = await open_session(live=False, completed_tool=True)
    task = asyncio.create_task(session.execute('Do this once'))
    try:
        await asyncio.wait_for(provider.entered.wait(), 2)
        provider.release.set()
        with pytest.raises((asyncio.CancelledError, RuntimeError), match='private admission detail'):
            await asyncio.wait_for(task, 2)
        assert tool.calls == 1 and provider.calls == 2
        assert not runtime.events
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await session.cleanup()



async def test_cancelled_turn_checkpoint_retains_exact_host_activation():
    from test_activation import Ownership

    ownership = Ownership()
    original = ownership.activate()
    session, runtime, provider, tool = await open_session(live=True, completed_tool=True)
    runtime.capture_activation = ownership.current
    checkpoints = []

    async def checkpoint(status=None):
        ownership.check(ownership.current())
        checkpoints.append((ownership.current(), status))

    session.coordinator.register_capability('live.activation', ownership)
    session.coordinator.register_capability('live.checkpoint', checkpoint)
    task = asyncio.create_task(session.execute(''))
    try:
        await runtime.wait_for(lambda e: e['type'] == 'session.ready', 2)
        await runtime.submit(Input('user', 'Do this once', id='owned', activation=original))
        await asyncio.wait_for(provider.entered.wait(), 2)
        provider.release.set()
        await asyncio.wait_for(provider.turn_done.wait(), 2)
        done, _ = await asyncio.wait({task}, timeout=0.2)
        assert task in done
        with pytest.raises((asyncio.CancelledError, RuntimeError), match='private admission detail'):
            await task
        assert checkpoints == [(original, 'cancelled')]
        assert ownership.current_valid
        assert tool.calls == 1 and provider.calls == 2
        assert [e['input_ids'] for e in runtime.events if e['type'] == 'generation.detached'] == [['owned']]
        assert all('activation' not in e for e in runtime.events)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await session.cleanup()
