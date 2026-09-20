"""Exercise the real streaming goal loop at its asynchronous evaluation race."""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from amplifier_module_loop_live.orchestrator import BundleLiveOrchestrator
from amplifier_module_loop_streaming import StreamingOrchestrator


@pytest.mark.asyncio
async def test_pause_during_evaluator_prevents_stale_automatic_turn(monkeypatch):
    loop = BundleLiveOrchestrator({'goal_stall_threshold': 100})
    entered, release = asyncio.Event(), asyncio.Event()
    paused = False
    async def guard(): return not paused
    coordinator = SimpleNamespace(session_state={'goal': {'turns_used': 0, 'last_reason': None, 'condition': 'finish task', 'cap': 4}},
        cancellation=SimpleNamespace(is_cancelled=False), get_capability=lambda name: guard if name == 'live.continuation_guard' else None)
    actual_turn = AsyncMock(return_value='First response')
    monkeypatch.setattr(StreamingOrchestrator, '_execute_one_turn', actual_turn)
    async def evaluate(*args):
        entered.set(); await release.wait(); return False, 'Continue toward the objective'
    loop._evaluate_goal = evaluate
    loop._flush_pending_complete = AsyncMock()
    hooks = SimpleNamespace(emit=AsyncMock())
    task = asyncio.create_task(loop._execute_guarded_goal('Explicit user input', None, {}, {}, hooks, coordinator))
    await asyncio.wait_for(entered.wait(), 2)
    paused = True
    coordinator.session_state['goal'] = None
    release.set()
    assert await task == 'First response'
    assert actual_turn.await_count == 1
    assert coordinator.session_state['goal'] is None
    # A later explicit input still receives a turn while automatic continuation
    # remains paused. No hidden prompt or replay is needed to answer a user.
    assert await loop._execute_guarded_goal('What happened?', None, {}, {}, hooks, coordinator) == 'First response'
    assert actual_turn.await_count == 2


@pytest.mark.asyncio
async def test_opt_in_guard_allows_normal_continuation(monkeypatch):
    loop = BundleLiveOrchestrator({'goal_stall_threshold': 100})
    guard = AsyncMock(return_value=True)
    coordinator = SimpleNamespace(session_state={'goal': {'turns_used': 0, 'last_reason': None, 'condition': 'finish task', 'cap': 5}}, cancellation=SimpleNamespace(is_cancelled=False), get_capability=lambda name: guard)
    actual_turn = AsyncMock(side_effect=['First response', 'Final response'])
    monkeypatch.setattr(StreamingOrchestrator, '_execute_one_turn', actual_turn)
    loop._evaluate_goal = AsyncMock(side_effect=[(False, 'Continue'), (True, 'Evidence verified')])
    loop._flush_pending_complete = AsyncMock()
    loop._summarize_goal_run = AsyncMock(return_value='Done')
    assert await loop._execute_guarded_goal('Go', None, {}, {}, SimpleNamespace(emit=AsyncMock()), coordinator) == 'Final response'
    assert guard.await_count == 1 and actual_turn.await_count == 2
