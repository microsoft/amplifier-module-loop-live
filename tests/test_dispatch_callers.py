"""New controllers use exactly the ordinary engine/approval lease, opt-in only."""
import json
from types import SimpleNamespace
import pytest
from amplifier_core import HookResult
from amplifier_core.message_models import ToolCall, ChatResponse, TextBlock
from test_approved_dispatch import environment, Client


async def invoke(environment, name="ui_use", handler=None):
    _, coordinator, loop, _ = environment
    client = Client(coordinator, handler)
    client.name = name
    await coordinator.mount("tools", client, name=name)
    with loop.approved_dispatch.run_scope(coordinator):
        _, _, result = await loop._execute_tool_only(ToolCall(id="ui-parent", name=name, arguments={}),
            coordinator.get("tools"), coordinator.hooks, "ui-group", coordinator)
    return result, client


async def test_nondefault_caller_cannot_bind_without_host_opt_in(environment):
    result, client = await invoke(environment)
    assert client.lease is None
    assert environment[3].calls == 0
    assert "authorized" in result


async def test_selected_caller_preserves_hook_denial_and_parent_identity(environment):
    _, coordinator, loop, echo = environment
    loop.config["programmatic_callers"] = ["tool_exec", "ui_use"]
    observed = []
    async def deny(event, data):
        if data["tool_name"] == "read":
            observed.append(data)
            return HookResult(action="deny", reason="host denied")
        return HookResult()
    coordinator.hooks.register("tool:pre", deny)
    result, client = await invoke(environment)
    assert json.loads(result)["output"]["status"] == "denied"
    assert echo.calls == 0 and observed[0]["parent_tool_call_id"] == "ui-parent"
    assert "ui_use" not in client.lease.tools and "tool_exec" not in client.lease.tools


async def test_caller_policy_change_revokes_lease(environment):
    _, _, loop, echo = environment
    loop.config["programmatic_callers"] = ["ui_use"]
    async def revoke(lease):
        loop.config["programmatic_callers"] = []
        with pytest.raises(RuntimeError): await lease.call("read", {})
        return {"revoked": True}
    await invoke(environment, handler=revoke)
    assert echo.calls == 0


async def test_selected_caller_runs_normal_tool_and_cannot_recurse(environment):
    _, _, loop, echo = environment
    loop.config["programmatic_callers"] = ["ui_use"]
    result, client = await invoke(environment)
    assert json.loads(result)["output"]["status"] == "completed" and echo.calls == 1
    assert "ui_use" not in client.lease.tools


@pytest.mark.parametrize("independent", [False, True])
async def test_selected_caller_nested_hold_is_nonretryable_and_not_replayed(environment, independent):
    from amplifier_module_loop_streaming.suspension import ToolExecutionSuspended

    _, coordinator, loop, echo = environment
    loop.config["programmatic_callers"] = ["ui_use"]
    echo.allows_concurrent_execution = lambda arguments: independent
    effects = []
    events = []

    async def suspend(arguments):
        effects.append("started")
        raise ToolExecutionSuspended({"version": 1, "position": "after-start"})

    async def capture(event, data):
        events.append(data)
        return HookResult()

    echo.execute = suspend
    coordinator.hooks.register("tool:dispatch_suspended", capture)
    raw, client = await invoke(environment)
    result = json.loads(raw)
    assert result["success"] is False
    assert result["output"]["status"] == "held"
    assert result["output"]["retryable"] is False
    assert result["output"]["effects"] == "not_rolled_back"
    assert result["error"]["code"] == "nested_execution_held"
    assert effects == ["started"]
    assert len(events) == 1 and events[0]["source"]["parent_tool_call_id"] == "ui-parent"
    with pytest.raises(RuntimeError, match="not active"):
        await client.lease.call("read", {})
    assert effects == ["started"]


async def test_resume_retains_nested_model_budget_and_expires_old_lease(environment):
    from amplifier_core.message_models import ChatRequest, Message
    from amplifier_module_loop_streaming import ExecutionSuspended
    from amplifier_module_loop_live.orchestrator import BundleLiveOrchestrator

    _, coordinator, loop, _ = environment
    context = coordinator.get("context")
    loop.config.update(programmatic_max_model_calls=1, max_iterations=4)
    owner = SimpleNamespace(hold=False, saved=None)
    owner.requested = lambda: owner.hold
    async def checkpoint(frame):
        owner.saved = frame
        return True
    owner.checkpoint = checkpoint
    coordinator.register_capability("execution.suspension", owner)
    coordinator.register_capability("execution.resume", lambda frame: True)
    root_calls, utility_calls, controller_calls = [], [], []

    class Provider:
        name, config = "fixture", {}
        def get_info(self):
            return SimpleNamespace(defaults={"model": "fixture"})
        def parse_tool_calls(self, response):
            return response.tool_calls or []
        async def complete(self, request, **kwargs):
            if any(message.content == "utility" for message in request.messages):
                utility_calls.append(request)
                return ChatResponse(content=[TextBlock(text="selected")])
            root_calls.append(request)
            if len(root_calls) <= 2:
                return ChatResponse(content=[], tool_calls=[
                    ToolCall(id=f"controller-{len(root_calls)}", name="tool_exec", arguments={})
                ])
            return ChatResponse(content=[TextBlock(text="done")])

    provider = Provider()
    await coordinator.mount("providers", provider, name="fixture")
    request = ChatRequest(messages=[Message(role="user", content="utility")])
    async def program(lease):
        controller_calls.append(lease)
        complete = coordinator.get_capability("model.complete")
        if len(controller_calls) == 1:
            await complete(request)
            owner.hold = True
        else:
            with pytest.raises(RuntimeError, match="budget exhausted"):
                await complete(request)
        return {"settled": True}
    client = Client(coordinator, program)
    await coordinator.mount("tools", client, name="tool_exec")
    with pytest.raises(ExecutionSuspended) as held:
        await loop.execute("original", context, {"fixture": provider},
                           coordinator.get("tools"), coordinator.hooks, coordinator)
    assert len(utility_calls) == 1 and len(controller_calls) == 1
    first_lease = controller_calls[0]
    with pytest.raises(RuntimeError):
        await first_lease.call("read", {})
    owner.hold = False
    restored = BundleLiveOrchestrator(dict(loop.config))
    coordinator.register_capability("tools.dispatch", restored.approved_dispatch)
    coordinator.register_capability("model.complete", restored.approved_dispatch.complete)
    assert await restored.resume(held.value.continuation, context, {"fixture": provider},
                                 coordinator.get("tools"), coordinator.hooks, coordinator) == "done"
    assert len(utility_calls) == 1
    assert len(controller_calls) == 2 and len(root_calls) == 3
    assert restored._llm_calls_this_turn == 4
