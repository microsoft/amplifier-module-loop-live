"""New controllers use exactly the ordinary engine/approval lease, opt-in only."""
import json
import pytest
from amplifier_core import HookResult
from amplifier_core.message_models import ToolCall
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
