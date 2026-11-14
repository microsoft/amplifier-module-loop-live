"""Actual configured host entrypoint with a controlled prepared-session factory.

The factory uses actual Core initialization/loader, not bundle resolution or a
production provider. Workspace configuration, UI and provider SDK setup are
isolated; those are not claimed as full installed-host qualification.
"""
import asyncio
import base64
import copy
import sys
from importlib.machinery import ModuleSpec
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

pytest.importorskip("amplifier_app_cli.session_hooks")
from amplifier_core import AmplifierSession, ProviderInfo, ToolResult
from amplifier_core.message_models import ChatResponse, TextBlock, ToolCall, ImageBlock
from amplifier_module_loop_live.runtime import Runtime, Input
from test_cli_media_provider import no_network  # noqa: F401
from test_media_positive import image_bytes, POLICY


@pytest.mark.parametrize("format", ["PNG", "JPEG"])
@pytest.mark.parametrize("failure", [None, "before-initialize", "after-initialize"])
async def test_configured_root_owns_preinit_graph_media_and_failure_cleanup(tmp_path, monkeypatch, format, failure):
    from amplifier_loop_live_cli import configured, provider_imports, selection
    from amplifier_app_cli import session_runner, session_store
    from amplifier_app_cli.runtime import config as runtime_config
    from amplifier_app_cli.lib import settings as settings_module
    from amplifier_app_cli.lib import bundle_loader
    from amplifier_app_cli import paths
    from amplifier_foundation.session.media_provider import MediaPreparedProvider
    if "request_scope" not in __import__("inspect").signature(MediaPreparedProvider).parameters:
        pytest.skip("Requires current graph-bound media provider")
    monkeypatch.chdir(tmp_path)
    workspace_module = ModuleType("amplifier_workspace")
    workspace_config = ModuleType("amplifier_workspace.config")
    workspace_config.load_config = lambda: SimpleNamespace(bundle="fixture")
    monkeypatch.setitem(sys.modules, workspace_module.__name__, workspace_module)
    monkeypatch.setitem(sys.modules, workspace_config.__name__, workspace_config)
    settings = SimpleNamespace(get_active_bundle=lambda: "fixture", get_app_bundles=lambda: [],
                               get_merged_settings=lambda: {"configurator": {}})
    monkeypatch.setattr(settings_module, "AppSettings", lambda: settings)
    native = session_store.SessionStore(tmp_path / "sessions")
    monkeypatch.setattr(session_store, "SessionStore", lambda: native)
    monkeypatch.setattr(paths, "create_foundation_resolver", lambda: None)
    monkeypatch.setattr(bundle_loader, "AppModuleResolver", lambda **kwargs: kwargs["bundle_resolver"])
    monkeypatch.setattr(runtime_config, "inject_user_providers", lambda *args: None)
    monkeypatch.setattr(session_runner, "_inject_observability_events", lambda *args: None)
    monkeypatch.setattr(session_runner, "_inject_invocation_metadata", lambda *args, **kwargs: None)
    monkeypatch.setattr(session_runner, "register_mention_handling", lambda *args: None)
    monkeypatch.setattr(session_runner, "register_session_spawning", lambda *args, **kwargs: None)
    monkeypatch.setattr(provider_imports, "prepare_openai_import", AsyncMock())
    # Driver SDK installation/selection policy has its own reviewed controls.
    native_module = ModuleType("amplifier_loop_live_cli.bundle_astra")
    native_module.install = AsyncMock(return_value=[])
    monkeypatch.setitem(sys.modules, native_module.__name__, native_module)
    monkeypatch.setattr(selection, "choices_for", AsyncMock(return_value=[]))
    data = base64.b64encode(image_bytes(format)).decode()
    sent, created, media_owners = asyncio.Event(), [], []
    calls = []
    class Provider:
        default_model, name = "fixture", "fixture"
        def get_info(self):
            return ProviderInfo(id="fixture", display_name="Offline", capabilities=["tools"], defaults={"model": "fixture"})
        def get_media_request_support(self, **kwargs):
            return {"v": 1, "positions": ["tool"]}
        async def request_budget(self, request, **kwargs):
            self.counted = request
            return {"estimated_input_tokens": 10, "input_limit_tokens": 10000,
                    "context_token_budget": 10000, "measurement": {
                        "kind": "provider_count", "source": "offline synthetic owner", "input_tokens": 10}}
        async def complete(self, request, **kwargs):
            assert request is self.counted
            if any(isinstance(block, ImageBlock) for message in request.messages
                   for block in message.content if isinstance(message.content, list)):
                sent.set()
            if not calls:
                calls.append("requested")
                return ChatResponse(content=[], tool_calls=[ToolCall(id="configured-image", name="image_fixture", arguments={"async": True})])
            return ChatResponse(content=[TextBlock(text="done")])
        def parse_tool_calls(self, response):
            return response.tool_calls or []
    class Tool:
        name, description, input_schema = "image_fixture", "Offline", {"type": "object"}
        async def execute(self, arguments):
            calls.append("executed")
            return ToolResult(success=True, output="caption", content=[{"type": "image", "source": {
                "type": "base64", "media_type": "image/" + format.lower(), "data": data}}])
    for identity, kind, value in (("provider-configured-fixture", "providers", Provider()),
                                  ("tool-configured-fixture", "tools", Tool())):
        name = "amplifier_module_" + identity.replace("-", "_")
        module = ModuleType(name)
        module.__file__, module.__spec__ = __file__, ModuleSpec(name, loader=None, origin=__file__)
        async def mount(current, config=None, *, kind=kind, value=value):
            await current.mount(kind, value, name="fixture" if kind == "providers" else value.name)
        module.mount = mount
        monkeypatch.setitem(sys.modules, name, module)
    plan = {"session": {"orchestrator": {"module": "loop-live", "config": {
                "min_delay_between_calls_ms": 0, "background_tools": ["image_fixture"]}},
            "context": {"module": "context-simple"}},
            "providers": [{"module": "provider-configured-fixture"}],
            "tools": [{"module": "tool-configured-fixture"}], "session_media": POLICY}
    class Prepared:
        def __init__(self):
            self.mount_plan = copy.deepcopy(plan)
            from amplifier_foundation.bundle import Bundle
            self.bundle = Bundle(name="controlled", session={}, agents={})
            self.resolver = SimpleNamespace(get_module_source=lambda identity: None)
        async def create_session(self, *, before_initialize, session_id, **kwargs):
            actual = AmplifierSession(copy.deepcopy(self.mount_plan), session_id=session_id)
            created.append(actual)
            await before_initialize(actual)
            media_owners.append(actual.coordinator.get_capability("cli.session_media"))
            assert actual.coordinator.get_capability("cli.media.execution_registration")
            if failure == "before-initialize":
                raise RuntimeError("fixture before initialization failed")
            await actual.initialize()
            if failure == "after-initialize":
                raise RuntimeError("fixture after initialization failed")
            return actual
        def create_system_prompt_factory(self, *args, **kwargs):
            return AsyncMock(return_value="controlled fixture prompt")
    prepared = Prepared()
    monkeypatch.setattr(runtime_config, "resolve_bundle_config", AsyncMock(return_value=(copy.deepcopy(plan), prepared)))
    runtime = Runtime()
    task = None
    token = configured.APPROVAL_CHANNEL.set(None)
    try:
        if failure:
            with pytest.raises(RuntimeError, match="fixture .* initialization failed"):
                await configured.prepare_manager(tmp_path, runtime=runtime, bundle="fixture", report_dir=tmp_path / "reports")
            assert len(created) == 1 and media_owners[0].closed
            assert calls == []
            return
        actual, returned, report = await configured.prepare_manager(
            tmp_path, runtime=runtime, bundle="fixture", report_dir=tmp_path / "reports")
        assert actual is created[0] and returned is runtime
        owner = actual.coordinator
        bridge = owner.get_capability("session.media.tool_post")
        task = asyncio.create_task(actual.execute(""))
        await runtime.wait_for(lambda event: event["type"] == "session.ready", 3)
        assert bridge.validate_receipt(bridge.graph_receipt)
        await runtime.submit(Input("user", "produce fixture image"))
        await asyncio.wait_for(sent.wait(), 5)
        rows, _ = native.load(runtime.session_id)
        original = next(row for row in rows if row.get("tool_call_id") == "configured-image")
        assert len(original["metadata"]["experimental.media.v1"]["refs"]) == 1
        assert calls == ["requested", "executed"] and data not in str(rows)
        assert bridge.validate_receipt(bridge.graph_receipt)
        await runtime.submit(Input("stop"))
        await asyncio.wait_for(task, 3)
    finally:
        if task and not task.done():
            task.cancel()
        if task:
            await asyncio.gather(task, return_exceptions=True)
        if created:
            await created[0].cleanup()
        configured.APPROVAL_CHANNEL.reset(token)