"""Late CLI provider selection/projection boundaries; offline, no host activation."""
import asyncio
import copy
import inspect
from pathlib import Path
import socket
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "adapters/cli"))
from amplifier_core.message_models import ChatRequest, ChatResponse, Message, TextBlock
from amplifier_loop_live_cli.host_adapter import CLIHostAdapter
from amplifier_loop_live_cli.selection import RootProvider
from amplifier_loop_live_cli.worker_settings import ObservedWorkerProvider
from amplifier_module_loop_live.host import HostAdapter
from amplifier_module_loop_live.orchestrator import BundleLiveOrchestrator, StreamingOrchestrator
from amplifier_module_loop_live.runtime import Input, Runtime
from test_job_media import Context


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def reject(*args, **kwargs):
        raise AssertionError("Offline provider fixtures cannot access the network")
    monkeypatch.setattr(socket.socket, "connect", reject)
    monkeypatch.setattr(socket.socket, "connect_ex", reject)


class Provider:
    name, config, default_model = "fixture", {}, "default"

    def __init__(self):
        self.requests = []

    def get_info(self):
        return SimpleNamespace(id="fixture", defaults={"model": self.default_model})

    def parse_tool_calls(self, response):
        return []

    async def complete(self, request, **kwargs):
        self.requests.append((request, kwargs))
        return ChatResponse(content=[TextBlock(text="done")])


def coordinator(providers, factory=None, runtime=None):
    caps = {"cli.media.prepare_provider": factory, "live.runtime": runtime}
    return SimpleNamespace(
        session_id="fixture", config={}, get_capability=caps.get,
        register_capability=lambda k, v: caps.__setitem__(k, v),
        get=lambda kind: providers if kind == "providers" else {},
    )


def selection():
    return {"instance": "selected-account", "model": "selected-model", "effort": "high", "thinkingBudget": None}


@pytest.mark.parametrize("native", [False, True])
def test_factory_runs_after_computer_on_actual_selected_driver_with_alias(monkeypatch, native):
    from amplifier_loop_live_cli import computer_results
    driver, order = Provider(), []
    if native:
        driver.native_bundle_live = True
        driver.steer_live, driver.close_live = AsyncMock(), AsyncMock()
    def computer(provider, owner):
        assert provider is driver
        order.append("computer")
    def factory(provider, *, alias=None):
        assert provider is driver and alias == "selected-account" and order == ["computer"]
        order.append("media")
        return Provider()
    monkeypatch.setattr(computer_results, "prepare_computer_provider", computer)
    prepared = CLIHostAdapter().prepare_provider(driver, coordinator({"selected-account": driver}, factory))
    assert prepared is not driver and order == ["computer", "media"]
    assert not hasattr(prepared, "stream") and not hasattr(prepared, "refit_request")
    assert bool(getattr(prepared, "native_bundle_live", False)) is native
    if native:
        assert prepared.steer_live is driver.steer_live and prepared.close_live is driver.close_live


def test_missing_factory_is_legacy_identity_and_invalid_return_fails_closed():
    driver = Provider()
    assert CLIHostAdapter().prepare_provider(driver, coordinator({"fixture": driver})) is driver
    with pytest.raises(TypeError, match="factory"):
        CLIHostAdapter().prepare_provider(driver, coordinator({"fixture": driver}, lambda *a, **k: None))


@pytest.mark.parametrize("runtime_present", [False, True])
@pytest.mark.parametrize("legacy", [False, True])
async def test_host_returned_provider_is_used_by_finite_and_live_engine(monkeypatch, runtime_present, legacy):
    original, prepared = Provider(), Provider()
    runtime = Runtime() if runtime_present else None
    owner = coordinator({"fixture": original}, runtime=runtime)
    reached, preparations = asyncio.Event(), []
    class Host(HostAdapter):
        def prepare_provider(self, provider, coordinator):
            assert provider is original
            preparations.append("prepare")
            return None if legacy else prepared
    owner.register_capability("live.host", Host())
    async def execute(self, prompt, context, providers, *args):
        assert self._select_provider(providers) is (original if legacy else prepared)
        await self._select_provider(providers).complete(ChatRequest(messages=[Message(role="user", content="text")]))
        reached.set()
        return "done"
    monkeypatch.setattr(StreamingOrchestrator, "execute", execute)
    hooks = SimpleNamespace(register=lambda *a, **kw: lambda: None, emit=AsyncMock())
    loop = BundleLiveOrchestrator({})
    task = asyncio.create_task(loop.execute("text", Context(), {"fixture": original}, {}, hooks, owner))
    try:
        await asyncio.wait_for(reached.wait(), 2)
        if runtime:
            await runtime.submit(Input("stop"))
        await asyncio.wait_for(task, 2)
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    assert preparations == ["prepare"]
    assert len((original if legacy else prepared).requests) == 1


async def test_worker_wrapper_retains_type_alias_and_optional_absence():
    driver, prepared, records = Provider(), Provider(), []
    registry = SimpleNamespace(configuration=lambda *args, **kwargs: records.append((args, kwargs)))
    original = ObservedWorkerProvider(driver, {"selected-account": driver}, registry, "child")
    view = CLIHostAdapter().prepare_provider(original, coordinator({"selected-account": driver}, lambda *a, **k: prepared))
    assert type(view) is ObservedWorkerProvider and view.original is prepared
    assert original.original is driver and not hasattr(view, "stream")
    await view.complete(ChatRequest(messages=[Message(role="user", content="text")]))
    assert records[-1][0][1]["instance"] == "selected-account"
    assert len(prepared.requests) == 1 and not driver.requests


@pytest.mark.parametrize("boundary", ["complete", "stream"])
@pytest.mark.parametrize("controls", ["effort", "none", "thinking"])
async def test_selected_root_preserves_canonical_diagnostics_and_exact_private_count_candidate(monkeypatch, boundary, controls):
    media_provider = pytest.importorskip("amplifier_foundation.session.media_provider")
    # Actual F1 provider mechanism with a small private-projection spy. This is
    # not an assertion about a vendor serializer or the tool-post bridge.
    prepared_rows, observed, traces = [], [], []
    class Prepared:
        projection_digest = "private-fixture"
        def __init__(self, canonical):
            self.request = canonical.model_copy(deep=True)
            self.request.messages[0].content = "PRIVATE PROJECTION SENTINEL"
            self.released = False
        def validate(self, **kwargs):
            assert not self.released
            return {"valid": True}
        def release(self):
            self.released = True
    async def prepare(request, **binding):
        assert request.messages[0].content == "canonical refs"
        item = Prepared(request)
        prepared_rows.append(item)
        return item
    class Selected(Provider):
        def get_media_request_support(self, **kwargs):
            return {"v": 1, "positions": []}
        async def request_budget(self, request, *, context_estimate=0, request_options=None):
            observed.append(("count", request, {"request_options": copy.deepcopy(request_options)}))
            return {"estimated_input_tokens": 10, "input_limit_tokens": 100, "context_token_budget": 100,
                    "measurement": {"kind": "provider_count", "source": "fixture", "input_tokens": 10}}
        async def complete(self, request, **kwargs):
            observed.append(("send", request, copy.deepcopy(kwargs)))
            return "done"
        async def stream(self, request, **kwargs):
            observed.append(("send", request, copy.deepcopy(kwargs)))
            yield "done"
    driver = Selected()
    cap = SimpleNamespace(prepare_request=prepare)
    def factory(provider, *, alias=None):
        assert provider is driver and alias == "selected-account"
        return media_provider.MediaPreparedProvider(provider, cap, provider_identity={"account": alias})
    chosen = selection()
    if controls == "none":
        chosen["effort"] = "none"
    elif controls == "thinking":
        chosen["effort"], chosen["thinkingBudget"] = None, 1000
    raw = RootProvider(driver, chosen, "openai" if controls == "none" else "fixture")
    view = CLIHostAdapter().prepare_provider(raw, coordinator({"selected-account": driver}, factory))
    assert type(view) is RootProvider and raw.provider is driver
    assert not hasattr(view, "refit_request")
    from amplifier_loop_live_cli import diagnostics
    monkeypatch.setattr(diagnostics, "trace", lambda name, data: traces.append((name, copy.deepcopy(data))))
    request = ChatRequest(messages=[Message(role="user", content="canonical refs")])
    before = request.model_dump()
    decision = await view.request_budget(request, context_estimate=10)
    assert decision["experimental.media.v1"]["projection_digest"] == "private-fixture"
    assert view.media_budget_witness(request)["validated"] is True
    if boundary == "complete":
        assert await view.complete(request) == "done"
    else:
        assert [part async for part in view.stream(request)] == ["done"]
    assert len(prepared_rows) == 1 and prepared_rows[0].released
    count, send = observed
    assert count[1] is send[1]
    assert count[1].model == "selected-model" and count[1].reasoning_effort == chosen["effort"]
    assert count[2] == {"request_options": send[2]}
    assert request.model_dump() == before
    assert all("PRIVATE PROJECTION SENTINEL" not in str(data) for _, data in traces)


@pytest.mark.parametrize("kind", ["root", "worker"])
async def test_optional_budget_and_refit_keep_named_options_and_strict_provider_signatures(kind):
    observed = []
    class Strict(Provider):
        async def request_budget(self, request, *, context_estimate, request_options=None):
            observed.append((request, request_options, context_estimate))
            return "counted"
        async def refit_request(self, request, *, error, context_estimate, request_options=None):
            observed.append((request, request_options, context_estimate))
            return "refitted"
    driver = Strict()
    registry = SimpleNamespace(configuration=lambda *a: None)
    view = (RootProvider(driver, selection(), "fixture") if kind == "root" else
            ObservedWorkerProvider(driver, {"account": driver}, registry, "child"))
    request = ChatRequest(messages=[Message(role="user", content="refs")])
    options = {"extended_thinking": True}
    assert "request_options" in inspect.signature(view.request_budget).parameters
    assert "request_options" in inspect.signature(view.refit_request).parameters
    assert await view.request_budget(request, context_estimate=1, request_options=options) == "counted"
    assert await view.refit_request(request, error=ValueError(), context_estimate=1, request_options=options) == "refitted"
    assert observed[0][0] is observed[1][0]
    expected = ({**options, "model": "selected-model", "reasoning_effort": "high"} if kind == "root" else options)
    assert observed[0][1] == observed[1][1] == expected


async def test_strict_legacy_counter_does_not_receive_transport_kwargs_or_retry():
    calls = []
    class Legacy(Provider):
        def request_budget(self, request, *, context_estimate):
            calls.append(request)
            return "legacy"
    view = RootProvider(Legacy(), selection(), "fixture")
    request = ChatRequest(messages=[])
    assert view.request_budget(request, context_estimate=1) == "legacy"
    assert calls[0].model == "selected-model"
    assert view.request_budget(request, context_estimate=1, request_options={"temperature": 0.5}) is None
    assert len(calls) == 1


def test_stream_request_support_is_forwarded_only_from_actual_provider_with_selected_options():
    driver, observed = Provider(), []
    raw = RootProvider(driver, selection(), "fixture")
    assert not hasattr(raw, "get_stream_request_support")
    def support(*, model, request_options=None):
        observed.append((model, request_options))
        return {"v": 1, "tools": "chat_request"}
    driver.stream = AsyncMock()
    driver.get_stream_request_support = support
    view = RootProvider(driver, selection(), "fixture")
    assert view.get_stream_request_support(model="ignored", request_options={"temperature": 0.3}) == {
        "v": 1, "tools": "chat_request"}
    assert observed == [("selected-model", {"temperature": 0.3, "model": "selected-model",
                                            "reasoning_effort": "high"})]


async def test_all_mounted_aliases_are_prepared_before_later_pin_or_utility_selection(monkeypatch):
    drivers = {"first": Provider(), "second": Provider()}
    views = {name: Provider() for name in drivers}
    preparations = []
    class Host(HostAdapter):
        def prepare_provider(self, provider, coordinator):
            alias = next(name for name, original in drivers.items() if original is provider)
            preparations.append(alias)
            return views[alias]
    owner = coordinator(drivers)
    owner.register_capability("live.host", Host())
    async def execute(self, prompt, context, providers, *args):
        assert providers == views
        self._pinned_provider_name = "second"
        assert self._select_provider(providers) is views["second"]
        assert self._select_provider(owner.get("providers")) is views["second"]
        return "done"
    monkeypatch.setattr(StreamingOrchestrator, "execute", execute)
    await BundleLiveOrchestrator({}).execute("text", Context(), drivers, {}, None, owner)
    assert preparations == ["first", "second"]


@pytest.mark.parametrize("boundary", ["complete", "stream"])
async def test_worker_foreign_tools_share_selected_request_for_count_and_send(boundary):
    from amplifier_core.message_models import ToolSpec
    observed, records = [], []
    class Selected(Provider):
        async def request_budget(self, request, **kwargs):
            observed.append(request)
        async def complete(self, request, **kwargs):
            observed.append(request)
            return "done"
        async def stream(self, request, **kwargs):
            observed.append(request)
            yield "done"
    driver = Selected()
    registry = SimpleNamespace(configuration=lambda *a: records.append(a))
    view = ObservedWorkerProvider(driver, {"account": driver}, registry, "child")
    request = ChatRequest(messages=[Message(role="user", content="refs")], tools=[
        ToolSpec(name="computer", parameters={}, description="fixture").model_copy(update={"type": "computer"})])
    before = request.model_dump()
    await view.request_budget(request)
    if boundary == "complete":
        await view.complete(request)
    else:
        assert [part async for part in view.stream(request)] == ["done"]
    assert observed[0] is observed[1]
    assert getattr(observed[0].tools[0], "type", "function") == "function"
    assert request.model_dump() == before and len(records) == 1
    assert records[0][1]["instance"] == "account"


@pytest.mark.parametrize("persistent", [False, True])
async def test_raw_child_native_install_precedes_late_factory(monkeypatch, persistent):
    from amplifier_loop_live_cli import children, configured, diagnostics
    # The actual child setup seam, with no host filesystem, settings or spawner.
    order, driver, old = [], Provider(), Provider()
    runtime = Runtime()
    providers = {"selected-account": old}
    registry = SimpleNamespace(
        root="root", attach=lambda *a: runtime,
        configuration=lambda *a, **kw: order.append("configuration"),
        finite_finished=lambda *a: None,
    )
    async def install(owner, *, native):
        assert native is persistent
        order.append("native")
        providers["selected-account"] = driver
    async def install_children(*a, **kw):
        order.append("children")
    from types import ModuleType
    native_module = ModuleType("amplifier_loop_live_cli.bundle_astra")
    native_module.install = install
    monkeypatch.setitem(sys.modules, native_module.__name__, native_module)
    monkeypatch.setattr(children, "install", install_children)
    monkeypatch.setattr(diagnostics, "install_hooks", lambda *a: None)
    scope = {"registry": registry, "persistent": persistent}
    token = children.CHILD_SCOPE.set(scope)
    from amplifier_loop_live_cli import computer_results
    monkeypatch.setattr(computer_results, "prepare_computer_provider", lambda *a: order.append("computer"))
    def factory(provider, *, alias=None):
        assert provider is driver and alias == "selected-account"
        assert order[:2] == ["native", "children"]
        order.append("media")
        return Provider()
    owner = coordinator(providers, factory)
    loop, host = BundleLiveOrchestrator({}), CLIHostAdapter()
    try:
        selected_runtime, returned_providers, finite_scope = await host.prepare_execution(loop, owner, providers)
        assert selected_runtime is (runtime if persistent else None)
        assert finite_scope is (None if persistent else scope)
        view = host.prepare_provider(loop._select_provider(returned_providers), owner)
        assert isinstance(view, ObservedWorkerProvider) and view.original is not driver
        assert order[-2:] == ["computer", "media"]
    finally:
        children.CHILD_SCOPE.reset(token)
