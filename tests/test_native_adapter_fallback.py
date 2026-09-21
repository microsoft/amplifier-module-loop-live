"""Selection works with the ordinary provider even without optional native code.

This minimal provider contract keeps these tests runnable in the core-only CI
environment: selection and the CLI adapter themselves are the production code.
"""

import asyncio
import importlib.util
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "adapters/cli"))
from amplifier_loop_live_cli.selection import select_root
from amplifier_module_loop_live.scope import NATIVE_REQUEST


@pytest.fixture
def ordinary_adapter(monkeypatch):
    class OpenAIProvider:
        default_model = "gpt-6-astra"

        def __init__(self):
            self.closed = False
            self.observed = []

        def get_info(self):
            return SimpleNamespace(defaults={"model": self.default_model})

        async def complete(self, request, **kwargs):
            self.observed.append(NATIVE_REQUEST.get())
            if kwargs.get("cancel"):
                raise asyncio.CancelledError
            return "ordinary response"

        async def close(self):
            self.closed = True

    package = ModuleType("amplifier_module_provider_openai")
    package.__path__ = []
    package.OpenAIProvider = OpenAIProvider
    monkeypatch.setitem(sys.modules, package.__name__, package)
    monkeypatch.setitem(sys.modules, package.__name__ + ".native", None)
    name = "amplifier_loop_live_cli.bundle_astra"
    path = (
        Path(__file__).resolve().parents[1]
        / "adapters/cli/amplifier_loop_live_cli/bundle_astra.py"
    )
    spec = importlib.util.spec_from_file_location(name, path)
    adapter = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, name, adapter)
    spec.loader.exec_module(adapter)
    assert not adapter.NATIVE_AVAILABLE
    return adapter, OpenAIProvider


@pytest.mark.asyncio
async def test_selected_astra_without_native_registers_and_runs_ordinary_cleanup(
    ordinary_adapter,
):
    adapter, provider_class = ordinary_adapter
    provider = adapter.BundleAstraProvider.wrap(provider_class())
    cleanups = []
    loop = SimpleNamespace()
    coordinator = SimpleNamespace(
        get=lambda name: {"fixture": provider} if name == "providers" else loop,
        get_capability=lambda name: None,
        register_cleanup=cleanups.append,
    )
    await select_root(
        SimpleNamespace(coordinator=coordinator),
        {"providers": [{"id": "fixture", "module": "provider-openai"}]},
        {"instance": "fixture", "model": "gpt-6-astra", "effort": "low"},
    )
    selected = loop.root_provider.provider
    assert isinstance(selected, adapter.SafeOpenAIProvider)
    assert not loop.root_provider.native_bundle_live
    assert len(cleanups) == 1 and cleanups[0].__self__ is selected
    assert await selected.complete(None) == "ordinary response"
    await cleanups[0]()
    assert selected.closed


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel", [False, True])
async def test_ordinary_complete_clears_and_restores_nested_native_attribution(
    ordinary_adapter, cancel
):
    adapter, provider_class = ordinary_adapter
    provider = adapter.BundleAstraProvider.wrap(provider_class())
    previous = object()
    token = NATIVE_REQUEST.set(previous)
    try:
        if cancel:
            with pytest.raises(asyncio.CancelledError):
                await provider.complete(None, cancel=True)
        else:
            assert await provider.complete(None) == "ordinary response"
        assert provider.observed == [None]
        assert NATIVE_REQUEST.get() is previous
    finally:
        NATIVE_REQUEST.reset(token)
