"""CLI wrappers preserve the installed provider's real budget boundaries."""

import copy
import inspect
from pathlib import Path
import socket
import sys
from types import SimpleNamespace

import pytest

pytest.importorskip("amplifier_module_provider_openai.native")
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "adapters/cli"))
from amplifier_core.llm_errors import ContextLengthError
from amplifier_core.message_models import ChatRequest, Message
from amplifier_loop_live_cli.bundle_astra import BundleAstraProvider, SafeOpenAIProvider
from amplifier_module_provider_openai import OpenAIProvider


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def reject(*args, **kwargs):
        raise AssertionError("Budget compatibility tests cannot use the network")

    monkeypatch.setattr(socket.socket, "connect", reject)
    monkeypatch.setattr(socket.socket, "connect_ex", reject)


@pytest.fixture(params=[SafeOpenAIProvider, BundleAstraProvider])
def provider(request):
    return request.param.wrap(
        OpenAIProvider(api_key="fixture", config={"default_model": "gpt-6-astra"})
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["request_budget", "final_dispatch_guard"])
@pytest.mark.parametrize("over_limit", [False, True])
async def test_text_uses_real_provider_budget_and_retains_limit_enforcement(
    provider, boundary, over_limit
):
    request = ChatRequest(messages=[Message(role="user", content="ordinary text")])
    params = provider._budget_params(request)
    before = copy.deepcopy(params)
    if over_limit:
        provider._budget_calibration[params["model"]] = (1_000_000.0, 1, 1_000_000)

    if boundary == "request_budget":
        decision = provider.request_budget(request, context_estimate=1000)
        if inspect.isawaitable(decision):
            decision = await decision
        assert decision is not None
        assert (decision["context_token_budget"] < 1000) is over_limit
    elif over_limit:
        with pytest.raises(ContextLengthError, match="local input allowance"):
            provider._guard_assembled_params(params)
    else:
        provider._guard_assembled_params(params)
    assert params == before


@pytest.mark.asyncio
async def test_typed_media_keeps_provider_fallback_and_does_not_calibrate_text(provider):
    # A valid, tiny PNG. Its transport bytes must not become text-token evidence.
    png = (
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk"
        "+A8AAQUBAScY42YAAAAASUVORK5CYII="
    )
    request = ChatRequest(messages=[Message(role="user", content=[{
        "type": "image",
        "source": {"type": "base64", "media_type": "image/png", "data": png},
    }])])
    params = provider._budget_params(request)
    before = copy.deepcopy(params)
    provider._budget_calibration[params["model"]] = (1_000_000.0, 1, 1_000_000)
    calibration = dict(provider._budget_calibration)
    decision = provider.request_budget(request, context_estimate=1000)
    if inspect.isawaitable(decision):
        decision = await decision
    assert decision is None
    provider._guard_assembled_params(params)
    provider._record_budget_calibration(
        params,
        SimpleNamespace(status="completed", usage=SimpleNamespace(input_tokens=100)),
    )
    assert provider._budget_calibration == calibration
    assert params == before
