"""CLI binding delegates transport while retaining local computer policy."""
import importlib.util
from pathlib import Path
import sys

import pytest

pytest.importorskip('amplifier_module_provider_openai.native')
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'adapters/cli'))
from amplifier_loop_live_cli.bundle_astra import BundleAstraProvider, SafeOpenAIProvider
from amplifier_module_provider_openai.native import NativeResponsesProvider
from amplifier_module_provider_openai import OpenAIProvider


def test_cli_is_thin_provider_owned_transport_with_existing_image_policy():
    original=OpenAIProvider(api_key='fixture',config={'default_model':'gpt-6-astra'})
    provider=BundleAstraProvider.wrap(original)
    assert isinstance(provider,NativeResponsesProvider)
    assert provider._api_key==original._api_key
    assert provider.owner_getter() is None
    assert provider.trace.__module__=='amplifier_loop_live_cli.diagnostics'
    assert provider._prepare_native_messages([])==[]
    assert provider._validate_native_items([])==[]
    assert provider._create_response.__func__ is NativeResponsesProvider._create_response


def test_existing_safe_provider_retains_normal_driver_and_policy():
    p=SafeOpenAIProvider.wrap(OpenAIProvider(api_key='fixture',config={'default_model':'gpt-5.6-terra'}))
    assert not getattr(p,'native_bundle_live',False)
    assert p._convert_messages([{'role':'user','content':'normal'}])


def test_cli_old_configured_provider_keeps_ordinary_transport(monkeypatch):
    import amplifier_loop_live_cli.bundle_astra as adapter
    monkeypatch.setattr(adapter,'NATIVE_AVAILABLE',False)
    p=adapter.BundleAstraProvider.wrap(OpenAIProvider(api_key='fixture',config={'default_model':'gpt-6-astra'}))
    assert isinstance(p,SafeOpenAIProvider)
    assert not getattr(p,'native_bundle_live',False)
