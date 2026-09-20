"""Real Foundation composition; no provider calls in ordinary tests."""
import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest

foundation = pytest.importorskip("amplifier_foundation")
ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.asyncio
async def test_root_loads_from_an_unrelated_workspace(tmp_path, monkeypatch):
    monkeypatch.setenv("AMPLIFIER_HOME", str(tmp_path / "shared"))
    monkeypatch.chdir(tmp_path)
    bundle = await foundation.load_bundle(str(ROOT / "bundle.md"), strict=True)
    plan = bundle.to_mount_plan()
    assert plan["session"]["context"]["config"]["engine"] == "boundary"
    assert {tool["module"] for tool in bundle.tools} >= {"tool-delegate", "tool-transcript", "tool-bash"}
    assert not bundle.providers
    assert "agent: self" in bundle.instruction


@pytest.mark.asyncio
async def test_overlay_preserves_provider_and_unrelated_tools(tmp_path, monkeypatch):
    monkeypatch.setenv("AMPLIFIER_HOME", str(tmp_path / "shared"))
    base = foundation.Bundle(name="configured", providers=[{"module": "provider-test", "config": {"default_model": "chosen"}}],
                             tools=[{"module": "tool-extra"}], session={"context": {"module": "context-simple"}})
    overlay = await foundation.load_bundle(str(ROOT / "behaviors/work-local.yaml"), strict=True)
    result = base.compose(overlay)
    assert result.providers == base.providers
    assert result.session["context"]["module"] == "context-managed"
    assert {tool["module"] for tool in result.tools} == {"tool-extra", "tool-transcript"}
    assert result.session["orchestrator"]["config"]["background_delegate"] is False


@pytest.mark.asyncio
async def test_comparison_changes_context_only(tmp_path, monkeypatch):
    monkeypatch.setenv("AMPLIFIER_HOME", str(tmp_path / "shared"))
    pytest.importorskip("amplifier_web")
    spec = importlib.util.spec_from_file_location("work_acceptance", ROOT / "adapters/unified/acceptance.py")
    acceptance = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(acceptance)
    import yaml
    plans = []
    for profile in ("work", "baseline"):
        directory = tmp_path / profile
        directory.mkdir()
        path, _ = await acceptance.make_bundle(directory, SimpleNamespace(profile=profile, scenario="compaction"))
        loaded = await foundation.load_bundle(str(path), strict=True)
        assert loaded.instruction
        plan = yaml.safe_load(path.read_text().split("---", 2)[1])
        assert plan["session"]["context"]["config"]["max_tokens"] == 24000
        plan.pop("bundle")
        plan["session"].pop("context")
        plans.append(plan)
    assert plans[0] == plans[1]
