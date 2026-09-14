"""CLI-equivalent bundle preparation for a live Amplifier host process."""

import copy
import hashlib
import json
import os
import re
import contextvars
import subprocess
import logging
from pathlib import Path

from amplifier_core import ApprovalResponse
from amplifier_core.approval import ApprovalTimeoutError
from amplifier_foundation.bundle._prepared import BundleModuleSource
from amplifier_module_loop_live.runtime import Runtime

import amplifier_module_loop_live
LOCAL_MODULE = Path(amplifier_module_loop_live.__file__).resolve().parent.parent
LOOP_SOURCE = "git+https://github.com/bkrabach/amplifier-module-loop-live@main"
if not (LOCAL_MODULE / "pyproject.toml").is_file():
    # A wheel's site-packages directory is not an installable module source.
    # Preserve the exact Git revision when the package was installed from Git.
    from importlib.metadata import distribution
    direct = json.loads(distribution("amplifier-module-loop-live").read_text("direct_url.json") or "{}")
    revision = direct.get("vcs_info", {}).get("commit_id")
    if revision and direct.get("url", "").startswith("https://"):
        LOOP_SOURCE = "git+" + direct["url"] + "@" + revision
APPROVAL_CHANNEL = contextvars.ContextVar("converge_live_approval_channel", default=None)
from amplifier_module_loop_live.instructions import MANAGER_INSTRUCTIONS

SECRET_KEY = re.compile(r"(?:secret|password|credential|authorization|api[_-]?key|access[_-]?token|refresh[_-]?token|^token$|^key$|^auth$)", re.I)


def redacted(value):
    """Redact credential fields AND their copies elsewhere in a resolved plan."""
    secrets = set()
    def collect(item):
        if isinstance(item, dict):
            for key, val in item.items():
                if SECRET_KEY.search(key) and isinstance(val, str) and len(val) >= 4:
                    secrets.add(val)
                collect(val)
        elif isinstance(item, list):
            for val in item:
                collect(val)
    collect(value)
    # Credential-related feature flags are not credential values. Redacting a
    # literal "true" also corrupts JSON app-action booleans in voice replies.
    flag_values={"true", "false", "none", "null", "yes", "no"}
    secrets.update(val for key, val in os.environ.items() if SECRET_KEY.search(key) and len(val) >= 4 and val.lower() not in flag_values)
    def clean(item):
        if isinstance(item, dict):
            return {key: "[REDACTED]" if SECRET_KEY.search(key) else clean(val) for key, val in item.items()}
        if isinstance(item, (list, tuple)):
            return [clean(val) for val in item]
        if isinstance(item, str):
            if item.lstrip().startswith(("{", "[")):
                try:
                    parsed=json.loads(item)
                    if isinstance(parsed,(dict,list)): return json.dumps(clean(parsed),ensure_ascii=False)
                except (ValueError,RecursionError): pass
            for secret in sorted(secrets, key=len, reverse=True):
                item = item.replace(secret, "[REDACTED]")
            return re.sub(r"(?i)([?&](?:key|token|api_key)=)[^&#\s]+", r"\1[REDACTED]", item)
        return item
    return clean(value)


def write_report(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_suffix(path.suffix + ".tmp")
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as file:
        json.dump(redacted(value), file, indent=2, default=str)
        file.write("\n")
    os.replace(temporary, path)


def source_revision(path):
    if not path:
        return None
    result = subprocess.run(["git", "-C", str(path), "rev-parse", "HEAD"],
                            capture_output=True, text=True, timeout=5)
    return result.stdout.strip() if result.returncode == 0 else None


class CompositionFailures(logging.Handler):
    def __init__(self):
        super().__init__(logging.WARNING)
        self.behaviors = []

    def emit(self, record):
        match = re.match(r"Failed to compose behavior '([^']+)'", record.getMessage())
        if match:
            self.behaviors.append(match.group(1))


class LiveResolver:
    """Keep the prepared bundle's resolver, adding this local module only."""
    def __init__(self, original):
        self.original = original
        self.overlays = {}

    def _overlay(self,module_id,source):
        if module_id!="tool-computer-use":return source
        from amplifier_loop_live_cli.module_overlays import computer_overlay
        path,manifest=computer_overlay(source.resolve())
        if manifest:self.overlays[module_id]={"path":str(path),**manifest}
        return BundleModuleSource(path)

    def resolve(self, module_id, source_hint=None, **kwargs):
        if module_id == "loop-live" and (LOCAL_MODULE / "pyproject.toml").is_file():
            return BundleModuleSource(LOCAL_MODULE)
        if module_id == "loop-live":
            return self.original.resolve(module_id, source_hint=LOOP_SOURCE, **kwargs)
        return self._overlay(module_id,self.original.resolve(module_id, source_hint=source_hint, **kwargs))

    async def async_resolve(self, module_id, source_hint=None, **kwargs):
        if module_id == "loop-live" and (LOCAL_MODULE / "pyproject.toml").is_file():
            return BundleModuleSource(LOCAL_MODULE)
        if module_id == "loop-live":
            return await self.original.async_resolve(module_id, source_hint=LOOP_SOURCE, **kwargs)
        return self._overlay(module_id,await self.original.async_resolve(module_id, source_hint=source_hint, **kwargs))

    def get_module_source(self, module_id):
        if module_id == "loop-live" and (LOCAL_MODULE / "pyproject.toml").is_file():
            return str(LOCAL_MODULE)
        if module_id in self.overlays:return self.overlays[module_id]["path"]
        return self.original.get_module_source(module_id)

    def __getattr__(self, name):
        return getattr(self.original, name)


def replace_streaming(plan, *, background_delegate=True):
    result = copy.deepcopy(plan)
    replacements = []
    def replace(node, path):
        session = node.get("session", {})
        loop = session.get("orchestrator", {})
        if loop.get("module") in {"loop-streaming", "loop-live"}:
            loop["module"] = "loop-live"
            loop["source"] = str(LOCAL_MODULE) if (LOCAL_MODULE / "pyproject.toml").is_file() else LOOP_SOURCE
            loop.setdefault("config", {}).update(configured_bundle=True,
                                                background_delegate=background_delegate)
            replacements.append(path + "session.orchestrator")
        for name, agent in node.get("agents", {}).items():
            replace(agent, path + "agents." + name + ".")
    replace(result, "")
    if result["session"]["orchestrator"]["module"] != "loop-live":
        raise ValueError("Configured manager must use loop-streaming or loop-live")
    return result, replacements


class ManagerApprovals:
    """Both Amplifier approval contracts, sharing the terminal's async prompt."""
    def __init__(self, runtime, ask=None):
        self.runtime, self.ask = runtime, ask

    async def request_approval(self, prompt, options=None, timeout=None, default="deny"):
        import asyncio
        request = prompt if not isinstance(prompt, str) else None
        if request is not None:
            text = f"{request.tool_name}: {request.action}\n" + json.dumps(redacted(request.details), default=str)
            options, timeout = ["allow", "deny"], request.timeout
        else:
            text = prompt
        await self.runtime.emit("approval.requested", prompt=text, options=options)
        if self.ask is None:
            answer = "deny"  # Inspection/headless mode never grants approvals.
        else:
            try:
                answer = await asyncio.wait_for(self.ask(text, options), timeout)
            except TimeoutError:
                await self.runtime.emit("approval.expired")
                if request is not None:
                    raise
                raise ApprovalTimeoutError("Manager approval timed out")
        if answer not in options:
            raise ValueError("Unknown approval option")
        await self.runtime.emit("approval.resolved", decision=answer)
        if request is not None:
            return ApprovalResponse(approved=answer == "allow", reason="Local user decision" if self.ask else "No interactive approval channel")
        return answer


def mark_interrupted_receipts(transcript):
    """A resumed process has no running jobs from its predecessor.

Keep call identity and make uncertainty explicit; never replay a delegation just
because its last checkpoint contained an early queued receipt.
"""
    for message in transcript:
        if message.get("role") != "tool":
            continue
        try:
            receipt = json.loads(message.get("content", ""))
        except (TypeError, ValueError):
            continue
        if not isinstance(receipt, dict) or receipt.get("status") != "queued" or not receipt.get("job_id"):
            continue
        if receipt.get("call_id") != message.get("tool_call_id"):
            continue
        if not str(receipt.get("instruction", "")).startswith("Delegation is pending, including any approval checks."):
            continue
        message["content"] = json.dumps({"status": "interrupted", "job_id": receipt["job_id"],
            "call_id": receipt["call_id"], "outcome": "unconfirmed", "effects": "not_rolled_back",
            "instruction": "The prior process ended without a saved result. Inspect actual state before deciding whether to repeat work."})
    return transcript


async def prepare_manager(workspace, *, runtime=None, bundle=None, background_delegate=True,
                          ask=None, report_dir=None, pin_provider=None, resume=False, selection=None, worker_settings=None,
                          application_host="Amplifier Live CLI"):
    # Like the upstream CLI, this host owns a process and CWD. A future web host
    # must isolate these sessions in worker processes instead of os.chdir races.
    from amplifier_app_cli.lib.settings import AppSettings
    from amplifier_app_cli.runtime.config import resolve_bundle_config, inject_user_providers
    from amplifier_app_cli.lib.bundle_loader import AppModuleResolver
    from amplifier_app_cli.paths import create_foundation_resolver
    from amplifier_app_cli.session_runner import (
        register_mention_handling, register_session_spawning,
        _inject_observability_events, _inject_invocation_metadata,
    )
    from amplifier_app_cli.ui.display import CLIDisplaySystem
    from amplifier_foundation.configurator import SessionConfigurator
    from amplifier_workspace.config import load_config

    workspace = Path(workspace).expanduser().resolve(strict=True)
    os.chdir(workspace)
    runtime = runtime or Runtime()
    settings = AppSettings()
    bundle = bundle or settings.get_active_bundle() or load_config().bundle
    # The pinned CLI warns and continues when an app behavior cannot compose.
    # A configured manager must surface that missing environment explicitly.
    logger = logging.getLogger("amplifier_app_cli.lib.bundle_loader.prepare")
    failures = CompositionFailures()
    previous_level = logger.level
    logger.addHandler(failures)
    logger.setLevel(logging.WARNING)
    try:
        config, prepared = await resolve_bundle_config(bundle, settings)
    finally:
        logger.removeHandler(failures)
        logger.setLevel(previous_level)
    if failures.behaviors:
        directory = Path(report_dir or Path.home() / ".amplifier" / "converge-live" / runtime.session_id)
        write_report(directory / "preparation-failures.json", {"missing_behaviors": failures.behaviors})
        raise RuntimeError("Configured behaviors did not compose; see preparation-failures.json")
    inject_user_providers(config, prepared)
    _inject_observability_events(prepared)
    _inject_invocation_metadata(prepared, mode="interactive")
    baseline = copy.deepcopy(prepared.mount_plan)
    plan, replacements = replace_streaming(baseline, background_delegate=background_delegate)
    prepared.mount_plan = plan
    prepared.bundle.session = copy.deepcopy(plan["session"])
    prepared.bundle.agents = copy.deepcopy(plan.get("agents", {}))
    prepared.resolver = LiveResolver(AppModuleResolver(
        bundle_resolver=prepared.resolver, settings_resolver=create_foundation_resolver()))
    report_dir = Path(report_dir or Path.home() / ".amplifier" / "converge-live" / runtime.session_id)
    write_report(report_dir / "baseline-mount-plan.json", baseline)
    write_report(report_dir / "live-mount-plan.json", plan)
    plan.update(application_host=application_host, root_session_id=runtime.session_id,
                bundle_name=bundle, project_dir=str(workspace), project_name=workspace.name)
    approvals = ManagerApprovals(runtime, ask)
    APPROVAL_CHANNEL.set(approvals)
    session = None
    jobs = None
    try:
        from amplifier_app_cli.session_store import SessionStore
        from amplifier_module_loop_live.job_store import JobStore
        store = SessionStore()
        jobs = JobStore(store.base_dir / runtime.session_id / "live-jobs")
        transcript = None
        if resume and (store.base_dir / runtime.session_id / "metadata.json").exists():
            transcript, _ = store.load(runtime.session_id)
        if jobs.rows:
            if not resume:
                raise RuntimeError("Saved jobs exist; resume this session explicitly")
            transcript, recovered = jobs.recover(transcript or [])
        else:
            recovered = []
        session = await prepared.create_session(session_id=runtime.session_id, session_cwd=workspace,
            approval_system=approvals, display_system=CLIDisplaySystem(), is_resumed=transcript is not None)
        if transcript is not None:
            await session.coordinator.get("context").set_messages(mark_interrupted_receipts(transcript))
        from .host_adapter import CLIHostAdapter
        session.coordinator.register_capability("live.host", CLIHostAdapter())
        session.coordinator.register_capability("live.runtime", runtime)
        session.coordinator.register_capability("live.jobs", jobs)
        session.coordinator.register_capability("live.recovered_jobs", [row["job_id"] for row in recovered])
        async def close_jobs():
            jobs.close()
        session.coordinator.register_cleanup(close_jobs)
        register_mention_handling(session)
        register_session_spawning(session, prepared_bundle=prepared)
        register = session.coordinator.get_capability("approval.register_provider")
        if register:
            register(approvals)
        configurator = SessionConfigurator(session, prepared)
        await configurator.apply_saved_settings(settings.get_merged_settings().get("configurator") or {})
        configurator.take_snapshot()
        from amplifier_loop_live_cli.bundle_astra import install
        native_providers = await install(session.coordinator)
        if pin_provider:
            session.coordinator.get_capability("conversation.provider_pin").pin(pin_provider)
        from amplifier_loop_live_cli.selection import select_root
        provider_choices, root_selection = await select_root(session, plan, selection)
        if (selection or {}).get("allowNestedDelegation"):
            delegate=(session.coordinator.get("tools") or {}).get("delegate")
            if delegate is None or not hasattr(delegate,"exclude_tools"):
                raise ValueError("Configured delegate cannot grant bounded nested delegation")
            # Only the root tool grants inheritance. Children retain the bundle's
            # original exclusion policy, so this opt-in adds one level only.
            delegate.exclude_tools=[name for name in delegate.exclude_tools if name!="tool-delegate"]
            root_selection["allowNestedDelegation"]=True
        loop = session.coordinator.get("orchestrator")
        from amplifier_loop_live_cli.children import install as install_children
        from amplifier_loop_live_cli.worker_settings import WorkerSettings
        await install_children(session.coordinator, runtime,
            settings=WorkerSettings(worker_settings,{row["id"]:row["provider"] for row in provider_choices}))
        selected_provider = loop._select_provider(session.coordinator.get("providers"))
        effective_selection = root_selection or next(({k:row.get(k) for k in ("provider","model","effort")} | {"instance":row["id"]}
            for row in provider_choices if session.coordinator.get("providers").get(row["id"]) is selected_provider), None)
        # Fingerprint the bundle's base prompt without replacing the installed
        # factory (other modules may wrap it). loop-live injects its operational
        # instructions through the normal ephemeral provider:request hook.
        prompt = await prepared.create_system_prompt_factory(session, session_cwd=workspace)()
        module_ids = {entry["module"] for key in ("providers", "tools", "hooks") for entry in plan.get(key, [])}
        module_ids.update(plan["session"][key]["module"] for key in ("orchestrator", "context"))
        sources = {}
        for module_id in sorted(module_ids):
            path = prepared.resolver.get_module_source(module_id)
            sources[module_id] = {"path": path, "revision": source_revision(path)}
            if module_id in prepared.resolver.overlays:
                sources[module_id].update(extension=prepared.resolver.overlays[module_id],upstream_revision=source_revision(prepared.resolver.overlays[module_id]["upstreamPath"]))
        mounted = {
            "bundle": bundle, "workspace": str(workspace), "session_id": runtime.session_id,
            "resumed": transcript is not None,
            "replaced": replacements, "native_providers": native_providers,
            "provider_choices": provider_choices, "selection": root_selection,"effective_selection":effective_selection,
            "native": bool(getattr(loop._select_provider(session.coordinator.get("providers")), "native_bundle_live", False)),
            "steering": "native_if_selected_else_request_boundary",
            "providers": list((session.coordinator.get("providers") or {}).keys()),
            "tools": list((session.coordinator.get("tools") or {}).keys()),
            "agents": list(plan.get("agents", {})),
            "agent_definitions": [{"name": name, "roles": agent.get("model_role",[])} for name,agent in plan.get("agents",{}).items()],
            "hooks": session.coordinator.hooks.list_handlers(),
            "module_load_failures": loop.load_failures,
            "bundle_namespaces": sorted(prepared.bundle.source_base_paths),
            "module_sources": sources,
            "app_bundles": settings.get_app_bundles(),
            "prompt": {"characters": len(prompt), "sha256": hashlib.sha256(prompt.encode()).hexdigest(),
                       "manager_request_instructions": MANAGER_INSTRUCTIONS},
            "capabilities": {key: session.coordinator.get_capability(key) is not None for key in
                ("session.spawn", "session.resume", "session.partial", "model_role_resolver", "mention_resolver")},
            "report_directory": str(report_dir),
            "recovered_jobs": [{"job_id": row["job_id"], "call_id": row["call_id"], "status": row["status"]} for row in recovered],
        }
        write_report(report_dir / "mounted.json", mounted)
        if not mounted["providers"]:
            raise RuntimeError("No configured providers mounted; see mounted.json")
        if any(f["type"] in {"tool", "hook", "tools", "hooks"} for f in loop.load_failures):
            raise RuntimeError("Configured tool/hook failed to mount; see mounted.json")
        from datetime import UTC, datetime
        from amplifier_app_cli.session_spawner import _install_transcript_checkpoint
        metadata = {"session_id": runtime.session_id, "parent_id": None, "bundle_name": bundle,
                    "created": datetime.now(UTC).isoformat(), "working_dir": str(workspace),
                    "application_host": application_host, "config": redacted(session.config)}
        await _install_transcript_checkpoint(session, store, runtime.session_id, metadata)
        async def checkpoint(status="in_progress"):
            transcript = await session.coordinator.get("context").get_messages()
            snapshot = {**metadata, "status": status, "last_updated": datetime.now(UTC).isoformat(),
                        "turn_count": sum(m.get("role") == "user" for m in transcript)}
            store.save(runtime.session_id, transcript, snapshot)
        session.coordinator.register_capability("live.checkpoint", checkpoint)
        return session, runtime, mounted
    except BaseException:
        try:
            if session:
                await session.cleanup()
        finally:
            if jobs:
                jobs.close()
        raise
