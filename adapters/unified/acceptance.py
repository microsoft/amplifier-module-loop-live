"""Opt-in, real-provider acceptance against Unified's HTTP/SSE and worker path.

Run in a dedicated environment with the reviewed Unified host installed. Only
synthetic workspace/history data is sent. Settings and transcripts stay in a new
private output directory, never in this repository. No model calls are mocked.
"""
from __future__ import annotations

import argparse
import asyncio
import copy
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import shlex
import sys
import time
from types import SimpleNamespace
import uuid

import yaml

ROOT = Path(__file__).resolve().parents[2]
PROVIDER_SOURCES = {
    "provider-openai": "git+https://github.com/microsoft/amplifier-module-provider-openai@c9b0e8e60702269c7a637875b89d011facfde020",
    "provider-anthropic": "git+https://github.com/microsoft/amplifier-module-provider-anthropic@12fffb6ad2bafb1bb7999ab02245a35e42ca8672",
}
SIMPLE_SOURCE = "git+https://github.com/microsoft/amplifier-module-context-simple@2bc8b15770f4ecb49bd6216a8b5336e9c36adfc6"


def private_json(path, value):
    path.write_text(json.dumps(value, indent=2, default=str))
    path.chmod(0o600)


def setup(args):
    """Copy only the explicitly selected provider, with no real session access."""
    from amplifier_web.host.config import _load_keys, expand_environment

    settings_path = args.settings.expanduser().resolve()
    settings = yaml.safe_load(settings_path.read_text()) or {}
    providers = settings.get("config", {}).get("providers", [])
    matches = [r for r in providers if (r.get("id") or r["module"]) == args.provider]
    if len(matches) != 1 or matches[0]["module"] not in PROVIDER_SOURCES:
        raise ValueError("Select exactly one configured OpenAI or Anthropic provider instance")
    _load_keys(settings_path.with_name("keys.env"))
    row = expand_environment(copy.deepcopy(matches[0]))
    row["source"] = PROVIDER_SOURCES[row["module"]]
    # Keep the chosen model/effort intact, but bound this acceptance workload.
    row.setdefault("config", {}).update(max_output_tokens=8192)
    row["config"].setdefault("timeout", 90)
    folder = args.output.expanduser().resolve()
    folder.mkdir(mode=0o700, parents=True, exist_ok=False)
    for name in ("shared", "app", "workspace", "ownership"):
        (folder / name).mkdir(mode=0o700)
    os.environ.update(AMPLIFIER_HOME=str(folder / "shared"),
                      AMPLIFIER_WEB_HOME=str(folder / "app"),
                      AMPLIFIER_SESSION_STATE_HOME=str(folder / "ownership"))
    # The provider configuration can contain secrets; write it privately, and
    # never put its values in the public report or a committed fixture.
    target = folder / "shared/settings.yaml"
    target.write_text(yaml.safe_dump({"config": {"providers": [row]}}))
    target.chmod(0o600)
    return folder, row


def installed_revisions():
    result = {}
    for dist in importlib.metadata.distributions():
        name = dist.metadata["Name"]
        if not name.startswith("amplifier-"):
            continue
        direct = json.loads(dist.read_text("direct_url.json") or "{}")
        # Do not serialize source URLs: Git credentials can be embedded in them.
        result[name] = {"version": dist.version,
                        "commit": direct.get("vcs_info", {}).get("commit_id"),
                        "editable": direct.get("dir_info", {}).get("editable", False)}
    return result


async def make_bundle(folder, args):
    from amplifier_foundation import load_bundle

    bundle = await load_bundle(str(ROOT / "bundle.md"), strict=True)
    plan = bundle.to_mount_plan()
    plan["bundle"] = {"name": "acceptance-" + args.profile, "version": "0.2.0"}
    # Flatten the small root so baseline and Work have identical instructions,
    # tools and model selection. This comparison isolates context policy.
    plan["session"]["orchestrator"]["config"].update(max_iterations=20)
    if args.profile == "baseline":
        plan["session"]["context"] = {"module": "context-simple", "source": SIMPLE_SOURCE,
                                      "config": {"token_meter": "actual"}}
    if args.scenario == "compaction":
        # Synthetic pressure, identical across profiles; never lower the user's
        # real session budget. Keep enough room for Unified's app tool schema.
        plan["session"]["context"]["config"].update(max_tokens=24000)
        if args.profile == "work":
            plan["session"]["context"]["config"].update(summarize_trigger=0.12)
    path = folder / "profile.md"
    path.write_text("---\n" + yaml.safe_dump(plan, sort_keys=False) + "---\n\n" + bundle.instruction)
    return path, hashlib.sha256(path.read_bytes()).hexdigest()


class Observation:
    def __init__(self):
        self.start = time.monotonic()
        self.events = []
        self.changed = asyncio.Event()

    def add(self, kind, data):
        allowed = ("sessionId", "inputId", "generation_id", "input_ids", "event",
                   "status", "phase", "detail", "id", "tool", "callId", "text", "active_job_ids")
        self.events.append({"seconds": round(time.monotonic()-self.start, 4),
                            "kind": kind, **{k: data[k] for k in allowed if k in data},
                            **({"executionKind": data.get("kind")} if kind == "execution.event" else {})})
        self.changed.set()

    async def wait(self, predicate, timeout=180):
        async with asyncio.timeout(timeout):
            while True:
                self.changed.clear()
                if predicate():
                    return
                await self.changed.wait()


async def run(args):
    from aiohttp import web
    import amplifier_web.runtime_worker as worker
    from amplifier_web.server import create_app
    from amplifier_web.session_client import SessionClient
    import amplifier_web.automatic_history as history_module

    folder, provider = setup(args)
    bundle, digest = await make_bundle(folder, args)
    report = {"schema_version": 1, "scenario": args.scenario, "profile": args.profile,
              "provider_instance": args.provider, "provider": provider["module"],
              "model": provider["config"].get("default_model"),
              "effort": provider["config"].get("reasoning_effort"),
              "bundle_sha256": digest, "installed": installed_revisions(),
              "evidence": "real provider, Unified host, isolated worker, authenticated HTTP/SSE",
              "audio_transport": "not tested", "visual_browser": "not tested",
              "input_mode": args.input_mode,
              "checks": {}, "passed": False}
    observation = Observation()
    history_errors = []
    original_read = history_module.read_transcript

    def diagnosed_read(*args, **kwargs):
        try:
            return original_read(*args, **kwargs)
        except Exception as exc:
            # Observe exceptions that the app converts to a generic read error;
            # do not change the reader, retry its effects, or fabricate a result.
            history_errors.append({"seconds": time.monotonic()-observation.start,
                                   "type": type(exc).__name__, "message": str(exc)})
            raise

    history_module.read_transcript = diagnosed_read
    app = await create_app(folder / "app", workspace=folder / "workspace",
                           voice=False, background_updates=False, preload_providers=False)
    service = app["service"]
    service.runtime.command = [sys.executable, str(Path(worker.__file__).resolve())]
    original = service.on_runtime_event

    async def observed(kind, data):
        observation.add(kind, data)
        await original(kind, data)

    service.on_runtime_event = observed
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    url = f"http://127.0.0.1:{port}"
    print(json.dumps({"phase": "host-ready", "url": url, "provider": args.provider,
                      "scenario": args.scenario, "profile": args.profile}), flush=True)
    tasks = []
    try:
        async with SessionClient(url, app["control_token"], "acceptance-a") as client:
            result = await client.create_session({"title": "Synthetic Work acceptance", "bundle": str(bundle),
                                                  "workspace": str(folder / "workspace")}, command_id="create")
            sid = result["state"]["selectedSessionId"]
            report["session_id"] = sid
            if args.scenario == "compaction":
                # Earlier turns are synthetic imported history, not fabricated
                # live model responses. The live summarizer sees real pressure.
                history = [{"role": "user", "content": "Original objective: prepare the ORCHID delivery report. Preserve reference code OLIVE-731."}]
                for n in range(8):
                    history += [{"role": "assistant", "content": f"Archived evidence {n}. " + ("The delivery has four crates and the manifest was checked. " * 150)},
                                {"role": "user", "content": f"Continue review, stage {n}."}]
                before = {row["id"] for row in service.state["sessions"]}
                await client._json("POST", "/api/actions", {"action": "history.importFile", "id": "seed",
                    "args": {"content": json.dumps(history), "format": "json", "title": "Synthetic context pressure", "bundle": str(bundle)}})
                imported = [row for row in service.state["sessions"] if row["id"] not in before]
                if len(imported) != 1:
                    raise RuntimeError("Expected exactly one synthetic imported conversation")
                sid = imported[0]["id"]
                report["session_id"] = sid

            await client._json("POST", "/api/actions", {"action": "session.select", "id": "select",
                                                       "args": {"id": sid}})

            snapshots = []

            async def watch():
                async for value in client.snapshots(sid):
                    snapshots.append({"revision": value["revision"], "seconds": time.monotonic()-observation.start,
                                      "streaming": bool(value.get("session", {}).get("streaming"))})

            tasks.append(asyncio.create_task(watch()))
            await client._json("POST", "/api/actions", {"action": "view.update", "id": "draft",
                                                       "args": {"sessionId": sid, "patch": {"draft": "UNSENT-PRIVATE-DRAFT"}}})
            # Start/prepare before timing inference or sending the workload.
            await service.runtime.start(service._session(sid), service.on_runtime_event)
            controls = await service.runtime.control(sid, "configuration.inspect")
            report["effective_context"] = controls["plan"]["session"]["context"]
            report["effective_loop"] = controls["plan"]["session"]["orchestrator"]
            print(json.dumps({"phase": "runtime-ready", "session": sid}), flush=True)

            if args.scenario in {"interaction", "cancellation"}:
                await interaction(client, service, sid, folder, observation, report, args)
            else:
                await compaction(client, service, sid, observation, report, args.profile)

            # A second client reads the actual completed conversation without
            # re-sending an input. Replaying a command keeps its original ID.
            async with SessionClient(url, app["control_token"], "acceptance-b") as second:
                try:
                    restored = await second.snapshot(sid)
                    report["checks"]["reconnect_first_attempt"] = True
                except Exception:
                    report["checks"]["reconnect_first_attempt"] = False
                    # A recovered retry is recorded, never substituted for an
                    # initial success. Preserve the failed acceptance verdict.
                    await asyncio.sleep(.25)
                    restored = await second.snapshot(sid)
                    report["reconnect_retry_recovered"] = True
                report["checks"]["reconnect_same_history"] = restored["session"]["id"] == sid and bool(restored["session"]["messages"])
                stream = second.snapshots(sid, reconnect=False)
                first = await asyncio.wait_for(anext(stream), 10)
                await stream.aclose()
                report["checks"]["reconnect_sse_snapshot"] = first["session"]["id"] == sid
                await second._json("POST", "/api/actions", {"action": "view.update", "id": "draft-b",
                                                            "args": {"sessionId": sid, "patch": {"draft": "B-DRAFT"}}})
            astate = await client._json("GET", "/api/state")
            report["checks"]["independent_draft"] = astate["view"]["draft"] == "UNSENT-PRIVATE-DRAFT"
            report["sse_snapshots"] = len(snapshots)
            report["sse_streaming_snapshots"] = sum(s["streaming"] for s in snapshots)
            report["checks"]["sse_received"] = len(snapshots) > 1
            report["text_delta_count"] = sum(e["kind"] == "assistant.delta" for e in observation.events)
            report["checks"]["public_text_stream"] = report["text_delta_count"] > 0
            report["usage"] = await service.runtime.control(sid, "usage.inspect")
            report["passed"] = all(report["checks"].values())
    except Exception as exc:
        report["error_type"] = type(exc).__name__
        # Exception details and provider diagnostics stay private. Public output
        # deliberately excludes SDK logs, endpoints and authentication fields.
        (folder / "error.txt").write_text(str(exc))
        (folder / "error.txt").chmod(0o600)
        for sid, row in service.runtime.workers.items():
            private_json(folder / (sid + "-worker-diagnostics.json"), row.get("stderr", []))
    finally:
        # A failed assertion must not leave the synthetic child gated forever.
        if (folder / "workspace/slow_job.py").exists():
            (folder / "workspace/release").touch()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        report["elapsed_seconds"] = round(time.monotonic()-observation.start, 3)
        report["history_read_errors"] = history_errors
        private_json(folder / "report.json", report)
        private_json(folder / "events.json", observation.events)
        await runner.cleanup()
        history_module.read_transcript = original_read
    print(json.dumps({"passed": report["passed"], "checks": report["checks"],
                      "error_type": report.get("error_type"), "report": str(folder / "report.json")}), flush=True)
    return report["passed"]


def assistant_text(service, sid):
    return "\n".join(m.get("text", "") for m in service._session(sid)["messages"] if m["role"] == "assistant")


async def finished(observation, service, sid, token, timeout=180):
    await observation.wait(lambda: token in assistant_text(service, sid)
                           and service._session(sid)["status"] == "idle", timeout)


async def interaction(client, service, sid, folder, observation, report, args):
    workspace = folder / "workspace"
    secret_result = "CRATE-" + uuid.uuid4().hex[:8]
    # The child must actually run the slow tool to learn this synthetic result.
    script = '''from pathlib import Path
import time
p=Path(__file__).parent
with (p/'executions.txt').open('a') as f: f.write('run\\n')
(p/'started').touch()
deadline=time.monotonic()+120
while not (p/'release').exists():
    if time.monotonic()>deadline: raise TimeoutError('acceptance gate expired')
    time.sleep(.1)
print((p/'result.txt').read_text())
'''
    (workspace / "slow_job.py").write_text(script)
    (workspace / "result.txt").write_text(secret_result)
    command = shlex.quote(sys.executable) + " slow_job.py"
    prompt = ("Bounded integration task: delegate exactly one task with agent=self and async=true. "
              f"The child must run `{command}` using bash exactly once, wait for that command, "
              "and return its stdout. No other child actions. Do not read the script or result yourself, "
              "do not edit any files, and do not launch another child. The original delivery color is BLUE. "
              "Keep responding to my messages while the child runs. Use live_job to await its real result. "
              "Once complete, report the color and the child's exact result, ending with WORK_COMPLETE.")
    start = time.monotonic()
    await client.command(sid, "conversation.send", {"text": prompt}, command_id="initial-task")
    repeated = await client.command(sid, "conversation.send", {"text": prompt}, command_id="initial-task")
    report["checks"]["input_retry_deduplicated"] = repeated.get("duplicate") is True
    async with asyncio.timeout(120):
        while not (workspace / "started").exists():
            await asyncio.sleep(.1)
    if args.scenario == "cancellation":
        await client.command(sid, "conversation.send", {"text": "Cancel the pending background job now using live_job cancel. Wait for confirmed cancellation. Do not retry or restart it. Report cancellation without claiming rollback and finish with CANCEL_COMPLETE."}, command_id="cancel")
        await finished(observation, service, sid, "CANCEL_COMPLETE")
        report["checks"]["job_cancelled"] = any(e.get("event") == "job.cancelled" for e in observation.events)
        report["checks"]["cancel_request_observed"] = any(e.get("event") == "job.cancel_requested" for e in observation.events)
        report["checks"]["cancel_did_not_reexecute"] = (workspace / "executions.txt").read_text().splitlines() == ["run"]
        # Effects before cancellation remain. We deliberately do not assert that
        # a generic tool cancellation killed every subprocess it may have made.
        report["checks"]["existing_effect_preserved"] = (workspace / "started").exists()
        (workspace / "release").touch()
        return
    correction = "Change delivery color to ORANGE. While the child is still working, what is 17 + 25? Answer that now; continue the same task."
    correction_time = time.monotonic()
    voice_task = call = None
    if args.input_mode == "voice-backend":
        from amplifier_web.voice import VoiceCall
        # Exercise the real voice adapter's input/wait/close behavior. Audio and
        # speech recognition are deliberately not simulated or claimed here.
        call = VoiceCall(SimpleNamespace(service=service), sid)
        call.id = "synthetic-voice-backend"
        voice_task = asyncio.create_task(call.execute(correction, "correction"))
    else:
        await client.command(sid, "conversation.send", {"text": correction}, command_id="correction")
    try:
        await observation.wait(lambda: "42" in assistant_text(service, sid), timeout=90)
        report["checks"]["side_answer_while_tool_pending"] = not (workspace / "release").exists()
        report["correction_to_answer_seconds"] = round(time.monotonic()-correction_time, 3)
        if call:
            closed = await call.close()
            report["checks"]["voice_end_keeps_work"] = closed["workContinues"] is True
    finally:
        (workspace / "release").touch()
    await finished(observation, service, sid, "WORK_COMPLETE")
    if voice_task:
        report["voice_backend_result"] = await asyncio.wait_for(voice_task, 30)
    text = assistant_text(service, sid)
    report["checks"].update(child_result_verified=secret_result in text,
                             correction_retained="ORANGE" in text,
                             tool_executed_once=(workspace / "executions.txt").read_text().splitlines() == ["run"])
    native = await service.runtime.control(sid, "history.snapshot")
    report["checks"]["correction_in_history_once"] = sum(isinstance(m.get("content"), str) and m["content"].endswith(correction) for m in native["messages"]) == 1
    nodes = service._session(sid).get("execution", {}).get("nodes", [])
    children = {n["sessionId"] for n in nodes if n.get("kind") == "worker" and n.get("sessionId") != sid}
    report["checks"]["one_real_child"] = len(children) == 1
    calls = [n for n in nodes if n.get("kind") == "llm" and n.get("phase") == "completed"]
    report["child_models"] = sorted({n.get("model", "") for n in calls if n.get("sessionId") in children})
    report["checks"]["child_model_inherited"] = report["child_models"] == [report["model"]]
    report["workload_seconds"] = round(time.monotonic()-start, 3)
    deltas = [e for e in observation.events if e["kind"] == "assistant.delta" and e["seconds"] >= start-observation.start]
    report["time_to_first_public_text_seconds"] = round(deltas[0]["seconds"]-(start-observation.start), 3) if deltas else None
    # Seed a separate, explicitly synthetic prior chat; reading it must neither
    # mount another worker nor retarget this client's conversation or draft.
    known = {row["id"] for row in service.state["sessions"]}
    await client._json("POST", "/api/actions", {"action": "history.importFile", "id": "prior-chat",
        "args": {"content": json.dumps([{"role": "user", "content": "Earlier decision: HISTORY-ALDER-29."}]),
                 "format": "json", "title": "Earlier synthetic decision"}})
    prior = next(row["id"] for row in service.state["sessions"] if row["id"] not in known)
    # Import is an explicit navigation action. Restore the caller's presentation
    # before measuring whether the subsequent passive reads change anything.
    await client._json("POST", "/api/actions", {"action": "session.select", "id": "restore-after-import",
                                               "args": {"id": sid}})
    await client._json("POST", "/api/actions", {"action": "view.update", "id": "restore-draft-after-import",
                                               "args": {"sessionId": sid, "patch": {"draft": "UNSENT-PRIVATE-DRAFT"}}})
    before = await client._json("GET", "/api/state")
    workers = set(service.runtime.workers)
    search = await service.app_bridge("history", {"action": "search", "query": "HISTORY-ALDER-29"}, sid)
    read = await service.app_bridge("history", {"action": "read", "session_id": prior}, sid)
    after = await client._json("GET", "/api/state")
    report["checks"]["passive_cross_chat_history"] = (
        any(r["id"] == prior for r in search["items"]) and bool(read["messages"])
        and before["selectedSessionId"] == after["selectedSessionId"] == sid
        and before["view"]["draft"] == after["view"]["draft"]
        and set(service.runtime.workers) == workers)


async def compaction(client, service, sid, observation, report, profile):
    start = time.monotonic()
    initial = "Continue the original objective. Briefly state the reference code and delivery report name."
    await client.command(sid, "conversation.send", {"text": initial}, command_id="continue")
    if profile == "work":
        await observation.wait(lambda: any(e.get("phase") == "compacting" for e in observation.events), 90)
    else:
        # Do not inject the baseline correction before its first request starts;
        # that would merge the inputs and artificially save a foreground call.
        await observation.wait(lambda: any(e.get("executionKind") == "llm" and e.get("phase") == "running"
                                          and e.get("sessionId") == sid for e in observation.events), 90)
    correction = "Correction: delivery color is ORANGE. Preserve the original report name and reference code; give all three, then say CONTEXT_COMPLETE."
    await client.command(sid, "conversation.send", {"text": correction}, command_id="during-compaction")
    await finished(observation, service, sid, "CONTEXT_COMPLETE")
    report["workload_seconds"] = round(time.monotonic()-start, 3)
    text = assistant_text(service, sid)
    transcript = await service.runtime.control(sid, "history.snapshot")
    messages = transcript["messages"]
    report["checks"].update(objective_retained="ORCHID" in text,
                             reference_retained="OLIVE-731" in text,
                             correction_retained="ORANGE" in text,
                             original_history_retrievable=any("Archived evidence 0" in str(m.get("content")) for m in messages),
                             correction_in_history_once=sum(m.get("content") == correction for m in messages) == 1)
    report["compaction_events"] = [e for e in observation.events if e.get("phase") == "compacting"]
    if profile == "work":
        report["checks"]["visible_compaction"] = bool(report["compaction_events"])
        completed = [e for e in observation.events if e.get("detail") == "Conversation context prepared; continuing work."]
        report["checks"]["semantic_compaction_completed"] = bool(completed)
        if completed:
            start, end = report["compaction_events"][0]["seconds"], completed[0]["seconds"]
            report["compaction_seconds"] = round(end-start, 3)
            report["checks"]["summary_text_not_public"] = not any(e["kind"] == "assistant.delta" and start < e["seconds"] < end for e in observation.events)


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--allow-live", action="store_true", help="Explicitly enable paid provider calls")
    parser.add_argument("--settings", type=Path, default=Path.home()/".amplifier/settings.yaml")
    parser.add_argument("--provider", required=True, help="Existing provider instance ID; model and effort are preserved")
    parser.add_argument("--output", type=Path, required=True, help="New private directory, outside the repository")
    parser.add_argument("--profile", choices=("work", "baseline"), default="work")
    parser.add_argument("--scenario", choices=("interaction", "compaction", "cancellation"), default="interaction")
    parser.add_argument("--input-mode", choices=("text", "voice-backend"), default="text")
    args = parser.parse_args()
    if not args.allow_live:
        parser.error("Live acceptance requires --allow-live; it can incur provider charges")
    if args.output.expanduser().resolve().is_relative_to(ROOT):
        parser.error("Store private acceptance output outside this repository")
    if args.input_mode != "text" and args.scenario != "interaction":
        parser.error("Voice backend mode applies to the interaction scenario")
    raise SystemExit(0 if asyncio.run(run(args)) else 1)


if __name__ == "__main__":
    main()
