"""Opt-in browser acceptance with real providers and isolated synthetic data.

Uses the shipped UI, two Chromium clients and a real Unified worker. Optional
audio uses macOS synthetic speech as Chromium's microphone; it never records a
physical microphone. Authentication is scoped to this temporary local origin.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import time
import uuid
import wave

from acceptance import (ROOT, Observation, assistant_text, finished,
                        installed_revisions, make_bundle, private_json, setup)


async def wait_js(page, expression, timeout=30000):
    # Poll through the automation protocol without adding unsafe-eval to the CSP.
    async with asyncio.timeout(timeout / 1000):
        while not await page.evaluate("() => (" + expression + ")"):
            await asyncio.sleep(.1)


async def until(predicate, timeout=120):
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(.1)


def speech(folder, phrasing):
    """Create a labeled, finite synthetic microphone input on macOS."""
    words = ("Please tell Amplifier to change the delivery color to orange. "
             "Ask Amplifier what seventeen plus twenty five is while the child is working. "
             "Please delegate this update to Amplifier now.")
    if phrasing == "direct":
        words = ("Change delivery color to orange. While the child is still working, "
                 "what is seventeen plus twenty-five? Answer that now; continue the same task.")
    (folder / "synthetic-utterance.txt").write_text(words)
    source, pcm, target = (folder / name for name in ("speech.aiff", "speech.wav", "microphone.wav"))
    subprocess.run(["/usr/bin/say", "-v", "Samantha", "-o", str(source), words], check=True)
    subprocess.run(["/usr/bin/afconvert", "-f", "WAVE", "-d", "LEI16@48000", str(source), str(pcm)], check=True)
    with wave.open(str(pcm), "rb") as src, wave.open(str(target), "wb") as dst:
        dst.setparams(src.getparams())
        silence = b"\0" * src.getframerate() * src.getnchannels() * src.getsampwidth()
        # Give the real handshake time to finish before the spoken instruction.
        dst.writeframes(silence * 30)
        dst.writeframes(src.readframes(src.getnframes()))
        dst.writeframes(silence * 60)
    return target


BROWSER_OBSERVER = """(() => {
  window.__acceptance = {streamingFrames: 0, peers: [], audioTracks: 0};
  setInterval(() => {
    const state = window.amplifier?.getState();
    const session = state?.sessions?.find(s => s.id === state.selectedSessionId);
    if (session?.streaming && [...document.querySelectorAll('.a-assistant')]
        .some(el => el.textContent.includes('Working…'))) window.__acceptance.streamingFrames++;
  }, 50);
  const Native = window.RTCPeerConnection;
  window.RTCPeerConnection = class extends Native {
    constructor(...args) { super(...args); window.__acceptance.peers.push(this);
      this.addEventListener('track', e => { if(e.track.kind === 'audio') window.__acceptance.audioTracks++; });
    }
  };
})();"""


async def run(args):
    from aiohttp import web
    from playwright.async_api import async_playwright
    import amplifier_web.runtime_worker as worker
    from amplifier_web.server import create_app
    from amplifier_web.session_client import SessionClient

    folder, provider = setup(args)
    bundle, digest = await make_bundle(folder, args)
    report = {"schema_version": 1, "scenario": args.scenario,
              "provider_instance": args.provider, "model": provider["config"].get("default_model"),
              "installed": installed_revisions(), "bundle_sha256": digest,
              "evidence": "real Chromium UI, provider and isolated Unified worker",
              "authentication": "origin-scoped local control token; PAM login not tested",
              "audio": "synthetic microphone, real transport" if args.voice_audio else "not tested",
              "voice_phrasing": args.voice_phrasing if args.voice_audio else None,
              "physical_microphone_and_speakers": "not tested", "checks": {}, "passed": False}
    observation, errors, http_errors = Observation(), [], []
    app = await create_app(folder / "app", workspace=folder / "workspace",
                           voice=args.voice_audio, background_updates=False, preload_providers=False)
    service = app["service"]
    service.runtime.command = [sys.executable, str(Path(worker.__file__).resolve())]
    original = service.on_runtime_event

    async def observed(kind, data):
        observation.add(kind, data)
        if kind == "runtime.error":
            private_json(folder / "runtime-error.json", data)
            private_json(folder / "worker-stderr.json", {sid: row.get("stderr", []) for sid, row in service.runtime.workers.items()})
        await original(kind, data)

    service.on_runtime_event = observed
    runner = web.AppRunner(app)
    browser = None
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    url = f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}"
    app["allowed_origins"] = app["allowed_origins"] | {url}
    try:
        async with SessionClient(url, app["control_token"], "browser-setup") as client:
            result = await client.create_session({"title": "Synthetic browser acceptance", "bundle": str(bundle),
                                                  "workspace": str(folder / "workspace")}, command_id="create")
            sid = result["state"]["selectedSessionId"]
            if args.scenario == "compaction":
                history = [{"role": "user", "content": "Original objective: prepare the ORCHID delivery report. Preserve reference code OLIVE-731."}]
                for n in range(8):
                    history += [{"role": "assistant", "content": f"Archived evidence {n}. " + "The delivery has four crates and the manifest was checked. " * 150},
                                {"role": "user", "content": f"Continue review, stage {n}."}]
                before = {r["id"] for r in service.state["sessions"]}
                await client._json("POST", "/api/actions", {"action": "history.importFile", "id": "seed",
                    "args": {"content": json.dumps(history), "format": "json", "title": "Synthetic browser context", "bundle": str(bundle)}})
                sid = next(r["id"] for r in service.state["sessions"] if r["id"] not in before)
            report["session_id"] = sid
            await service.runtime.start(service._session(sid), service.on_runtime_event)
            print(json.dumps({"phase": "browser-host-ready", "scenario": args.scenario, "audio": args.voice_audio}), flush=True)
            async with async_playwright() as playwright:
                flags = []
                if args.voice_audio:
                    microphone = speech(folder, args.voice_phrasing)
                    flags = ["--use-fake-ui-for-media-stream", "--use-fake-device-for-media-stream",
                             f"--use-file-for-fake-audio-capture={microphone}%noloop",
                             "--autoplay-policy=no-user-gesture-required"]
                browser = await playwright.chromium.launch(headless=True, args=flags)
                report["browser"] = browser.version
                contexts = [await browser.new_context(viewport={"width": 1440, "height": 1000}) for _ in range(2)]

                async def authorize(route):
                    # This route matches only the temporary host, never providers.
                    await route.continue_(headers={**route.request.headers, "Authorization": "Bearer " + app["control_token"]})

                pages = []
                for context in contexts:
                    await context.route(url + "/**", authorize)
                    await context.add_init_script(BROWSER_OBSERVER)
                    page = await context.new_page()
                    page.on("pageerror", lambda error: errors.append(str(error)))
                    page.on("response", lambda response: http_errors.append({"path": response.url.split("?")[0].removeprefix(url), "status": response.status}) if response.status >= 400 and response.url.startswith(url + "/api/") else None)
                    await page.goto(url)
                    await page.screenshot(path=str(folder / f"startup-{len(pages)+1}.png"))
                    await wait_js(page, "window.amplifier?.getState()?.client?.id")
                    await asyncio.wait_for(page.evaluate("id => window.amplifier.dispatch('session.select', {id})", sid), 30)
                    pages.append(page)
                a, b = pages
                print(json.dumps({"phase": "browser-clients-ready"}), flush=True)
                composer = lambda page: page.get_by_role("textbox", name="Message Amplifier")

                async def send(text):
                    await composer(a).fill(text)
                    await a.locator("button.a-send").click()
                    await a.screenshot(path=str(folder / "after-send.png"))

                await composer(a).fill("Draft belonging to A")
                await composer(b).fill("Draft belonging to B")
                await wait_js(b, "window.amplifier.getState().view.draft === 'Draft belonging to B'")
                report["checks"]["independent_drafts"] = await composer(a).input_value() == "Draft belonging to A"
                if args.scenario == "compaction":
                    await send("Continue the original objective. Briefly state the reference code and delivery report name.")
                    await a.get_by_text("Making room in the conversation. You can keep sending updates.", exact=True).first.wait_for(timeout=90000)
                    await a.screenshot(path=str(folder / "compacting.png"))
                    report["checks"]["compaction_visible_in_browser"] = True
                    correction = "Correction: delivery color is ORANGE. Preserve the original report name and reference code; give all three, then say CONTEXT_COMPLETE."
                    await send(correction)
                    report["checks"]["typed_during_compaction"] = not any(e.get("detail") == "Conversation context prepared; continuing work." for e in observation.events)
                    await finished(observation, service, sid, "CONTEXT_COMPLETE")
                    answer = assistant_text(service, sid)
                    report["checks"]["facts_survive_compaction"] = all(t in answer for t in ("ORCHID", "OLIVE-731", "ORANGE"))
                    marker = "CONTEXT_COMPLETE"
                else:
                    workspace = folder / "workspace"
                    result = "CRATE-" + uuid.uuid4().hex[:8]
                    (workspace / "result.txt").write_text(result)
                    (workspace / "slow_job.py").write_text("from pathlib import Path\nimport time\np=Path(__file__).parent\nwith (p/'executions.txt').open('a') as f: f.write('run\\n')\n(p/'started').touch()\ndeadline=time.monotonic()+240\nwhile not (p/'release').exists():\n    if time.monotonic()>deadline: raise TimeoutError('acceptance gate expired')\n    time.sleep(.1)\nprint((p/'result.txt').read_text())\n")
                    prompt = ("Bounded integration task: delegate exactly one task with agent=self and async=true. "
                              f"The child must run `{shlex.quote(sys.executable)} slow_job.py` using bash exactly once, wait for that command, and return its stdout. "
                              "No other child actions. Do not read the script or result yourself, do not edit files, and do not launch another child. "
                              "The original delivery color is BLUE. Keep responding to my messages while the child runs. "
                              "Use live_job to await its real result. Once complete, report the color and the child's exact result, ending with WORK_COMPLETE.")
                    await send(prompt)
                    print(json.dumps({"phase": "browser-task-submitted"}), flush=True)
                    await until(lambda: (workspace / "started").exists() or (folder / "runtime-error.json").exists())
                    if (folder / "runtime-error.json").exists():
                        raise RuntimeError("Worker reported an error before the child started; see private diagnostics")
                    await a.screenshot(path=str(folder / "child-working.png"))
                    correction = "Change delivery color to ORANGE. While the child is still working, what is 17 + 25? Answer that now; continue the same task."
                    start = time.monotonic()
                    if args.voice_audio:
                        await a.get_by_role("button", name="Start voice call", exact=True).click()
                        # Wait for either a successful real handshake or a visible failure.
                        await wait_js(a, "document.querySelector('.a-call-strip')?.textContent.includes('connected') || document.querySelector('[role=alert]')", timeout=45000)
                        connected = await a.locator(".a-call-strip").count() > 0 and "connected" in await a.locator(".a-call-strip").inner_text()
                        report["checks"]["audio_connection"] = connected
                        await a.screenshot(path=str(folder / "voice-connection.png"))
                        report["voice_model_and_state"] = await a.locator(".a-call-strip").inner_text() if connected else None
                        if not connected:
                            raise RuntimeError("Real voice connection did not reach connected state; see private diagnostics")
                    else:
                        await send(correction)
                    await observation.wait(lambda: "42" in assistant_text(service, sid), 110)
                    report["checks"]["side_answer_while_child_pending"] = not (workspace / "release").exists()
                    report["correction_to_answer_seconds"] = round(time.monotonic() - start, 3)
                    await a.locator(".a-assistant").filter(has_text="42").or_(a.locator('.a-assistant ol[start="42"]')).first.wait_for()
                    await a.screenshot(path=str(folder / "side-answer.png"))
                    if args.voice_audio:
                        # Observe actual RTP audio; this does not prove physical speaker output.
                        await wait_js(a, "window.__acceptance.audioTracks > 0")
                        await asyncio.sleep(5)
                        stats = await a.evaluate("""async () => { const result=[]; for(const p of window.__acceptance.peers) for(const s of (await p.getStats()).values()) if(['inbound-rtp','outbound-rtp'].includes(s.type) && s.kind==='audio') result.push({type:s.type,packetsReceived:s.packetsReceived,packetsSent:s.packetsSent,totalAudioEnergy:s.totalAudioEnergy}); return result; }""")
                        report["audio_rtp"] = stats
                        report["checks"]["audio_received"] = any((s.get("packetsReceived") or 0) > 0 and (s.get("totalAudioEnergy") or 0) > 0 for s in stats)
                        report["checks"]["audio_sent"] = any((s.get("packetsSent") or 0) > 0 for s in stats)
                        await a.get_by_role("button", name="End call", exact=True).click()
                        report["checks"]["ending_call_keeps_child"] = not (workspace / "release").exists() and service._session(sid)["status"] != "idle"
                    await contexts[0].set_offline(True)
                    (workspace / "release").touch()
                    await finished(observation, service, sid, "WORK_COMPLETE")
                    await b.locator(".a-assistant").filter(has_text="WORK_COMPLETE").first.wait_for()
                    report["checks"]["work_finishes_while_browser_offline"] = True
                    await contexts[0].set_offline(False)
                    marker = "WORK_COMPLETE"
                    answer = assistant_text(service, sid)
                    report["checks"].update(child_result_verified=result in answer,
                                             correction_retained="ORANGE" in answer,
                                             tool_executed_once=(workspace / "executions.txt").read_text().splitlines() == ["run"])
                for index, page in enumerate(pages):
                    await page.locator(".a-assistant").filter(has_text=marker).first.wait_for(timeout=20000)
                    await page.screenshot(path=str(folder / f"completed-{index+1}.png"))
                report["checks"]["second_client_draft_preserved"] = await composer(b).input_value() == "Draft belonging to B"
                report["streaming_frames"] = [await page.evaluate("window.__acceptance.streamingFrames") for page in pages]
                report["checks"]["visible_public_streaming_both_clients"] = all(report["streaming_frames"])
                await composer(a).fill("Survives reload")
                await wait_js(a, "window.amplifier.getState().view.draft === 'Survives reload'")
                await a.reload()
                await wait_js(a, "window.amplifier?.getState()?.client?.id")
                await composer(a).wait_for()
                report["checks"]["draft_survives_reload"] = await composer(a).input_value() == "Survives reload"
                report["checks"]["no_browser_errors"] = not errors and not http_errors
                report["checks"]["no_visible_alerts"] = await a.get_by_role("alert").count() == await b.get_by_role("alert").count() == 0
                report["passed"] = all(report["checks"].values())
                await browser.close()
                browser = None
    except Exception as exc:
        report["error_type"] = type(exc).__name__
        (folder / "error.txt").write_text(str(exc))
    finally:
        (folder / "workspace/release").touch()
        if browser and browser.is_connected():
            await browser.close()
        private_json(folder / "browser-errors.json", {"page_errors": errors, "http_errors": http_errors})
        private_json(folder / "events.json", observation.events)
        private_json(folder / "report.json", report)
        await runner.cleanup()
    print(json.dumps({"passed": report["passed"], "checks": report["checks"], "error_type": report.get("error_type"), "report": str(folder / "report.json")}), flush=True)
    return report["passed"]


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--allow-live", action="store_true")
    parser.add_argument("--settings", type=Path, default=Path.home()/".amplifier/settings.yaml")
    parser.add_argument("--provider", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--scenario", choices=("interaction", "compaction"), default="interaction")
    parser.add_argument("--voice-audio", action="store_true")
    parser.add_argument("--voice-phrasing", choices=("direct", "forwarded"), default="direct")
    args = parser.parse_args()
    args.profile = "work"
    if not args.allow_live:
        parser.error("Requires --allow-live; real provider calls can incur charges")
    if args.output.expanduser().resolve().is_relative_to(ROOT):
        parser.error("Store private acceptance output outside this repository")
    if args.voice_audio and args.scenario != "interaction":
        parser.error("Audio applies only to the interaction scenario")
    raise SystemExit(0 if asyncio.run(run(args)) else 1)


if __name__ == "__main__":
    main()
