"""Run the user's complete configured Amplifier bundle with loop-live."""

import argparse
import asyncio
import json
import logging
import sys
import uuid
from pathlib import Path

from prompt_toolkit import PromptSession
from prompt_toolkit.patch_stdout import patch_stdout
from amplifier_loop_live_cli.configured import prepare_manager
from amplifier_module_loop_live.runtime import Input, Runtime


class Terminal:
    def __init__(self, runtime):
        self.runtime = runtime
        self.approvals = {}

    async def ask(self, prompt, options):
        identity = str(uuid.uuid4())[:8]
        future = asyncio.get_running_loop().create_future()
        self.approvals[identity] = (future, options)
        print(f"\nApproval {identity}: {prompt}\nOptions: {', '.join(options)}\n"
              f"Use /approve {identity} OPTION", flush=True)
        try:
            return await future
        finally:
            self.approvals.pop(identity, None)

    async def interact(self):
        await self.runtime.wait_for(lambda e: e["type"] == "session.ready")
        prompt = PromptSession()
        print("Requests, /steer TEXT, /service SOURCE TEXT, /jobs, /cancel ID, /approve ID OPTION, /quit")
        with patch_stdout():
            while not self.runtime.closed:
                try:
                    line = (await prompt.prompt_async("You> ")).strip()
                except (EOFError, KeyboardInterrupt):
                    line = "/quit"
                if not line:
                    continue
                if line == "/quit":
                    await self.runtime.submit(Input("stop"))
                    return
                try:
                    if line.startswith("/approve "):
                        _, identity, answer = line.split(" ", 2)
                        future, options = self.approvals[identity]
                        if answer not in options or future.done():
                            raise ValueError()
                        future.set_result(answer)
                        continue
                    if len(self.approvals) == 1 and line.lower() in {"approve", "approved", "i approve", "deny"}:
                        future, options = next(iter(self.approvals.values()))
                        answer = "deny" if line.lower() == "deny" else "allow"
                        if answer not in options or future.done():
                            raise ValueError()
                        future.set_result(answer)
                        continue
                    if line == "/jobs":
                        jobs = {}
                        for event in self.runtime.events:
                            if event["type"].startswith("job."):
                                jobs[event["job_id"]] = event["type"]
                        print(json.dumps(jobs, indent=2))
                        continue
                    if line.startswith("/steer "):
                        command = Input("steer", line[7:])
                    elif line.startswith("/service "):
                        source, text = line[9:].split(" ", 1)
                        command = Input("service", text, source=source)
                    elif line.startswith("/cancel "):
                        command = Input("cancel_job", target=line[8:].strip())
                    elif line.startswith("/"):
                        raise ValueError()
                    else:
                        command = Input("user", line)
                    await self.runtime.submit(command)
                except (KeyError, ValueError, asyncio.QueueFull):
                    print("Input rejected; check the command, approval ID, and available options.")


async def single_prompt(runtime, session, text, timeout):
    await runtime.wait_for(lambda e: e["type"] == "session.ready")
    await runtime.submit(Input("user", text))
    previous = 0
    async with asyncio.timeout(timeout):
        while True:
            event = await runtime.wait_for(lambda e: e["type"] == "generation.finished" and
                                          e["sequence"] > previous, timeout=timeout)
            previous = event["sequence"]
            loop = session.coordinator.get("orchestrator")
            if not loop._active_jobs() and not loop.pending and runtime.inbox.empty():
                break
    await runtime.submit(Input("stop"))


async def run(args):
    workspace = Path(args.workspace).expanduser().resolve()
    if args.create_workspace:
        from amplifier_workspace.config import load_config
        from amplifier_workspace.workspace import setup_workspace
        # Creation is explicit; never update an existing workspace's submodules.
        if workspace.exists() and any(workspace.iterdir()):
            raise ValueError("--create-workspace requires a new or empty directory")
        await asyncio.to_thread(setup_workspace, workspace, load_config())

    def display(event):
        if args.json:
            print(json.dumps(event), flush=True)
        elif event["type"] == "assistant.message":
            print("\nAmplifier:", event["text"], flush=True)
        elif event["type"] in {"session.ready", "session.closed", "job.queued", "job.returned",
                "job.failed", "job.cancelled", "job.recovered", "provider.error", "input.delivered", "observation.received",
                "persistence.failed", "steering.sent", "steering.accepted", "steering.pending", "steering.failed", "native.error"}:
            print("\n[" + event["type"] + "]", json.dumps({key: value for key, value in event.items()
                if key not in {"time", "version", "sequence", "type"}}), flush=True)

    runtime = Runtime(session_id=args.resume, observer=display)
    terminal = Terminal(runtime)
    report_dir = args.report_dir or Path.home() / ".amplifier" / "converge-live" / runtime.session_id
    print(f"Preparing configured manager in {workspace}. Reports: {report_dir}", flush=True)
    session, runtime, report = await prepare_manager(workspace, runtime=runtime, bundle=args.bundle,
        background_delegate=not args.synchronous_delegate, pin_provider=args.pin_provider,
        ask=None if args.inspect or args.prompt else terminal.ask, report_dir=report_dir, resume=bool(args.resume),
        selection=args.selection)
    print(f"Mounted {report['bundle']}: {len(report['providers'])} providers, {len(report['tools'])} tools, "
          f"{len(report['agents'])} agents. Reports: {report['report_directory']}", flush=True)
    if report["module_load_failures"]:
        print("Module load failures:", json.dumps(report["module_load_failures"]), flush=True)
    execution = interaction = None
    try:
        if args.resume and not report["resumed"]:
            raise ValueError("No saved transcript or job evidence exists for this session in this workspace")
        if args.inspect:
            return
        execution = asyncio.create_task(session.execute(""))
        interaction = asyncio.create_task(single_prompt(runtime, session, args.prompt, args.timeout)
            if args.prompt else terminal.interact())
        done, _ = await asyncio.wait({execution, interaction}, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            task.result()
        if interaction in done:
            await asyncio.wait_for(execution, 30)
    finally:
        tasks = [task for task in (execution, interaction) if task]
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await session.cleanup()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", default=".", help="Existing workspace (default: current directory)")
    parser.add_argument("--create-workspace", action="store_true", help="Scaffold a NEW workspace using amplifier-workspace's configuration")
    parser.add_argument("--bundle", help="Override the active bundle for this session only")
    parser.add_argument("--pin-provider", help="Pin the manager to an existing provider instance ID; worker routing stays configured")
    parser.add_argument("--selection", type=json.loads, help='Root-only selection JSON: instance, model, effort, thinkingBudget')
    parser.add_argument("--resume", type=lambda value: str(uuid.UUID(value)), help="Resume a saved root session UUID, including its job evidence; never restart interrupted jobs")
    parser.add_argument("--synchronous-delegate", action="store_true", help="Wait inline for delegate results")
    parser.add_argument("--report-dir", type=Path, help="Local output directory for redacted mount plans and inventory")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--inspect", action="store_true", help="Prepare and mount the environment, then exit without a model turn")
    mode.add_argument("--prompt", help="Run one request, including background delegates, then exit; approvals default to denial")
    parser.add_argument("--timeout", type=int, default=600, help="Headless request deadline in seconds")
    parser.add_argument("--json", action="store_true", help="Print live events as JSON (configured hooks may also print)")
    args = parser.parse_args(argv)
    if args.report_dir:
        args.report_dir = args.report_dir.expanduser().resolve()
    logging.basicConfig(level=logging.ERROR)
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        raise SystemExit(130)
    except Exception as exc:
        print("Manager failed:", type(exc).__name__, file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
