"""Prepare or serve a persistent, isolated Work preview for hands-on testing."""
from __future__ import annotations

import argparse
import asyncio
import copy
import hashlib
import json
import os
from pathlib import Path
import shlex

import yaml

from acceptance import ROOT, PROVIDER_SOURCES, installed_revisions, private_json, setup


def environment(folder):
    os.environ.update(AMPLIFIER_HOME=str(folder / "shared"),
                      AMPLIFIER_WEB_HOME=str(folder / "app"),
                      AMPLIFIER_SESSION_STATE_HOME=str(folder / "ownership"))


async def initialize(args):
    from amplifier_foundation import load_bundle
    from amplifier_web.host.config import expand_environment

    args.output = args.directory
    folder, first = setup(args)
    providers = [first]
    configured = yaml.safe_load(args.settings.expanduser().read_text()) or {}
    for identity in args.also_provider:
        matches = [p for p in configured.get("config", {}).get("providers", [])
                   if (p.get("id") or p["module"]) == identity]
        if len(matches) != 1 or matches[0]["module"] not in PROVIDER_SOURCES:
            raise ValueError("Additional provider must identify one configured OpenAI or Anthropic instance")
        row = expand_environment(copy.deepcopy(matches[0]))
        row["source"] = PROVIDER_SOURCES[row["module"]]
        if not any((p.get("id") or p["module"]) == identity for p in providers):
            providers.append(row)
    bundle = await load_bundle(str(ROOT / "bundle.md"), strict=True)
    plan = bundle.to_mount_plan()
    plan["bundle"] = {"name": "work-preview", "version": "0.2.0"}
    profile = folder / "profile.md"
    profile.write_text("---\n" + yaml.safe_dump(plan, sort_keys=False) + "---\n\n" + bundle.instruction)
    settings = {"bundle": {"active": str(profile)}, "config": {"providers": providers}}
    (folder / "shared/settings.yaml").write_text(yaml.safe_dump(settings, sort_keys=False))
    if key := os.environ.get("OPENAI_API_KEY"):
        # Persist only the voice credential, not the user's entire keys file.
        (folder / "shared/keys.env").write_text("OPENAI_API_KEY=" + shlex.quote(key) + "\n")
    (folder / "workspace/delivery.txt").write_text(
        "Synthetic delivery exercise\nReport: ORCHID\nReference: OLIVE-731\nColor: BLUE\nCrates: 4\n")
    (folder / "workspace/collect_delivery.py").write_text(
        '"""Synthetic 45-second data collection for testing conversation during work."""\n'
        'from pathlib import Path\nimport time\ntime.sleep(45)\n'
        'print(Path(__file__).with_name("delivery.txt").read_text())\n')
    (folder / "workspace/README.md").write_text(
        "# Work preview workspace\n\nThis is a disposable test workspace with synthetic delivery data.\n"
        "`collect_delivery.py` waits 45 seconds and prints `delivery.txt`.\n"
        "Use it to try a background helper while asking side questions or changing the report.\n"
        "Any reports you ask Amplifier to create here remain available across restarts.\n")
    private_json(folder / "preview.json", {
        "schema_version": 1, "port": args.port, "profile": str(profile),
        "bundle_sha256": hashlib.sha256(profile.read_bytes()).hexdigest(),
        "provider_instances": [p.get("id") or p["module"] for p in providers],
        "installed": installed_revisions(), "voice_configured": bool(os.environ.get("OPENAI_API_KEY")),
    })
    print(json.dumps({"directory": str(folder), "url": f"http://127.0.0.1:{args.port}"}))


def serve(args):
    from aiohttp import web
    from amplifier_web.deployment import DEFAULT_SERVER
    from amplifier_web.host.config import load_config
    from amplifier_web.server import create_app

    folder = args.directory.expanduser().resolve()
    config = json.loads((folder / "preview.json").read_text())
    environment(folder)
    load_config(folder / "workspace")
    server = {**DEFAULT_SERVER, "bind": ["127.0.0.1"], "port": config["port"]}
    # Use the normal packaged worker and its dependency environment. No fixture,
    # alternate worker command, auth bypass, or model mocking in this preview.
    app = create_app(folder / "app", workspace=folder / "workspace",
                     background_updates=False, server_config=server)
    print(f"Work preview: http://127.0.0.1:{config['port']}", flush=True)
    web.run_app(app, host="127.0.0.1", port=config["port"], print=None)


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    init = commands.add_parser("init", help="Create a new private preview; existing directories are never overwritten")
    init.add_argument("--directory", type=Path, required=True)
    init.add_argument("--settings", type=Path, default=Path.home()/".amplifier/settings.yaml")
    init.add_argument("--provider", required=True)
    init.add_argument("--also-provider", action="append", default=[])
    init.add_argument("--port", type=int, default=8956)
    server = commands.add_parser("serve", help="Run the existing preview, preserving its conversations and files")
    server.add_argument("--directory", type=Path, required=True)
    args = parser.parse_args()
    if args.directory.expanduser().resolve().is_relative_to(ROOT):
        parser.error("Keep private preview data outside this repository")
    if args.command == "init":
        if not 1024 <= args.port <= 65535:
            parser.error("Choose an unprivileged local port between 1024 and 65535")
        asyncio.run(initialize(args))
    else:
        serve(args)


if __name__ == "__main__":
    main()
