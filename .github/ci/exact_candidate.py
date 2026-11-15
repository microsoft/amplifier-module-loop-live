"""Offline CI component pairing only; never production/media-graph acceptance.

The shipped Live metadata still declares streaming@main. This separate profile
uses an explicit installer override, and verifies the resulting wheel bytes.
No runtime, maintained test, metadata, lock or ordinary workflow is rewritten.
"""
from __future__ import annotations

import base64
import collections
import csv
import hashlib
import importlib
import importlib.metadata as md
import importlib.machinery
import io
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import sysconfig
import traceback
import xml.etree.ElementTree as ET
import zipfile

LIVE = "9fcd416ee4ca76059b262fe8981c941e47d88f4d"
LIVE_TREE = "c4782f24aaa8884feecb9fbd0553d4fa145420a6"
LIVE_RUNTIME_SHA = "db773ae9f45a97c63aa3d694ed89bb0237bb7a1ae846cbb8ead5f46a53be7d62"
STREAM = "313ceba94ae0909ee6c835714fb116478cecb7d0"
STREAM_TREE = "89b31be276e34c4822c672ad2a82107c016afe0d"
CONTEXT = "67b3170c47e63f490f5524a2a0ebd500cfd3a73c"
OPENAI = "c8104f327b9286bff49c589f804969a5a9d0ba9d"
BASELINE = "https://github.com/microsoft/amplifier-module-loop-live/actions/runs/43678189969"

COMPONENT = [
    "test_activation.py", "test_approved_dispatch.py", "test_continuation_guard.py",
    "test_dispatch_callers.py", "test_failures.py", "test_generation_correlation.py",
    "test_job_media.py", "test_job_store.py", "test_loop.py",
    "test_media_pairing.py", "test_mount_diagnostics.py", "test_provenance.py",
    "test_recovery_index.py", "test_recovery_notifications.py", "test_runtime.py",
    "test_turn_cancellation.py",
]
NATIVE = [
    "test_native_provider_adapter.py", "test_native_history_options.py",
    "test_native_provider_budget_compatibility.py", "test_native_job_attribution.py",
    "test_native_adapter_fallback.py", "test_image_budget.py",
    "test_computer_preflight.py", "test_provider_imports.py",
]
FILES = {"component": COMPONENT, "native-adapter": NATIVE,
         "windows-ledger": ["test_job_store.py"]}
# SHA-256 of the JSON-encoded *full ordered node-ID list*, not a test count.
# Captured from the immutable reviewed test tree with the pinned pytest version.
# Full readable inventories are retained with every run.
INVENTORY_SHA = {
    "component": "4baa26e78df7f97a4819a03724a79d6bb3a441a8f968a6a3b50b22ad1b7855d5",
    "native-adapter": "040f799aba592ec5ee40991d6a78d6077ecde2e5b2fc909a38580edcf90765a5",
    "windows-ledger": "698d95967d1ee754e85d3980922d9232451641983bb91730ccfa4e5c2d690fab",
}


def digest(data):
    return hashlib.sha256(data).hexdigest()


def save(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def command(args, *, cwd=None, log=None):
    result = subprocess.run([str(x) for x in args], cwd=cwd, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, check=False)
    if log:
        log.write_bytes(result.stdout)
    print(result.stdout.decode("utf-8", errors="replace"), end="", flush=True)
    result.check_returncode()
    return result.stdout.decode("utf-8").strip()


def validate(expected, collected, reports, collection_errors, exitstatus, skip_allowed):
    """Fail closed on identity drift, every phase, xfail/XPASS and empty runs."""
    errors = []
    if not expected or len(expected) != len(set(expected)):
        errors.append("empty or duplicate expected inventory")
    if collections.Counter(expected) != collections.Counter(collected):
        errors.append("exact collection inventory mismatch")
    if len(collected) != len(set(collected)):
        errors.append("duplicate collection node ID")
    if collection_errors or exitstatus != 0:
        errors.append("collection/session failure")
    by_node = collections.defaultdict(list)
    for row in reports:
        if set(row) != {"nodeid", "when", "outcome", "wasxfail", "reason"}:
            errors.append("malformed typed report")
            continue
        if row["nodeid"] not in expected or row["when"] not in {"setup", "call", "teardown"}:
            errors.append("unknown node/phase")
        if row["outcome"] not in {"passed", "failed", "skipped"} or row["wasxfail"] is not None:
            errors.append("unknown outcome or xfail/XPASS")
        by_node[row["nodeid"]].append(row)
    passed = []
    skipped = []
    for node in expected:
        rows = by_node[node]
        phases = [r["when"] for r in rows]
        skip_rows = [r for r in rows if r["outcome"] == "skipped"]
        if skip_rows:
            allowed_reason = skip_allowed.get(node)
            # Both existing optional contracts skip in unittest's call phase.
            # A matching node/reason never excuses skipped setup or teardown.
            if (not allowed_reason or len(skip_rows) != 1
                    or skip_rows[0]["when"] != "call"
                    or phases != ["setup", "call", "teardown"]
                    or [r["outcome"] for r in rows] != ["passed", "skipped", "passed"]
                    or allowed_reason not in skip_rows[0]["reason"]):
                errors.append(f"unqualified skip: {node}")
            skipped.append(node)
        elif phases != ["setup", "call", "teardown"] or any(r["outcome"] != "passed" for r in rows):
            errors.append(f"missing/failed/duplicate phase: {node}")
        else:
            passed.append(node)
    required = set(expected) - set(skip_allowed)
    if not required or not required <= set(passed):
        errors.append("required regressions did not all execute and pass")
    return {"errors": errors, "passed": passed, "skipped": skipped,
            "required": sorted(required), "qualified": not errors}


def junit_errors(expected, reports, xml):
    """Join each JUnit identity AND outcome to its typed phase reports."""
    identities = {}
    for node in expected:
        parts = node.split("::")
        module = parts[0].removesuffix(".py").replace("/", ".")
        key = (".".join([module, *parts[1:-1]]), parts[-1])
        if key in identities:
            return ["ambiguous JUnit identity"]
        identities[key] = node
    observed = {}
    errors = []
    root = ET.fromstring(xml)
    for case in root.iter("testcase"):
        key = (case.get("classname"), case.get("name"))
        node = identities.get(key)
        if not node or node in observed:
            errors.append("unknown/duplicate JUnit module/class/parameter identity")
            continue
        statuses = [child.tag for child in case if child.tag in {"failure", "error", "skipped"}]
        if len(statuses) > 1:
            errors.append(f"ambiguous JUnit outcome: {node}")
        status = "failed" if any(s in {"failure", "error"} for s in statuses) else "skipped" if statuses else "passed"
        observed[node] = status
    typed = collections.defaultdict(list)
    for row in reports:
        typed[row["nodeid"]].append(row["outcome"])
    for node in expected:
        phases = typed[node]
        status = "failed" if "failed" in phases else "skipped" if "skipped" in phases else "passed"
        if node not in observed or observed[node] != status:
            errors.append(f"JUnit/typed per-node outcome mismatch: {node}")
    return errors


def finalize_verdict(path, verdict, check):
    """A pending/failed post-test integrity check can never leave success."""
    result = {**verdict, "qualified": False, "integrity": "pending"}
    save(path, result)
    try:
        check()
    except BaseException:
        result["integrity"] = "failed"
        save(path, result)
        raise
    result.update(qualified=verdict["qualified"], integrity="passed")
    save(path, result)


class Evidence:
    def __init__(self):
        self.collected, self.reports, self.collection_errors = [], [], []
        self.subtests = []
        self.exitstatus = None

    def pytest_collection_finish(self, session):
        self.collected = [item.nodeid for item in session.items]

    def pytest_collectreport(self, report):
        if report.failed or report.skipped:
            self.collection_errors.append({"nodeid": report.nodeid, "outcome": report.outcome,
                                           "reason": str(report.longrepr)})

    def pytest_runtest_logreport(self, report):
        from _pytest.subtests import SubtestReport
        row = {
            "nodeid": report.nodeid, "when": report.when, "outcome": report.outcome,
            "wasxfail": getattr(report, "wasxfail", None), "reason": str(report.longrepr or ""),
        }
        if isinstance(report, SubtestReport):
            row["context"] = report.context._to_json()
            self.subtests.append(row)
        else:
            self.reports.append(row)

    def pytest_sessionfinish(self, session, exitstatus):
        self.exitstatus = int(exitstatus)


def distribution_evidence(name, package, source=None, wheel=None, commit=None):
    dist = md.distribution(name)
    module = importlib.import_module(package)
    site = Path(sysconfig.get_paths()["purelib"]).resolve()
    origin = Path(module.__file__).resolve()
    assert origin.is_relative_to(site), (name, origin, site)
    direct = json.loads(dist.read_text("direct_url.json") or "{}")
    assert not direct.get("dir_info", {}).get("editable")
    if commit:
        assert direct["vcs_info"]["vcs"] == "git"
        assert direct["vcs_info"]["commit_id"] == commit
        assert direct["vcs_info"]["requested_revision"] == commit
    record = dist.read_text("RECORD")
    assert record
    checked = {}
    for relative, hash_field, size in csv.reader(io.StringIO(record)):
        if hash_field:
            algorithm, encoded = hash_field.split("=", 1)
            data = Path(dist.locate_file(relative)).read_bytes()
            assert algorithm == "sha256"
            assert base64.urlsafe_b64encode(hashlib.sha256(data).digest()).decode().rstrip("=") == encoded
            assert len(data) == int(size)
            checked[relative] = digest(data)
    sources = {}
    for path in sorted(source.rglob("*.py")) if source else []:
        relative = path.relative_to(source)
        installed = origin.parent / relative
        assert installed.read_bytes() == path.read_bytes(), (name, relative)
        sources[str(relative)] = digest(installed.read_bytes())
    if source:
        assert sources
    result = {"version": dist.version, "origin": str(origin), "direct_url": direct,
              "record_sha256": digest(record.encode()), "record_files": checked,
              "source_modules": sources, "requires_dist": dist.requires}
    if wheel:
        assert direct["url"].endswith(wheel.name) and "archive_info" in direct
        # uv may emit archive_info={} for a local wheel. Join every wheel
        # payload (including METADATA) against the installed RECORD instead.
        declared_hash = direct["archive_info"].get("hashes", {}).get("sha256")
        if declared_hash:
            assert declared_hash == digest(wheel.read_bytes())
        with zipfile.ZipFile(wheel) as archive:
            for relative in archive.namelist():
                if not relative.endswith("/") and not relative.endswith(".dist-info/RECORD"):
                    assert digest(archive.read(relative)) == checked[relative]
        result["wheel_sha256"] = digest(wheel.read_bytes())
    return result


def package_evidence(package, info):
    return distribution_evidence(info["distribution"], package,
        Path(info["source"]) if info.get("source") else None,
        Path(info["wheel"]) if info.get("wheel") else None, info.get("commit"))


def path_evidence(spec):
    """No checkout, PYTHONPATH or dependency source may supply imports."""
    site = Path(sysconfig.get_paths()["purelib"]).resolve()
    checks = Path(spec["root"]).resolve()
    result = []
    for entry in sys.path:
        assert entry, "CWD in import path"
        path = Path(entry).resolve()
        for source in spec["source_roots"]:
            assert not path.is_relative_to(Path(source).resolve()) or (
                path.is_relative_to(site) or path.is_relative_to(checks)), (entry, source)
        result.append(str(path))
    return result


def probe(spec):
    """Runs in the installed environment, outside every source checkout."""
    os.environ["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] = "1"
    import pytest

    spec = json.loads(Path(spec).read_text(encoding="utf-8"))
    out, root = Path(spec["evidence"]), Path(spec["root"])
    os.chdir(root)
    provenance = {"python": sys.version, "executable": sys.executable, "platform": platform.platform(),
                  "sys_path": sys.path, "environment": {k: os.environ.get(k) for k in
                  ("ImageOS", "ImageVersion", "RUNNER_OS", "RUNNER_ARCH", "GITHUB_SHA", "GITHUB_RUN_ID")},
                  "live_commit": LIVE, "live_tree": LIVE_TREE, "stream_commit": STREAM,
                  "stream_tree": STREAM_TREE, "profile": "CI-only explicit wheel override; not production composition"}
    provenance["sys_path"] = path_evidence(spec)
    for package, info in spec["packages"].items():
        provenance[package] = package_evidence(package, info)
    from amplifier_module_loop_streaming import SettledToolResult
    assert SettledToolResult
    import amplifier_core
    assert md.version("amplifier-core") == "2.0.2"
    native = [str(Path(module.__file__).resolve()) for name, module in sys.modules.copy().items()
              if name.startswith("amplifier_core") and getattr(module, "__file__", None)
              and any(module.__file__.endswith(s) for s in importlib.machinery.EXTENSION_SUFFIXES)]
    assert native, "Core must load its actual binary extension"
    assert all(Path(p).is_relative_to(Path(sysconfig.get_paths()["purelib"]).resolve()) for p in native)
    provenance["core"] = {"version": md.version("amplifier-core"), "origin": amplifier_core.__file__,
                          "extensions": {p: digest(Path(p).read_bytes()) for p in native}}
    provenance["dependencies"] = {
        d.metadata["Name"]: {"version": d.version,
            "direct_url": json.loads(d.read_text("direct_url.json") or "{}"),
            "record_sha256": digest((d.read_text("RECORD") or "").encode())}
        for d in md.distributions()
    }
    if spec["lane"] == "windows-ledger":
        import codecs
        import locale
        import struct
        from amplifier_module_loop_live import job_store
        assert os.name == "nt" and sys.version_info[:2] == (3, 13) and struct.calcsize("P") == 8
        assert not sysconfig.get_config_var("Py_GIL_DISABLED")
        assert sys.flags.utf8_mode == 0 and job_store._WINDOWS and "fcntl" not in sys.modules
        encodings = [locale.getencoding(), locale.getpreferredencoding(False),
                     io.TextIOWrapper(io.BytesIO()).encoding]
        assert all(codecs.lookup(e).name == "cp1252" for e in encodings), encodings
        provenance["windows_default_encodings"] = encodings
    save(out / "provenance-before.json", provenance)
    # These legacy file-loader fixtures resolve to the installed wheel, not a source copy.
    if spec["lane"] == "native-adapter":
        import amplifier_loop_live_cli
        alias = root / "adapters" / "cli"
        alias.mkdir(parents=True)
        alias.joinpath("amplifier_loop_live_cli").symlink_to(
            Path(amplifier_loop_live_cli.__file__).resolve().parent, target_is_directory=True)
    evidence = Evidence()
    code = pytest.main(["-p", "pytest_asyncio.plugin", "-o", "asyncio_mode=auto",
                        "--rootdir=.", "-q", "--junitxml=" + str(out / "junit.xml"),
                        *["tests/" + f for f in FILES[spec["lane"]]]], plugins=[evidence])
    expected = evidence.collected
    inventory_sha = digest(json.dumps(expected, ensure_ascii=True).encode())
    skips = {}
    if spec["lane"] == "component" and os.name != "nt":
        skips.update({n: "Native Windows kernel/CRT checks" for n in expected if "::NativeWindowsLedgerTests::" in n})
    # Historical optional context-managed integration; this profile installs context-simple.
    for n in expected:
        if n == "tests/test_loop.py::BundleTests::test_compaction_accepts_input_and_continues_same_task":
            skips[n] = "Install the matching context-managed development module for this integration test"
    save(out / "collection.json", evidence.collected)
    save(out / "reports.json", evidence.reports)
    save(out / "subtests.json", evidence.subtests)
    save(out / "collection-errors.json", evidence.collection_errors)
    verdict = validate(expected, evidence.collected, evidence.reports, evidence.collection_errors, int(code), skips)
    verdict["errors"].extend(junit_errors(expected, evidence.reports,
                                         (out / "junit.xml").read_text(encoding="utf-8")))
    if any(r["outcome"] != "passed" or r["wasxfail"] is not None or r["nodeid"] not in expected
           for r in evidence.subtests):
        verdict["errors"].append("failed/skipped/xfail/unknown typed unittest subtest")
    if inventory_sha != INVENTORY_SHA[spec["lane"]]:
        verdict["errors"].append("full node-ID inventory SHA differs from reviewed collection")
    verdict["qualified"] = not verdict["errors"]
    verdict["inventory_sha256"] = inventory_sha
    save(out / "test-verdict.json", verdict)
    # Recheck installed bytes after fixtures and retain all loaded real package origins.
    def integrity():
        loaded = {}
        for name, module in sys.modules.copy().items():
            if name.startswith(tuple(spec["packages"])) and getattr(module, "__file__", None):
                path = Path(module.__file__).resolve()
                assert path.is_relative_to(Path(sysconfig.get_paths()["purelib"]).resolve()), (name, path)
                loaded[name] = {"origin": str(path), "sha256": digest(path.read_bytes())}
        save(out / "loaded-origins.json", loaded)
        save(out / "sys-path-after.json", path_evidence(spec))
        for package, info in spec["packages"].items():
            package_evidence(package, info)
    finalize_verdict(out / "verdict.json", verdict, integrity)
    assert verdict["qualified"], verdict["errors"]


def driver():
    repo = Path(__file__).resolve().parents[2]
    lane = os.environ["CANDIDATE_LANE"]
    assert lane in FILES
    work = Path(os.environ["RUNNER_TEMP"]) / "exact-live-candidate"
    work.mkdir()
    out = work / "evidence"
    out.mkdir()
    sequence = 0

    def run(args, cwd=None):
        nonlocal sequence
        sequence += 1
        return command(args, cwd=cwd, log=out / f"command-{sequence:02d}.log")

    try:
        head = run(["git", "rev-parse", "HEAD"], repo)
        checkout_tree = run(["git", "rev-parse", "HEAD^{tree}"], repo)
        checkout_parents = run(["git", "show", "-s", "--format=%P", "HEAD"], repo).split()
        event_path = os.environ.get("GITHUB_EVENT_PATH")
        event = json.loads(Path(event_path).read_text(encoding="utf-8")) if event_path else {}
        pr = event.get("pull_request", {})
        checkout = {"commit": head, "tree": checkout_tree, "parents": checkout_parents,
                    "event_name": os.environ.get("GITHUB_EVENT_NAME"),
                    "github_sha": os.environ.get("GITHUB_SHA"),
                    "github_ref": os.environ.get("GITHUB_REF"),
                    "pr_number": event.get("number"),
                    "pr_head": pr.get("head", {}).get("sha"),
                    "pr_base": pr.get("base", {}).get("sha"),
                    "qualification": "checkout may be a synthetic PR merge, not the runtime source commit"}
        save(out / "checkout.json", checkout)
        # Require the reviewed runtime tree byte-for-byte; only additive CI files may differ.
        assert run(["git", "rev-parse", LIVE + "^{tree}"], repo) == LIVE_TREE
        assert digest((repo / "amplifier_module_loop_live/orchestrator.py").read_bytes()) == LIVE_RUNTIME_SHA
        changed = run(["git", "diff", "--name-only", LIVE, "HEAD"], repo).splitlines()
        assert all(p.startswith(".github/ci/") or p == ".github/workflows/exact-candidate.yml" for p in changed), changed
        assert not run(["git", "status", "--porcelain", "--untracked-files=no"], repo)
        stream = work / "stream"
        run(["git", "clone", "--filter=blob:none", "--no-checkout",
             "https://github.com/microsoft/amplifier-module-loop-streaming.git", stream])
        run(["git", "checkout", "--detach", STREAM], stream)
        assert run(["git", "rev-parse", "HEAD^{tree}"], stream) == STREAM_TREE
        wheels = out / "wheels"
        wheels.mkdir()
        run(["uv", "build", "--wheel", "--out-dir", wheels, str(repo)], repo)
        run(["uv", "build", "--wheel", "--out-dir", wheels, str(stream)], stream)
        live_wheel = next(wheels.glob("amplifier_module_loop_live-*.whl"))
        stream_wheel = next(wheels.glob("amplifier_module_loop_streaming-*.whl"))
        packages = {
            "amplifier_module_loop_live": {"distribution": "amplifier-module-loop-live",
                "source": str(repo / "amplifier_module_loop_live"), "wheel": str(live_wheel)},
            "amplifier_module_loop_streaming": {"distribution": "amplifier-module-loop-streaming",
                "source": str(stream / "amplifier_module_loop_streaming"), "wheel": str(stream_wheel)},
            "amplifier_module_context_simple": {"distribution": "amplifier-module-context-simple", "commit": CONTEXT},
            "amplifier_core": {"distribution": "amplifier-core"},
        }
        override = work / "overrides.txt"
        override.write_text("amplifier-module-loop-streaming @ " + stream_wheel.as_uri() + "\n", encoding="utf-8")
        env = work / "venv"
        run(["uv", "venv", "--python", sys.executable, env])
        python = env / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
        requirements = ["amplifier-core==2.0.2", "pytest==9.0.3", "pytest-asyncio==1.3.0",
                        "amplifier-module-context-simple @ git+https://github.com/microsoft/amplifier-module-context-simple@" + CONTEXT,
                        str(live_wheel)]
        if lane == "native-adapter":
            run(["uv", "build", "--wheel", "--out-dir", wheels, str(repo / "adapters/cli")], repo)
            adapter = next(wheels.glob("amplifier_loop_live_cli-*.whl"))
            # Optional adapter has a full host dependency graph. This lane installs
            # only its offline component dependencies, not a floating CLI/Foundation.
            run(["uv", "pip", "install", "--python", python, "--no-deps", adapter])
            requirements += ["amplifier-module-provider-openai[native] @ git+https://github.com/microsoft/amplifier-module-provider-openai@" + OPENAI]
            packages["amplifier_loop_live_cli"] = {"distribution": "amplifier-loop-live-cli",
                "source": str(repo / "adapters/cli/amplifier_loop_live_cli"), "wheel": str(adapter)}
            packages["amplifier_module_provider_openai"] = {
                "distribution": "amplifier-module-provider-openai", "commit": OPENAI}
        run(["uv", "pip", "install", "--python", python, "--only-binary", "amplifier-core",
             "--overrides", override, *requirements])
        root = work / "checks"
        root.mkdir()
        shutil.copytree(repo / "tests", root / "tests")
        save(out / "test-source-join.json", {
            str(p.relative_to(repo)): digest(p.read_bytes())
            for p in sorted((repo / "tests").glob("test_*.py"))
            if (root / p.relative_to(repo)).read_bytes() == p.read_bytes()})
        # Include declared files for the structural branch-tracking test only in
        # the checkout; it is deliberately not claimed by installed test lanes.
        spec = {"lane": lane, "root": str(root), "evidence": str(out),
                "packages": packages, "source_roots": [str(repo), str(stream)]}
        save(work / "spec.json", spec)
        save(out / "pairing.json", {"workflow_commit": head, "checkout": checkout,
            "live_commit": LIVE, "live_tree": LIVE_TREE, "live_runtime_sha256": LIVE_RUNTIME_SHA,
            "stream_commit": STREAM, "stream_tree": STREAM_TREE, "context_commit": CONTEXT,
            "openai_commit": OPENAI if lane == "native-adapter" else None,
            "baseline_failed_run": BASELINE, "baseline_status": "failure with cancelled lanes; unchanged",
            "selected_files": FILES[lane], "not_selected": sorted(set(p.name for p in (repo / "tests").glob("test_*.py")) - set(FILES[lane])),
            "limits": ["offline component fixtures only; no vendor calls",
                       "Foundation/CLI actual-media graph belongs to E87, not qualified here",
                       "adapter component installed --no-deps; no full CLI application composition",
                       "runtime metadata still streaming@main: upstream merge hold remains"]})
        shutil.copyfile(Path(__file__), work / "probe.py")
        run([python, "-I", "-X", "utf8=0", work / "probe.py", "--probe", work / "spec.json"], root)
    except BaseException:
        (out / "failure.txt").write_text(traceback.format_exc(), encoding="utf-8")
        raise
    finally:
        # Snapshot first, then write: the manifest never includes itself.
        hashes = {p.relative_to(out).as_posix(): digest(p.read_bytes())
                  for p in sorted(out.rglob("*")) if p.is_file() and p.name != "hashes.json"}
        save(out / "hashes.json", hashes)


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--probe":
        probe(sys.argv[2])
    else:
        driver()