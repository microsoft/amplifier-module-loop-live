import codecs
import copy
import ctypes
import inspect
import io
import json
import locale
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest import mock

from amplifier_core.message_models import ToolCall
from amplifier_module_loop_live import job_store
from amplifier_module_loop_live.job_store import JobStore


def _python_command(code, *arguments):
    # A Windows venv executable can be a redirector. Terminate the actual
    # interpreter we own, not a launcher that could leave the lock holder alive.
    executable = sys.executable
    if sys.platform == "win32":
        executable = sys._base_executable
        if not executable or not Path(executable).is_file():
            raise RuntimeError("Subprocess checks require the actual base interpreter")
    # Preserve this test's selected candidate/dependencies without an install,
    # user-site lookup or a checkout added by the child working directory.
    bootstrap = f"import sys\nsys.path[:] = {sys.path!r}\nassert sys.flags.utf8_mode == 0\n"
    if sys.platform == "win32":
        bootstrap += (
            "import codecs, io, locale\n"
            "for encoding in (locale.getencoding(), locale.getpreferredencoding(False), "
            "io.TextIOWrapper(io.BytesIO()).encoding):\n"
            "    assert codecs.lookup(encoding).name == 'cp1252', "
            "f'Cannot qualify non-UTF-8 Windows ledger with default encoding {encoding}'\n"
        )
    return [executable, "-I", "-X", "utf8=0", "-c", bootstrap + code, *arguments]


class RecoveryTests(unittest.TestCase):
    def test_process_death_after_result_commit_recovers_call_without_checkpoint_or_reexecution(self):
        with tempfile.TemporaryDirectory() as directory:
            code = """
import os,sys
from pathlib import Path
from amplifier_core.message_models import ToolCall
from amplifier_module_loop_live.job_store import JobStore
root=Path(sys.argv[1]); ledger=JobStore(root/'jobs')
call=ToolCall(id='original-call',name='delegate',arguments={'instruction':'controlled'})
native={'type':'function_call','call_id':call.id,'name':call.name,'arguments':'{}','async':True}
ledger.begin(call,'original-job','QUEUED-RECEIPT',native)
(root/'effect-count').write_text('1')
ledger.finish(call.id,'POST-HOOK-REPORT','returned')
os._exit(91)
"""
            process = subprocess.run(_python_command(code, directory), capture_output=True, timeout=15)
            self.assertEqual(process.returncode, 91, process.stderr.decode())
            ledger = JobStore(Path(directory) / "jobs")
            try:
                messages, recovered = ledger.recover([])
                self.assertEqual(recovered[0]["job_id"], "original-job")
                tool = next(m for m in messages if m["role"] == "tool")
                self.assertEqual(tool["tool_call_id"], "original-call")
                self.assertEqual(tool["content"], "POST-HOOK-REPORT")
                self.assertTrue(messages[0]["metadata"]["converge_native_async_calls"][0]["async"])
                notice = next(m for m in messages if m["role"] == "user")
                self.assertEqual(notice["metadata"]["amplifier_input"], {
                    "version": 1, "kind": "service", "id": "original-job",
                    "source": "local-job-recovery", "call_id": "original-call"})
                again, recovered_again = ledger.recover(messages)
                self.assertEqual(again, messages)
                self.assertEqual(recovered_again, [])
                with self.assertRaisesRegex(RuntimeError, "cannot be dispatched"):
                    ledger.begin(ToolCall(id="original-call", name="delegate", arguments={}), "new-job", "receipt")
                self.assertEqual((Path(directory) / "effect-count").read_text(), "1")
                if os.name != "nt":
                    self.assertEqual(next((Path(directory) / "jobs").glob("job-*.json")).stat().st_mode & 0o777, 0o600)
            finally:
                ledger.close()

    def test_pending_intent_becomes_unknown_and_conflicting_checkpoint_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            ledger = JobStore(directory)
            call = ToolCall(id="call", name="delegate", arguments={})
            ledger.begin(call, "job", "receipt")
            ledger.close()
            ledger = JobStore(directory)
            try:
                messages, recovered = ledger.recover([{"role": "tool", "tool_call_id": "call", "content": "receipt"}])
                result = json.loads(next(m for m in messages if m["role"] == "tool")["content"])
                self.assertEqual(result["outcome"], "unconfirmed")
                self.assertEqual(result["effects"], "not_rolled_back")
                self.assertEqual(recovered[0]["status"], "interrupted")
                with self.assertRaisesRegex(RuntimeError, "conflicts"):
                    ledger.recover([{"role": "tool", "tool_call_id": "call", "content": "OTHER-RESULT"}])
            finally:
                ledger.close()

    def test_second_owner_is_rejected_until_owner_releases_lock(self):
        with tempfile.TemporaryDirectory() as directory:
            owner = JobStore(directory)
            try:
                with self.assertRaisesRegex(RuntimeError, "already has a local owner"):
                    JobStore(directory)
            finally:
                owner.close()
            resumed = JobStore(directory)
            resumed.close()

    def test_corrupt_evidence_does_not_silently_start_empty(self):
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, "job-broken.json").write_text("{")
            with self.assertRaises(ValueError):
                JobStore(directory)


class LedgerPortabilityTests(unittest.TestCase):
    def test_stable_lock_rejects_competing_process_and_preserves_existing_bytes(self):
        code = """
import sys
from amplifier_module_loop_live.job_store import JobStore
try:
    owner = JobStore(sys.argv[1])
except RuntimeError as error:
    assert "already has a local owner" in str(error)
    print("REJECTED")
else:
    owner.close()
    print("ACQUIRED")
"""
        with tempfile.TemporaryDirectory() as directory:
            lock = Path(directory, "owner.lock")
            lock.write_bytes(b"stable-lock-do-not-truncate")
            before = lock.stat()
            owner = JobStore(directory)
            try:
                self.assertFalse(os.get_inheritable(owner.lock))
                result = subprocess.run(
                    _python_command(code, directory), capture_output=True, timeout=15)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout.strip(), b"REJECTED")
                # Another handle cannot read MSVCRT-locked byte zero, even in
                # this process. Keep ownership intact and inspect the held fd.
                self.assertEqual(os.fstat(owner.lock).st_size, before.st_size)
            finally:
                owner.close()
            owner.close()  # Closing twice must not close a later owner's fd.
            result = subprocess.run(
                _python_command(code, directory), capture_output=True, timeout=15)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout.strip(), b"ACQUIRED")
            self.assertEqual((lock.stat().st_dev, lock.stat().st_ino), (before.st_dev, before.st_ino))
            self.assertEqual(lock.read_bytes(), b"stable-lock-do-not-truncate")

    def test_owner_close_crash_and_termination_release_without_replay(self):
        code = """
import os, sys
from pathlib import Path
from amplifier_core.message_models import ToolCall
from amplifier_module_loop_live.job_store import JobStore
root = Path(sys.argv[1])
owner = JobStore(root / "jobs")
owner.begin(ToolCall(id="once", name="delegate", arguments={}), "job", "receipt")
(root / "starting").write_bytes(str(os.getpid()).encode("ascii"))
os.replace(root / "starting", root / "ready")
sys.stdin.buffer.read(1)
if sys.argv[2] == "crash":
    os._exit(91)
owner.close()
"""
        for ending in ("close", "crash", "terminate"):
            with self.subTest(ending=ending), tempfile.TemporaryDirectory() as directory:
                process = subprocess.Popen(
                    _python_command(code, directory, ending),
                    stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                try:
                    deadline = time.monotonic() + 15
                    while not Path(directory, "ready").exists() and process.poll() is None:
                        if time.monotonic() >= deadline:
                            self.fail("Owned subprocess did not acquire the ledger within 15 seconds")
                        time.sleep(0.01)
                    self.assertTrue(Path(directory, "ready").exists(), "Owner exited before readiness")
                    self.assertEqual(int(Path(directory, "ready").read_bytes()), process.pid,
                                     "The owned process must be the interpreter holding the lock")
                    lock = Path(directory, "jobs", "owner.lock")
                    before = lock.stat()
                    with self.assertRaisesRegex(RuntimeError, "already has a local owner"):
                        JobStore(Path(directory, "jobs"))
                    if ending == "terminate":
                        process.terminate()
                        _, stderr = process.communicate(timeout=5)
                        self.assertNotEqual(process.returncode, 0, stderr)
                    else:
                        _, stderr = process.communicate(input=b"x", timeout=5)
                        self.assertEqual(process.returncode, 91 if ending == "crash" else 0, stderr)
                    resumed = JobStore(Path(directory, "jobs"))
                    try:
                        self.assertEqual(resumed.rows["once"]["status"], "pending")
                        messages, recovered = resumed.recover([])
                        self.assertEqual(recovered[0]["status"], "interrupted")
                        result = json.loads(next(m for m in messages if m["role"] == "tool")["content"])
                        self.assertEqual(result["outcome"], "unconfirmed")
                        with self.assertRaisesRegex(RuntimeError, "cannot be dispatched"):
                            resumed.begin(ToolCall(id="once", name="delegate", arguments={}), "new", "receipt")
                        self.assertEqual((lock.stat().st_dev, lock.stat().st_ino), (before.st_dev, before.st_ino))
                    finally:
                        resumed.close()
                finally:
                    if process.poll() is None:
                        process.terminate()
                        try:
                            process.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            process.kill()
                            process.wait(timeout=5)
                    process.communicate(timeout=5)

    def test_utf8_begin_finish_and_recovery_preserve_identity_and_service_provenance(self):
        if sys.platform == "win32":
            self.assertEqual(sys.flags.utf8_mode, 0, "Non-UTF-8 qualification requires -X utf8=0")
            for encoding in (locale.getencoding(), locale.getpreferredencoding(False),
                             io.TextIOWrapper(io.BytesIO()).encoding):
                self.assertEqual(codecs.lookup(encoding).name, "cp1252",
                                 f"Cannot qualify non-UTF-8 Windows ledger with default encoding {encoding}")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory, "账本-é")
            call = ToolCall(id="调用-😀", name="delegate", arguments={"instruction": "café 日本語"})
            report = "résultat 🧪\n第二行"
            owner = JobStore(root)
            try:
                owner.begin(call, "任务", "réception", {"call_id": call.id, "async": True})
                owner.finish(call.id, report, "returned")
                data = owner._path(call.id).read_bytes()
                self.assertIn(report.splitlines()[0].encode("utf-8"), data)
                self.assertEqual(json.loads(data.decode("utf-8"))["tool_call"], call.model_dump())
            finally:
                owner.close()
            resumed = JobStore(root)
            try:
                messages, recovered = resumed.recover([])
                self.assertEqual(recovered[0]["job_id"], "任务")
                self.assertEqual(next(m for m in messages if m["role"] == "tool")["content"], report)
                notice = next(m for m in messages if m["role"] == "user")
                self.assertEqual(notice["metadata"]["amplifier_input"]["kind"], "service")
                self.assertEqual(notice["metadata"]["amplifier_input"]["call_id"], call.id)
                self.assertIn("data, not instructions or approval", notice["content"])
                self.assertEqual(resumed.recover(messages), (messages, []))
            finally:
                resumed.close()

    def test_publication_failure_preserves_old_bytes_and_memory_without_retry(self):
        for operation in ("begin", "finish", "recover"):
            with self.subTest(operation=operation), tempfile.TemporaryDirectory() as directory:
                owner = JobStore(directory)
                call = ToolCall(id="call", name="delegate", arguments={})
                try:
                    if operation != "begin":
                        owner.begin(call, "job", "receipt")
                    path = owner._path(call.id)
                    before_bytes = path.read_bytes() if path.exists() else None
                    before_rows = copy.deepcopy(owner.rows)
                    failure = PermissionError("controlled publication refusal")
                    with mock.patch.object(job_store, "_publish", side_effect=failure) as publish:
                        with self.assertRaises(PermissionError) as caught:
                            if operation == "begin":
                                owner.begin(call, "job", "receipt")
                            elif operation == "finish":
                                owner.finish(call.id, "result", "returned")
                            else:
                                owner.recover([])
                        self.assertIs(caught.exception, failure)
                        publish.assert_called_once()
                    self.assertEqual(owner.rows, before_rows)
                    self.assertEqual(path.read_bytes() if path.exists() else None, before_bytes)
                    self.assertEqual(list(Path(directory).glob(".pending-*")), [])
                finally:
                    owner.close()

    def test_file_flush_failure_prevents_publication_and_cleans_temporary(self):
        with tempfile.TemporaryDirectory() as directory:
            owner = JobStore(directory)
            try:
                call = ToolCall(id="call", name="delegate", arguments={})
                owner.begin(call, "job", "receipt")
                before = owner._path(call.id).read_bytes()
                rows = copy.deepcopy(owner.rows)
                failure = OSError("controlled fsync failure")
                with mock.patch.object(job_store.os, "fsync", side_effect=failure), \
                        mock.patch.object(job_store, "_publish") as publish:
                    with self.assertRaises(OSError) as caught:
                        owner.finish(call.id, "result", "returned")
                    self.assertIs(caught.exception, failure)
                    publish.assert_not_called()
                self.assertEqual(owner.rows, rows)
                self.assertEqual(owner._path(call.id).read_bytes(), before)
                self.assertEqual(list(Path(directory).glob(".pending-*")), [])
            finally:
                owner.close()

    def test_cleanup_failure_retains_original_publication_error(self):
        with tempfile.TemporaryDirectory() as directory:
            owner = JobStore(directory)
            try:
                failure = OSError("original replacement failure")
                with mock.patch.object(job_store, "_publish", side_effect=failure), \
                        mock.patch.object(job_store.os, "unlink", side_effect=PermissionError("cleanup refused")):
                    with self.assertRaises(OSError) as caught:
                        owner.begin(ToolCall(id="call", name="delegate", arguments={}), "job", "receipt")
                self.assertIs(caught.exception, failure)
                self.assertIn("temporary cleanup failed", failure.__notes__[0])
                self.assertEqual(owner.rows, {})
                self.assertFalse(owner._path("call").exists())
                for temporary in Path(directory).glob(".pending-*"):
                    temporary.unlink()  # Only the test's owned temporary; no operation replay.
            finally:
                owner.close()


class PlatformContractModels(unittest.TestCase):
    def test_held_lock_model_rejects_original_body_and_accepts_size_check(self):
        method = LedgerPortabilityTests.test_stable_lock_rejects_competing_process_and_preserves_existing_bytes
        source = "class OriginalHeldLockBody:\n" + inspect.getsource(method)
        fixed = "self.assertEqual(os.fstat(owner.lock).st_size, before.st_size)"
        original = 'self.assertEqual(lock.read_bytes(), b"stable-lock-do-not-truncate")'
        self.assertEqual(source.count(fixed), 1)
        namespace = {}
        exec(compile(source.replace(fixed, original), "<original-held-lock-body>", "exec"),
             globals(), namespace)
        original_method = getattr(namespace["OriginalHeldLockBody"], method.__name__)
        real_owner = JobStore
        real_read = Path.read_bytes
        owners = []
        refused = []

        def tracked_owner(directory):
            owner = real_owner(directory)
            owners.append(owner)
            return owner

        def guarded_read(path):
            if path.name == "owner.lock" and any(
                    owner.lock is not None and path.parent == owner.directory for owner in owners):
                refused.append(path.name)
                raise PermissionError("Modeled Windows byte-zero lock forbids another-handle read")
            return real_read(path)

        with mock.patch(__name__ + ".JobStore", side_effect=tracked_owner), \
                mock.patch.object(Path, "read_bytes", guarded_read):
            with self.assertRaisesRegex(PermissionError, "byte-zero lock"):
                original_method(self)
            self.assertTrue(all(owner.lock is None for owner in owners))
            method(self)
            self.assertTrue(all(owner.lock is None for owner in owners))
        self.assertEqual(refused, ["owner.lock"])

    def test_owned_subprocess_command_avoids_windows_venv_redirector(self):
        with mock.patch.object(sys, "platform", "win32"), \
                mock.patch.object(sys, "_base_executable", sys.executable):
            command = _python_command("print('controlled')", "argument")
            self.assertEqual(command[:5], [sys.executable, "-I", "-X", "utf8=0", "-c"])
            self.assertIn(repr(sys.path), command[5])
            self.assertIn("sys.flags.utf8_mode == 0", command[5])
            self.assertIn("locale.getencoding()", command[5])
            self.assertIn("locale.getpreferredencoding(False)", command[5])
            self.assertIn("io.TextIOWrapper(io.BytesIO()).encoding", command[5])
            self.assertIn("'cp1252'", command[5])
            self.assertEqual(command[6:], ["argument"])
        with mock.patch.object(sys, "platform", "win32"), \
                mock.patch.object(sys, "_base_executable", ""):
            with self.assertRaisesRegex(RuntimeError, "actual base interpreter"):
                _python_command("pass")

    def test_child_explicit_utf8_mode_overrides_ignored_environment_and_rejects_utf8(self):
        command = _python_command("print(sys.flags.utf8_mode)")
        for environment_mode in ("0", "1"):
            with self.subTest(environment_mode=environment_mode):
                result = subprocess.run(command, env={**os.environ, "PYTHONUTF8": environment_mode},
                                        capture_output=True, timeout=15)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout.strip(), b"0")
        wrong_mode = command.copy()
        wrong_mode[3] = "utf8=1"
        result = subprocess.run(wrong_mode, env={**os.environ, "PYTHONUTF8": "0"},
                                capture_output=True, timeout=15)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(b"AssertionError", result.stderr)

    def test_windows_import_and_lock_model_never_imports_fcntl_or_truncates(self):
        source = Path(job_store.__file__).read_text(encoding="utf-8")
        events = []
        fake_os = SimpleNamespace(name="nt", SEEK_SET=0,
                                  lseek=lambda *args: events.append(("seek", args)))
        crt = SimpleNamespace(LK_NBLCK=99,
                              locking=lambda *args: events.append(("lock", args)))
        real_import = __import__

        def guarded_import(name, *args, **kwargs):
            if name == "fcntl":
                raise AssertionError("Windows ledger imported fcntl")
            return real_import(name, *args, **kwargs)

        namespace = {"__name__": "windows_ledger_model"}
        with mock.patch.dict(sys.modules, {"os": fake_os, "msvcrt": crt}), \
                mock.patch("builtins.__import__", side_effect=guarded_import):
            exec(compile(source, job_store.__file__, "exec"), namespace)
        self.assertTrue(namespace["_WINDOWS"])
        namespace["_lock_owner"](42)
        self.assertEqual(events, [("seek", (42, 0, 0)), ("lock", (42, 99, 1))])
        failure = OSError("contended")
        crt.locking = mock.Mock(side_effect=failure)
        with self.assertRaises(OSError) as caught:
            namespace["_lock_owner"](43)
        self.assertIs(caught.exception, failure)

    def test_windows_move_model_uses_wide_write_through_replace_and_original_error(self):
        from ctypes import wintypes

        move = mock.Mock(return_value=1)
        library = SimpleNamespace(MoveFileExW=move)
        failure = OSError("native move failed")
        with mock.patch.object(ctypes, "WinDLL", return_value=library, create=True) as load, \
                mock.patch.object(ctypes, "get_last_error", return_value=32, create=True), \
                mock.patch.object(ctypes, "WinError", return_value=failure, create=True) as win_error:
            job_store._windows_replace("临时", "账本")
            load.assert_called_once_with("kernel32", use_last_error=True)
            move.assert_called_once_with("临时", "账本", 9)
            self.assertEqual(move.argtypes, [wintypes.LPCWSTR, wintypes.LPCWSTR, wintypes.DWORD])
            self.assertIs(move.restype, wintypes.BOOL)
            move.return_value = 0
            with self.assertRaises(OSError) as caught:
                job_store._windows_replace("临时", "账本")
            self.assertIs(caught.exception, failure)
            win_error.assert_called_once_with(32)
            self.assertEqual(move.call_count, 2)  # One call per explicit request, no retry.

    def test_windows_write_model_flushes_utf8_before_native_publication_without_directory_open(self):
        with tempfile.TemporaryDirectory() as directory:
            owner = JobStore(directory)
            try:
                events = []
                real_fsync = os.fsync
                real_replace = os.replace

                def fsync(fd):
                    events.append("fsync")
                    return real_fsync(fd)

                def replace(source, target):
                    self.assertEqual(Path(source).parent, Path(target).parent)
                    self.assertIn("日本語".encode("utf-8"), Path(source).read_bytes())
                    events.append("publish")
                    real_replace(source, target)  # Model only; not Windows evidence.

                with mock.patch.object(job_store, "_WINDOWS", True), \
                        mock.patch.object(job_store.os, "fsync", side_effect=fsync), \
                        mock.patch.object(job_store, "_windows_replace", side_effect=replace), \
                        mock.patch.object(job_store.os, "open", wraps=os.open) as opened:
                    owner.begin(ToolCall(id="call", name="delegate", arguments={"x": "日本語"}), "job", "receipt")
                self.assertEqual(events, ["fsync", "publish"])
                self.assertEqual(opened.call_count, 1)  # mkstemp only, no directory fd.
                self.assertEqual(list(Path(directory).glob(".pending-*")), [])
            finally:
                owner.close()

    def test_posix_lock_and_publication_contract_model_is_unchanged(self):
        events = []
        flock = SimpleNamespace(LOCK_EX=2, LOCK_NB=4,
                                flock=lambda *args: events.append(("lock", args)))
        with mock.patch.object(job_store, "_WINDOWS", False), \
                mock.patch.object(job_store, "fcntl", flock, create=True), \
                mock.patch.object(job_store.os, "O_DIRECTORY", 65536, create=True), \
                mock.patch.object(job_store.os, "replace", side_effect=lambda *args: events.append(("replace", args))), \
                mock.patch.object(job_store.os, "open", return_value=44) as opened, \
                mock.patch.object(job_store.os, "fsync", side_effect=lambda fd: events.append(("fsync", fd))), \
                mock.patch.object(job_store.os, "close", side_effect=lambda fd: events.append(("close", fd))):
            job_store._lock_owner(43)
            job_store._publish("source", "target", "directory")
            opened.assert_called_once_with("directory", os.O_RDONLY | 65536)
        self.assertEqual(events, [
            ("lock", (43, 6)), ("replace", ("source", "target")), ("fsync", 44), ("close", 44)])


@unittest.skipUnless(os.name == "nt", "Native Windows kernel/CRT checks")
class NativeWindowsLedgerTests(unittest.TestCase):
    def test_native_windows_import_without_fcntl(self):
        code = """
import builtins, sys
original = builtins.__import__
def guarded(name, *args, **kwargs):
    if name == "fcntl":
        raise AssertionError("Unix-only import")
    return original(name, *args, **kwargs)
builtins.__import__ = guarded
from amplifier_module_loop_live import mount
from amplifier_module_loop_live.job_store import JobStore, _WINDOWS
assert _WINDOWS and callable(mount) and callable(JobStore)
assert "fcntl" not in sys.modules
"""
        result = subprocess.run(_python_command(code), capture_output=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_native_windows_byte_zero_lock_on_empty_stable_file(self):
        import msvcrt

        with tempfile.TemporaryDirectory() as directory:
            owner = JobStore(directory)
            competing = os.open(Path(directory, "owner.lock"), os.O_RDWR)
            try:
                self.assertEqual(os.fstat(competing).st_size, 0)
                os.lseek(competing, 0, os.SEEK_SET)
                with self.assertRaises(OSError):
                    msvcrt.locking(competing, msvcrt.LK_NBLCK, 1)
                owner.close()
                os.lseek(competing, 0, os.SEEK_SET)
                msvcrt.locking(competing, msvcrt.LK_NBLCK, 1)
                with self.assertRaisesRegex(RuntimeError, "already has a local owner"):
                    JobStore(directory)
                self.assertEqual(os.fstat(competing).st_size, 0)
            finally:
                os.close(competing)
                owner.close()
            resumed = JobStore(directory)
            resumed.close()

    def test_native_windows_refused_replacement_keeps_old_bytes_memory_and_error(self):
        with tempfile.TemporaryDirectory() as directory:
            owner = JobStore(directory)
            try:
                call = ToolCall(id="call", name="delegate", arguments={})
                owner.begin(call, "job", "receipt")
                path = owner._path(call.id)
                before = path.read_bytes()
                rows = copy.deepcopy(owner.rows)
                # Ordinary CRT readers do not share DELETE. Exercise the real
                # kernel refusal, not a monkeypatched publication function.
                with path.open("rb", buffering=0) as reader:
                    with self.assertRaises(OSError) as caught:
                        owner.finish(call.id, "result", "returned")
                    self.assertIn(caught.exception.winerror, (5, 32))
                    self.assertEqual(reader.read(), before)
                    self.assertEqual(path.read_bytes(), before)
                    self.assertEqual(owner.rows, rows)
                    self.assertEqual(list(Path(directory).glob(".pending-*")), [])
            finally:
                owner.close()
