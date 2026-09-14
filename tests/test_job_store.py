import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from amplifier_core.message_models import ToolCall
from amplifier_module_loop_live.job_store import JobStore


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
            process = subprocess.run([sys.executable, "-c", code, directory], capture_output=True)
            self.assertEqual(process.returncode, 91, process.stderr.decode())
            ledger = JobStore(Path(directory) / "jobs")
            try:
                messages, recovered = ledger.recover([])
                self.assertEqual(recovered[0]["job_id"], "original-job")
                tool = next(m for m in messages if m["role"] == "tool")
                self.assertEqual(tool["tool_call_id"], "original-call")
                self.assertEqual(tool["content"], "POST-HOOK-REPORT")
                self.assertTrue(messages[0]["metadata"]["converge_native_async_calls"][0]["async"])
                again, recovered_again = ledger.recover(messages)
                self.assertEqual(again, messages)
                self.assertEqual(recovered_again, [])
                with self.assertRaisesRegex(RuntimeError, "cannot be dispatched"):
                    ledger.begin(ToolCall(id="original-call", name="delegate", arguments={}), "new-job", "receipt")
                self.assertEqual((Path(directory) / "effect-count").read_text(), "1")
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
