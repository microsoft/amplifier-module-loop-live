"""Pure standard-library adversarial tests for the CI evidence gate."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("candidate", Path(__file__).with_name("exact_candidate.py"))
candidate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(candidate)
NODE = "tests/test_fixture.py::TestRegression::test_control[exact-param]"
OPTIONAL = "tests/test_loop.py::BundleTests::test_compaction_accepts_input_and_continues_same_task"
OPTIONAL_REASON = "Install the matching context-managed development module for this integration test"


def rows():
    return [dict(nodeid=NODE, when=p, outcome="passed", wasxfail=None, reason="")
            for p in ("setup", "call", "teardown")]


def optional_rows():
    reports = [dict(row, nodeid=OPTIONAL) for row in rows()]
    reports[1].update(outcome="skipped", reason=OPTIONAL_REASON)
    return reports


class GateTests(unittest.TestCase):
    def check_gate(self, reports=None, expected=None, collected=None, **kwargs):
        return candidate.validate(
            [NODE] if expected is None else expected, [NODE] if collected is None else collected,
            rows() if reports is None else reports, kwargs.get("collection_errors", []),
            kwargs.get("exitstatus", 0), kwargs.get("skip_allowed", {}))["qualified"]

    def test_complete_phases_pass(self):
        self.assertTrue(self.check_gate())

    def test_missing_duplicate_renamed_or_empty_collection_rejected(self):
        for nodes in ([], [NODE, NODE], [NODE.replace("exact-param", "renamed")]):
            with self.subTest(nodes=nodes):
                self.assertFalse(self.check_gate(collected=nodes))
        self.assertFalse(self.check_gate(expected=[], collected=[]))

    def test_setup_call_teardown_failure_rejected(self):
        for index in range(3):
            reports = rows()
            reports[index]["outcome"] = "failed"
            self.assertFalse(self.check_gate(reports))

    def test_missing_duplicate_and_unknown_phase_rejected(self):
        self.assertFalse(self.check_gate(rows()[:-1]))
        self.assertFalse(self.check_gate(rows() + [rows()[-1]]))
        reports = rows()
        reports[1]["when"] = "collect"
        self.assertFalse(self.check_gate(reports))

    def test_non_strict_xpass_and_xfail_rejected(self):
        for outcome in ("passed", "skipped"):
            reports = rows()
            reports[1].update(outcome=outcome, wasxfail="strict=False")
            self.assertFalse(self.check_gate(reports, skip_allowed={NODE: "fixture"}))

    def test_collection_error_or_collect_only_rejected(self):
        self.assertFalse(self.check_gate(collection_errors=[{"outcome": "failed"}]))
        self.assertFalse(self.check_gate(exitstatus=2))
        self.assertFalse(self.check_gate([]))

    def test_unknown_node_and_malformed_report_rejected(self):
        reports = rows()
        reports[1]["nodeid"] += "-other"
        self.assertFalse(self.check_gate(reports))
        reports = rows()
        del reports[1]["when"]
        self.assertFalse(self.check_gate(reports))

    def test_required_tests_cannot_all_skip(self):
        reports = [dict(nodeid=NODE, when=p, outcome=o, wasxfail=None, reason="fixture")
                   for p, o in (("setup", "skipped"), ("teardown", "passed"))]
        self.assertFalse(self.check_gate(reports))
        self.assertFalse(self.check_gate(reports, skip_allowed={NODE: "fixture"}))

    def optional_verdict(self, reports):
        return candidate.validate([NODE, OPTIONAL], [NODE, OPTIONAL],
                                  rows() + reports, [], 0, {OPTIONAL: OPTIONAL_REASON})

    def test_historical_optional_call_skip_with_passing_teardown_qualifies(self):
        reports = optional_rows()
        verdict = self.optional_verdict(reports)
        self.assertTrue(verdict["qualified"], verdict["errors"])
        self.assertEqual(verdict["passed"], [NODE])
        self.assertEqual(verdict["skipped"], [OPTIONAL])
        xml = '<testsuite><testcase classname="tests.test_fixture.TestRegression" name="test_control[exact-param]"/><testcase classname="tests.test_loop.BundleTests" name="test_compaction_accepts_input_and_continues_same_task"><skipped/></testcase></testsuite>'
        self.assertEqual(candidate.junit_errors([NODE, OPTIONAL], rows() + reports, xml), [])

    def test_allowlisted_reason_in_setup_or_teardown_cannot_qualify(self):
        for phase in ("setup", "teardown"):
            reports = [dict(row, nodeid=OPTIONAL) for row in rows()]
            reports[("setup", "call", "teardown").index(phase)].update(
                outcome="skipped", reason=OPTIONAL_REASON)
            with self.subTest(phase=phase):
                self.assertFalse(self.optional_verdict(reports)["qualified"])
        # No existing contract permits pytest-style setup SKIP / teardown PASS.
        reports = optional_rows()
        self.assertFalse(self.optional_verdict([
            reports[0] | {"outcome": "skipped", "reason": OPTIONAL_REASON},
            reports[2]])["qualified"])

    def test_optional_skip_requires_exact_passing_surrounding_phases(self):
        good = optional_rows()
        for reports in (
            good[1:], good[:-1], good + [good[-1]],
            [good[1], good[0], good[2]],
            [good[0], good[1], good[2] | {"outcome": "failed"}],
            [good[0] | {"outcome": "skipped"}, good[1], good[2]],
            [good[0], good[1] | {"reason": "different optional reason"}, good[2]],
        ):
            with self.subTest(reports=reports):
                self.assertFalse(self.optional_verdict(reports)["qualified"])
        for marker in ("strict=False", "", False):
            reports = optional_rows()
            reports[1]["wasxfail"] = marker
            self.assertFalse(self.optional_verdict(reports)["qualified"])

    def test_junit_exact_identity_and_phase_outcome_join(self):
        good = '<testsuites><testsuite><testcase classname="tests.test_fixture.TestRegression" name="test_control[exact-param]"/></testsuite></testsuites>'
        self.assertEqual(candidate.junit_errors([NODE], rows(), good), [])
        for bad in (
            good.replace("TestRegression", "WrongClass"),
            good.replace("tests.test_fixture", "other.test_fixture"),
            good.replace("[exact-param]", ""),
            good.replace("/></testsuite>", "><skipped/></testcase></testsuite>"),
            good.replace("/></testsuite>", "><failure/></testcase></testsuite>"),
            good.replace("</testsuite>", '<testcase classname="tests.test_fixture.TestRegression" name="test_control[exact-param]"/></testsuite>'),
            "<testsuites/>",
        ):
            self.assertTrue(candidate.junit_errors([NODE], rows(), bad))

    def test_count_preserving_junit_skip_swap_is_rejected(self):
        optional = NODE.replace("test_control", "test_optional")
        reports = rows() + [dict(nodeid=optional, when=p, outcome=o, wasxfail=None, reason="optional")
                           for p, o in (("setup", "passed"), ("call", "skipped"), ("teardown", "passed"))]
        good = '<testsuite><testcase classname="tests.test_fixture.TestRegression" name="test_control[exact-param]"/><testcase classname="tests.test_fixture.TestRegression" name="test_optional[exact-param]"><skipped/></testcase></testsuite>'
        bad = '<testsuite><testcase classname="tests.test_fixture.TestRegression" name="test_control[exact-param]"><skipped/></testcase><testcase classname="tests.test_fixture.TestRegression" name="test_optional[exact-param]"/></testsuite>'
        self.assertEqual(candidate.junit_errors([NODE, optional], reports, good), [])
        self.assertEqual(len(candidate.junit_errors([NODE, optional], reports, bad)), 2)

    def test_posttest_origin_failure_cannot_leave_qualified_artifact(self):
        def broken_origin():
            raise AssertionError("origin or RECORD changed")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "verdict.json"
            with self.assertRaises(AssertionError):
                candidate.finalize_verdict(path, {"qualified": True}, broken_origin)
            result = json.loads(path.read_text())
            self.assertFalse(result["qualified"])
            self.assertEqual(result["integrity"], "failed")
            candidate.finalize_verdict(path, {"qualified": True}, lambda: None)
            self.assertTrue(json.loads(path.read_text())["qualified"])


if __name__ == "__main__":
    unittest.main()