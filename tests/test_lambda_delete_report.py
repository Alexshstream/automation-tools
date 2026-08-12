import os
import sys
import tempfile
import unittest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.python.common.boto_common import (
    write_plan_file,
    print_lambda_plan,
    print_lambda_summary,
    format_plan_lines,
)


class TestWritePlanFile(unittest.TestCase):
    def test_writes_sorted_lines(self):
        results = [
            {"account": "222", "name": "d", "region": "us-east-1", "function": "z"},
            {"account": "111", "name": "p", "region": "us-east-1", "function": "a"},
        ]
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "plan.txt")
            write_plan_file(results, path)
            with open(path) as f:
                lines = f.read().splitlines()
        self.assertEqual(lines, ["111 | us-east-1 | a", "222 | us-east-1 | z"])


class TestPrintSummaryExitCount(unittest.TestCase):
    def test_only_operation_failures_count_toward_exit_code(self):
        code = print_lambda_summary(
            deleted=[{"account": "1"}],
            already_gone=[],
            failed=[{"account": "2", "region": "us-east-1", "function": "f", "reason": "boom"}],
            skipped_cfn=[{"account": "3", "function": "g", "stack": "s"}],
            assume_role_failures=[("9", "acc9", "denied")],
        )
        # Only operation failures count toward the exit code; the unreachable
        # account is reported but does not.
        self.assertEqual(code, 1)

    def test_unreachable_accounts_do_not_affect_exit_code(self):
        code = print_lambda_summary(
            deleted=[{"account": "1"}], already_gone=[], failed=[], skipped_cfn=[],
            assume_role_failures=[("9", "acc9", "denied"), ("8", "acc8", "denied")])
        self.assertEqual(code, 0)

    def test_clean_run_returns_zero(self):
        code = print_lambda_summary([{"account": "1"}], [], [], [], [])
        self.assertEqual(code, 0)


class TestPrintPlanEmptySafe(unittest.TestCase):
    def test_no_crash_on_empty(self):
        print_lambda_plan([], [])


class TestOrphanReporting(unittest.TestCase):
    def test_format_plan_lines_annotates_orphans_only(self):
        results = [
            {"account": "111", "region": "us-east-1", "function": "plain"},
            {"account": "222", "region": "us-east-1", "function": "orph",
             "orphaned_stack": "dead-stack"},
        ]
        lines = format_plan_lines(results)
        self.assertEqual(lines[0], "111 | us-east-1 | plain")
        self.assertEqual(lines[1], "222 | us-east-1 | orph (orphaned; stack dead-stack gone)")

    def test_summary_reports_orphan_subcount(self):
        import io, contextlib
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            code = print_lambda_summary(
                deleted=[{"account": "1", "region": "us-east-1", "function": "a",
                          "orphaned_stack": "gone"},
                         {"account": "1", "region": "us-east-1", "function": "b"}],
                already_gone=[], failed=[], skipped_cfn=[], assume_role_failures=[])
        out = buf.getvalue()
        self.assertIn("2 deleted", out)
        self.assertIn("1 orphaned CFN", out)
        self.assertEqual(code, 0)


if __name__ == "__main__":
    unittest.main()
