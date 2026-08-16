import contextlib
import io
import os
import sys
import unittest
from unittest.mock import patch

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.python.utilities import organization_delete_integration as mod


class TestReverifyOrphans(unittest.TestCase):
    def _items(self):
        return [
            {"account": "111", "name": "a", "region": "us-east-1", "function": "plain"},
            {"account": "111", "name": "a", "region": "us-east-1", "function": "orph",
             "orphaned_stack": "dead"},
        ]

    def test_orphan_still_gone_is_kept_nonorphan_passthrough(self):
        failed = []
        with patch.object(mod, "list_live_stack_names", return_value={"other"}):
            kept, reprotected = mod._reverify_orphans(object(), "111", self._items(), failed)
        self.assertEqual({k["function"] for k in kept}, {"plain", "orph"})
        self.assertEqual(reprotected, [])
        self.assertEqual(failed, [])

    def test_recreated_stack_orphan_is_reprotected_not_failed(self):
        # Stack came back -> a correct protection, NOT an operation failure.
        failed = []
        with patch.object(mod, "list_live_stack_names", return_value={"dead"}):
            kept, reprotected = mod._reverify_orphans(object(), "111", self._items(), failed)
        self.assertEqual([k["function"] for k in kept], ["plain"])
        self.assertEqual([r["function"] for r in reprotected], ["orph"])
        self.assertEqual(failed, [])            # not a failure -> no non-zero exit

    def test_unverifiable_orphan_is_failed_not_reprotected(self):
        failed = []
        with patch.object(mod, "list_live_stack_names", side_effect=RuntimeError("boom")):
            kept, reprotected = mod._reverify_orphans(object(), "111", self._items(), failed)
        self.assertEqual([k["function"] for k in kept], ["plain"])
        self.assertEqual(reprotected, [])
        self.assertEqual(len(failed), 1)        # real gap -> drives non-zero exit

    def test_no_orphans_returns_items_unchanged_without_listing(self):
        failed = []
        items = [{"account": "111", "name": "a", "region": "us-east-1", "function": "plain"}]
        with patch.object(mod, "list_live_stack_names") as lls:
            kept, reprotected = mod._reverify_orphans(object(), "111", items, failed)
        self.assertEqual(kept, items)
        self.assertEqual(reprotected, [])
        lls.assert_not_called()


class TestReverifyWiredIntoRun(unittest.TestCase):
    """_reverify_orphans is exercised in isolation elsewhere; these confirm
    _run_lambda_mode actually calls it and that its outcomes drive the exit code."""

    def _run(self, list_kwargs):
        accounts = [("111", "acct-a")]
        orphan = {"account": "111", "name": "acct-a", "region": "us-east-1",
                  "function": "orph", "orphaned_stack": "dead"}

        def fake_scan(sub_account, session, regions, pattern):
            return [dict(orphan)], [], []

        deleted = []
        with patch.object(mod, "_session_for_account", return_value=object()), \
                patch.object(mod, "_scan_account_lambdas", side_effect=fake_scan), \
                patch.object(mod, "confirm_deletion", return_value=True), \
                patch.object(mod, "_reverify_orphans", wraps=mod._reverify_orphans) as spy, \
                patch.object(mod, "delete_lambda_function",
                             side_effect=lambda s, r, f: deleted.append(f) or "deleted"), \
                patch.object(mod, "list_live_stack_names", **list_kwargs):
            with contextlib.redirect_stdout(io.StringIO()):
                rc = mod._run_lambda_mode(accounts, sts_client=None,
                                          management_account_id="999",
                                          regions=["us-east-1"], pattern="orph",
                                          just_print=False)
        spy.assert_called()          # the delete phase is wired to call _reverify_orphans
        return rc, deleted

    def test_recreated_stack_orphan_not_deleted_exit_zero(self):
        rc, deleted = self._run({"return_value": {"dead"}})   # stack came back
        self.assertEqual(deleted, [])          # protected, not deleted
        self.assertEqual(rc, 0)                # correct protection -> clean exit

    def test_unverifiable_orphan_not_deleted_exit_nonzero(self):
        rc, deleted = self._run({"side_effect": RuntimeError("boom")})
        self.assertEqual(deleted, [])          # not deleted
        self.assertNotEqual(rc, 0)             # a real gap -> non-zero exit


class TestLambdaScanErrorsAffectExit(unittest.TestCase):
    def test_scan_gap_causes_nonzero_exit_even_on_just_print(self):
        accounts = [("111", "acct-a")]

        def fake_scan(sub_account, session, regions, pattern):
            # No deletable functions, but one region/function couldn't be scanned.
            return [], [], [{"account": "111", "name": "acct-a", "region": "us-east-1",
                             "function": "f", "reason": "could not read tags"}]

        with patch.object(mod, "_session_for_account", return_value=object()), \
                patch.object(mod, "_scan_account_lambdas", side_effect=fake_scan):
            rc = mod._run_lambda_mode(accounts, sts_client=None, management_account_id="999",
                                      regions=["us-east-1"], pattern="stream", just_print=True)

        self.assertEqual(rc, 1)   # scan gap surfaced -> non-zero, not a silent success

    def test_clean_scan_returns_zero(self):
        accounts = [("111", "acct-a")]

        def fake_scan(sub_account, session, regions, pattern):
            return [], [], []

        with patch.object(mod, "_session_for_account", return_value=object()), \
                patch.object(mod, "_scan_account_lambdas", side_effect=fake_scan):
            rc = mod._run_lambda_mode(accounts, sts_client=None, management_account_id="999",
                                      regions=["us-east-1"], pattern="stream", just_print=True)

        self.assertEqual(rc, 0)

    def test_non_tty_refusal_with_pending_work_exits_nonzero(self):
        accounts = [("111", "acct-a")]

        def fake_scan(sub_account, session, regions, pattern):
            # A clean scan (no gaps) that DID find a function to delete.
            return ([{"account": "111", "name": "acct-a", "region": "us-east-1",
                      "function": "target"}], [], [])

        with patch.object(mod, "_session_for_account", return_value=object()), \
                patch.object(mod, "_scan_account_lambdas", side_effect=fake_scan), \
                patch.object(mod.sys.stdin, "isatty", return_value=False):
            # just_print=False so it reaches confirm_deletion, which refuses on the
            # non-TTY stdin — the run must NOT report success.
            rc = mod._run_lambda_mode(accounts, sts_client=None, management_account_id="999",
                                      regions=["us-east-1"], pattern="stream", just_print=False)

        self.assertGreaterEqual(rc, 1)

    def test_account_scan_crash_is_recorded_not_fatal(self):
        accounts = [("111", "acct-a"), ("222", "acct-b")]

        def fake_scan(sub_account, session, regions, pattern):
            if sub_account[0] == "111":
                raise RuntimeError("boom")   # one account blows up mid-scan
            return [], [], []

        with patch.object(mod, "_session_for_account", return_value=object()), \
                patch.object(mod, "_scan_account_lambdas", side_effect=fake_scan):
            # Must not raise; the crashing account becomes a scan gap (exit non-zero),
            # the other account is still scanned.
            rc = mod._run_lambda_mode(accounts, sts_client=None, management_account_id="999",
                                      regions=["us-east-1"], pattern="stream", just_print=True)

        self.assertGreaterEqual(rc, 1)

    def test_delete_phase_assume_role_failure_counted_once_as_failed(self):
        accounts = [("111", "acct-a")]

        def fake_scan(sub_account, session, regions, pattern):
            return ([{"account": "111", "name": "acct-a", "region": "us-east-1",
                      "function": "target"}], [], [])

        # Session succeeds during scan, then fails when re-assumed for the delete.
        outcomes = [object(), Exception("creds expired")]

        def fake_session(sub_account, sts_client, mgmt):
            r = outcomes.pop(0)
            if isinstance(r, Exception):
                raise r
            return r

        buf = io.StringIO()
        with patch.object(mod, "_session_for_account", side_effect=fake_session), \
                patch.object(mod, "_scan_account_lambdas", side_effect=fake_scan), \
                patch.object(mod, "confirm_deletion", return_value=True), \
                contextlib.redirect_stdout(buf):
            rc = mod._run_lambda_mode(accounts, sts_client=None, management_account_id="999",
                                      regions=["us-east-1"], pattern="target", just_print=False)

        out = buf.getvalue()
        self.assertEqual(rc, 1)                        # the planned function counts as one failure
        self.assertIn("1 failed", out)
        self.assertIn("0 accounts unreachable", out)   # NOT also double-reported as unreachable


if __name__ == "__main__":
    unittest.main()
