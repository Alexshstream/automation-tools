"""
main()'s exit code and the dry-run "created in StreamSecurity" report.

main() returns 0 only when every account integrated and every swept stack
reached CREATE_COMPLETE/UPDATE_COMPLETE (or nothing needed deploying);
anything failed, errored or unconfirmed (TIMED_OUT) returns 1, so CI and
wrapper scripts cannot read a failed run as a success.
"""
import contextlib
import io
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.python.utilities import organization_integration as oi


def _record(account, stack_id="sid-1", final_status=None):
    record = {"account": account, "name": f"acct-{account}", "region": "us-east-1",
              "stack_type": "init", "stack_name": "LightlyticsStack-abc", "stack_id": stack_id}
    if final_status:
        record["final_status"] = final_status
        record["status_reason"] = None
    return record


class TestMainExitCode(unittest.TestCase):
    def _run_main(self, integrate, sweep_statuses=None, confirm="yes", dry_run=False, **main_kwargs):
        """Run main() against one fake account. `integrate` is the
        integrate_sub_account side effect; `sweep_statuses` maps stack_id to the
        final_status the sweep reports. Returns (exit_code, stdout)."""
        org_client = MagicMock()
        org_client.list_accounts.return_value = {
            "Accounts": [{"Id": "111111111111", "Name": "acct", "Status": "ACTIVE"}]}
        sts_client = MagicMock()
        sts_client.get_caller_identity.return_value = {"Account": "999999999999"}
        ec2_client = MagicMock()
        ec2_client.describe_regions.return_value = {"Regions": [{"RegionName": "us-east-1"}]}

        def boto3_client(service, **kwargs):
            return {"organizations": org_client, "sts": sts_client, "ec2": ec2_client}[service]

        def fake_sweep(records, *args, **kwargs):
            for r in records:
                if r.get("final_status") is None:
                    r["final_status"] = (sweep_statuses or {}).get(r["stack_id"], "CREATE_COMPLETE")
                    r["status_reason"] = None
            return records

        kwargs = dict(environment_url="https://example.streamsec.io", ll_username=None,
                      ll_password=None, aws_profile_name=None, accounts="111111111111",
                      parallel=None, ws_id="ws-1", api_token="fake-token", dry_run=dry_run)
        kwargs.update(main_kwargs)

        out = io.StringIO()
        with patch.object(oi, "boto3") as mock_boto3, \
                patch.object(oi, "GraphCommon", return_value=MagicMock()), \
                patch.object(oi, "integrate_sub_account", side_effect=integrate), \
                patch.object(oi, "sweep_stack_statuses", side_effect=fake_sweep), \
                patch("builtins.input", return_value=confirm), \
                contextlib.redirect_stdout(out):
            mock_boto3.client.side_effect = boto3_client
            code = oi.main(**kwargs)
        return code, out.getvalue()

    def test_all_stacks_complete_returns_0(self):
        code, _ = self._run_main(lambda *a, **kw: [_record(a[1][0])])
        self.assertEqual(code, 0)

    def test_nothing_deployed_and_no_failures_returns_0(self):
        code, out = self._run_main(lambda *a, **kw: [])
        self.assertEqual(code, 0)
        self.assertIn("Integration finished successfully!", out)

    def test_failed_stack_returns_1(self):
        code, _ = self._run_main(lambda *a, **kw: [_record(a[1][0])],
                                 sweep_statuses={"sid-1": "ROLLBACK_COMPLETE"})
        self.assertEqual(code, 1)

    def test_timed_out_stack_returns_1(self):
        # TIMED_OUT means the run could not confirm the stack, not that it worked.
        code, _ = self._run_main(lambda *a, **kw: [_record(a[1][0])],
                                 sweep_statuses={"sid-1": "TIMED_OUT"})
        self.assertEqual(code, 1)

    def test_submit_failed_record_returns_1(self):
        code, _ = self._run_main(
            lambda *a, **kw: [_record(a[1][0], stack_id=None, final_status="SUBMIT_FAILED")])
        self.assertEqual(code, 1)

    def test_account_failure_returns_1_even_when_its_stacks_complete(self):
        def integrate(*a, **kw):
            e = Exception("regions update failed")
            e.deployed_stacks = [_record(a[1][0])]
            raise e
        code, _ = self._run_main(integrate)
        self.assertEqual(code, 1)

    def test_pure_dry_run_returns_0(self):
        code, _ = self._run_main(
            lambda *a, **kw: [_record(a[1][0], stack_id=None, final_status="DRY_RUN")],
            dry_run=True)
        self.assertEqual(code, 0)

    def test_user_cancel_returns_0(self):
        integrate = MagicMock()
        code, _ = self._run_main(integrate, confirm="no")
        self.assertEqual(code, 0)
        integrate.assert_not_called()

    def test_missing_environment_url_returns_1(self):
        integrate = MagicMock()
        code, _ = self._run_main(integrate, environment_url=None)
        self.assertEqual(code, 1)
        integrate.assert_not_called()


class TestDryRunCreatedAccountsReport(unittest.TestCase):
    def _run(self, dry_run, created):
        def integrate(*a, **kw):
            if created:
                kw["created_in_stream"].append(a[1][0])
            status = "DRY_RUN" if dry_run else None
            return [_record(a[1][0], stack_id=None if dry_run else "sid-1", final_status=status)]
        return TestMainExitCode()._run_main(integrate, dry_run=dry_run)[1]

    def test_dry_run_lists_accounts_created_in_stream(self):
        out = self._run(dry_run=True, created=True)
        self.assertIn("1 account(s) were created for real in StreamSecurity", out)
        self.assertIn("111111111111", out.split("created for real in StreamSecurity")[1])

    def test_dry_run_without_new_accounts_prints_no_report(self):
        out = self._run(dry_run=True, created=False)
        self.assertNotIn("account(s) were created for real", out)

    def test_real_run_prints_no_report(self):
        # In a real run creating the account is the expected outcome, not a side effect.
        out = self._run(dry_run=False, created=True)
        self.assertNotIn("account(s) were created for real", out)


class TestIntegrateSubAccountRecordsCreatedAccount(unittest.TestCase):
    """integrate_sub_account() must add an account to created_in_stream only when
    it actually called create_account, not for accounts that already existed."""

    def _run(self, existing_accounts):
        graph_client = MagicMock()
        account_information = {
            "cloud_account_id": "111111111111", "cloud_regions": ["us-east-1"],
            "status": "UNINITIALIZED", "lightlytics_collection_token": "tok",
            "template_url": "https://example.com/t.yaml",
            "collection_template_url": "https://example.com/c.yaml"}
        graph_client.get_accounts.side_effect = [existing_accounts, [account_information]]
        graph_client.create_account.return_value = True

        session = MagicMock()
        session.region_name = "us-east-1"
        created = []
        with patch.object(oi, "boto3") as oi_boto3, \
                patch.object(oi, "get_active_regions", return_value=["us-east-1"]):
            oi_boto3.Session.return_value = session
            oi.integrate_sub_account(
                "https://example.streamsec.io", ("111111111111", "acct"), MagicMock(), graph_client,
                ["us-east-1"], "abc123", None, None, "OrganizationAccountAccessRole",
                "111111111111", dry_run=True, created_in_stream=created)
        return graph_client, created

    def test_new_account_is_recorded(self):
        graph_client, created = self._run(existing_accounts=[])
        graph_client.create_account.assert_called_once()
        self.assertEqual(created, ["111111111111"])

    def test_existing_uninitialized_account_is_not_recorded(self):
        existing = [{"cloud_account_id": "111111111111", "status": "UNINITIALIZED"}]
        graph_client, created = self._run(existing_accounts=existing)
        graph_client.create_account.assert_not_called()
        self.assertEqual(created, [])


if __name__ == "__main__":
    unittest.main()
