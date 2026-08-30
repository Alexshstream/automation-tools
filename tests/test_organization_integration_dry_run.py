"""
Tests for --dry_run: every CloudFormation create_stack call must be skipped
(zero AWS cost), while the StreamSecurity backend account-creation call
still happens for real (needed to preview real template URLs/tokens/etc.,
zero AWS cost either way). Region updates (edit_regions,
wait_for_account_connection) are also preview-only, since without a real
init stack the backend will never transition the account out of
UNINITIALIZED - a real update_regions() call would just hang until its
5-minute timeout.
"""
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.python.common import boto_common
from src.python.utilities import organization_integration as oi


class TestTryCreateStackDryRun(unittest.TestCase):
    def test_dry_run_never_calls_create_stack(self):
        cf_client = MagicMock()
        stack_id, record = boto_common._try_create_stack(
            ("111111111111", "acct"), "us-east-1", "init", "MyStack", cf_client,
            {"StackName": "MyStack"}, dry_run=True)
        cf_client.create_stack.assert_not_called()
        self.assertIsNone(stack_id)
        self.assertEqual(record["final_status"], "DRY_RUN")
        self.assertIsNone(record["stack_id"])

    def test_normal_run_unaffected(self):
        cf_client = MagicMock()
        cf_client.create_stack.return_value = {"StackId": "arn:fake"}
        stack_id, record = boto_common._try_create_stack(
            ("111111111111", "acct"), "us-east-1", "init", "MyStack", cf_client,
            {"StackName": "MyStack"})
        cf_client.create_stack.assert_called_once()
        self.assertEqual(stack_id, "arn:fake")
        self.assertIsNone(record)


class TestDeployHelpersDryRun(unittest.TestCase):
    def _account_information(self):
        return {
            "template_url": "https://example.com/t.yaml",
            "collection_template_url": "https://example.com/c.yaml",
            "lightlytics_collection_token": "tok",
            "external_id": "ext",
            "cloud_regions": ["us-east-1"],
        }

    def test_deploy_init_stack_dry_run_is_success_not_failure(self):
        session = MagicMock()
        session.region_name = "us-east-1"
        cf_client = MagicMock()
        session.client.return_value = cf_client
        graph_client = MagicMock()

        ok, record = boto_common.deploy_init_stack(
            self._account_information(), graph_client, ("111111111111", "acct"), session, "abc123",
            wait=True, dry_run=True)

        cf_client.create_stack.assert_not_called()
        graph_client.wait_for_account_connection.assert_not_called()
        self.assertTrue(ok, "dry run must not be treated as a failure")
        self.assertEqual(record["final_status"], "DRY_RUN")

    def test_deploy_response_stack_dry_run(self):
        session = MagicMock()
        cf_client = MagicMock()
        session.client.return_value = cf_client
        record = boto_common.deploy_response_stack(
            "https://example.streamsec.io", self._account_information(), session,
            ("111111111111", "acct"), "us-east-1", "abc123", None, "", wait=False, dry_run=True)
        cf_client.create_stack.assert_not_called()
        self.assertEqual(record["final_status"], "DRY_RUN")

    def test_deploy_collection_stack_dry_run(self):
        session = MagicMock()
        cf_client = MagicMock()
        session.client.return_value = cf_client
        record = boto_common.deploy_collection_stack(
            self._account_information(), session, ("111111111111", "acct"), "us-west-2",
            "abc123", None, wait=False, dry_run=True)
        cf_client.create_stack.assert_not_called()
        self.assertEqual(record["final_status"], "DRY_RUN")

    def test_deploy_all_collection_stacks_dry_run(self):
        session = MagicMock()
        cf_client = MagicMock()
        session.client.return_value = cf_client
        records = boto_common.deploy_all_collection_stacks(
            ["us-east-1", "us-west-2"], session, "abc123", self._account_information(),
            ("111111111111", "acct"), dry_run=True)
        cf_client.create_stack.assert_not_called()
        self.assertEqual(len(records), 2)
        for r in records:
            self.assertEqual(r["final_status"], "DRY_RUN")

    def test_deploy_all_collection_stacks_dry_run_does_not_claim_submission(self):
        # The end-of-function summary line used to say "Collection stacks
        # submitted" even under --dry_run, where no create_stack call is ever
        # made - a false claim of action in the mode whose purpose is to make
        # none (same class as the EKS warning's "would be deployed" fix).
        session = MagicMock()
        session.client.return_value = MagicMock()
        with patch("builtins.print") as mock_print:
            boto_common.deploy_all_collection_stacks(
                ["us-east-1"], session, "abc123", self._account_information(),
                ("111111111111", "acct"), dry_run=True)
        printed = " ".join(str(call) for call in mock_print.call_args_list)
        self.assertIn("would be submitted", printed)
        self.assertNotIn("stacks submitted for regions", printed)

    def test_deploy_eks_audit_logs_stacks_dry_run(self):
        session = MagicMock()
        cf_client = MagicMock()
        lambda_client = MagicMock()
        not_found = type("ResourceNotFoundException", (Exception,), {})
        lambda_client.exceptions.ResourceNotFoundException = not_found
        lambda_client.get_function.side_effect = not_found()
        session.client.side_effect = lambda service, **kw: (
            lambda_client if service == "lambda" else cf_client)

        records = boto_common.deploy_eks_audit_logs_stacks(
            "https://example.streamsec.io", self._account_information(), session,
            ("111111111111", "acct"), ["us-east-1"], "abc123", None, wait=False, dry_run=True)

        cf_client.create_stack.assert_not_called()
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["final_status"], "DRY_RUN")


class TestUpdateRegionsDryRun(unittest.TestCase):
    def test_dry_run_skips_polling_and_backend_mutation(self):
        graph_client = MagicMock()
        result = oi.update_regions(
            graph_client, ("111111111111", "acct"), ["us-east-1", "us-west-2"], dry_run=True)
        self.assertTrue(result)
        graph_client.get_accounts.assert_not_called()
        graph_client.edit_regions.assert_not_called()
        graph_client.wait_for_account_connection.assert_not_called()


class TestSweepSkipsDryRunRecords(unittest.TestCase):
    def test_dry_run_records_never_trigger_a_describe_stacks_call(self):
        sts_client = MagicMock()
        records = [{
            "account": "111111111111", "name": "acct", "region": "us-east-1",
            "stack_type": "init", "stack_name": "LightlyticsStack-abc123", "stack_id": None,
            "final_status": "DRY_RUN", "status_reason": None,
        }]
        with patch.object(boto_common, "boto3") as mock_boto3:
            swept = boto_common.sweep_stack_statuses(records, sts_client, "111111111111")
        mock_boto3.Session.assert_not_called()
        sts_client.assume_role.assert_not_called()
        self.assertEqual(swept, records)


class TestIntegrateSubAccountDryRunEndToEnd(unittest.TestCase):
    def test_brand_new_account_dry_run_creates_zero_stacks_but_registers_account(self):
        sts_client = MagicMock()
        graph_client = MagicMock()
        # First get_accounts() call (existence check) -> IndexError so
        # ll_integrated stays False and create_account() runs (for real -
        # that's the one real backend side effect dry_run still performs);
        # second call (fetching account_information) returns what a real
        # create_account response would look like.
        graph_client.get_accounts.side_effect = [
            [],
            [{"cloud_account_id": "111111111111", "cloud_regions": ["us-east-1"],
             "lightlytics_collection_token": "tok", "external_id": "ext",
             "template_url": "https://example.com/t.yaml",
             "collection_template_url": "https://example.com/c.yaml"}],
        ]
        graph_client.create_account.return_value = True

        cf_client = MagicMock()
        # eks_audit_logs_auto_detect=True below must reach real EKS
        # discovery too - a distinct 'eks' client with an actual cluster,
        # not the same undifferentiated mock as every other service (a
        # bare MagicMock's list_clusters()['clusters'] has __len__==0 by
        # default, which would silently make deploy_eks_audit_logs_stacks
        # report "no active EKS regions found" and this test would pass
        # without ever exercising its dry_run wiring at all).
        eks_client = MagicMock()
        eks_client.list_clusters.return_value = {"clusters": ["fake-cluster"]}
        lambda_client = MagicMock()
        not_found = type("ResourceNotFoundException", (Exception,), {})
        lambda_client.exceptions.ResourceNotFoundException = not_found
        lambda_client.get_function.side_effect = not_found()

        session = MagicMock()
        session.region_name = "us-east-1"
        session.client.side_effect = lambda service, **kw: (
            {"cloudformation": cf_client, "eks": eks_client, "lambda": lambda_client}[service])

        with patch.object(oi, "boto3") as oi_boto3, \
                patch.object(oi, "get_active_regions", return_value=["us-east-1", "us-west-2"]):
            oi_boto3.Session.return_value = session

            deployed_stacks = oi.integrate_sub_account(
                "https://example.streamsec.io", ("111111111111", "acct"), sts_client, graph_client,
                ["us-east-1", "us-west-2"], "abc123", None, None, "OrganizationAccountAccessRole",
                "111111111111",  # org_account_id == sub_account -> no assume_role needed
                parallel=False, response=True, eks_audit_logs_auto_detect=True, dry_run=True,
            )

        # The one real side effect: account creation in StreamSecurity.
        graph_client.create_account.assert_called_once()
        # Never a real region update (would hang waiting for a real init
        # stack that dry_run never actually created).
        graph_client.edit_regions.assert_not_called()
        graph_client.wait_for_account_connection.assert_not_called()
        # Zero AWS cost: no CloudFormation stack ever actually submitted -
        # init + response + 2 collection stacks + 2 eks_audit stacks
        # (us-east-1, us-west-2 - the eks client above reports a cluster in
        # every region queried), all previewed only.
        cf_client.create_stack.assert_not_called()

        self.assertTrue(deployed_stacks, "dry run should still return preview records")
        self.assertTrue(all(r["final_status"] == "DRY_RUN" for r in deployed_stacks))
        stack_types = sorted(r["stack_type"] for r in deployed_stacks)
        self.assertEqual(
            stack_types, ["collection", "collection", "eks_audit", "eks_audit", "init", "response"])


class TestMainDryRunSummary(unittest.TestCase):
    """No test anywhere else in the suite runs main() itself in dry_run
    mode - _classify_stack_status("DRY_RUN") and the "N dry-run (not
    actually created)" summary line/color in main() are only reachable
    through the full CLI flow, not through any lower-level unit test."""

    def test_dry_run_summary_counts_and_color(self):
        import io
        import contextlib

        org_client = MagicMock()
        org_client.list_accounts.return_value = {
            "Accounts": [{"Id": "111111111111", "Name": "acct", "Status": "ACTIVE"}]}
        sts_client = MagicMock()
        sts_client.get_caller_identity.return_value = {"Account": "999999999999"}
        ec2_client = MagicMock()
        ec2_client.describe_regions.return_value = {"Regions": [{"RegionName": "us-east-1"}]}

        def boto3_client_dispatch(service, region_name=None, **kwargs):
            return {"organizations": org_client, "sts": sts_client, "ec2": ec2_client}[service]

        dry_run_record = {"account": "111111111111", "name": "acct", "region": "us-east-1",
                          "stack_type": "init", "stack_name": "LightlyticsStack-abc",
                          "stack_id": None, "final_status": "DRY_RUN", "status_reason": None}

        color_calls = []
        real_color = boto_common.color

        def spy_color(text, c):
            color_calls.append((text, c))
            return real_color(text, c)

        with patch.object(oi, "boto3") as mock_boto3, \
                patch.object(oi, "GraphCommon", return_value=MagicMock()), \
                patch.object(oi, "integrate_sub_account", return_value=[dry_run_record]), \
                patch.object(oi, "color", side_effect=spy_color), \
                patch("builtins.input", return_value="yes"):
            mock_boto3.client.side_effect = boto3_client_dispatch
            stdout_buf = io.StringIO()
            with contextlib.redirect_stdout(stdout_buf):
                oi.main(
                    environment_url="https://example.streamsec.io",
                    ll_username=None, ll_password=None, aws_profile_name=None,
                    accounts="111111111111", parallel=None,
                    ws_id="ws-1", api_token="fake-token", dry_run=True,
                )

        output = stdout_buf.getvalue()
        self.assertIn("1 dry-run (not actually created)", output)
        self.assertIn("0 succeeded, 0 failed, 0 timed out, 0 errored", output)

        summary_calls = [c for c in color_calls if "dry-run (not actually created)" in c[0]]
        self.assertEqual(len(summary_calls), 1)
        self.assertEqual(summary_calls[0][1], "cyan",
                         "a pure dry-run summary (no real successes/failures) must print cyan, "
                         "not fall through to red/green")


if __name__ == "__main__":
    unittest.main()
