"""
Coverage for main()'s --parallel (ThreadPoolExecutor) code path, which had
zero test coverage - every other test in this suite calls with
parallel=None/False, exercising only the sequential branch. integrate_sub_account
itself is mocked directly (not the deep AWS/GraphQL layer under it) to
isolate main()'s own aggregation/failure-recovery logic around the executor.
"""
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.python.common import boto_common
from src.python.utilities import organization_integration as oi


class TestMainParallelPath(unittest.TestCase):
    def setUp(self):
        # Populated by each test's fake integrate_sub_account side_effect
        # (both normal returns and records attached to a raised exception)
        # so the fake sweep session below knows what stack_ids to report
        # CREATE_COMPLETE for - real describe_stacks() takes no StackId
        # filter, it returns everything in the account/region and the
        # caller matches locally, so the mock needs the full real set.
        self._all_fake_records = []

    def _run_main(self, accounts, side_effect):
        org_client = MagicMock()
        org_client.list_accounts.return_value = {
            "Accounts": [{"Id": acc_id, "Name": f"acct-{acc_id}", "Status": "ACTIVE"}
                        for acc_id in accounts]}
        sts_client = MagicMock()
        sts_client.get_caller_identity.return_value = {"Account": "999999999999"}
        ec2_client = MagicMock()
        ec2_client.describe_regions.return_value = {"Regions": [{"RegionName": "us-east-1"}]}

        def boto3_client_dispatch(service, region_name=None, **kwargs):
            if service == "organizations":
                return org_client
            if service == "sts":
                return sts_client
            if service == "ec2":
                return ec2_client
            raise AssertionError(f"unexpected bare boto3.client call: {service}")

        # sweep_stack_statuses (called at the end of main(), real - not
        # mocked, since it's exactly the aggregation this test is proving
        # works in the --parallel path) uses boto_common's OWN boto3
        # reference, not oi's - it must be patched separately or the sweep
        # phase would make real AWS describe_stacks calls against the fake
        # stack_ids these tests fabricate.
        sweep_cf_client = MagicMock()

        def sweep_describe_stacks(**kwargs):
            return {"Stacks": [{"StackId": r["stack_id"], "StackStatus": "CREATE_COMPLETE"}
                               for r in self._all_fake_records]}
        sweep_cf_client.describe_stacks.side_effect = sweep_describe_stacks
        sweep_session = MagicMock()
        sweep_session.client.return_value = sweep_cf_client

        with patch.object(oi, "boto3") as mock_boto3, \
                patch.object(boto_common, "boto3") as mock_bc_boto3, \
                patch.object(oi, "GraphCommon", return_value=MagicMock()), \
                patch.object(oi, "integrate_sub_account", side_effect=side_effect), \
                patch("builtins.input", return_value="yes"):
            mock_boto3.client.side_effect = boto3_client_dispatch
            mock_bc_boto3.Session.return_value = sweep_session

            oi.main(
                environment_url="https://example.streamsec.io",
                ll_username=None, ll_password=None, aws_profile_name=None,
                accounts=",".join(accounts), parallel=4,
                ws_id="ws-1", api_token="fake-token",
            )

    def test_parallel_aggregates_deployed_stacks_from_every_account(self):
        # Each account's integrate_sub_account return value must end up in
        # the sweep, not just the account that happens to finish first.
        def fake_integrate(environment_url, sub_account, *args, **kwargs):
            record = {"account": sub_account[0], "name": sub_account[1], "region": "us-east-1",
                     "stack_type": "init", "stack_name": f"stack-{sub_account[0]}",
                     "stack_id": f"arn:fake:{sub_account[0]}"}
            self._all_fake_records.append(record)
            return [record]

        captured_swept = []
        real_sweep = oi.sweep_stack_statuses

        def spy_sweep(*a, **kw):
            result = real_sweep(*a, **kw)
            captured_swept.extend(result)
            return result

        with patch.object(oi, "sweep_stack_statuses", side_effect=spy_sweep):
            self._run_main(["111111111111", "222222222222", "333333333333"], fake_integrate)

        swept_accounts = {r["account"] for r in captured_swept}
        self.assertEqual(swept_accounts, {"111111111111", "222222222222", "333333333333"},
                         "every parallel account's deployed stacks must reach the sweep, "
                         "not just whichever future completed first")

    def test_parallel_preserves_partial_stacks_from_a_failing_account(self):
        # A future that raises must not lose stacks already created before
        # the failure (attached to the exception as .deployed_stacks) - the
        # same recovery the sequential path already has, but exercised here
        # through concurrent.futures.as_completed instead.
        partial_record = {"account": "222222222222", "name": "acct-222222222222",
                          "region": "us-east-1", "stack_type": "init",
                          "stack_name": "stack-222", "stack_id": "arn:fake:222",
                          "final_status": "CREATE_COMPLETE"}

        def fake_integrate(environment_url, sub_account, *args, **kwargs):
            if sub_account[0] == "111111111111":
                record = {"account": "111111111111", "name": "acct-111111111111",
                         "region": "us-east-1", "stack_type": "init",
                         "stack_name": "stack-111", "stack_id": "arn:fake:111"}
                self._all_fake_records.append(record)
                return [record]
            wrapped = Exception(f"Account: {sub_account[0]} | Something went wrong: boom")
            wrapped.deployed_stacks = [partial_record]
            raise wrapped

        captured_swept = []
        real_sweep = oi.sweep_stack_statuses

        def spy_sweep(*a, **kw):
            result = real_sweep(*a, **kw)
            captured_swept.extend(result)
            return result

        with patch.object(oi, "sweep_stack_statuses", side_effect=spy_sweep):
            self._run_main(["111111111111", "222222222222"], fake_integrate)

        by_account = {r["account"]: r for r in captured_swept}
        self.assertIn("222222222222", by_account,
                      "a failing account's already-created stack must still be swept/reported, "
                      "not silently lost, in the --parallel path")
        # It arrived with final_status already set (pre-set on the record
        # by whatever raised) - sweep_stack_statuses must leave it as-is.
        self.assertEqual(by_account["222222222222"]["final_status"], "CREATE_COMPLETE")
        self.assertIn("111111111111", by_account)


if __name__ == "__main__":
    unittest.main()
