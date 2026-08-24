"""
Regression test for a bug in EKS audit-log region detection: for a brand-new
account, deploy_eks_audit_logs_stacks was called with account_information
whose "cloud_regions" is still the backend's stale value from account
creation (at most the caller's own session region), instead of the
already-computed active_regions (real EC2-instance detection across all
candidate regions). A brand-new account with EKS clusters only in a
secondary region would silently never get an audit-log stack deployed
there.
"""
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.python.utilities import organization_integration as oi


class TestEksAuditLogsActiveRegions(unittest.TestCase):
    def _run_brand_new_account(self, active_regions, backend_cloud_regions):
        sts_client = MagicMock()
        graph_client = MagicMock()
        # First get_accounts() call (existence check) -> IndexError, so
        # ll_integrated stays False and create_account() runs; second call
        # (fetching account_information) returns the backend's own stale
        # cloud_regions, mirroring what a real GraphQL create_account
        # response looks like right after creation.
        graph_client.get_accounts.side_effect = [
            [],
            [{"cloud_account_id": "111111111111", "cloud_regions": backend_cloud_regions,
             "lightlytics_collection_token": "tok", "template_url": "https://example.com/t.yaml"}],
        ]
        graph_client.create_account.return_value = True

        session = MagicMock()
        session.region_name = "us-east-1"

        with patch.object(oi, "boto3") as oi_boto3, \
                patch.object(oi, "deploy_init_stack", return_value=True) as mock_init, \
                patch.object(oi, "get_active_regions", return_value=active_regions), \
                patch.object(oi, "deploy_eks_audit_logs_stacks") as mock_eks, \
                patch.object(oi, "update_regions", return_value=True), \
                patch.object(oi, "deploy_all_collection_stacks", return_value=None):
            oi_boto3.Session.return_value = session

            oi.integrate_sub_account(
                "https://example.streamsec.io", ("111111111111", "acct"), sts_client, graph_client,
                ["us-east-1", "us-west-2"], 12345678, None, None, "OrganizationAccountAccessRole",
                "111111111111",  # org_account_id == sub_account -> no assume_role needed
                parallel=False, response=False, eks_audit_logs=True, eks_audit_logs_regions=None,
            )

        return mock_eks

    def test_eks_detection_uses_active_regions_not_stale_backend_regions(self):
        mock_eks = self._run_brand_new_account(
            active_regions=["us-east-1", "us-west-2"],
            backend_cloud_regions=["us-east-1"],
        )
        mock_eks.assert_called_once()
        call_args = mock_eks.call_args
        passed_account_information = call_args[0][1]
        self.assertEqual(
            sorted(passed_account_information["cloud_regions"]), ["us-east-1", "us-west-2"],
            "EKS detection must scan active_regions (real EC2-instance detection), "
            "not the backend's stale cloud_regions from account creation")

    def test_eks_detection_still_works_when_backend_regions_already_match(self):
        mock_eks = self._run_brand_new_account(
            active_regions=["us-east-1"],
            backend_cloud_regions=["us-east-1"],
        )
        mock_eks.assert_called_once()
        passed_account_information = mock_eks.call_args[0][1]
        self.assertEqual(passed_account_information["cloud_regions"], ["us-east-1"])

    def test_other_account_information_fields_are_preserved(self):
        mock_eks = self._run_brand_new_account(
            active_regions=["us-east-1", "us-west-2"],
            backend_cloud_regions=["us-east-1"],
        )
        passed_account_information = mock_eks.call_args[0][1]
        self.assertEqual(passed_account_information["lightlytics_collection_token"], "tok")
        self.assertEqual(passed_account_information["cloud_account_id"], "111111111111")


if __name__ == "__main__":
    unittest.main()
