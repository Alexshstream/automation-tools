"""
Regression test for a bug in EKS audit-log region detection: for a brand-new
account, deploy_eks_audit_logs_stacks was called with account_information
whose "cloud_regions" is still the backend's stale value from account
creation (at most the caller's own session region), instead of the
already-computed active_regions (real EC2-instance detection across all
candidate regions). A brand-new account with EKS clusters only in a
secondary region would silently never get an audit-log stack deployed
there. Also covers --eks_audit_logs_auto_detect, a dedicated flag that
always runs detection (independent of --eks_audit_logs/--eks_audit_logs_regions),
including the same staleness bug for already-integrated (READY) accounts.
"""
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.python.utilities import organization_integration as oi


class TestEksAuditLogsActiveRegions(unittest.TestCase):
    def _run_brand_new_account(self, active_regions, backend_cloud_regions,
                               eks_audit_logs=True, eks_audit_logs_regions=None,
                               eks_audit_logs_auto_detect=False):
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
                patch.object(oi, "deploy_init_stack", return_value=(True, None)), \
                patch.object(oi, "get_active_regions", return_value=active_regions), \
                patch.object(oi, "deploy_eks_audit_logs_stacks") as mock_eks, \
                patch.object(oi, "update_regions", return_value=True), \
                patch.object(oi, "deploy_all_collection_stacks", return_value=None):
            oi_boto3.Session.return_value = session

            oi.integrate_sub_account(
                "https://example.streamsec.io", ("111111111111", "acct"), sts_client, graph_client,
                ["us-east-1", "us-west-2"], 12345678, None, None, "OrganizationAccountAccessRole",
                "111111111111",  # org_account_id == sub_account -> no assume_role needed
                parallel=False, response=False, eks_audit_logs=eks_audit_logs,
                eks_audit_logs_regions=eks_audit_logs_regions,
                eks_audit_logs_auto_detect=eks_audit_logs_auto_detect,
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

    def test_auto_detect_flag_alone_triggers_deploy_with_active_regions(self):
        # --eks_audit_logs_auto_detect on its own (no --eks_audit_logs) must
        # still trigger detection+deploy, scanning active_regions.
        mock_eks = self._run_brand_new_account(
            active_regions=["us-east-1", "us-west-2"],
            backend_cloud_regions=["us-east-1"],
            eks_audit_logs=False, eks_audit_logs_regions=None,
            eks_audit_logs_auto_detect=True,
        )
        mock_eks.assert_called_once()
        passed_account_information = mock_eks.call_args[0][1]
        self.assertEqual(
            sorted(passed_account_information["cloud_regions"]), ["us-east-1", "us-west-2"])
        # regions_arg (5th positional) must be None so
        # deploy_eks_audit_logs_stacks runs its own auto-detection.
        self.assertIsNone(mock_eks.call_args[0][4])

    def test_auto_detect_flag_overrides_explicit_regions(self):
        # If both --eks_audit_logs_regions and --eks_audit_logs_auto_detect
        # are passed, auto-detect wins - the explicit region list is ignored
        # in favor of real detection.
        mock_eks = self._run_brand_new_account(
            active_regions=["us-east-1", "us-west-2"],
            backend_cloud_regions=["us-east-1"],
            eks_audit_logs=True, eks_audit_logs_regions=["eu-west-1"],
            eks_audit_logs_auto_detect=True,
        )
        mock_eks.assert_called_once()
        self.assertIsNone(mock_eks.call_args[0][4])

    def test_neither_flag_skips_eks_entirely(self):
        mock_eks = self._run_brand_new_account(
            active_regions=["us-east-1", "us-west-2"],
            backend_cloud_regions=["us-east-1"],
            eks_audit_logs=False, eks_audit_logs_regions=None,
            eks_audit_logs_auto_detect=False,
        )
        mock_eks.assert_not_called()

    def test_explicit_eks_audit_logs_regions_still_respected_without_auto_detect(self):
        # Existing behavior preserved: --eks_audit_logs_regions still passes
        # through untouched when --eks_audit_logs_auto_detect isn't set.
        mock_eks = self._run_brand_new_account(
            active_regions=["us-east-1", "us-west-2"],
            backend_cloud_regions=["us-east-1"],
            eks_audit_logs=True, eks_audit_logs_regions=["eu-west-1"],
            eks_audit_logs_auto_detect=False,
        )
        mock_eks.assert_called_once()
        self.assertEqual(mock_eks.call_args[0][4], ["eu-west-1"])

    def _run_ready_account(self, potential_regions, registered_cloud_regions,
                           eks_audit_logs=True, eks_audit_logs_regions=None,
                           eks_audit_logs_auto_detect=False):
        sts_client = MagicMock()
        graph_client = MagicMock()
        graph_client.get_accounts.return_value = [
            {"cloud_account_id": "111111111111", "status": "READY",
             "cloud_regions": registered_cloud_regions,
             "realtime_regions": [{"region_name": r} for r in registered_cloud_regions],
             "lightlytics_collection_token": "tok"},
        ]
        graph_client.get_account_response_config.return_value = {"remediation": {"status": "done"}}

        session = MagicMock()
        session.region_name = "us-east-1"

        with patch.object(oi, "boto3") as oi_boto3, \
                patch.object(oi, "get_active_regions", return_value=potential_regions), \
                patch.object(oi, "deploy_eks_audit_logs_stacks") as mock_eks, \
                patch.object(oi, "update_regions", return_value=True), \
                patch.object(oi, "deploy_all_collection_stacks", return_value=None):
            oi_boto3.Session.return_value = session

            oi.integrate_sub_account(
                "https://example.streamsec.io", ("111111111111", "acct"), sts_client, graph_client,
                ["us-east-1", "us-west-2"], 12345678, None, None, "OrganizationAccountAccessRole",
                "111111111111",  # org_account_id == sub_account -> no assume_role needed
                parallel=False, response=False, eks_audit_logs=eks_audit_logs,
                eks_audit_logs_regions=eks_audit_logs_regions,
                eks_audit_logs_auto_detect=eks_audit_logs_auto_detect,
            )

        return mock_eks

    def test_ready_account_auto_detect_uses_potential_regions_not_stale_registered_regions(self):
        # Already-READY account whose real active AWS regions (potential_regions,
        # via get_active_regions) have grown since last onboarding -
        # --eks_audit_logs_auto_detect must scan the fresh set too, not just
        # the account's currently-registered cloud_regions.
        mock_eks = self._run_ready_account(
            potential_regions=["us-east-1", "us-west-2"],
            registered_cloud_regions=["us-east-1"],
            eks_audit_logs=False, eks_audit_logs_regions=None,
            eks_audit_logs_auto_detect=True,
        )
        mock_eks.assert_called_once()
        passed_account_information = mock_eks.call_args[0][1]
        self.assertEqual(
            sorted(passed_account_information["cloud_regions"]), ["us-east-1", "us-west-2"],
            "auto-detect must scan potential_regions in addition to registered cloud_regions")
        self.assertIsNone(mock_eks.call_args[0][4])

    def test_ready_account_auto_detect_still_scans_a_registered_region_with_no_running_instances(self):
        # A registered region can host a real EKS cluster (e.g. Fargate-only,
        # or simply no EC2 instances running right now) even though
        # get_active_regions() - which only looks for running EC2 instances -
        # would never report it as "active". potential_regions alone would
        # silently drop this region from the EKS scan; the union with
        # current_regions (the account's real registered regions) must not.
        mock_eks = self._run_ready_account(
            potential_regions=["us-east-1", "us-west-2"],
            registered_cloud_regions=["us-east-1", "eu-west-1"],
            eks_audit_logs=False, eks_audit_logs_regions=None,
            eks_audit_logs_auto_detect=True,
        )
        mock_eks.assert_called_once()
        passed_account_information = mock_eks.call_args[0][1]
        self.assertEqual(
            sorted(passed_account_information["cloud_regions"]),
            ["eu-west-1", "us-east-1", "us-west-2"],
            "a registered region with no currently-running EC2 instances (e.g. "
            "Fargate-only EKS) must still be scanned, not silently dropped")

    def test_ready_account_plain_eks_audit_logs_keeps_existing_behavior(self):
        # Plain --eks_audit_logs (no auto-detect) must keep scanning the
        # account's registered cloud_regions exactly as before - no behavior
        # change for anyone already relying on it.
        mock_eks = self._run_ready_account(
            potential_regions=["us-east-1", "us-west-2"],
            registered_cloud_regions=["us-east-1"],
            eks_audit_logs=True, eks_audit_logs_regions=None,
            eks_audit_logs_auto_detect=False,
        )
        mock_eks.assert_called_once()
        passed_account_information = mock_eks.call_args[0][1]
        self.assertEqual(passed_account_information["cloud_regions"], ["us-east-1"])


if __name__ == "__main__":
    unittest.main()
