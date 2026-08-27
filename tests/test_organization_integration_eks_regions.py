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
                               eks_audit_logs_auto_detect=False, org_regions=None,
                               eks_records_return=None):
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
            if eks_records_return is not None:
                mock_eks.return_value = eks_records_return

            oi.integrate_sub_account(
                "https://example.streamsec.io", ("111111111111", "acct"), sts_client, graph_client,
                org_regions or ["us-east-1", "us-west-2"], 12345678, None, None,
                "OrganizationAccountAccessRole",
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

    def test_auto_detect_flag_alone_triggers_deploy_with_every_org_enabled_region(self):
        # --eks_audit_logs_auto_detect on its own (no --eks_audit_logs) must
        # still trigger detection+deploy, and must scan EVERY region enabled
        # for the org (the `regions` parameter), not just active_regions
        # (EC2-instance-based) - active_regions alone would miss a
        # Fargate-only EKS cluster in a region with no EC2 instances at all.
        # get_active_eks_regions does its own real, authoritative
        # list_clusters() check per region, so scanning everything is safe.
        mock_eks = self._run_brand_new_account(
            active_regions=["us-east-1", "us-west-2"],
            backend_cloud_regions=["us-east-1"],
            eks_audit_logs=False, eks_audit_logs_regions=None,
            eks_audit_logs_auto_detect=True,
            org_regions=["us-east-1", "us-west-2", "eu-west-1"],
        )
        mock_eks.assert_called_once()
        passed_account_information = mock_eks.call_args[0][1]
        self.assertEqual(
            sorted(passed_account_information["cloud_regions"]),
            ["eu-west-1", "us-east-1", "us-west-2"],
            "auto-detect must scan every org-enabled region, including one not "
            "reported active by get_active_regions")
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
                           eks_audit_logs_auto_detect=False, org_regions=None,
                           eks_records_return=None):
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
            if eks_records_return is not None:
                mock_eks.return_value = eks_records_return

            oi.integrate_sub_account(
                "https://example.streamsec.io", ("111111111111", "acct"), sts_client, graph_client,
                org_regions or ["us-east-1", "us-west-2"], 12345678, None, None,
                "OrganizationAccountAccessRole",
                "111111111111",  # org_account_id == sub_account -> no assume_role needed
                parallel=False, response=False, eks_audit_logs=eks_audit_logs,
                eks_audit_logs_regions=eks_audit_logs_regions,
                eks_audit_logs_auto_detect=eks_audit_logs_auto_detect,
            )

        return mock_eks

    def test_ready_account_auto_detect_scans_every_org_enabled_region(self):
        # --eks_audit_logs_auto_detect must scan EVERY region enabled for
        # the org (the `regions` parameter this function already receives),
        # not just get_active_regions()'s EC2-instance-based subset or the
        # account's currently-registered cloud_regions - either alone would
        # miss a Fargate-only EKS cluster (no EC2 instances at all) in a
        # region the account isn't otherwise active or registered in.
        # get_active_eks_regions does its own real, authoritative
        # list_clusters() check per region, so scanning everything is safe
        # and cheap, not just "broader for its own sake".
        mock_eks = self._run_ready_account(
            potential_regions=["us-east-1", "us-west-2"],
            registered_cloud_regions=["us-east-1"],
            eks_audit_logs=False, eks_audit_logs_regions=None,
            eks_audit_logs_auto_detect=True,
            org_regions=["us-east-1", "us-west-2", "eu-west-1"],
        )
        mock_eks.assert_called_once()
        passed_account_information = mock_eks.call_args[0][1]
        self.assertEqual(
            sorted(passed_account_information["cloud_regions"]),
            ["eu-west-1", "us-east-1", "us-west-2"],
            "auto-detect must scan every org-enabled region, including one neither "
            "active (no EC2 instances) nor registered")
        self.assertIsNone(mock_eks.call_args[0][4])

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


def _eks_record(region, final_status=None):
    record = {"account": "111111111111", "name": "acct", "region": region,
              "stack_type": "eks_audit", "stack_name": f"StreamSecurity-eks-audit-logs-{region}-abc",
              "stack_id": None if final_status else f"arn:aws:cloudformation:{region}:111111111111:stack/x/y"}
    if final_status:
        record["final_status"] = final_status
        record["status_reason"] = None
    return record


class TestUnregisteredEksRegionWarning(unittest.TestCase):
    """The backend ingests EKS audit events from any region (collection is
    token-authenticated, not region-gated), but a cluster in an unregistered
    region never enters inventory, so its events are stored unlinked. The
    operator must be told at deploy time - a silent capability gap is the
    exact failure mode this PR exists to eliminate."""

    def _printed(self, mock_print):
        return " ".join(str(call) for call in mock_print.call_args_list)

    def test_warns_for_deployed_region_not_in_registered_set(self):
        with patch("builtins.print") as mock_print:
            oi._warn_unregistered_eks_regions(
                ("111111111111", "acct"),
                [_eks_record("eu-west-1"), _eks_record("us-east-1")],
                ["us-east-1", "us-west-2"])
        printed = self._printed(mock_print)
        self.assertIn("eu-west-1", printed)
        self.assertIn("--regions", printed)
        self.assertIn("not linked to inventory", printed)

    def test_silent_when_every_deployed_region_is_registered(self):
        with patch("builtins.print") as mock_print:
            oi._warn_unregistered_eks_regions(
                ("111111111111", "acct"),
                [_eks_record("us-east-1")],
                ["us-east-1", "us-west-2"])
        mock_print.assert_not_called()

    def test_dry_run_records_warn_with_conditional_verb(self):
        # --dry_run deliberately creates nothing, so the warning must say
        # "would be deployed", not "deployed" - the preview of the
        # enrichment gap is still wanted, the false claim of action is not.
        with patch("builtins.print") as mock_print:
            oi._warn_unregistered_eks_regions(
                ("111111111111", "acct"),
                [_eks_record("eu-west-1", final_status="DRY_RUN")],
                ["us-east-1"])
        printed = self._printed(mock_print)
        self.assertIn("EKS audit collector would be deployed in eu-west-1", printed)
        self.assertNotIn("collector deployed in", printed)

    def test_submit_failed_records_do_not_warn(self):
        # A SUBMIT_FAILED record deployed nothing - its failure is already
        # reported on its own; warning about enrichment for a collector that
        # doesn't exist would be misleading.
        with patch("builtins.print") as mock_print:
            oi._warn_unregistered_eks_regions(
                ("111111111111", "acct"),
                [_eks_record("eu-west-1", final_status="SUBMIT_FAILED")],
                ["us-east-1"])
        mock_print.assert_not_called()

    def test_brand_new_account_warning_wired_against_final_registered_set(self):
        # End-to-end through integrate_sub_account's brand-new branch:
        # auto-detect deploys a collector in eu-west-1, but only
        # us-east-1/us-west-2 (active_regions) are about to be registered by
        # update_regions - the warning must fire for eu-west-1 only.
        harness = TestEksAuditLogsActiveRegions()
        with patch("builtins.print") as mock_print:
            harness._run_brand_new_account(
                active_regions=["us-east-1", "us-west-2"],
                backend_cloud_regions=["us-east-1"],
                eks_audit_logs=False, eks_audit_logs_auto_detect=True,
                org_regions=["us-east-1", "us-west-2", "eu-west-1"],
                eks_records_return=[_eks_record("eu-west-1"), _eks_record("us-east-1")])
        printed = self._printed(mock_print)
        self.assertIn("EKS audit collector deployed in eu-west-1", printed)
        self.assertNotIn("EKS audit collector deployed in us-east-1", printed)

    def test_ready_account_warning_compares_against_union_of_current_and_potential(self):
        # The READY branch registers the UNION of the account's current
        # regions and the freshly-detected potential regions - a collector in
        # us-west-2 (potential but not yet current) must NOT warn, while
        # eu-west-1 (in neither) must.
        harness = TestEksAuditLogsActiveRegions()
        with patch("builtins.print") as mock_print:
            harness._run_ready_account(
                potential_regions=["us-east-1", "us-west-2"],
                registered_cloud_regions=["us-east-1"],
                eks_audit_logs=False, eks_audit_logs_auto_detect=True,
                org_regions=["us-east-1", "us-west-2", "eu-west-1"],
                eks_records_return=[_eks_record("eu-west-1"), _eks_record("us-west-2")])
        printed = self._printed(mock_print)
        self.assertIn("EKS audit collector deployed in eu-west-1", printed)
        self.assertNotIn("EKS audit collector deployed in us-west-2", printed)


if __name__ == "__main__":
    unittest.main()
