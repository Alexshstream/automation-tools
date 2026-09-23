"""
Two bugs found by running the script against a real READY account with a
bare host (--environment_url stpg.stage.lightops.io):

1. main() passed the raw --environment_url to integrate_sub_account, and the EKS
   helper parses the collector prefix with environment_url.split("//")[1], which
   raised IndexError for a host without a scheme. The fix passes https://<host>
   with no /graphql: a live response stack failed its acknowledge call with the
   /graphql form, and every console-deployed response/EKS stack uses the base URL.
2. That IndexError was swallowed by an `except IndexError: pass` meant only for
   "account not found in StreamSecurity", so an existing READY account fell
   through to create_account.
"""
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.python.utilities import organization_integration as oi


class TestUnrelatedIndexErrorDoesNotCreateAccount(unittest.TestCase):
    def test_ready_account_error_fails_the_account_without_create_account(self):
        graph_client = MagicMock()
        graph_client.get_accounts.return_value = [{
            "cloud_account_id": "111111111111", "status": "READY",
            "cloud_regions": ["us-east-1"], "realtime_regions": [{"region_name": "us-east-1"}]}]
        graph_client.get_account_response_config.return_value = {"remediation": None}

        with patch.object(oi, "boto3"), \
                patch.object(oi, "get_active_regions", return_value=["us-east-1"]), \
                patch.object(oi, "deploy_eks_audit_logs_stacks", side_effect=IndexError("list index out of range")):
            with self.assertRaisesRegex(Exception, "list index out of range"):
                oi.integrate_sub_account(
                    "https://example.streamsec.io/graphql", ("111111111111", "acct"), MagicMock(),
                    graph_client, ["us-east-1"], "abc123", None, None,
                    "OrganizationAccountAccessRole", "111111111111", eks_audit_logs_auto_detect=True)

        graph_client.create_account.assert_not_called()

    def test_account_missing_from_stream_is_still_created(self):
        graph_client = MagicMock()
        graph_client.get_accounts.side_effect = [
            [],
            [{"cloud_account_id": "111111111111", "cloud_regions": ["us-east-1"],
              "template_url": "https://example.com/t.yaml"}]]
        graph_client.create_account.return_value = True

        with patch.object(oi, "boto3"), \
                patch.object(oi, "deploy_init_stack", return_value=(False, None)):
            with self.assertRaisesRegex(Exception, "init stack deployment"):
                oi.integrate_sub_account(
                    "https://example.streamsec.io/graphql", ("111111111111", "acct"), MagicMock(),
                    graph_client, ["us-east-1"], "abc123", None, None,
                    "OrganizationAccountAccessRole", "111111111111")

        graph_client.create_account.assert_called_once()


class TestMainPassesNormalizedApiUrl(unittest.TestCase):
    def _url_passed_for(self, environment_url):
        org_client = MagicMock()
        org_client.list_accounts.return_value = {
            "Accounts": [{"Id": "111111111111", "Name": "acct", "Status": "ACTIVE"}]}
        sts_client = MagicMock()
        sts_client.get_caller_identity.return_value = {"Account": "999999999999"}
        ec2_client = MagicMock()
        ec2_client.describe_regions.return_value = {"Regions": [{"RegionName": "us-east-1"}]}

        integrate = MagicMock(return_value=[])
        with patch.object(oi, "boto3") as mock_boto3, \
                patch.object(oi, "GraphCommon", return_value=MagicMock()), \
                patch.object(oi, "integrate_sub_account", integrate), \
                patch("builtins.input", return_value="yes"), \
                patch("builtins.print"):
            mock_boto3.client.side_effect = lambda service, **kw: {
                "organizations": org_client, "sts": sts_client, "ec2": ec2_client}[service]
            oi.main(environment_url, None, None, None, "111111111111", None,
                    ws_id="ws-1", api_token="fake-token")
        return integrate.call_args.args[0]

    def test_bare_host(self):
        self.assertEqual(self._url_passed_for("stpg.stage.lightops.io"),
                         "https://stpg.stage.lightops.io")

    def test_full_url(self):
        self.assertEqual(self._url_passed_for("https://acme.streamsec.io"),
                         "https://acme.streamsec.io")


if __name__ == "__main__":
    unittest.main()
