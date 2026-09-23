"""
The Lambda (lambda/organization_integration/app.py) must still count an account
as failed when a response or EKS audit-logs stack fails to submit.

Before this PR, create_stack raised straight out of deploy_response_stack /
deploy_eks_audit_logs_stacks and lambda_handler recorded the failure. Those
helpers now return a SUBMIT_FAILED record instead of raising, so the Lambda has
to check the record itself or it reports "Integration finished successfully!".
"""
import importlib.util
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


def _load_lambda_app_module():
    # 'lambda' is a reserved keyword, so app.py cannot be imported normally.
    app_path = os.path.abspath(os.path.join(
        os.path.dirname(__file__), '..', 'lambda', 'organization_integration', 'app.py'))
    spec = importlib.util.spec_from_file_location('organization_integration_lambda_app', app_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _record(stack_type, final_status=None):
    return {"account": "123456789012", "name": "acct-name", "region": "us-east-1",
            "stack_type": stack_type, "stack_name": "s", "stack_id": None if final_status else "sid",
            "final_status": final_status, "status_reason": "AccessDenied" if final_status else None}


class TestRaiseOnSubmitFailure(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = _load_lambda_app_module()

    def test_single_submit_failed_record_raises(self):
        with self.assertRaisesRegex(Exception, "response in us-east-1: AccessDenied"):
            self.app._raise_on_submit_failure(("123456789012", "a"), _record("response", "SUBMIT_FAILED"))

    def test_submit_failed_in_list_raises(self):
        records = [_record("eks_audit"), _record("eks_audit", "SUBMIT_FAILED")]
        with self.assertRaises(Exception):
            self.app._raise_on_submit_failure(("123456789012", "a"), records)

    def test_submitted_records_do_not_raise(self):
        self.app._raise_on_submit_failure(("123456789012", "a"), _record("response"))
        self.app._raise_on_submit_failure(("123456789012", "a"), [_record("eks_audit")])

    def test_empty_results_do_not_raise(self):
        # No EKS regions found -> [], nothing to check.
        self.app._raise_on_submit_failure(("123456789012", "a"), [])
        self.app._raise_on_submit_failure(("123456789012", "a"), None)


class TestLambdaHandlerReportsSubmitFailures(unittest.TestCase):
    """End to end through integrate_sub_account for a brand-new account."""

    @classmethod
    def setUpClass(cls):
        cls.app = _load_lambda_app_module()

    def _run(self, response_result, eks_result):
        app = self.app
        sub_account = ("123456789012", "acct-name")
        graph_client = MagicMock()
        graph_client.get_accounts.side_effect = [[], [{"cloud_account_id": sub_account[0]}]]
        graph_client.create_account.return_value = True
        session = MagicMock()
        session.region_name = "us-east-1"

        with patch.object(app, "boto3") as boto3_mock, \
                patch.object(app, "deploy_init_stack", return_value=(True, _record("init"))), \
                patch.object(app, "get_active_regions", return_value=["us-east-1"]), \
                patch.object(app, "deploy_response_stack", return_value=response_result), \
                patch.object(app, "deploy_eks_audit_logs_stacks", return_value=eks_result), \
                patch.object(app, "update_regions", return_value=True) as update_regions, \
                patch.object(app, "deploy_all_collection_stacks", return_value=[]):
            boto3_mock.Session.return_value = session
            app.integrate_sub_account(
                sub_account, MagicMock(), graph_client, ["us-east-1"], "abc123",
                None, None, "OrganizationAccountAccessRole", sub_account[0],
                response=True, eks_audit_logs=True, environment="env", domain="streamsec.io")
        return update_regions

    def test_response_submit_failure_fails_the_account(self):
        with self.assertRaisesRegex(Exception, "Failed to submit stack"):
            self._run(_record("response", "SUBMIT_FAILED"), [])

    def test_eks_submit_failure_fails_the_account(self):
        with self.assertRaisesRegex(Exception, "Failed to submit stack"):
            self._run(_record("response"), [_record("eks_audit", "SUBMIT_FAILED")])

    def test_all_submitted_continues_to_region_update(self):
        update_regions = self._run(_record("response"), [_record("eks_audit")])
        update_regions.assert_called_once()

    def test_stacks_get_base_api_url_without_graphql(self):
        app = self.app
        with patch.object(app, "deploy_response_stack", return_value=_record("response")) as response, \
                patch.object(app, "deploy_eks_audit_logs_stacks", return_value=[]) as eks:
            sub_account = ("123456789012", "acct-name")
            graph_client = MagicMock()
            graph_client.get_accounts.side_effect = [[], [{"cloud_account_id": sub_account[0]}]]
            graph_client.create_account.return_value = True
            with patch.object(app, "boto3"), \
                    patch.object(app, "deploy_init_stack", return_value=(True, _record("init"))), \
                    patch.object(app, "get_active_regions", return_value=["us-east-1"]), \
                    patch.object(app, "update_regions", return_value=True), \
                    patch.object(app, "deploy_all_collection_stacks", return_value=[]):
                app.integrate_sub_account(
                    sub_account, MagicMock(), graph_client, ["us-east-1"], "abc123",
                    None, None, "OrganizationAccountAccessRole", sub_account[0],
                    response=True, eks_audit_logs=True, environment="acme", domain="streamsec.io")
        self.assertEqual(response.call_args.args[0], "https://acme.streamsec.io")
        self.assertEqual(eks.call_args.args[0], "https://acme.streamsec.io")


if __name__ == "__main__":
    unittest.main()
