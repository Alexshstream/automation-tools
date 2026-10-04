"""
Collection-stack regions for the organization Lambda.

Without REGIONS, a brand-new account resolves its regions through
get_active_regions(), which only counts regions with EC2 instances. A freshly
vended account has none, so it was onboarded in the Lambda's region and
us-east-1 only. app.py has honoured REGIONS since 2024, but org_lambda.py had
no way to set it, and RESPONSE_REGION (which customers assumed controls this)
only places the response stack.
"""
import importlib.util
import io
import os
import sys
import unittest
from contextlib import redirect_stdout
from unittest.mock import MagicMock, patch

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

LAMBDA_DIR = os.path.abspath(os.path.join(
    os.path.dirname(__file__), '..', 'lambda', 'organization_integration'))


def _load(filename, module_name, argv=None):
    # 'lambda' is a reserved keyword, so these cannot be imported normally.
    # org_lambda.py parses sys.argv at import time, so argv is patched here.
    spec = importlib.util.spec_from_file_location(module_name, os.path.join(LAMBDA_DIR, filename))
    module = importlib.util.module_from_spec(spec)
    with patch.object(sys, "argv", [filename] + (argv or [])):
        spec.loader.exec_module(module)
    return module


BASE_ARGV = ["--environment", "acme", "--ws-id", "ws1", "--api-token", "tok"]


class TestOrgLambdaRegionsFlag(unittest.TestCase):
    def test_regions_flag_sets_regions_env_var(self):
        mod = _load("org_lambda.py", "org_lambda_regions", BASE_ARGV + ["--regions", "us-east-1,us-west-2"])
        env_vars = mod._build_env_vars(mod.args)
        self.assertEqual(env_vars["REGIONS"], "us-east-1,us-west-2")

    def test_regions_flag_is_normalized(self):
        # Same stripping app.py does, so what is stored on the Lambda is what runs.
        mod = _load("org_lambda.py", "org_lambda_regions_ws", BASE_ARGV + ["--regions", " us-east-1, eu-west-1 ,"])
        self.assertEqual(mod._build_env_vars(mod.args)["REGIONS"], "us-east-1,eu-west-1")

    def test_no_regions_flag_leaves_regions_unset(self):
        mod = _load("org_lambda.py", "org_lambda_no_regions", BASE_ARGV)
        self.assertNotIn("REGIONS", mod._build_env_vars(mod.args))

    def test_regions_does_not_change_response_region(self):
        mod = _load("org_lambda.py", "org_lambda_resp", BASE_ARGV + ["--regions", "eu-west-1"])
        env_vars = mod._build_env_vars(mod.args)
        self.assertEqual(env_vars["RESPONSE_REGION"], "us-east-1")

    def test_existing_env_vars_unchanged(self):
        mod = _load("org_lambda.py", "org_lambda_existing", BASE_ARGV + [
            "--accounts", "111,222", "--eks-audit-logs", "--eks-audit-logs-regions", "us-east-1"])
        env_vars = mod._build_env_vars(mod.args)
        self.assertEqual(env_vars["ENVIRONMENT"], "acme")
        self.assertEqual(env_vars["WS_ID"], "ws1")
        self.assertEqual(env_vars["API_TOKEN"], "tok")
        self.assertNotIn("ENVIRONMENT_USER_NAME", env_vars)
        self.assertEqual(env_vars["ACCOUNTS"], "111,222")
        self.assertEqual(env_vars["EKS_AUDIT_LOGS"], "true")
        self.assertEqual(env_vars["EKS_AUDIT_LOGS_REGIONS"], "us-east-1")

    def test_help_text_separates_collection_and_response_regions(self):
        mod = _load("org_lambda.py", "org_lambda_help", BASE_ARGV)
        helps = {a.dest: a.help for a in mod.parser._actions}
        self.assertIn("collection", helps["regions"])
        self.assertIn("response stack only", helps["response_region"])

    def test_blank_regions_flag_leaves_regions_unset(self):
        mod = _load("org_lambda.py", "org_lambda_blank", BASE_ARGV + ["--regions", " , "])
        self.assertNotIn("REGIONS", mod._build_env_vars(mod.args))


class TestNewAccountRegions(unittest.TestCase):
    """End to end through integrate_sub_account for a brand-new account."""

    @classmethod
    def setUpClass(cls):
        cls.app = _load("app.py", "organization_integration_lambda_app_regions")

    def _run(self, regions_to_integrate, active_regions=("us-east-1",)):
        app = self.app
        sub_account = ("123456789012", "acct-name")
        graph_client = MagicMock()
        graph_client.get_accounts.side_effect = [[], [{"cloud_account_id": sub_account[0]}]]
        graph_client.create_account.return_value = True
        session = MagicMock()
        session.region_name = "us-east-1"
        out = io.StringIO()
        with patch.object(app, "boto3") as boto3_mock, \
                patch.object(app, "deploy_init_stack", return_value=(True, None)), \
                patch.object(app, "get_active_regions", return_value=list(active_regions)) as get_active, \
                patch.object(app, "update_regions", return_value=True) as update_regions, \
                patch.object(app, "deploy_all_collection_stacks", return_value=[]) as collection, \
                redirect_stdout(out):
            boto3_mock.Session.return_value = session
            app.integrate_sub_account(
                sub_account, MagicMock(), graph_client, ["us-east-1", "us-west-2", "eu-west-1"], "abc123",
                None, regions_to_integrate, "OrganizationAccountAccessRole", sub_account[0],
                environment="env", domain="streamsec.io")
        return get_active, update_regions, collection, out.getvalue()

    def test_explicit_regions_are_used_and_ec2_detection_is_skipped(self):
        get_active, update_regions, collection, _ = self._run(["us-east-1", "us-west-2"])
        get_active.assert_not_called()
        self.assertEqual(update_regions.call_args.args[2], ["us-east-1", "us-west-2"])
        self.assertEqual(collection.call_args.args[0], ["us-east-1", "us-west-2"])

    def test_explicit_regions_do_not_warn(self):
        *_, output = self._run(["us-east-1", "us-west-2"])
        self.assertNotIn("REGIONS", output)

    def test_default_regions_only_warns(self):
        # No EC2 anywhere: only the Lambda's region and us-east-1 come back.
        *_, output = self._run(None, active_regions=["us-east-1"])
        self.assertIn("no EC2 instances found", output)
        self.assertIn("REGIONS", output)
        self.assertIn("--regions", output)

    def test_ec2_detected_regions_do_not_warn(self):
        *_, output = self._run(None, active_regions=["us-east-1", "us-west-2"])
        self.assertNotIn("no EC2 instances found", output)


if __name__ == '__main__':
    unittest.main()
