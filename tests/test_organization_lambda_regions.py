"""
Collection-stack regions for the organization Lambda.

Without REGIONS, a brand-new account resolves its regions through
get_active_regions(), which only counts regions with EC2 instances. A freshly
vended account has none, so it was onboarded in the Lambda's region and
us-east-1 only. app.py honours REGIONS, but org_lambda.py had no way to set
it, and RESPONSE_REGION (which customers assumed controls this) only places
the response stack.
"""
import importlib.util
import io
import os
import sys
import unittest
from contextlib import redirect_stdout
from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError, EndpointConnectionError

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


def _ec2_with_regions(regions):
    """ec2 client whose describe_regions returns the given {name: OptInStatus}."""
    ec2 = MagicMock()
    ec2.describe_regions.return_value = {
        "Regions": [{"RegionName": name, "OptInStatus": status} for name, status in regions.items()]}
    return ec2


class TestOrgLambdaRegionsFlag(unittest.TestCase):
    def test_regions_flag_sets_regions_env_var(self):
        mod = _load("org_lambda.py", "org_lambda_regions", BASE_ARGV + ["--regions", "us-east-1,us-west-2"])
        env_vars = mod._build_env_vars(mod.args)
        self.assertEqual(env_vars["REGIONS"], "us-east-1,us-west-2")

    def test_regions_flag_is_normalized(self):
        # Same stripping app.py does, so what is stored on the Lambda is what runs.
        mod = _load("org_lambda.py", "org_lambda_regions_ws", BASE_ARGV + ["--regions", " us-east-1, eu-west-1 ,"])
        self.assertEqual(mod._build_env_vars(mod.args)["REGIONS"], "us-east-1,eu-west-1")

    def test_repeated_regions_are_stored_once(self):
        mod = _load("org_lambda.py", "org_lambda_dupes", BASE_ARGV + ["--regions", "us-west-2,us-east-1,us-west-2"])
        self.assertEqual(mod._build_env_vars(mod.args)["REGIONS"], "us-west-2,us-east-1")

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


class TestOrgLambdaRegionsValidation(unittest.TestCase):
    """REGIONS applies to every account, so a bad value must stop the deploy."""

    def _main(self, regions_argv, ec2=None, extra_argv=()):
        mod = _load("org_lambda.py", "org_lambda_validate", BASE_ARGV + regions_argv + list(extra_argv))
        out = io.StringIO()
        session = MagicMock()
        session.region_name = "us-east-1"
        with patch.object(mod, "boto3") as boto3_mock, \
                patch.object(mod, "_aws_clients", return_value=(MagicMock(), MagicMock(), MagicMock(), MagicMock())) \
                as clients, \
                patch("builtins.input", return_value="no") as ask, \
                redirect_stdout(out):
            boto3_mock.Session.return_value = session
            boto3_mock.client.return_value = ec2 or _ec2_with_regions({})
            mod.main()
        return out.getvalue(), clients, ask

    def test_typo_stops_before_anything_is_created(self):
        ec2 = _ec2_with_regions({"us-east-1": "opt-in-not-required", "us-west-2": "opt-in-not-required"})
        output, _, ask = self._main(["--regions", "us-east-1,us-west2"], ec2)
        self.assertIn("unknown region(s) in --regions: us-west2", output)
        ask.assert_not_called()  # never reached the "proceed?" prompt

    def test_valid_regions_continue(self):
        ec2 = _ec2_with_regions({"us-east-1": "opt-in-not-required", "us-west-2": "opt-in-not-required"})
        output, _, ask = self._main(["--regions", "us-east-1,us-west-2"], ec2)
        self.assertNotIn("Error", output)
        ask.assert_called_once()

    def test_not_enabled_region_warns_but_continues(self):
        ec2 = _ec2_with_regions({"us-east-1": "opt-in-not-required", "ap-east-1": "not-opted-in"})
        output, _, ask = self._main(["--regions", "us-east-1,ap-east-1"], ec2)
        self.assertIn("Warning: region(s) not enabled in the management account: ap-east-1", output)
        ask.assert_called_once()

    def test_empty_regions_flag_is_an_error(self):
        # e.g. --regions "$REGIONS" with the variable unset.
        output, clients, _ = self._main(["--regions", " , "])
        self.assertIn("--regions was given but contains no regions", output)
        clients.assert_not_called()

    def test_region_check_failure_warns_and_continues(self):
        ec2 = MagicMock()
        ec2.describe_regions.side_effect = ClientError(
            {"Error": {"Code": "UnauthorizedOperation", "Message": "denied"}}, "DescribeRegions")
        output, _, ask = self._main(["--regions", "us-east-1,us-west-2"], ec2)
        self.assertIn("could not check --regions", output)
        self.assertIn("ec2:DescribeRegions", output)
        ask.assert_called_once()

    def test_negative_schedule_scan_days_stops_before_anything_is_created(self):
        output, clients, _ = self._main([], extra_argv=["--schedule-scan-days", "-1"])
        self.assertIn("--schedule-scan-days must be a positive integer", output)
        clients.assert_not_called()

    def test_management_account_note_is_boxed(self):
        output, _, _ = self._main([])
        self.assertIn("*" * 80 + "\n* NOTE: The Lambda onboards member accounts only.", output)
        box = [l for l in output.splitlines() if l.startswith("*")]
        self.assertTrue(all(len(l) == 80 for l in box), box)

    def test_regions_shown_before_prompt(self):
        ec2 = _ec2_with_regions({"us-east-1": "opt-in-not-required", "us-west-2": "opt-in-not-required"})
        output, _, ask = self._main(["--regions", "us-east-1, us-west-2"], ec2)
        self.assertIn("Collection regions: us-east-1, us-west-2 (from --regions)", output)
        ask.assert_called_once()

    def test_us_east_1_shown_as_included_when_not_listed(self):
        ec2 = _ec2_with_regions({"eu-west-1": "opt-in-not-required"})
        output, _, _ = self._main(["--regions", "eu-west-1"], ec2)
        self.assertIn("Collection regions: eu-west-1 (from --regions), plus us-east-1 (always included)", output)

    def test_auto_detection_shown_before_prompt_when_regions_omitted(self):
        output, _, ask = self._main([])
        self.assertIn("Collection regions: auto-detected per account", output)
        ask.assert_called_once()


class _IntegrateHarness(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = _load("app.py", "organization_integration_lambda_app_regions")

    def _session(self, enabled_regions):
        session = MagicMock()
        session.region_name = "us-east-1"
        if isinstance(enabled_regions, Exception):
            session.client.return_value.describe_regions.side_effect = enabled_regions
        else:
            session.client.return_value = _ec2_with_regions({r: "opt-in-not-required" for r in enabled_regions})
        return session

    def _integrate(self, graph_client, session, regions_to_integrate, active_regions, **patches):
        app = self.app
        sub_account = ("123456789012", "acct-name")
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


class TestNewAccountRegions(_IntegrateHarness):
    """End to end through integrate_sub_account for a brand-new account."""

    def _run(self, regions_to_integrate, active_regions=("us-east-1",),
             enabled=("us-east-1", "us-west-2", "eu-west-1")):
        graph_client = MagicMock()
        graph_client.get_accounts.side_effect = [[], [{"cloud_account_id": "123456789012"}]]
        graph_client.create_account.return_value = True
        return self._integrate(graph_client, self._session(enabled), regions_to_integrate, active_regions)

    def test_explicit_regions_are_used_and_ec2_detection_is_skipped(self):
        get_active, update_regions, collection, _ = self._run(["us-east-1", "us-west-2"])
        get_active.assert_not_called()
        self.assertEqual(update_regions.call_args.args[2], ["us-east-1", "us-west-2"])
        self.assertEqual(collection.call_args.args[0], ["us-east-1", "us-west-2"])

    def test_explicit_regions_do_not_warn(self):
        *_, output = self._run(["us-east-1", "us-west-2"])
        self.assertNotIn("Warning", output)

    def test_us_east_1_is_always_kept(self):
        # edit_regions replaces the account's list; global events land in us-east-1.
        _, update_regions, collection, _ = self._run(["eu-west-1"])
        self.assertEqual(update_regions.call_args.args[2], ["eu-west-1", "us-east-1"])
        self.assertEqual(collection.call_args.args[0], ["eu-west-1", "us-east-1"])

    def test_region_not_enabled_in_account_is_skipped_with_warning(self):
        _, update_regions, _, output = self._run(["us-east-1", "ap-east-1"])
        self.assertEqual(update_regions.call_args.args[2], ["us-east-1"])
        self.assertIn("skipping ['ap-east-1'] from REGIONS", output)

    def test_repeated_regions_deploy_once(self):
        _, update_regions, collection, _ = self._run(["us-west-2", "us-east-1", "us-west-2"])
        self.assertEqual(update_regions.call_args.args[2], ["us-west-2", "us-east-1"])
        self.assertEqual(collection.call_args.args[0], ["us-west-2", "us-east-1"])

    def test_enabled_regions_network_failure_keeps_regions(self):
        _, update_regions, _, output = self._run(
            ["us-east-1", "us-west-2"], enabled=EndpointConnectionError(endpoint_url="https://ec2"))
        self.assertEqual(update_regions.call_args.args[2], ["us-east-1", "us-west-2"])
        self.assertIn("Could not list enabled regions", output)

    def test_enabled_regions_lookup_failure_keeps_regions(self):
        err = ClientError({"Error": {"Code": "UnauthorizedOperation", "Message": "denied"}}, "DescribeRegions")
        _, update_regions, _, output = self._run(["us-east-1", "us-west-2"], enabled=err)
        self.assertEqual(update_regions.call_args.args[2], ["us-east-1", "us-west-2"])
        self.assertIn("Could not list enabled regions", output)

    def test_default_regions_only_warns(self):
        # No EC2 anywhere: only the Lambda's region and us-east-1 come back.
        *_, output = self._run(None, active_regions=["us-east-1"])
        self.assertIn("no EC2 instances detected outside the default regions", output)
        self.assertIn("--regions", output)

    def test_ec2_detected_regions_do_not_warn(self):
        *_, output = self._run(None, active_regions=["us-east-1", "us-west-2"])
        self.assertNotIn("Warning", output)


class TestReadyAccountRegions(_IntegrateHarness):
    """Scheduled scans go through the READY path for onboarded accounts."""

    def _run(self, regions_to_integrate, current_regions, active_regions=("us-east-1",)):
        graph_client = MagicMock()
        graph_client.get_accounts.return_value = [{
            "cloud_account_id": "123456789012", "status": "READY", "display_name": "acct-name",
            "cloud_regions": list(current_regions),
            "realtime_regions": [{"region_name": r} for r in current_regions]}]
        graph_client.get_account_response_config.return_value = {"remediation": {"status": "OK"}}
        session = self._session(("us-east-1", "us-west-2", "eu-west-1"))
        return self._integrate(graph_client, session, regions_to_integrate, active_regions)

    def test_regions_replace_ec2_detection_for_ready_accounts(self):
        get_active, update_regions, collection, _ = self._run(["us-east-1", "us-west-2"], ["us-east-1"])
        get_active.assert_not_called()
        self.assertEqual(sorted(update_regions.call_args.args[2]), ["us-east-1", "us-west-2"])
        self.assertEqual(collection.call_args.args[0], ["us-west-2"])

    def test_narrowing_regions_never_removes_onboarded_regions(self):
        _, update_regions, collection, _ = self._run(["us-east-1"], ["us-east-1", "eu-west-1"])
        self.assertEqual(sorted(update_regions.call_args.args[2]), ["eu-west-1", "us-east-1"])
        collection.assert_not_called()

    def test_account_stuck_on_defaults_is_flagged_on_scans(self):
        *_, output = self._run(None, ["us-east-1"], active_regions=["us-east-1"])
        self.assertIn("no EC2 instances detected outside the default regions", output)

    def test_account_with_onboarded_regions_is_not_flagged_on_scans(self):
        # Detection finds nothing new, but eu-west-1 is already onboarded and kept.
        *_, output = self._run(None, ["us-east-1", "eu-west-1"], active_regions=["us-east-1"])
        self.assertNotIn("Warning", output)


class TestManagementAccountIsSkipped(unittest.TestCase):
    """The Lambda can't deploy stacks in the management account: its own role
    only lists accounts/regions, and OrganizationAccountAccessRole only exists
    in member accounts. It must skip it with a clear message, not fail."""

    @classmethod
    def setUpClass(cls):
        cls.app = _load("app.py", "organization_integration_lambda_app_mgmt")

    def _handler(self, env_extra=None):
        app = self.app
        env = {"ENVIRONMENT": "acme", "API_TOKEN": "t", "WS_ID": "ws", "PARALLEL": "0"}
        env.update(env_extra or {})
        sts = MagicMock()
        sts.get_caller_identity.return_value = {"Account": "111111111111"}
        ec2 = MagicMock()
        ec2.describe_regions.return_value = {"Regions": [{"RegionName": "us-east-1"}]}
        out = io.StringIO()
        with patch.dict(os.environ, env, clear=False), \
                patch.object(app, "GraphCommon"), \
                patch.object(app, "boto3") as boto3_mock, \
                patch.object(app, "get_all_accounts", return_value=[
                    {"Id": "111111111111", "Name": "mgmt", "Status": "ACTIVE"},
                    {"Id": "222222222222", "Name": "member", "Status": "ACTIVE"}]), \
                patch.object(app, "integrate_sub_account") as integrate, \
                redirect_stdout(out):
            boto3_mock.client.side_effect = lambda service, **kw: {"sts": sts, "ec2": ec2}.get(service, MagicMock())
            app.lambda_handler({}, None)
        return [c.args[0][0] for c in integrate.call_args_list], out.getvalue()

    def test_management_account_is_not_integrated(self):
        integrated, output = self._handler()
        self.assertEqual(integrated, ["222222222222"])
        self.assertIn("Skipping management account 111111111111", output)
        self.assertIn("its own role, which can't deploy stacks", output)
        self.assertIn("organization_integration.py", output)

    def test_management_account_skipped_even_when_listed_in_accounts(self):
        integrated, output = self._handler({"ACCOUNTS": "111111111111,222222222222"})
        self.assertEqual(integrated, ["222222222222"])
        self.assertIn("Skipping management account 111111111111", output)

    def test_no_skip_message_when_management_account_not_selected(self):
        integrated, output = self._handler({"ACCOUNTS": "222222222222"})
        self.assertEqual(integrated, ["222222222222"])
        self.assertNotIn("Skipping management account", output)


if __name__ == '__main__':
    unittest.main()
