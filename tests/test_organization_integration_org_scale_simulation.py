"""
Large-scale resilience simulation for organization_integration.py's main().

Real AWS testing only ever covers one account at a time. This runs the ACTUAL
main()/integrate_sub_account()/sweep_stack_statuses() orchestration against a
simulated "org" of ~17 accounts, each injected with a different failure mode
(bad control role, submit-time failures, describe_stacks throttling/timeout,
backend inconsistencies, already-integrated states, EKS variations, etc.),
using mocks instead of real AWS/GraphQL calls.

Goal: prove the orchestration layer (not individual AWS calls, already
verified against real AWS elsewhere) survives many simultaneous failure types
without crashing, correctly isolates each account's failures from every other
account, and produces a sane, honest final report.
"""
import contextlib
import io
import os
import re
import sys
import unittest
from unittest.mock import MagicMock, patch

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.python.utilities import organization_integration as oi
from src.python.common import boto_common


MANAGEMENT_ACCOUNT_ID = "999999999999"
CANDIDATE_REGIONS = ["us-east-1", "us-west-2", "eu-west-1"]

# Each scenario describes one simulated account's behavior. Accounts not
# already "in_registry" are treated as brand new - the real create_account()
# flow runs for them first.
SCENARIOS = {
    "100000000001": {"desc": "happy path, brand new account, no EKS"},
    "100000000002": {"desc": "happy path, brand new account, has EKS cluster",
                     "has_eks": True},
    "100000000003": {"desc": "bad control role - assume_role denied",
                     "assume_role_fails": True},
    "100000000004": {"desc": "init stack create_stack fails outright",
                     "init_create_stack_fails": True},
    "100000000005": {"desc": "collection stack: one region fails, one succeeds",
                     "collection_region_fails": "us-west-2"},
    "100000000006": {"desc": "EKS: create_stack fails for detected region",
                     "has_eks": True, "eks_create_stack_fails": True},
    "100000000007": {"desc": "EKS: get_function throttled (non-ResourceNotFound)",
                     "has_eks": True, "eks_get_function_throttled": True},
    "100000000008": {"desc": "EKS: lambda already exists, should skip",
                     "has_eks": True, "eks_lambda_already_exists": True},
    "100000000009": {"desc": "already READY, needs a new region added",
                     "status": "READY", "in_registry": True,
                     "cloud_regions": ["us-east-1"], "realtime_regions": ["us-east-1"]},
    "100000000010": {"desc": "already READY, fully quiescent, nothing to do",
                     "status": "READY", "in_registry": True,
                     # Matches exactly what get_active_regions() will compute
                     # (session region us-east-1 + us-west-2, which has the
                     # mocked EC2 instance) - both cloud_regions and
                     # realtime_regions already cover it, so no region
                     # update and no new collection stack should be
                     # submitted for this account at all.
                     "cloud_regions": ["us-east-1", "us-west-2"],
                     "realtime_regions": ["us-east-1", "us-west-2"],
                     "quiescent": True},
    "100000000011": {"desc": "backend reports an unexpected status",
                     "status": "SUSPENDED", "in_registry": True},
    "100000000012": {"desc": "backend inconsistency: connection-wait endpoint reports "
                             "READY but account-list endpoint never leaves "
                             "UNINITIALIZED (update_regions local polling timeout)",
                     "never_leaves_uninitialized": True},
    "100000000013": {"desc": "sweep: describe_stacks fails for this account",
                     "sweep_describe_stacks_fails": True},
    "100000000014": {"desc": "sweep: stack never resolves (times out)",
                     "sweep_never_resolves": True},
    MANAGEMENT_ACCOUNT_ID: {"desc": "management account itself as a target "
                                    "(no assume-role needed)"},
    "100000000016": {"desc": "response stack requested and its create_stack fails"},
    "100000000017": {"desc": "wait_for_account_connection returns non-READY "
                             "after edit_regions",
                     "connection_check_fails": True},
}


_seen_stack_ids = {}


class _FakeClock:
    """time.time()/time.sleep() must stay logically consistent for the
    sweep's deadline math to behave correctly, but must not actually block
    real wall-clock time. sleep() advances the fake clock instead of
    blocking, so a 300s timeout with 10s polls "elapses" after 30 sleep()
    calls, in milliseconds of real time, not 5 real minutes."""

    def __init__(self, start=1_700_000_000.0):
        self.now = start

    def time(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


class FakeGraphClient:
    """Stands in for GraphCommon. `registry` maps account_id -> mutable state
    dict; accounts absent/not "in_registry" are "not yet integrated" (mirrors
    a real IndexError -> pass in integrate_sub_account)."""

    def __init__(self, registry):
        self.registry = registry
        self._uninitialized_poll_count = {}

    def get_accounts(self):
        out = []
        for acc_id, st in self.registry.items():
            if not st.get("in_registry"):
                continue
            if st.get("never_leaves_uninitialized"):
                # Models a genuine backend inconsistency: deploy_init_stack's
                # wait_for_account_connection() (a separate endpoint) reports
                # READY and st["status"] gets set accordingly, but the
                # account-list endpoint (this method) never reflects it - so
                # it must always report UNINITIALIZED here regardless of
                # st["status"], to actually exercise update_regions()'s own
                # local 300-iteration polling timeout instead of being
                # resolved by the earlier wait_for_account_connection() gate.
                status = "UNINITIALIZED"
            elif st.get("status") == "UNINITIALIZED":
                self._uninitialized_poll_count[acc_id] = \
                    self._uninitialized_poll_count.get(acc_id, 0) + 1
                if self._uninitialized_poll_count[acc_id] >= 2:
                    st["status"] = "PENDING"  # left UNINITIALIZED, not yet READY
                status = st.get("status", "UNINITIALIZED")
            else:
                status = st.get("status", "UNINITIALIZED")
            out.append({
                "cloud_account_id": acc_id,
                "status": status,
                "cloud_regions": st.get("cloud_regions", []),
                "realtime_regions": [{"region_name": r} for r in st.get("realtime_regions", [])],
                "display_name": st.get("display_name", ""),
                "lightlytics_collection_token": f"tok-{acc_id}",
                "external_id": f"ext-{acc_id}",
                "template_url": "https://example.com/template.yaml",
                "collection_template_url": "https://example.com/collection.yaml",
            })
        return out

    def create_account(self, account_id, regions, display_name=None):
        st = self.registry[account_id]
        st["in_registry"] = True
        st["status"] = "UNINITIALIZED"
        return True

    def get_account_response_config(self, account_id):
        return {"remediation": self.registry[account_id].get("remediation")}

    def update_account_display_name(self, account_id, name):
        pass

    def edit_regions(self, account_id, regions):
        self.registry[account_id]["cloud_regions"] = regions

    def wait_for_account_connection(self, account_id):
        st = self.registry[account_id]
        if st.get("connection_check_fails"):
            return "ERROR"
        st["status"] = "READY"
        return "READY"


def _account_id_from_access_key(access_key):
    # Fake credentials always encode the account id as "FAKEKEY-<account_id>".
    return access_key.replace("FAKEKEY-", "")


def _make_client_for(account_id, service, region_name):
    st = SCENARIOS[account_id]
    client = MagicMock()

    if service == "ec2":
        # One active region beyond the session's own, so multi-region
        # collection-stack behavior is still exercised.
        has_instance = region_name == "us-west-2"
        client.describe_instances.return_value = {
            "Reservations": [{"Instances": [{"InstanceId": "i-fake"}] if has_instance else []}]}
        return client

    if service == "cloudformation":
        def create_stack(**kwargs):
            name = kwargs["StackName"]
            if name.startswith("LightlyticsStack-collection-") and \
                    st.get("collection_region_fails") == region_name:
                raise Exception("LimitExceededException: simulated collection create_stack failure")
            if name.startswith("LightlyticsStack-response-") and account_id == "100000000016":
                raise Exception("simulated response stack create_stack failure")
            if name.startswith("StreamSecurity-eks-audit-logs-") and st.get("eks_create_stack_fails"):
                raise Exception("simulated EKS audit stack create_stack failure")
            if name.startswith("LightlyticsStack-") and "-collection-" not in name and \
                    "-response-" not in name and st.get("init_create_stack_fails"):
                raise Exception("simulated init stack create_stack failure")
            stack_id = f"arn:aws:cloudformation:{region_name}:{account_id}:stack/{name}/fake-uuid"
            _seen_stack_ids.setdefault(account_id, set()).add(stack_id)
            return {"StackId": stack_id}
        client.create_stack.side_effect = create_stack

        def _statuses_for_account():
            if st.get("sweep_describe_stacks_fails"):
                raise Exception("simulated ThrottlingException during sweep")
            live_status = "CREATE_IN_PROGRESS" if st.get("sweep_never_resolves") else "CREATE_COMPLETE"
            return {"Stacks": [{"StackId": sid, "StackStatus": live_status}
                               for sid in _seen_stack_ids.get(account_id, set())]}
        client.describe_stacks.side_effect = lambda **kw: _statuses_for_account()
        # Legacy wait_for_cloudformation() path (used when wait=True, e.g. the
        # init stack in sequential/non---parallel mode) polls list_stacks()
        # instead of describe_stacks() - always resolve immediately so this
        # pre-existing, untouched-by-this-session code doesn't spin.
        client.list_stacks.side_effect = lambda **kw: {
            "StackSummaries": [{"StackId": sid, "StackStatus": "CREATE_COMPLETE"}
                               for sid in _seen_stack_ids.get(account_id, set())]}
        return client

    if service == "eks":
        client.list_clusters.return_value = {
            "clusters": ["fake-cluster"] if st.get("has_eks") else []}
        return client

    if service == "lambda":
        not_found = type("ResourceNotFoundException", (Exception,), {})
        client.exceptions.ResourceNotFoundException = not_found
        if st.get("eks_lambda_already_exists"):
            client.get_function.return_value = {}
        elif st.get("eks_get_function_throttled"):
            client.get_function.side_effect = Exception("simulated throttling on get_function")
        else:
            client.get_function.side_effect = not_found()
        return client

    return client


def _make_session(*args, **kwargs):
    session = MagicMock()
    if "aws_access_key_id" in kwargs:
        acc_id = _account_id_from_access_key(kwargs["aws_access_key_id"])
    else:
        acc_id = MANAGEMENT_ACCOUNT_ID
    session.region_name = "us-east-1"
    session.client.side_effect = lambda service, region_name=None, **kw: \
        _make_client_for(acc_id, service, region_name or session.region_name)
    return session


class TestOrgScaleSimulation(unittest.TestCase):
    def test_full_org_run_survives_every_injected_failure_mode(self):
        _seen_stack_ids.clear()

        registry = {acc_id: dict(scenario) for acc_id, scenario in SCENARIOS.items()}
        graph_client = FakeGraphClient(registry)

        org_client = MagicMock()
        org_client.list_accounts.return_value = {
            "Accounts": [{"Id": acc_id, "Name": f"acct-{acc_id}", "Status": "ACTIVE"}
                        for acc_id in SCENARIOS]}

        sts_client = MagicMock()
        sts_client.get_caller_identity.return_value = {"Account": MANAGEMENT_ACCOUNT_ID}

        def assume_role(RoleArn, RoleSessionName):
            acc_id = RoleArn.split("::")[1].split(":")[0]
            if SCENARIOS[acc_id].get("assume_role_fails"):
                raise Exception("AccessDenied: simulated bad control role")
            return {"Credentials": {
                "AccessKeyId": f"FAKEKEY-{acc_id}",
                "SecretAccessKey": "fake", "SessionToken": "fake"}}
        sts_client.assume_role.side_effect = assume_role

        ec2_client = MagicMock()
        ec2_client.describe_regions.return_value = {
            "Regions": [{"RegionName": r} for r in CANDIDATE_REGIONS]}

        def boto3_client_dispatch(service, region_name=None, **kwargs):
            if service == "organizations":
                return org_client
            if service == "sts":
                return sts_client
            if service == "ec2" and region_name is None:
                return ec2_client
            raise AssertionError(f"unexpected bare boto3.client call: {service}/{region_name}")

        clock = _FakeClock()

        # Spy on sweep_stack_statuses (called once, at the very end of
        # main()) to capture its real return value - main() itself doesn't
        # return anything (it's a CLI entrypoint), so this is the only way
        # to assert on each stack's actual final_status/status_reason
        # without parsing print output for everything.
        real_sweep = oi.sweep_stack_statuses
        captured_swept = []

        def _spy_sweep(*a, **kw):
            result = real_sweep(*a, **kw)
            captured_swept.extend(result)
            return result

        # wait_for_cloudformation measures elapsed time via datetime.utcnow()
        # rather than time.time() - freeze it so the mocked-instant sleep()
        # above doesn't leave it spinning near-forever waiting for real
        # wall-clock time to pass. This MUST be built before boto_common's
        # `datetime.datetime` is patched below: boto_common.datetime is the
        # real stdlib datetime module (a process-wide singleton), so patching
        # its `datetime` attribute replaces the real constructor globally -
        # building "a real datetime" after that point would actually call the
        # mock's own constructor and produce another MagicMock, not a real one.
        import datetime as real_datetime
        fixed_now = real_datetime.datetime(2026, 1, 1)

        stdout_buf = io.StringIO()

        with patch.object(oi, "boto3") as oi_boto3, \
                patch.object(boto_common, "boto3") as bc_boto3, \
                patch.object(oi, "GraphCommon", return_value=graph_client), \
                patch.object(oi, "sweep_stack_statuses", side_effect=_spy_sweep), \
                patch.object(boto_common.time, "sleep", side_effect=clock.sleep), \
                patch.object(boto_common.time, "time", side_effect=clock.time), \
                patch.object(boto_common.datetime, "datetime") as fake_datetime, \
                patch("builtins.input", return_value="yes"), \
                contextlib.redirect_stdout(stdout_buf):
            oi_boto3.client.side_effect = boto3_client_dispatch
            oi_boto3.Session.side_effect = _make_session
            bc_boto3.Session.side_effect = _make_session
            fake_datetime.utcnow.return_value = fixed_now

            oi.main(
                environment_url="https://example.streamsec.io",
                ll_username=None, ll_password=None, aws_profile_name=None,
                accounts=",".join(SCENARIOS.keys()), parallel=None,
                ws_id="ws-1", api_token="fake-token",
                response=True, eks_audit_logs=True,
            )

        output = stdout_buf.getvalue()
        # main() doesn't return anything (CLI entrypoint) - the failures
        # summary block it prints (`"  {account_id}: {msg}"`, see
        # organization_integration.py's main()) is the only signal for which
        # accounts' overall runs raised, so parse it instead of re-deriving
        # the same logic here.
        failed_accounts = set(re.findall(r"^  (\d{12}):", output, re.MULTILINE))

        by_account = {}
        for r in captured_swept:
            by_account.setdefault(r["account"], []).append(r)

        def stacks_of(acc_id):
            return sorted(
                (r["stack_type"], r["region"], r["final_status"]) for r in by_account.get(acc_id, []))

        def assert_reason_contains(acc_id, stack_type, region, needle):
            matches = [r for r in by_account.get(acc_id, [])
                      if r["stack_type"] == stack_type and r["region"] == region]
            self.assertEqual(len(matches), 1,
                             f"expected exactly one {stack_type}/{region} record for {acc_id}, "
                             f"got {matches}")
            self.assertIn(needle, matches[0].get("status_reason") or "",
                          f"{acc_id} {stack_type}/{region} status_reason mismatch: {matches[0]}")

        # 1: happy path, brand new, no EKS - full success, nothing left behind.
        self.assertNotIn("100000000001", failed_accounts)
        self.assertEqual(stacks_of("100000000001"), [
            ("collection", "us-east-1", "CREATE_COMPLETE"),
            ("collection", "us-west-2", "CREATE_COMPLETE"),
            ("init", "us-east-1", "CREATE_COMPLETE"),
            ("response", "us-east-1", "CREATE_COMPLETE"),
        ])

        # 2: happy path, brand new, has EKS - EKS audit stacks in both active
        # regions (regression test for the active_regions-vs-stale-
        # cloud_regions EKS detection bug fixed alongside this test).
        self.assertNotIn("100000000002", failed_accounts)
        acc2 = stacks_of("100000000002")
        self.assertIn(("eks_audit", "us-east-1", "CREATE_COMPLETE"), acc2)
        self.assertIn(("eks_audit", "us-west-2", "CREATE_COMPLETE"), acc2)

        # 3: bad control role - never got past assume_role, zero AWS calls.
        self.assertIn("100000000003", failed_accounts)
        self.assertEqual(len(_seen_stack_ids.get("100000000003", set())), 0,
                         "bad-control-role account should never have reached "
                         "any create_stack call")
        self.assertEqual(by_account.get("100000000003", []), [])

        # 4: init stack create_stack fails outright - SUBMIT_FAILED, fatal to
        # the whole account (nothing downstream of init can proceed).
        self.assertIn("100000000004", failed_accounts)
        assert_reason_contains("100000000004", "init", "us-east-1",
                               "simulated init stack create_stack failure")

        # 5: one collection region fails, the other succeeds - partial
        # failure is non-fatal to the account's overall run.
        self.assertNotIn("100000000005", failed_accounts)
        assert_reason_contains("100000000005", "collection", "us-west-2",
                               "LimitExceededException")
        self.assertIn(("collection", "us-east-1", "CREATE_COMPLETE"), stacks_of("100000000005"))

        # 6: EKS create_stack fails for the detected region(s) - non-fatal,
        # rest of the account still succeeds.
        self.assertNotIn("100000000006", failed_accounts)
        assert_reason_contains("100000000006", "eks_audit", "us-east-1",
                               "simulated EKS audit stack create_stack failure")
        self.assertIn(("init", "us-east-1", "CREATE_COMPLETE"), stacks_of("100000000006"))

        # 7: EKS get_function throttled (not ResourceNotFoundException) -
        # treated as a submit failure for that region, not silently lost or
        # propagated as a hard account failure.
        self.assertNotIn("100000000007", failed_accounts)
        assert_reason_contains("100000000007", "eks_audit", "us-east-1",
                               "simulated throttling on get_function")

        # 8: EKS lambda already exists - correctly skipped, no stack record
        # at all (not a failure, not a success record either).
        self.assertNotIn("100000000008", failed_accounts)
        self.assertFalse(
            [r for r in by_account.get("100000000008", []) if r["stack_type"] == "eks_audit"],
            "lambda-already-exists EKS region should produce no stack record")

        # 9: already READY, region set changed - no init stack (already
        # integrated), only the new region's collection stack.
        self.assertNotIn("100000000009", failed_accounts)
        self.assertEqual(stacks_of("100000000009"), [
            ("collection", "us-west-2", "CREATE_COMPLETE"),
            ("response", "us-east-1", "CREATE_COMPLETE"),
        ])

        # 10: already READY, fully quiescent - no collection stack at all,
        # only the always-evaluated response stack.
        self.assertNotIn("100000000010", failed_accounts)
        self.assertEqual(stacks_of("100000000010"), [
            ("response", "us-east-1", "CREATE_COMPLETE"),
        ])

        # 11: backend reports an unexpected status - hard failure, no stacks.
        self.assertIn("100000000011", failed_accounts)
        self.assertEqual(by_account.get("100000000011", []), [])

        # 12: backend inconsistency (connection-wait says READY, account-list
        # never leaves UNINITIALIZED) - update_regions' own local timeout
        # fires; init/response stacks already went out for real before that
        # and remain (honest reporting of real leftover AWS resources).
        self.assertIn("100000000012", failed_accounts)
        self.assertIn(("init", "us-east-1", "CREATE_COMPLETE"), stacks_of("100000000012"))
        self.assertIn(("response", "us-east-1", "CREATE_COMPLETE"), stacks_of("100000000012"))

        # 13: sweep-phase describe_stacks always fails for this account - the
        # sweep must isolate that (retry, not crash) and eventually give up
        # with TIMED_OUT rather than a false CREATE_COMPLETE or an unhandled
        # error; the account's own run still succeeds (deploy phase is fine).
        self.assertNotIn("100000000013", failed_accounts)
        self.assertTrue(by_account.get("100000000013"))
        for r in by_account["100000000013"]:
            self.assertEqual(r["final_status"], "TIMED_OUT", r)

        # 14: stack submitted but never resolves in CloudFormation - sweep
        # deadline fires, TIMED_OUT, not a false success or a hang.
        self.assertNotIn("100000000014", failed_accounts)
        self.assertTrue(by_account.get("100000000014"))
        for r in by_account["100000000014"]:
            self.assertEqual(r["final_status"], "TIMED_OUT", r)

        # management account as its own target - no assume_role needed, full
        # success just like any other account.
        self.assertNotIn(MANAGEMENT_ACCOUNT_ID, failed_accounts)
        self.assertIn(("init", "us-east-1", "CREATE_COMPLETE"), stacks_of(MANAGEMENT_ACCOUNT_ID))

        # 16: response stack create_stack fails - non-fatal, rest succeeds.
        self.assertNotIn("100000000016", failed_accounts)
        assert_reason_contains("100000000016", "response", "us-east-1",
                               "simulated response stack create_stack failure")
        self.assertIn(("init", "us-east-1", "CREATE_COMPLETE"), stacks_of("100000000016"))

        # 17: wait_for_account_connection returns non-READY after the init
        # stack itself succeeded - fatal to the account, but the init stack
        # that already went out for real is still reported honestly.
        self.assertIn("100000000017", failed_accounts)
        init17 = [r for r in by_account.get("100000000017", []) if r["stack_type"] == "init"]
        self.assertEqual(len(init17), 1)
        self.assertEqual(init17[0]["final_status"], "CREATE_COMPLETE")

        # Exactly the 5 genuinely-fatal scenarios should be in the failures
        # list - no more (nothing over-triggering), no fewer (nothing
        # silently swallowed).
        self.assertEqual(failed_accounts, {
            "100000000003", "100000000004", "100000000011",
            "100000000012", "100000000017",
        })


if __name__ == "__main__":
    unittest.main()
