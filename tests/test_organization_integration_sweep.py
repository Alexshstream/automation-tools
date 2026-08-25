import importlib.util
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.python.common import boto_common
from src.python.utilities import organization_integration


class TestSweepStackStatuses(unittest.TestCase):
    def _record(self, account, region, stack_id, stack_type="init", name="acct-name",
                stack_name="stack"):
        return {"account": account, "name": name, "region": region, "stack_type": stack_type,
                "stack_name": stack_name, "stack_id": stack_id}

    def _mgmt_session_patch(self, client):
        """Patch boto3.Session so the management account (no assume-role) gets a
        session whose .client(...) always returns `client`."""
        session = MagicMock()
        session.client.return_value = client
        return patch.object(boto_common.boto3, "Session", return_value=session)

    def test_resolves_on_first_tick(self):
        client = MagicMock()
        client.describe_stacks.return_value = {
            "Stacks": [{"StackId": "sid-1", "StackStatus": "CREATE_COMPLETE"}]}
        record = self._record("111", "us-east-1", "sid-1")

        with self._mgmt_session_patch(client):
            result = boto_common.sweep_stack_statuses(
                [record], sts_client=MagicMock(), management_account_id="111",
                poll_interval=0, timeout=30)

        self.assertEqual(result[0]["final_status"], "CREATE_COMPLETE")
        self.assertIsNone(result[0]["status_reason"])
        self.assertEqual(client.describe_stacks.call_count, 1)

    def test_cloudformation_client_uses_adaptive_retry_config(self):
        # Up to 32 concurrent workers polling describe_stacks every
        # poll_interval is exactly the sustained-throttling scenario adaptive
        # retries exist to absorb - without it, a throttled call is more
        # likely to exhaust boto3's default retries and get falsely bucketed
        # as ERROR for a stack that was actually deploying fine.
        client = MagicMock()
        client.describe_stacks.return_value = {
            "Stacks": [{"StackId": "sid-1", "StackStatus": "CREATE_COMPLETE"}]}
        session = MagicMock()
        session.client.return_value = client
        record = self._record("111", "us-east-1", "sid-1")

        with patch.object(boto_common.boto3, "Session", return_value=session):
            boto_common.sweep_stack_statuses(
                [record], sts_client=MagicMock(), management_account_id="111",
                poll_interval=0, timeout=30)

        session.client.assert_called_once_with(
            "cloudformation", region_name="us-east-1", config=boto_common.LAMBDA_CLIENT_CONFIG)

    def test_stays_in_progress_across_ticks_then_resolves(self):
        client = MagicMock()
        client.describe_stacks.side_effect = [
            {"Stacks": [{"StackId": "sid-1", "StackStatus": "CREATE_IN_PROGRESS"}]},
            {"Stacks": [{"StackId": "sid-1", "StackStatus": "CREATE_IN_PROGRESS"}]},
            {"Stacks": [{"StackId": "sid-1", "StackStatus": "CREATE_COMPLETE"}]},
        ]
        record = self._record("111", "us-east-1", "sid-1")

        with self._mgmt_session_patch(client), \
                patch.object(boto_common.time, "sleep") as sleep_mock:
            result = boto_common.sweep_stack_statuses(
                [record], sts_client=MagicMock(), management_account_id="111",
                poll_interval=7, timeout=120)

        self.assertEqual(result[0]["final_status"], "CREATE_COMPLETE")
        self.assertEqual(client.describe_stacks.call_count, 3)
        # Actually slept between ticks (not a busy-spin), once per gap.
        self.assertEqual(sleep_mock.call_count, 2)
        sleep_mock.assert_called_with(7)

    def test_never_resolves_times_out_and_is_not_a_failure_status(self):
        client = MagicMock()
        client.describe_stacks.return_value = {
            "Stacks": [{"StackId": "sid-1", "StackStatus": "CREATE_IN_PROGRESS"}]}
        record = self._record("111", "us-east-1", "sid-1")

        with self._mgmt_session_patch(client):
            # Negative timeout -> deadline is already in the past after the
            # mandatory first (un-slept) check, so this resolves deterministically
            # without waiting for a real clock timeout.
            result = boto_common.sweep_stack_statuses(
                [record], sts_client=MagicMock(), management_account_id="111",
                poll_interval=0, timeout=-1)

        self.assertEqual(result[0]["final_status"], "TIMED_OUT")
        self.assertIsNone(result[0]["status_reason"])
        # TIMED_OUT must be a distinct bucket, never conflated with a real
        # CloudFormation failure or success status.
        self.assertNotIn("FAILED", result[0]["final_status"])
        self.assertNotIn("COMPLETE", result[0]["final_status"])

    def test_deadline_reached_but_targeted_lookup_finds_a_real_deleted_stack(self):
        # A real, unfiltered describe_stacks() never returns a stack that's
        # reached DELETE_COMPLETE - so a stack that rolled back and was
        # deleted would be invisible to every batched tick above and, before
        # this fix, would be misreported as "still in progress, check
        # manually" (TIMED_OUT) instead of its real, genuinely terminal
        # status. One final targeted lookup by stack_id at the deadline
        # finds it (a targeted DescribeStacks-by-ID still returns deleted
        # stacks).
        client = MagicMock()

        def describe_stacks(**kwargs):
            if "StackName" in kwargs:
                return {"Stacks": [{"StackId": "sid-1", "StackStatus": "DELETE_COMPLETE",
                                    "StackStatusReason": "rolled back"}]}
            return {"Stacks": []}  # unfiltered batch query never sees the deleted stack
        client.describe_stacks.side_effect = describe_stacks
        record = self._record("111", "us-east-1", "sid-1")

        with self._mgmt_session_patch(client):
            result = boto_common.sweep_stack_statuses(
                [record], sts_client=MagicMock(), management_account_id="111",
                poll_interval=0, timeout=-1)

        self.assertEqual(result[0]["final_status"], "DELETE_COMPLETE")
        self.assertEqual(result[0]["status_reason"], "rolled back")

    def test_targeted_lookup_failure_at_deadline_still_falls_through_to_timed_out(self):
        # The final targeted lookup is best-effort - if it ALSO fails (e.g.
        # throttling, or the stack aged out of CloudFormation's short
        # retention for deleted stacks), the record must still land in the
        # honest TIMED_OUT bucket, not crash the sweep or silently vanish.
        client = MagicMock()

        def describe_stacks(**kwargs):
            if "StackName" in kwargs:
                raise Exception("simulated failure on targeted lookup")
            return {"Stacks": [{"StackId": "sid-1", "StackStatus": "CREATE_IN_PROGRESS"}]}
        client.describe_stacks.side_effect = describe_stacks
        record = self._record("111", "us-east-1", "sid-1")

        with self._mgmt_session_patch(client):
            result = boto_common.sweep_stack_statuses(
                [record], sts_client=MagicMock(), management_account_id="111",
                poll_interval=0, timeout=-1)

        self.assertEqual(result[0]["final_status"], "TIMED_OUT")

    def test_deadline_is_computed_fresh_when_this_accounts_worker_actually_starts(self):
        # Regression test for the large-org false-timeout bug: the deadline
        # must be computed the moment _sweep_account itself starts running -
        # not derived from some earlier reference point, e.g. when
        # sweep_stack_statuses was first called, before this account's
        # worker even got a turn behind a full 32-worker pool. Calls
        # _sweep_account directly (not through sweep_stack_statuses) twice,
        # simulating two accounts whose workers start at very different
        # absolute wall-clock times - as the second would if it queued
        # behind a full worker pool while the first account's sweep was
        # still running. Both must get an equally fair, full timeout window
        # measured from their OWN start, not from whichever time.time()
        # happened to read when some earlier, shared reference point was
        # captured.
        def run_with_fake_start_time(start_time, account_id):
            client = MagicMock()
            client.describe_stacks.side_effect = [
                {"Stacks": [{"StackId": "sid-1", "StackStatus": "CREATE_IN_PROGRESS"}]},
                {"Stacks": [{"StackId": "sid-1", "StackStatus": "CREATE_IN_PROGRESS"}]},
                {"Stacks": [{"StackId": "sid-1", "StackStatus": "CREATE_COMPLETE"}]},
            ]
            record = self._record(account_id, "us-east-1", "sid-1")
            # 3 ticks worth of "elapsed" time (1 unit per time.time() call:
            # the deadline computation, then one check per tick), well
            # within a real timeout=30 window measured from start_time, but
            # far past a hypothetical stale deadline computed from t=0.
            fake_now = iter([start_time, start_time + 1, start_time + 2])
            with self._mgmt_session_patch(client), \
                    patch.object(boto_common.time, "sleep"), \
                    patch.object(boto_common.time, "time", side_effect=lambda: next(fake_now)):
                boto_common._sweep_account(
                    account_id, [record], sts_client=MagicMock(), management_account_id=account_id,
                    control_role="OrganizationAccountAccessRole", poll_interval=0, timeout=30)
            return record

        # "Account A" starts near t=0 - the kind of account a shared,
        # up-front deadline would have been computed relative to.
        early_record = run_with_fake_start_time(0.0, "111")
        # "Account B" starts far later in absolute time - as if it had sat
        # queued behind a full 32-worker pool while other accounts' sweeps
        # ran first. Under the bug this fix removed, a shared deadline
        # captured back near t=0 (timeout=30 -> deadline=30) would already
        # be expired by t=100_000, and this account would report TIMED_OUT
        # on its very first check despite never having had a real chance to
        # resolve.
        late_record = run_with_fake_start_time(100_000.0, "222")

        self.assertEqual(early_record["final_status"], "CREATE_COMPLETE")
        self.assertEqual(late_record["final_status"], "CREATE_COMPLETE",
                         "a late-starting account's deadline must be measured from "
                         "its own start time, not an earlier shared reference point")

    def test_sweep_stack_statuses_passes_timeout_through_unchanged_not_a_precomputed_deadline(self):
        # Covers the actual sweep_stack_statuses -> _sweep_account boundary
        # the fix changed (test_deadline_is_computed_fresh_... above tests
        # _sweep_account in isolation and would NOT catch a regression where
        # sweep_stack_statuses itself started computing and passing a
        # shared deadline again). Mocking _sweep_account and inspecting
        # what it's actually called with proves sweep_stack_statuses hands
        # down the raw timeout, not any value derived from time.time() on
        # its own side.
        records = [self._record("111", "us-east-1", "sid-1"),
                  self._record("222", "us-west-2", "sid-2")]

        with patch.object(boto_common, "_sweep_account") as mock_sweep_account, \
                patch.object(boto_common.time, "time", return_value=999_999.0):
            boto_common.sweep_stack_statuses(
                records, sts_client=MagicMock(), management_account_id="111",
                poll_interval=7, timeout=45)

        self.assertEqual(mock_sweep_account.call_count, 2)
        for call in mock_sweep_account.call_args_list:
            # Last positional arg must be the literal timeout (45), never a
            # deadline computed from time.time() (which would be ~1000044
            # given the mocked clock above).
            self.assertEqual(call.args[-1], 45)
            self.assertEqual(call.args[-2], 7)  # poll_interval, unchanged too

    def test_timed_out_preserves_a_pre_existing_status_reason(self):
        # deploy_init_stack can set a status_reason on a record that has no
        # final_status yet (e.g. "account reached READY; local
        # CloudFormation wait had timed out first"). If that record never
        # resolves before the sweep's own deadline, the note must survive
        # into the TIMED_OUT bucket rather than being silently wiped to
        # None - it's still genuinely useful context for the operator.
        client = MagicMock()
        client.describe_stacks.return_value = {
            "Stacks": [{"StackId": "sid-1", "StackStatus": "CREATE_IN_PROGRESS"}]}
        record = self._record("111", "us-east-1", "sid-1")
        record["status_reason"] = "account reached READY; local CloudFormation wait had timed out first"

        with self._mgmt_session_patch(client):
            result = boto_common.sweep_stack_statuses(
                [record], sts_client=MagicMock(), management_account_id="111",
                poll_interval=0, timeout=-1)

        self.assertEqual(result[0]["final_status"], "TIMED_OUT")
        self.assertEqual(result[0]["status_reason"],
                         "account reached READY; local CloudFormation wait had timed out first")

    def test_terminal_status_preserves_a_pre_existing_status_reason_when_cfn_has_none(self):
        # Same preservation, but for the terminal (resolved) branch: a
        # clean CREATE_COMPLETE typically has no StackStatusReason of its
        # own, so the pre-existing note must be the one that survives.
        client = MagicMock()
        client.describe_stacks.return_value = {
            "Stacks": [{"StackId": "sid-1", "StackStatus": "CREATE_COMPLETE"}]}
        record = self._record("111", "us-east-1", "sid-1")
        record["status_reason"] = "account reached READY; local CloudFormation wait had timed out first"

        with self._mgmt_session_patch(client):
            result = boto_common.sweep_stack_statuses(
                [record], sts_client=MagicMock(), management_account_id="111",
                poll_interval=0, timeout=30)

        self.assertEqual(result[0]["final_status"], "CREATE_COMPLETE")
        self.assertEqual(result[0]["status_reason"],
                         "account reached READY; local CloudFormation wait had timed out first")

    def test_terminal_status_prefers_cloudformations_own_reason_over_pre_existing_one(self):
        # When CloudFormation DOES have its own reason (e.g. a real
        # failure), that real, current reason must win over a stale
        # pre-existing note, not the other way around.
        client = MagicMock()
        client.describe_stacks.return_value = {"Stacks": [
            {"StackId": "sid-1", "StackStatus": "CREATE_FAILED",
             "StackStatusReason": "Resource creation cancelled"}]}
        record = self._record("111", "us-east-1", "sid-1")
        record["status_reason"] = "some earlier, now-irrelevant note"

        with self._mgmt_session_patch(client):
            result = boto_common.sweep_stack_statuses(
                [record], sts_client=MagicMock(), management_account_id="111",
                poll_interval=0, timeout=30)

        self.assertEqual(result[0]["final_status"], "CREATE_FAILED")
        self.assertEqual(result[0]["status_reason"], "Resource creation cancelled")

    def test_create_failed_captures_status_reason(self):
        client = MagicMock()
        client.describe_stacks.return_value = {"Stacks": [
            {"StackId": "sid-1", "StackStatus": "CREATE_FAILED",
             "StackStatusReason": "Secret name already exists"}]}
        record = self._record("111", "us-east-1", "sid-1")

        with self._mgmt_session_patch(client):
            result = boto_common.sweep_stack_statuses(
                [record], sts_client=MagicMock(), management_account_id="111",
                poll_interval=0, timeout=30)

        self.assertEqual(result[0]["final_status"], "CREATE_FAILED")
        self.assertEqual(result[0]["status_reason"], "Secret name already exists")

    def test_two_stacks_same_account_region_use_single_describe_stacks_call(self):
        # Deliberately DIFFERENT statuses per stack_id - if the matching
        # logic were broken (e.g. always taking the first stack in the
        # response, or zipping records to response order instead of
        # keying by stack_id), both records would end up with the same
        # (wrong) status and this test would catch it; two identical
        # statuses could not.
        client = MagicMock()
        client.describe_stacks.return_value = {"Stacks": [
            {"StackId": "sid-1", "StackStatus": "CREATE_COMPLETE"},
            {"StackId": "sid-2", "StackStatus": "ROLLBACK_COMPLETE"},
        ]}
        records = [
            self._record("111", "us-east-1", "sid-1", stack_type="collection", stack_name="s1"),
            self._record("111", "us-east-1", "sid-2", stack_type="collection", stack_name="s2"),
        ]

        with self._mgmt_session_patch(client):
            result = boto_common.sweep_stack_statuses(
                records, sts_client=MagicMock(), management_account_id="111",
                poll_interval=0, timeout=30)

        by_stack_id = {r["stack_id"]: r["final_status"] for r in result}
        self.assertEqual(by_stack_id, {"sid-1": "CREATE_COMPLETE", "sid-2": "ROLLBACK_COMPLETE"})
        # Batched per region per tick, not once per stack.
        self.assertEqual(client.describe_stacks.call_count, 1)

    def test_account_describe_stacks_error_marks_unresolved_error_others_unaffected(self):
        mgmt_client = MagicMock()
        mgmt_client.describe_stacks.return_value = {
            "Stacks": [{"StackId": "sid-mgmt", "StackStatus": "CREATE_COMPLETE"}]}

        sub_client = MagicMock()
        sub_client.describe_stacks.side_effect = Exception("boom describe")

        def session_factory(*args, **kwargs):
            session = MagicMock()
            session.client.return_value = mgmt_client if not kwargs else sub_client
            return session

        sts_client = MagicMock()
        sts_client.assume_role.return_value = {"Credentials": {
            "AccessKeyId": "a", "SecretAccessKey": "b", "SessionToken": "c"}}

        records = [
            self._record("111", "us-east-1", "sid-mgmt", name="mgmt"),
            self._record("222", "us-east-1", "sid-sub", name="sub"),
        ]

        # Negative timeout -> deterministic resolution after one tick each,
        # no real clock wait for account 222's persistent describe_stacks
        # failure (which now retries rather than locking in ERROR - see
        # test_describe_stacks_error_in_one_region_does_not_fail_other_regions_same_account).
        with patch.object(boto_common.boto3, "Session", side_effect=session_factory):
            result = boto_common.sweep_stack_statuses(
                records, sts_client=sts_client, management_account_id="111",
                poll_interval=0, timeout=-1)

        by_account = {r["account"]: r for r in result}
        self.assertEqual(by_account["111"]["final_status"], "CREATE_COMPLETE")
        self.assertEqual(by_account["222"]["final_status"], "TIMED_OUT")

    def test_describe_stacks_error_in_one_region_does_not_fail_other_regions_same_account(self):
        # Isolation must be per-region within an account, not just per-account:
        # a describe_stacks failure specific to one region (throttling, an
        # access issue in that region) must not falsely mark another region's
        # records that are resolving fine.
        ok_client = MagicMock()
        ok_client.describe_stacks.return_value = {
            "Stacks": [{"StackId": "sid-ok", "StackStatus": "CREATE_COMPLETE"}]}
        bad_client = MagicMock()
        bad_client.describe_stacks.side_effect = Exception("boom region")

        def client_factory(service, region_name=None, **kwargs):
            return bad_client if region_name == "eu-west-1" else ok_client

        session = MagicMock()
        session.client.side_effect = client_factory

        records = [
            self._record("111", "us-east-1", "sid-ok", name="acct"),
            self._record("111", "eu-west-1", "sid-bad", name="acct"),
        ]

        # Negative timeout -> deadline is already in the past after the
        # mandatory first (un-slept) tick, so eu-west-1's persistent failure
        # resolves deterministically to TIMED_OUT without a real clock wait.
        with patch.object(boto_common.boto3, "Session", return_value=session):
            result = boto_common.sweep_stack_statuses(
                records, sts_client=MagicMock(), management_account_id="111",
                poll_interval=0, timeout=-1)

        by_region = {r["region"]: r for r in result}
        self.assertEqual(by_region["us-east-1"]["final_status"], "CREATE_COMPLETE")
        # A transient/persistent per-region describe_stacks failure must NOT
        # permanently lock the record into ERROR on the first bad tick - it
        # stays unresolved (retryable) and only becomes TIMED_OUT once the
        # deadline is reached, same honest "could not confirm" bucket a
        # genuinely slow-but-fine stack would get, not a false definitive
        # failure claim from one bad tick.
        self.assertEqual(by_region["eu-west-1"]["final_status"], "TIMED_OUT")

    def test_persistent_region_error_retries_and_recovers_before_deadline(self):
        # The region's failure must not be permanent: if a later tick
        # succeeds before the deadline, the record resolves normally.
        client = MagicMock()
        client.describe_stacks.side_effect = [
            Exception("boom region"),
            {"Stacks": [{"StackId": "sid-1", "StackStatus": "CREATE_COMPLETE"}]},
        ]
        record = self._record("111", "us-east-1", "sid-1")

        with self._mgmt_session_patch(client), \
                patch.object(boto_common.time, "sleep") as sleep_mock:
            result = boto_common.sweep_stack_statuses(
                [record], sts_client=MagicMock(), management_account_id="111",
                poll_interval=5, timeout=120)

        self.assertEqual(result[0]["final_status"], "CREATE_COMPLETE")
        self.assertEqual(client.describe_stacks.call_count, 2)
        sleep_mock.assert_called_once_with(5)

    def test_client_construction_failure_outside_describe_stacks_still_marks_error(self):
        # A failure building region_clients itself (session.client(...)
        # raising) happens OUTSIDE the per-region describe_stacks try/except.
        # _sweep_account runs in a worker thread whose result is never
        # collected by sweep_stack_statuses (it communicates purely by
        # mutating records in place), so if this were left unguarded the
        # exception would silently vanish, leaving the record without a
        # final_status forever - and later crash main()'s summary loop
        # (KeyError on r["final_status"]) instead of being reported cleanly.
        session = MagicMock()
        session.client.side_effect = Exception("could not construct cloudformation client")
        record = self._record("111", "us-east-1", "sid-1")

        with patch.object(boto_common.boto3, "Session", return_value=session):
            result = boto_common.sweep_stack_statuses(
                [record], sts_client=MagicMock(), management_account_id="111",
                poll_interval=0, timeout=30)

        self.assertIn("final_status", result[0])
        self.assertEqual(result[0]["final_status"], "ERROR")
        self.assertIn("could not construct cloudformation client", result[0]["status_reason"])

    def test_sweep_phase_assume_role_failure_for_non_management_account_marks_error(self):
        # The other session-construction-failure test above uses
        # management_account_id == the record's account, which takes the
        # boto3.Session() (no assume-role) branch entirely -
        # _session_for_sweep_account's own sts_client.assume_role() call,
        # specifically during the sweep phase (not the deploy phase, which
        # has its own separate assume_role and its own separate test
        # coverage), is never exercised failing anywhere else in the suite.
        # An indentation/refactor bug that stopped wrapping this call in
        # the account-wide ERROR try/except would ship undetected without
        # this.
        sts_client = MagicMock()
        sts_client.assume_role.side_effect = Exception("AccessDenied: assume-role failed")
        record = self._record("222222222222", "us-east-1", "sid-1")

        result = boto_common.sweep_stack_statuses(
            [record], sts_client=sts_client, management_account_id="111111111111",
            control_role="OrganizationAccountAccessRole", poll_interval=0, timeout=30)

        sts_client.assume_role.assert_called_once_with(
            RoleArn="arn:aws:iam::222222222222:role/OrganizationAccountAccessRole",
            RoleSessionName="MySessionName")
        self.assertEqual(result[0]["final_status"], "ERROR")
        self.assertIn("AccessDenied: assume-role failed", result[0]["status_reason"])

    def test_sweep_account_exception_escaping_the_safety_net_is_still_recovered(self):
        # Defense-in-depth: even if _sweep_account itself somehow violated its
        # "never raises" contract, sweep_stack_statuses must still recover
        # gracefully (mark that account's records ERROR) rather than letting
        # the exception crash the whole sweep/script and lose every other
        # account's results too.
        record = self._record("111", "us-east-1", "sid-1")

        with patch.object(boto_common, "_sweep_account",
                          side_effect=Exception("contract violated")):
            result = boto_common.sweep_stack_statuses(
                [record], sts_client=MagicMock(), management_account_id="111",
                poll_interval=0, timeout=30)

        self.assertEqual(result[0]["final_status"], "ERROR")
        self.assertIn("contract violated", result[0]["status_reason"])

    def test_worker_pool_sized_to_account_count_capped_at_32(self):
        # Sized up to the actual account count (capped, to bound concurrent
        # AWS API load) so as few accounts as possible ever have to queue -
        # though even a queued account's own per-account deadline (see
        # _sweep_account) only starts once its worker actually runs, so
        # queueing itself is no longer a correctness risk, just something
        # this cap still limits for API load reasons.
        seen_max_workers = []
        real_executor = boto_common.concurrent.futures.ThreadPoolExecutor

        def spy_executor(*args, **kwargs):
            seen_max_workers.append(kwargs.get("max_workers"))
            return real_executor(*args, **kwargs)

        client = MagicMock()
        client.describe_stacks.return_value = {
            "Stacks": [{"StackId": "sid-1", "StackStatus": "CREATE_COMPLETE"}]}
        session = MagicMock()
        session.client.return_value = client

        # 40 distinct accounts should cap the pool at 32, not scale unbounded.
        # Only account "0" is the management account (no assume-role); the
        # other 39 assume a role via sts_client, so that must be mocked too.
        records = [self._record(str(i), "us-east-1", "sid-1") for i in range(40)]
        sts_client = MagicMock()
        sts_client.assume_role.return_value = {"Credentials": {
            "AccessKeyId": "a", "SecretAccessKey": "b", "SessionToken": "c"}}

        with self._mgmt_session_patch(client), \
                patch.object(boto_common.concurrent.futures, "ThreadPoolExecutor",
                            side_effect=spy_executor):
            boto_common.sweep_stack_statuses(
                records, sts_client=sts_client, management_account_id="0",
                poll_interval=0, timeout=30)

        self.assertEqual(seen_max_workers, [32])

    def test_control_role_used_for_assume_role_not_hardcoded_default(self):
        # organization_integration.py's --control_role is caller-configurable;
        # the sweep must assume-role with the SAME role the deploy phase used,
        # not a hardcoded "OrganizationAccountAccessRole" - otherwise every
        # sub-account stack falsely comes back "ERROR" for any org using a
        # non-default control role.
        client = MagicMock()
        client.describe_stacks.return_value = {
            "Stacks": [{"StackId": "sid-1", "StackStatus": "CREATE_COMPLETE"}]}
        session = MagicMock()
        session.client.return_value = client

        sts_client = MagicMock()
        sts_client.assume_role.return_value = {"Credentials": {
            "AccessKeyId": "a", "SecretAccessKey": "b", "SessionToken": "c"}}

        record = self._record("222", "us-east-1", "sid-1")  # non-management account

        with patch.object(boto_common.boto3, "Session", return_value=session):
            result = boto_common.sweep_stack_statuses(
                [record], sts_client=sts_client, management_account_id="111",
                control_role="CustomCrossAccountRole", poll_interval=0, timeout=30)

        self.assertEqual(result[0]["final_status"], "CREATE_COMPLETE")
        called_role_arn = sts_client.assume_role.call_args.kwargs["RoleArn"]
        self.assertIn("CustomCrossAccountRole", called_role_arn)
        self.assertNotIn("OrganizationAccountAccessRole", called_role_arn)

    def test_control_role_defaults_to_organization_account_access_role(self):
        client = MagicMock()
        client.describe_stacks.return_value = {
            "Stacks": [{"StackId": "sid-1", "StackStatus": "CREATE_COMPLETE"}]}
        session = MagicMock()
        session.client.return_value = client

        sts_client = MagicMock()
        sts_client.assume_role.return_value = {"Credentials": {
            "AccessKeyId": "a", "SecretAccessKey": "b", "SessionToken": "c"}}

        record = self._record("222", "us-east-1", "sid-1")

        with patch.object(boto_common.boto3, "Session", return_value=session):
            boto_common.sweep_stack_statuses(
                [record], sts_client=sts_client, management_account_id="111",
                poll_interval=0, timeout=30)

        called_role_arn = sts_client.assume_role.call_args.kwargs["RoleArn"]
        self.assertIn("OrganizationAccountAccessRole", called_role_arn)

    def test_paginates_describe_stacks_across_pages(self):
        # A tracked stack landing past the first page of describe_stacks() must
        # still be found - otherwise it is misreported TIMED_OUT regardless of
        # its real outcome, no matter how many ticks run.
        client = MagicMock()
        client.describe_stacks.side_effect = [
            {"Stacks": [{"StackId": "sid-other", "StackStatus": "CREATE_COMPLETE"}],
             "NextToken": "page2"},
            {"Stacks": [{"StackId": "sid-target", "StackStatus": "CREATE_COMPLETE"}]},
        ]
        record = self._record("111", "us-east-1", "sid-target")

        with self._mgmt_session_patch(client):
            result = boto_common.sweep_stack_statuses(
                [record], sts_client=MagicMock(), management_account_id="111",
                poll_interval=0, timeout=30)

        self.assertEqual(result[0]["final_status"], "CREATE_COMPLETE")
        self.assertEqual(client.describe_stacks.call_count, 2)
        # Second call follows the NextToken from the first page.
        self.assertEqual(
            client.describe_stacks.call_args_list[1].kwargs.get("NextToken"), "page2")

    def test_pre_set_final_status_record_is_not_reswept(self):
        # A record the caller already resolved (e.g. a submit-time failure with
        # no stack_id to poll) must be passed through untouched, not clobbered
        # by the sweep's unconditional reset.
        client = MagicMock()
        record = self._record("111", "us-east-1", None)
        record["final_status"] = "SUBMIT_FAILED"
        record["status_reason"] = "create_stack raised"

        with self._mgmt_session_patch(client):
            result = boto_common.sweep_stack_statuses(
                [record], sts_client=MagicMock(), management_account_id="111",
                poll_interval=0, timeout=30)

        self.assertEqual(result[0]["final_status"], "SUBMIT_FAILED")
        self.assertEqual(result[0]["status_reason"], "create_stack raised")
        client.describe_stacks.assert_not_called()


class TestWaitForCloudformation(unittest.TestCase):
    """wait_for_cloudformation is the legacy synchronous single-stack wait
    (used for the init stack when wait=True, i.e. the default non-parallel
    path) - distinct from the sweep's own describe_stacks polling, which
    already handles pagination. This used to call the unpaginated,
    unfiltered list_stacks() and index [0] into the filtered result - in a
    busy account with enough other stacks that this one landed past page 1,
    that raised IndexError, which the caller (deploy_init_stack) converts
    into a false stack-deployment failure for a stack that was actually
    fine. describe_stacks(StackName=cft_id) targets the one stack directly,
    with no pagination hazard at all."""

    def test_targets_the_stack_directly_by_id_not_an_unfiltered_list(self):
        client = MagicMock()
        client.describe_stacks.return_value = {
            "Stacks": [{"StackId": "sid-1", "StackStatus": "CREATE_COMPLETE"}]}

        with patch.object(boto_common.time, "sleep"):
            result = boto_common.wait_for_cloudformation(("111", "acct"), "sid-1", client)

        self.assertTrue(result)
        client.describe_stacks.assert_called_once_with(StackName="sid-1")
        client.list_stacks.assert_not_called()

    def test_unaffected_by_how_many_other_stacks_exist_in_the_account(self):
        # The whole point of switching to a targeted, by-ID lookup: unlike
        # an unpaginated list_stacks() call, this never needs to see (or
        # care about) any other stack in the account at all.
        client = MagicMock()
        client.describe_stacks.return_value = {
            "Stacks": [{"StackId": "sid-1", "StackStatus": "CREATE_COMPLETE"}]}

        with patch.object(boto_common.time, "sleep"):
            result = boto_common.wait_for_cloudformation(("111", "acct"), "sid-1", client)

        self.assertTrue(result)

    def test_rollback_in_progress_still_raises(self):
        client = MagicMock()
        client.describe_stacks.return_value = {
            "Stacks": [{"StackId": "sid-1", "StackStatus": "ROLLBACK_IN_PROGRESS"}]}

        with patch.object(boto_common.time, "sleep"):
            with self.assertRaises(Exception):
                boto_common.wait_for_cloudformation(("111", "acct"), "sid-1", client)

    def test_timeout_returns_false_not_an_exception(self):
        client = MagicMock()
        client.describe_stacks.return_value = {
            "Stacks": [{"StackId": "sid-1", "StackStatus": "CREATE_IN_PROGRESS"}]}

        # Negative timeout -> the loop condition (dt_diff < timeout) is
        # already false after the mandatory first check, so this resolves
        # deterministically without a real clock wait.
        with patch.object(boto_common.time, "sleep"):
            result = boto_common.wait_for_cloudformation(("111", "acct"), "sid-1", client, timeout=-1)

        self.assertFalse(result)


class TestDeployInitStackReturnShape(unittest.TestCase):
    def _setup(self):
        account_information = {"template_url": "https://example.com/template.yaml"}
        sub_account = ("123456789012", "acct-name")
        session = MagicMock()
        session.region_name = "us-east-1"
        cf_client = MagicMock()
        cf_client.create_stack.return_value = {
            "StackId": "arn:aws:cloudformation:us-east-1:123456789012:stack/LightlyticsStack-42/uuid"}
        session.client.return_value = cf_client
        graph_client = MagicMock()
        return account_information, sub_account, session, cf_client, graph_client

    def test_wait_false_returns_true_and_record_without_waiting(self):
        account_information, sub_account, session, cf_client, graph_client = self._setup()

        with patch.object(boto_common, "wait_for_cloudformation") as wfc:
            ok, record = boto_common.deploy_init_stack(
                account_information, graph_client, sub_account, session, 42, wait=False)

        self.assertTrue(ok)
        self.assertEqual(record, {
            "account": "123456789012", "name": "acct-name", "region": "us-east-1",
            "stack_type": "init", "stack_name": "LightlyticsStack-42",
            "stack_id": "arn:aws:cloudformation:us-east-1:123456789012:stack/LightlyticsStack-42/uuid",
        })
        wfc.assert_not_called()
        graph_client.wait_for_account_connection.assert_not_called()

    def test_wait_true_ready_returns_true_and_record(self):
        account_information, sub_account, session, cf_client, graph_client = self._setup()
        graph_client.wait_for_account_connection.return_value = "READY"

        with patch.object(boto_common, "wait_for_cloudformation") as wfc:
            ok, record = boto_common.deploy_init_stack(
                account_information, graph_client, sub_account, session, 42, wait=True)

        self.assertTrue(ok)
        self.assertEqual(record["stack_type"], "init")
        self.assertEqual(record["stack_id"], cf_client.create_stack.return_value["StackId"])
        wfc.assert_called_once()
        graph_client.wait_for_account_connection.assert_called_once_with("123456789012")

    def test_create_stack_failure_returns_submit_failed_not_raise(self):
        # Matches the sibling deploy_* functions and this function's own
        # documented "always returns a 2-tuple" contract - an unguarded raise
        # here would break that contract.
        account_information, sub_account, session, cf_client, graph_client = self._setup()
        cf_client.create_stack.side_effect = Exception("LimitExceededException")

        ok, record = boto_common.deploy_init_stack(
            account_information, graph_client, sub_account, session, 42, wait=True)

        self.assertFalse(ok)
        self.assertEqual(record["final_status"], "SUBMIT_FAILED")
        self.assertIsNone(record["stack_id"])
        self.assertIn("LimitExceededException", record["status_reason"])
        graph_client.wait_for_account_connection.assert_not_called()

    def test_wait_for_cloudformation_timeout_but_account_reaches_ready_still_succeeds(self):
        # wait_for_cloudformation returning False means its own local 240s
        # wait timed out - NOT a confirmed failure (the stack may still be
        # creating). Before this diff existed, that return value was simply
        # discarded and execution always fell through to the account-
        # connection check, which is the more authoritative signal for
        # whether the account is actually usable. A slow-but-eventually-
        # successful deployment must not have its account setup aborted
        # before that check even runs - the account-connection check must
        # still be attempted, and if it reports READY, this succeeds.
        account_information, sub_account, session, cf_client, graph_client = self._setup()
        graph_client.wait_for_account_connection.return_value = "READY"

        with patch.object(boto_common, "wait_for_cloudformation", return_value=False):
            ok, record = boto_common.deploy_init_stack(
                account_information, graph_client, sub_account, session, 42, wait=True)

        self.assertTrue(ok)
        self.assertEqual(record["stack_id"], cf_client.create_stack.return_value["StackId"])
        graph_client.wait_for_account_connection.assert_called_once_with("123456789012")
        # Noted on the record (surfaces via the sweep's status_reason
        # fallback if CloudFormation's own query doesn't have a more
        # specific reason), but does not prevent success.
        self.assertIn("timed out", record["status_reason"])

    def test_wait_for_cloudformation_timeout_and_account_not_ready_fails_with_account_reason(self):
        # When the CF wait times out AND the account never reaches READY,
        # the failure reason reported is the account status (the more
        # specific, actionable signal), not a generic CF-timeout message.
        account_information, sub_account, session, cf_client, graph_client = self._setup()
        graph_client.wait_for_account_connection.return_value = "ERROR"

        with patch.object(boto_common, "wait_for_cloudformation", return_value=False):
            ok, record = boto_common.deploy_init_stack(
                account_information, graph_client, sub_account, session, 42, wait=True)

        self.assertFalse(ok)
        self.assertEqual(record["stack_id"], cf_client.create_stack.return_value["StackId"])
        self.assertIn("account status: ERROR", record["status_reason"])

    def test_wait_true_not_ready_returns_false_but_still_returns_record(self):
        account_information, sub_account, session, cf_client, graph_client = self._setup()
        graph_client.wait_for_account_connection.return_value = "ERROR"

        with patch.object(boto_common, "wait_for_cloudformation"):
            ok, record = boto_common.deploy_init_stack(
                account_information, graph_client, sub_account, session, 42, wait=True)

        self.assertFalse(ok)
        # The record is still returned (create_stack already succeeded by the
        # time this branch is reached) so the caller can still track the stack.
        self.assertIsNotNone(record)
        self.assertEqual(record["stack_name"], "LightlyticsStack-42")
        # A reason is stashed on the record so the caller's generic "init
        # stack deployment failed" message can include it, instead of it
        # only ever appearing in this function's own stdout print.
        self.assertIn("ERROR", record["status_reason"])

    def test_wait_true_wait_for_cloudformation_raises_still_returns_the_real_record(self):
        # The default (non-parallel) code path calls this with wait=True. If
        # wait_for_cloudformation raises (e.g. the stack hits
        # ROLLBACK_IN_PROGRESS), the record - built from a create_stack call
        # that already succeeded - must still be returned so the caller can
        # track and sweep this real, live stack, instead of the exception
        # propagating and silently losing it.
        account_information, sub_account, session, cf_client, graph_client = self._setup()

        with patch.object(boto_common, "wait_for_cloudformation",
                          side_effect=Exception("Stack ... failed")):
            ok, record = boto_common.deploy_init_stack(
                account_information, graph_client, sub_account, session, 42, wait=True)

        self.assertFalse(ok)
        self.assertIsNotNone(record)
        self.assertEqual(record["stack_id"], cf_client.create_stack.return_value["StackId"])
        graph_client.wait_for_account_connection.assert_not_called()
        self.assertIn("Stack ... failed", record["status_reason"])


class TestDeployHelperRecordShapes(unittest.TestCase):
    def test_deploy_all_collection_stacks_returns_records_per_region(self):
        account_information = {"collection_template_url": "https://x/collection.yaml"}
        sub_account = ("123456789012", "acct-name")
        session = MagicMock()

        def client_factory(service, region_name=None):
            client = MagicMock()
            client.create_stack.return_value = {
                "StackId": f"arn:aws:cloudformation:{region_name}:123456789012:"
                           f"stack/LightlyticsStack-collection-{region_name}-42/uuid"}
            return client
        session.client.side_effect = client_factory

        records = boto_common.deploy_all_collection_stacks(
            ["us-east-1", "eu-west-1"], session, 42, account_information, sub_account)

        self.assertEqual(len(records), 2)
        regions = {r["region"] for r in records}
        self.assertEqual(regions, {"us-east-1", "eu-west-1"})
        for r in records:
            self.assertEqual(r["stack_type"], "collection")
            self.assertEqual(r["account"], "123456789012")
            self.assertEqual(r["name"], "acct-name")
            self.assertEqual(r["stack_name"], f"LightlyticsStack-collection-{r['region']}-42")

    def test_deploy_all_collection_stacks_one_region_failing_does_not_lose_others(self):
        # A region whose create_stack call raises must not discard the record
        # of a sibling region that already succeeded - that stack is real and
        # live in AWS and still needs to be swept/reported.
        account_information = {"collection_template_url": "https://x/collection.yaml"}
        sub_account = ("123456789012", "acct-name")
        session = MagicMock()

        def client_factory(service, region_name=None):
            client = MagicMock()
            if region_name == "eu-west-1":
                client.create_stack.side_effect = Exception("LimitExceededException")
            else:
                client.create_stack.return_value = {
                    "StackId": f"arn:aws:cloudformation:{region_name}:123456789012:"
                               f"stack/LightlyticsStack-collection-{region_name}-42/uuid"}
            return client
        session.client.side_effect = client_factory

        records = boto_common.deploy_all_collection_stacks(
            ["us-east-1", "eu-west-1"], session, 42, account_information, sub_account)

        self.assertEqual(len(records), 2)
        by_region = {r["region"]: r for r in records}
        self.assertEqual(by_region["us-east-1"]["stack_type"], "collection")
        self.assertIsNotNone(by_region["us-east-1"]["stack_id"])
        self.assertNotIn("final_status", by_region["us-east-1"])
        self.assertEqual(by_region["eu-west-1"]["final_status"], "SUBMIT_FAILED")
        self.assertIsNone(by_region["eu-west-1"]["stack_id"])
        self.assertIn("LimitExceededException", by_region["eu-west-1"]["status_reason"])
        # The stack name is deterministic (region + random_int) and known even
        # though create_stack never returned - preserved for the sweep summary
        # the same way the equivalent eks_audit-stack failure already is.
        self.assertEqual(by_region["eu-west-1"]["stack_name"],
                         "LightlyticsStack-collection-eu-west-1-42")

    def test_deploy_collection_stack_wait_failure_still_returns_the_real_record(self):
        # create_stack already succeeded before wait_for_cloudformation runs -
        # a wait failure must not fabricate a stack_id=None SUBMIT_FAILED
        # record and discard the real, already-known stack_id.
        account_information = {"collection_template_url": "https://x/collection.yaml"}
        sub_account = ("123456789012", "acct-name")
        session = MagicMock()
        cf_client = MagicMock()
        cf_client.create_stack.return_value = {
            "StackId": "arn:aws:cloudformation:us-east-1:123456789012:"
                       "stack/LightlyticsStack-collection-us-east-1-42/uuid"}
        session.client.return_value = cf_client

        with patch.object(boto_common, "wait_for_cloudformation",
                          side_effect=Exception("ROLLBACK_IN_PROGRESS")):
            record = boto_common.deploy_collection_stack(
                account_information, session, sub_account, "us-east-1", 42, None, wait=True)

        self.assertEqual(record["stack_id"], cf_client.create_stack.return_value["StackId"])
        self.assertNotIn("final_status", record)

    def test_deploy_response_stack_returns_record_regardless_of_wait(self):
        account_information = {"lightlytics_collection_token": "tok", "external_id": "ext"}
        sub_account = ("123456789012", "acct-name")

        for wait in (False, True):
            with self.subTest(wait=wait):
                session = MagicMock()
                cf_client = MagicMock()
                cf_client.create_stack.return_value = {
                    "StackId": "arn:aws:cloudformation:us-east-1:123456789012:"
                               "stack/LightlyticsStack-response-us-east-1-42/uuid"}
                session.client.return_value = cf_client

                with patch.object(boto_common, "wait_for_cloudformation") as wfc:
                    record = boto_common.deploy_response_stack(
                        "https://env.streamsec.io/graphql", account_information, session,
                        sub_account, "us-east-1", 42, None, "", wait=wait)

                self.assertIsNotNone(record)
                self.assertEqual(record["stack_type"], "response")
                self.assertEqual(record["region"], "us-east-1")
                self.assertEqual(record["stack_name"], "LightlyticsStack-response-us-east-1-42")
                self.assertEqual(wfc.called, wait)

    def test_deploy_response_stack_create_stack_failure_returns_submit_failed_not_raise(self):
        # Unlike collection/eks stacks, this function is called directly (not
        # via a thread pool) - an unguarded create_stack failure would abort
        # the whole account's integration instead of being tracked.
        account_information = {"lightlytics_collection_token": "tok", "external_id": "ext"}
        sub_account = ("123456789012", "acct-name")
        session = MagicMock()
        cf_client = MagicMock()
        cf_client.create_stack.side_effect = Exception("LimitExceededException")
        session.client.return_value = cf_client

        record = boto_common.deploy_response_stack(
            "https://env.streamsec.io/graphql", account_information, session,
            sub_account, "us-east-1", 42, None, "", wait=True)

        self.assertEqual(record["stack_type"], "response")
        self.assertEqual(record["final_status"], "SUBMIT_FAILED")
        self.assertIsNone(record["stack_id"])
        self.assertEqual(record["stack_name"], "LightlyticsStack-response-us-east-1-42")
        self.assertIn("LimitExceededException", record["status_reason"])

    def test_deploy_response_stack_wait_timeout_false_does_not_claim_deployed_successfully(self):
        # wait_for_cloudformation returns False (not an exception) on its own
        # internal timeout - the record must still be returned (real stack_id
        # already known) but without printing a false "deployed successfully".
        account_information = {"lightlytics_collection_token": "tok", "external_id": "ext"}
        sub_account = ("123456789012", "acct-name")
        session = MagicMock()
        cf_client = MagicMock()
        cf_client.create_stack.return_value = {
            "StackId": "arn:aws:cloudformation:us-east-1:123456789012:"
                       "stack/LightlyticsStack-response-us-east-1-42/uuid"}
        session.client.return_value = cf_client

        with patch.object(boto_common, "wait_for_cloudformation", return_value=False):
            record = boto_common.deploy_response_stack(
                "https://env.streamsec.io/graphql", account_information, session,
                sub_account, "us-east-1", 42, None, "", wait=True)

        self.assertEqual(record["stack_id"], cf_client.create_stack.return_value["StackId"])
        self.assertNotIn("final_status", record)

    def test_deploy_eks_audit_logs_stacks_returns_records_only_for_new_regions(self):
        sub_account_information = {"lightlytics_collection_token": "tok", "cloud_regions": []}
        sub_account = ("123456789012", "acct-name")
        session = MagicMock()

        lambda_exists = MagicMock()
        lambda_exists.get_function.return_value = {}  # no exception -> lambda already there

        not_found_exc = type("ResourceNotFoundException", (Exception,), {})
        lambda_missing = MagicMock()
        lambda_missing.exceptions.ResourceNotFoundException = not_found_exc
        lambda_missing.get_function.side_effect = not_found_exc()

        cf_client = MagicMock()
        cf_client.create_stack.return_value = {
            "StackId": "arn:aws:cloudformation:eu-west-1:123456789012:"
                       "stack/StreamSecurity-eks-audit-logs-eu-west-1-42/uuid"}

        def client_factory(service, region_name=None):
            if service == "lambda":
                return lambda_exists if region_name == "us-east-1" else lambda_missing
            return cf_client
        session.client.side_effect = client_factory

        records = boto_common.deploy_eks_audit_logs_stacks(
            "https://env.streamsec.io/graphql", sub_account_information, session, sub_account,
            ["us-east-1", "eu-west-1"], 42, None, wait=False)

        # us-east-1 already has the lambda -> skipped, no record; only eu-west-1 deploys.
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["region"], "eu-west-1")
        self.assertEqual(records[0]["stack_type"], "eks_audit")
        self.assertEqual(records[0]["stack_name"], "StreamSecurity-eks-audit-logs-eu-west-1-42")

    def test_deploy_eks_audit_logs_stacks_one_region_failing_does_not_lose_others(self):
        # A later region's create_stack failure must not lose the record of an
        # earlier region that already deployed successfully.
        sub_account_information = {"lightlytics_collection_token": "tok", "cloud_regions": []}
        sub_account = ("123456789012", "acct-name")
        session = MagicMock()

        not_found_exc = type("ResourceNotFoundException", (Exception,), {})
        lambda_missing = MagicMock()
        lambda_missing.exceptions.ResourceNotFoundException = not_found_exc
        lambda_missing.get_function.side_effect = not_found_exc()

        cf_ok = MagicMock()
        cf_ok.create_stack.return_value = {
            "StackId": "arn:aws:cloudformation:us-east-1:123456789012:"
                       "stack/StreamSecurity-eks-audit-logs-us-east-1-42/uuid"}
        cf_failing = MagicMock()
        cf_failing.create_stack.side_effect = Exception("region not enabled")

        def client_factory(service, region_name=None):
            if service == "lambda":
                return lambda_missing
            return cf_ok if region_name == "us-east-1" else cf_failing
        session.client.side_effect = client_factory

        records = boto_common.deploy_eks_audit_logs_stacks(
            "https://env.streamsec.io/graphql", sub_account_information, session, sub_account,
            ["us-east-1", "ap-south-1"], 42, None, wait=False)

        self.assertEqual(len(records), 2)
        by_region = {r["region"]: r for r in records}
        self.assertIsNotNone(by_region["us-east-1"]["stack_id"])
        self.assertNotIn("final_status", by_region["us-east-1"])
        self.assertEqual(by_region["ap-south-1"]["final_status"], "SUBMIT_FAILED")
        self.assertIsNone(by_region["ap-south-1"]["stack_id"])
        self.assertIn("region not enabled", by_region["ap-south-1"]["status_reason"])

    def test_get_function_non_resource_not_found_error_does_not_lose_earlier_regions(self):
        # Only ResourceNotFoundException means "lambda doesn't exist, proceed
        # to deploy" - any OTHER exception from the existence check itself
        # (throttling, access denied) must be isolated to that region, not
        # propagate out of the whole function and discard the record already
        # collected for an earlier, successfully-deployed region.
        sub_account_information = {"lightlytics_collection_token": "tok", "cloud_regions": []}
        sub_account = ("123456789012", "acct-name")
        session = MagicMock()

        not_found_exc = type("ResourceNotFoundException", (Exception,), {})

        lambda_ok = MagicMock()
        lambda_ok.exceptions.ResourceNotFoundException = not_found_exc
        lambda_ok.get_function.side_effect = not_found_exc()

        lambda_throttled = MagicMock()
        lambda_throttled.exceptions.ResourceNotFoundException = not_found_exc
        lambda_throttled.get_function.side_effect = Exception("ThrottlingException")

        cf_client = MagicMock()
        cf_client.create_stack.return_value = {
            "StackId": "arn:aws:cloudformation:us-east-1:123456789012:"
                       "stack/StreamSecurity-eks-audit-logs-us-east-1-42/uuid"}

        def client_factory(service, region_name=None):
            if service == "lambda":
                return lambda_ok if region_name == "us-east-1" else lambda_throttled
            return cf_client
        session.client.side_effect = client_factory

        records = boto_common.deploy_eks_audit_logs_stacks(
            "https://env.streamsec.io/graphql", sub_account_information, session, sub_account,
            ["us-east-1", "eu-west-1"], 42, None, wait=False)

        self.assertEqual(len(records), 2)
        by_region = {r["region"]: r for r in records}
        self.assertIsNotNone(by_region["us-east-1"]["stack_id"])
        self.assertNotIn("final_status", by_region["us-east-1"])
        self.assertEqual(by_region["eu-west-1"]["final_status"], "SUBMIT_FAILED")
        self.assertIsNone(by_region["eu-west-1"]["stack_id"])
        self.assertIn("ThrottlingException", by_region["eu-west-1"]["status_reason"])

    def test_deploy_eks_audit_logs_stacks_wait_timeout_false_does_not_claim_deployed_successfully(self):
        sub_account_information = {"lightlytics_collection_token": "tok", "cloud_regions": []}
        sub_account = ("123456789012", "acct-name")
        session = MagicMock()

        not_found_exc = type("ResourceNotFoundException", (Exception,), {})
        lambda_missing = MagicMock()
        lambda_missing.exceptions.ResourceNotFoundException = not_found_exc
        lambda_missing.get_function.side_effect = not_found_exc()

        cf_client = MagicMock()
        cf_client.create_stack.return_value = {
            "StackId": "arn:aws:cloudformation:us-east-1:123456789012:"
                       "stack/StreamSecurity-eks-audit-logs-us-east-1-42/uuid"}

        def client_factory(service, region_name=None):
            return lambda_missing if service == "lambda" else cf_client
        session.client.side_effect = client_factory

        with patch.object(boto_common, "wait_for_cloudformation", return_value=False):
            records = boto_common.deploy_eks_audit_logs_stacks(
                "https://env.streamsec.io/graphql", sub_account_information, session, sub_account,
                ["us-east-1"], 42, None, wait=True)

        self.assertEqual(records[0]["stack_id"], cf_client.create_stack.return_value["StackId"])
        self.assertNotIn("final_status", records[0])

    def test_deploy_eks_audit_logs_stacks_returns_empty_list_when_no_regions(self):
        sub_account_information = {"lightlytics_collection_token": "tok", "cloud_regions": []}
        sub_account = ("123456789012", "acct-name")
        session = MagicMock()

        records = boto_common.deploy_eks_audit_logs_stacks(
            "https://env.streamsec.io/graphql", sub_account_information, session, sub_account,
            [], 42, None, wait=False)

        self.assertEqual(records, [])

    def test_deploy_eks_audit_logs_stacks_auto_detects_regions_when_none_passed(self):
        # Every other direct test of this function passes an explicit
        # eks_audit_logs_regions list (bypassing the auto-detect fallback
        # entirely) or an empty cloud_regions list (which trivially returns
        # [] from get_active_eks_regions without its loop body ever
        # running). None of them exercise the real "regions=None -> scan
        # cloud_regions -> deploy only where clusters are actually found"
        # path this function's docstring/behavior promises.
        sub_account_information = {
            "lightlytics_collection_token": "tok", "cloud_regions": ["us-east-1", "eu-west-1"]}
        sub_account = ("123456789012", "acct-name")

        not_found_exc = type("ResourceNotFoundException", (Exception,), {})
        lambda_client = MagicMock()
        lambda_client.exceptions.ResourceNotFoundException = not_found_exc
        lambda_client.get_function.side_effect = not_found_exc()

        # Only us-east-1 has a real cluster - eu-west-1 must be correctly
        # excluded, not just "some region got a stack".
        eks_client_east = MagicMock()
        eks_client_east.list_clusters.return_value = {"clusters": ["real-cluster"]}
        eks_client_west = MagicMock()
        eks_client_west.list_clusters.return_value = {"clusters": []}

        cf_client = MagicMock()
        cf_client.create_stack.return_value = {
            "StackId": "arn:aws:cloudformation:us-east-1:123456789012:"
                       "stack/StreamSecurity-eks-audit-logs-us-east-1-42/uuid"}

        session = MagicMock()

        def client_factory(service, region_name=None):
            if service == "eks":
                return eks_client_east if region_name == "us-east-1" else eks_client_west
            if service == "lambda":
                return lambda_client
            return cf_client
        session.client.side_effect = client_factory

        records = boto_common.deploy_eks_audit_logs_stacks(
            "https://env.streamsec.io/graphql", sub_account_information, session, sub_account,
            None, 42, None, wait=False)

        self.assertEqual(len(records), 1,
                         "auto-detect must deploy only to the region with a real cluster")
        self.assertEqual(records[0]["region"], "us-east-1")
        eks_client_west.list_clusters.assert_called_once()  # eu-west-1 WAS scanned, just excluded


def _load_lambda_app_module():
    """Load lambda/organization_integration/app.py directly via importlib, since
    'lambda' is a reserved keyword and cannot appear in a normal import
    statement. Assumes the repo root is already on sys.path (added above) so
    app.py's own `from src.python...` imports resolve."""
    app_path = os.path.abspath(os.path.join(
        os.path.dirname(__file__), '..', 'lambda', 'organization_integration', 'app.py'))
    spec = importlib.util.spec_from_file_location(
        'organization_integration_lambda_app', app_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestLambdaAppDeployInitStackCallSite(unittest.TestCase):
    """Regression coverage for the one-line fix in lambda/organization_integration/
    app.py: `deploy_init_stack` now returns (ok, record) instead of a bare bool,
    and the call site must unpack it while preserving the existing
    if-not-ok control flow exactly. Without this fix, `if not deploy_init_stack(...)`
    is always False (a non-empty tuple is always truthy) - the Lambda would
    never detect a real init-stack failure and would proceed against a dead
    account."""

    @classmethod
    def setUpClass(cls):
        cls.app = _load_lambda_app_module()

    def _run(self, deploy_init_stack_return):
        app = self.app
        sub_account = ("123456789012", "acct-name")
        org_account_id = "123456789012"  # same as sub_account -> no assume-role needed
        sts_client = MagicMock()
        graph_client = MagicMock()
        # First get_accounts() call: no existing integration -> IndexError -> pass.
        # Second get_accounts() call: used to fetch account_information.
        graph_client.get_accounts.side_effect = [
            [],
            [{"cloud_account_id": sub_account[0]}],
        ]
        graph_client.create_account.return_value = True

        session = MagicMock()
        session.region_name = "us-east-1"

        with patch.object(app, "boto3") as boto3_mock, \
                patch.object(app, "deploy_init_stack", return_value=deploy_init_stack_return) as dis, \
                patch.object(app, "get_active_regions", return_value=["us-east-1"]), \
                patch.object(app, "update_regions", return_value=True), \
                patch.object(app, "deploy_all_collection_stacks", return_value=None):
            boto3_mock.Session.return_value = session
            app.integrate_sub_account(
                sub_account, sts_client, graph_client, ["us-east-1"], 42,
                None, None, "OrganizationAccountAccessRole", org_account_id,
                environment="env", domain="streamsec.io")
        dis.assert_called_once()

    def test_ok_true_does_not_crash_and_continues(self):
        # Must not raise - the new (ok, record) tuple is unpacked correctly and
        # the existing "if not ok: raise" control flow allows the run to proceed.
        self._run((True, {"account": "123456789012", "name": "acct-name",
                          "region": "us-east-1", "stack_type": "init",
                          "stack_name": "s", "stack_id": "sid"}))

    def test_ok_false_still_raises_as_before(self):
        with self.assertRaises(Exception):
            self._run((False, {"account": "123456789012", "name": "acct-name",
                               "region": "us-east-1", "stack_type": "init",
                               "stack_name": "s", "stack_id": "sid"}))


class TestGetActiveEksRegions(unittest.TestCase):
    def test_returns_only_regions_with_clusters(self):
        session = MagicMock()

        def client_factory(service, region_name=None):
            client = MagicMock()
            has_cluster = region_name == "us-east-1"
            client.list_clusters.return_value = {"clusters": ["c1"] if has_cluster else []}
            return client
        session.client.side_effect = client_factory

        result = boto_common.get_active_eks_regions(session, ["us-east-1", "eu-west-1"])

        self.assertEqual(result, ["us-east-1"])

    def test_one_region_failure_does_not_abort_scanning_remaining_regions(self):
        # The bare except/continue must isolate a single region's failure -
        # it must not abort the whole scan and lose a real cluster in a
        # region checked afterward.
        session = MagicMock()

        def client_factory(service, region_name=None):
            client = MagicMock()
            if region_name == "us-east-1":
                client.list_clusters.side_effect = Exception("simulated throttling")
            else:
                client.list_clusters.return_value = {"clusters": ["c1"]}
            return client
        session.client.side_effect = client_factory

        result = boto_common.get_active_eks_regions(
            session, ["us-east-1", "eu-west-1", "us-west-2"])

        self.assertEqual(sorted(result), ["eu-west-1", "us-west-2"],
                         "one region's list_clusters() failure must not prevent scanning "
                         "the remaining regions")


class TestClassifyStackStatus(unittest.TestCase):
    """main()'s sweep-summary classification, extracted so it's directly
    testable rather than only reachable through the full CLI flow."""

    def test_timed_out_and_error_buckets(self):
        self.assertEqual(organization_integration._classify_stack_status("TIMED_OUT"),
                         ("timed_out", "yellow"))
        self.assertEqual(organization_integration._classify_stack_status("ERROR"),
                         ("errored", "red"))

    def test_create_and_update_complete_are_the_only_success_statuses(self):
        self.assertEqual(organization_integration._classify_stack_status("CREATE_COMPLETE"),
                         ("succeeded", "green"))
        self.assertEqual(organization_integration._classify_stack_status("UPDATE_COMPLETE"),
                         ("succeeded", "green"))

    def test_dry_run_is_its_own_bucket_not_failed(self):
        self.assertEqual(organization_integration._classify_stack_status("DRY_RUN"),
                         ("dry_run", "cyan"))

    def test_known_failure_statuses_are_failed(self):
        for status in ("CREATE_FAILED", "ROLLBACK_COMPLETE", "UPDATE_ROLLBACK_COMPLETE",
                       "UPDATE_ROLLBACK_FAILED", "SUBMIT_FAILED"):
            with self.subTest(status=status):
                self.assertEqual(organization_integration._classify_stack_status(status),
                                 ("failed", "red"))

    def test_unexpected_terminal_status_defaults_to_failed_not_succeeded(self):
        # Whitelist success, not blacklist failure: a stack deleted out from
        # under the sweep (external cleanup, a conflicting automation) is
        # terminal (DELETE_COMPLETE) but must never be miscounted as a
        # successful deployment just because it ends in "_COMPLETE".
        self.assertEqual(organization_integration._classify_stack_status("DELETE_COMPLETE"),
                         ("failed", "red"))
        # Same for a genuinely unrecognized status (including the "UNKNOWN"
        # fallback main() uses for a record missing final_status entirely).
        self.assertEqual(organization_integration._classify_stack_status("UNKNOWN"),
                         ("failed", "red"))
        self.assertEqual(organization_integration._classify_stack_status("SOME_NEW_CFN_STATUS"),
                         ("failed", "red"))


class TestRegionsToIntegrateNotMutatedAcrossAccounts(unittest.TestCase):
    """--regions is parsed once in main() into a single list object, then
    passed as regions_to_integrate to EVERY account processed in the run
    (and, under --parallel, to every account's thread concurrently).
    integrate_sub_account's READY-account branch used to bind
    potential_regions directly to that same object and then extend() it in
    place - one account's current_regions would leak into every account
    processed afterward, and could race under --parallel."""

    def _run_ready_account(self, sub_account, regions_to_integrate, registered_cloud_regions):
        sts_client = MagicMock()
        graph_client = MagicMock()
        graph_client.get_accounts.return_value = [
            {"cloud_account_id": sub_account[0], "status": "READY",
             "cloud_regions": registered_cloud_regions,
             "realtime_regions": [{"region_name": r} for r in registered_cloud_regions],
             "lightlytics_collection_token": "tok"},
        ]
        graph_client.get_account_response_config.return_value = {"remediation": {"status": "done"}}

        session = MagicMock()
        session.region_name = "us-east-1"

        with patch.object(organization_integration, "boto3") as mock_boto3:
            mock_boto3.Session.return_value = session
            organization_integration.integrate_sub_account(
                "https://example.streamsec.io", sub_account, sts_client, graph_client,
                ["us-east-1"], "abc123", None, regions_to_integrate,
                "OrganizationAccountAccessRole", sub_account[0],
                parallel=False, response=False,
            )

    def test_shared_regions_list_is_not_mutated_by_one_accounts_run(self):
        shared_regions_to_integrate = ["us-east-1", "us-west-2"]
        original_snapshot = list(shared_regions_to_integrate)

        with patch.object(organization_integration, "update_regions", return_value=True), \
                patch.object(organization_integration, "deploy_all_collection_stacks", return_value=None):
            self._run_ready_account(
                ("111111111111", "acct-1"), shared_regions_to_integrate,
                registered_cloud_regions=["eu-west-1"],  # different from regions_to_integrate -> triggers extend()
            )

        self.assertEqual(
            shared_regions_to_integrate, original_snapshot,
            "the caller's --regions list must never be mutated by any account's own run - "
            "main() passes this SAME object to every account in the run")

    def test_second_account_is_not_polluted_by_the_first_accounts_registered_regions(self):
        shared_regions_to_integrate = ["us-east-1"]

        with patch.object(organization_integration, "update_regions", return_value=True) as mock_update_regions, \
                patch.object(organization_integration, "deploy_all_collection_stacks", return_value=None):
            self._run_ready_account(
                ("111111111111", "acct-1"), shared_regions_to_integrate,
                registered_cloud_regions=["eu-west-1"],
            )
            self._run_ready_account(
                ("222222222222", "acct-2"), shared_regions_to_integrate,
                registered_cloud_regions=["ap-south-1"],
            )

        # Each account's update_regions() call must reflect ONLY its own
        # registered region plus the shared candidate list - never a region
        # leaked in from a DIFFERENT account's earlier run.
        first_call_regions = sorted(mock_update_regions.call_args_list[0].args[2])
        second_call_regions = sorted(mock_update_regions.call_args_list[1].args[2])
        self.assertEqual(first_call_regions, ["eu-west-1", "us-east-1"])
        self.assertEqual(second_call_regions, ["ap-south-1", "us-east-1"],
                         "account 2's region set must not include account 1's eu-west-1")


if __name__ == "__main__":
    unittest.main()
