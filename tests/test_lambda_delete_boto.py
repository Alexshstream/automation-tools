import os
import sys
import unittest
from unittest.mock import MagicMock, patch
from botocore.exceptions import ClientError

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.python.common import boto_common
from src.python.common.boto_common import (
    scan_lambdas_in_region,
    delete_lambda_function,
    list_live_stack_names,
    CFN_STACK_NAME_TAG,
)


def _session_with_lambda(client):
    session = MagicMock()
    session.client.return_value = client
    return session


class TestScanLambdasInRegion(unittest.TestCase):
    def test_splits_matches_and_cfn_managed(self):
        client = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [{
            "Functions": [
                {"FunctionName": "StreamSec_A", "FunctionArn": "arn:A"},
                {"FunctionName": "StreamSec_B", "FunctionArn": "arn:B"},
                {"FunctionName": "unrelated",   "FunctionArn": "arn:C"},
            ]
        }]
        client.get_paginator.return_value = paginator
        client.list_tags.side_effect = lambda Resource: (
            {"Tags": {CFN_STACK_NAME_TAG: "LightlyticsStack-x",
                      boto_common.CFN_STACK_ID_TAG:
                          "arn:aws:cloudformation:us-east-1:123:stack/LightlyticsStack-x/u"}}
            if Resource == "arn:B" else {"Tags": {}}
        )
        session = _session_with_lambda(client)

        # StreamSec_B's stack-id is in this region, so the scan actually lists live
        # stacks; its stack IS live, so it stays protected (skipped_cfn) via the
        # stack_name-in-live-set path rather than being reclassified as an orphan.
        with patch.object(boto_common, "list_live_stack_names",
                           return_value={"LightlyticsStack-x"}) as lls:
            to_delete, skipped, scan_errors = scan_lambdas_in_region(
                session, "us-east-1", "streamsec", "123")

        self.assertEqual([d["function"] for d in to_delete], ["StreamSec_A"])
        self.assertEqual(len(skipped), 1)
        self.assertEqual(skipped[0]["function"], "StreamSec_B")
        self.assertEqual(skipped[0]["stack"], "LightlyticsStack-x")
        self.assertEqual(scan_errors, [])
        # list_tags must be called only for name-matched functions (A and B), not C
        self.assertEqual(client.list_tags.call_count, 2)
        # the live-stack protection path was actually exercised (stack list consulted)
        lls.assert_called_once()

    def test_tag_failure_excludes_function_and_records_error(self):
        client = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [{
            "Functions": [
                {"FunctionName": "StreamSec_ok", "FunctionArn": "arn:ok"},
                {"FunctionName": "StreamSec_bad", "FunctionArn": "arn:bad"},
            ]
        }]
        client.get_paginator.return_value = paginator

        def _tags(Resource):
            if Resource == "arn:bad":
                raise Exception("AccessDenied on ListTags")
            return {"Tags": {}}
        client.list_tags.side_effect = _tags
        session = _session_with_lambda(client)

        to_delete, skipped, scan_errors = scan_lambdas_in_region(
            session, "us-east-1", "streamsec", "123456789012")

        # A tag failure must NOT abort the region: the good function is still found,
        # the bad one is left out of to_delete and recorded as a scan error.
        self.assertEqual([d["function"] for d in to_delete], ["StreamSec_ok"])
        self.assertEqual(len(scan_errors), 1)
        self.assertEqual(scan_errors[0]["function"], "StreamSec_bad")
        self.assertIn("could not read tags", scan_errors[0]["reason"])


class TestDeleteLambdaFunction(unittest.TestCase):
    def test_deleted(self):
        client = MagicMock()
        client.exceptions.ResourceNotFoundException = type(
            "ResourceNotFoundException", (Exception,), {})
        session = _session_with_lambda(client)
        self.assertEqual(delete_lambda_function(session, "us-east-1", "fn"), "deleted")

    def test_already_gone(self):
        client = MagicMock()
        not_found = type("ResourceNotFoundException", (Exception,), {})
        client.exceptions.ResourceNotFoundException = not_found
        client.delete_function.side_effect = not_found()
        session = _session_with_lambda(client)
        self.assertEqual(delete_lambda_function(session, "us-east-1", "fn"), "already gone")


class TestListLiveStackNames(unittest.TestCase):
    def _session_with_pages(self, pages):
        client = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = pages
        client.get_paginator.return_value = paginator
        session = MagicMock()
        session.client.return_value = client
        return session, client

    def test_returns_live_names_and_filters_delete_complete(self):
        pages = [{"StackSummaries": [
            {"StackName": "live-a", "StackStatus": "CREATE_COMPLETE"},
            {"StackName": "gone-b", "StackStatus": "DELETE_COMPLETE"},
            {"StackName": "live-c", "StackStatus": "UPDATE_COMPLETE"},
        ]}]
        session, _ = self._session_with_pages(pages)
        result = list_live_stack_names(session, "us-east-1")
        self.assertEqual(result, {"live-a", "live-c"})
        # Built for the cloudformation service in the right region with the
        # shared retry/timeout config — guards against a wrong service string.
        session.client.assert_called_once_with(
            "cloudformation", region_name="us-east-1",
            config=boto_common.LAMBDA_CLIENT_CONFIG)

    def test_paginates_across_pages(self):
        pages = [
            {"StackSummaries": [{"StackName": "a", "StackStatus": "CREATE_COMPLETE"}]},
            {"StackSummaries": [{"StackName": "b", "StackStatus": "CREATE_COMPLETE"}]},
        ]
        session, _ = self._session_with_pages(pages)
        self.assertEqual(list_live_stack_names(session, "us-east-1"), {"a", "b"})

    def test_api_error_propagates(self):
        session = MagicMock()
        client = MagicMock()
        client.get_paginator.side_effect = RuntimeError("access denied")
        session.client.return_value = client
        with self.assertRaises(RuntimeError):
            list_live_stack_names(session, "us-east-1")


class TestScanOrphanClassification(unittest.TestCase):
    def _lambda_session(self, functions_and_tags):
        # functions_and_tags: list of (name, tags_dict)
        client = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [{"Functions": [
            {"FunctionName": n, "FunctionArn": f"arn:{n}"} for n, _ in functions_and_tags]}]
        client.get_paginator.return_value = paginator
        tags_by_arn = {f"arn:{n}": t for n, t in functions_and_tags}
        client.list_tags.side_effect = lambda Resource: {"Tags": tags_by_arn[Resource]}
        session = MagicMock()
        session.client.return_value = client
        return session

    @staticmethod
    def _cfn_tags(stack_name, region="us-east-1"):
        # A CFN-managed lambda carries both the stack-name and a stack-id ARN; the
        # scan uses the ARN's region to verify the stack in its OWNING region.
        return {
            boto_common.CFN_STACK_NAME_TAG: stack_name,
            boto_common.CFN_STACK_ID_TAG:
                f"arn:aws:cloudformation:{region}:123456789012:stack/{stack_name}/uuid",
        }

    def test_non_cfn_goes_to_delete_without_annotation(self):
        session = self._lambda_session([("MyCloudWatchColl", {})])
        with patch.object(boto_common, "list_live_stack_names") as lls:
            to_delete, skipped, errors = boto_common.scan_lambdas_in_region(
                session, "us-east-1", "cloudwatch", "123456789012")
        self.assertEqual(len(to_delete), 1)
        self.assertNotIn("orphaned_stack", to_delete[0])
        lls.assert_not_called()            # lazy: no CFN match -> never listed

    def test_cfn_with_live_stack_is_skipped(self):
        session = self._lambda_session([("MyCloudWatchColl", self._cfn_tags("live-stack"))])
        with patch.object(boto_common, "list_live_stack_names", return_value={"live-stack"}):
            to_delete, skipped, errors = boto_common.scan_lambdas_in_region(
                session, "us-east-1", "cloudwatch", "123456789012")
        self.assertEqual(to_delete, [])
        self.assertEqual(len(skipped), 1)
        self.assertEqual(skipped[0]["stack"], "live-stack")

    def test_cfn_with_missing_stack_is_orphan(self):
        session = self._lambda_session([("MyCloudWatchColl", self._cfn_tags("gone-stack"))])
        with patch.object(boto_common, "list_live_stack_names", return_value={"other"}):
            to_delete, skipped, errors = boto_common.scan_lambdas_in_region(
                session, "us-east-1", "cloudwatch", "123456789012")
        self.assertEqual(skipped, [])
        self.assertEqual(len(to_delete), 1)
        self.assertEqual(to_delete[0]["orphaned_stack"], "gone-stack")

    def test_stack_in_another_region_is_protected_not_orphan(self):
        # Stack lives in eu-west-1; scanning us-east-1 can't confirm it is gone,
        # so it must be protected (skipped), never deleted.
        session = self._lambda_session(
            [("MyCloudWatchColl", self._cfn_tags("elsewhere", region="eu-west-1"))])
        with patch.object(boto_common, "list_live_stack_names",
                          return_value=set()) as lls:
            to_delete, skipped, errors = boto_common.scan_lambdas_in_region(
                session, "us-east-1", "cloudwatch", "123456789012")
        self.assertEqual(to_delete, [])
        self.assertEqual(len(skipped), 1)
        lls.assert_not_called()            # never even lists this region's stacks

    def test_empty_stack_name_tag_is_protected_not_deleted(self):
        # stack-name tag present but empty -> can't identify the stack -> protect,
        # never fall through and delete it as a plain (non-CFN) function.
        session = self._lambda_session(
            [("MyCloudWatchColl", {boto_common.CFN_STACK_NAME_TAG: ""})])
        with patch.object(boto_common, "list_live_stack_names") as lls:
            to_delete, skipped, errors = boto_common.scan_lambdas_in_region(
                session, "us-east-1", "cloudwatch", "123456789012")
        self.assertEqual(to_delete, [])
        self.assertEqual(len(skipped), 1)
        lls.assert_not_called()

    def test_stack_in_another_account_is_protected_not_orphan(self):
        # Same region but the stack-id points at a DIFFERENT account. Listing THIS
        # account's stacks can't confirm that stack is gone, so it must be protected
        # (never deleted while it may be live in the other account).
        tags = {boto_common.CFN_STACK_NAME_TAG: "foreign",
                boto_common.CFN_STACK_ID_TAG:
                    "arn:aws:cloudformation:us-east-1:999999999999:stack/foreign/u"}
        session = self._lambda_session([("MyCloudWatchColl", tags)])
        with patch.object(boto_common, "list_live_stack_names",
                          return_value=set()) as lls:
            to_delete, skipped, errors = boto_common.scan_lambdas_in_region(
                session, "us-east-1", "cloudwatch", "123456789012")
        self.assertEqual(to_delete, [])
        self.assertEqual(len(skipped), 1)
        lls.assert_not_called()            # never lists the wrong account's stacks

    def test_missing_stack_id_tag_is_protected_not_orphan(self):
        # stack-name present but no stack-id ARN -> region unknown -> protect.
        session = self._lambda_session(
            [("MyCloudWatchColl", {boto_common.CFN_STACK_NAME_TAG: "no-id"})])
        with patch.object(boto_common, "list_live_stack_names",
                          return_value=set()) as lls:
            to_delete, skipped, errors = boto_common.scan_lambdas_in_region(
                session, "us-east-1", "cloudwatch", "123456789012")
        self.assertEqual(to_delete, [])
        self.assertEqual(len(skipped), 1)
        lls.assert_not_called()

    def test_access_denied_falls_back_to_skip_not_gap(self):
        # No cloudformation:ListStacks permission -> behave as before orphan
        # detection: skip the CFN-tagged function, no scan gap.
        denied = ClientError({"Error": {"Code": "AccessDenied", "Message": "no"}},
                             "ListStacks")
        session = self._lambda_session([("MyCloudWatchColl", self._cfn_tags("some-stack"))])
        with patch.object(boto_common, "list_live_stack_names", side_effect=denied):
            to_delete, skipped, errors = boto_common.scan_lambdas_in_region(
                session, "us-east-1", "cloudwatch", "123456789012")
        self.assertEqual(to_delete, [])
        self.assertEqual(errors, [])
        self.assertEqual(len(skipped), 1)

    def test_liststacks_error_becomes_scan_gap_not_delete(self):
        session = self._lambda_session([("MyCloudWatchColl", self._cfn_tags("gone-stack"))])
        with patch.object(boto_common, "list_live_stack_names",
                          side_effect=RuntimeError("boom")):
            to_delete, skipped, errors = boto_common.scan_lambdas_in_region(
                session, "us-east-1", "cloudwatch", "123456789012")
        self.assertEqual(to_delete, [])
        self.assertEqual(skipped, [])
        self.assertEqual(len(errors), 1)

    def test_liststacks_fetched_once_for_multiple_cfn_matches(self):
        session = self._lambda_session([
            ("CloudWatchA", self._cfn_tags("gone-1")),
            ("CloudWatchB", self._cfn_tags("live-2"))])
        with patch.object(boto_common, "list_live_stack_names",
                          return_value={"live-2"}) as lls:
            to_delete, skipped, errors = boto_common.scan_lambdas_in_region(
                session, "us-east-1", "cloudwatch", "123456789012")
        lls.assert_called_once()           # cached, not per-match
        self.assertEqual(len(to_delete), 1)   # gone-1 orphan
        self.assertEqual(len(skipped), 1)     # live-2 protected


class TestStackIdRegion(unittest.TestCase):
    def test_parses_region_and_account_from_arn(self):
        arn = "arn:aws:cloudformation:eu-west-1:123456789012:stack/my-stack/abc"
        self.assertEqual(boto_common.region_from_stack_id(arn), "eu-west-1")
        self.assertEqual(boto_common.account_from_stack_id(arn), "123456789012")

    def test_none_for_missing_or_malformed(self):
        for bad in (None, "", "not-an-arn", "arn:aws:cloudformation:us-east-1"):
            self.assertIsNone(boto_common.region_from_stack_id(bad))
            self.assertIsNone(boto_common.account_from_stack_id(bad))

    def test_none_for_non_cloudformation_arn(self):
        # A stray non-CFN ARN must not be read as a stack reference.
        arn = "arn:aws:lambda:us-east-1:123456789012:function:foo"
        self.assertIsNone(boto_common.region_from_stack_id(arn))
        self.assertIsNone(boto_common.account_from_stack_id(arn))


class TestAccessDeniedDetection(unittest.TestCase):
    def test_true_for_access_denied_client_error(self):
        for code in ("AccessDenied", "AccessDeniedException"):
            err = ClientError({"Error": {"Code": code, "Message": "x"}}, "ListStacks")
            self.assertTrue(boto_common.is_access_denied_error(err))

    def test_false_for_non_cfn_denial_codes(self):
        # CloudFormation ListStacks emits AccessDenied; EC2-style codes like these
        # are not emitted by CFN, so they fall through to the safe scan-gap path.
        for code in ("AuthorizationError", "UnauthorizedOperation"):
            err = ClientError(
                {"Error": {"Code": code, "Message": "x"},
                 "ResponseMetadata": {"HTTPStatusCode": 403}}, "ListStacks")
            self.assertFalse(boto_common.is_access_denied_error(err))

    def test_false_for_expired_token_even_though_403(self):
        # Transient credential expiry is HTTP 403 but must NOT be treated as a
        # permission denial — it should fall through to the scan-gap path.
        for code in ("ExpiredToken", "ExpiredTokenException", "RequestExpired"):
            err = ClientError(
                {"Error": {"Code": code, "Message": "x"},
                 "ResponseMetadata": {"HTTPStatusCode": 403}}, "ListStacks")
            self.assertFalse(boto_common.is_access_denied_error(err))

    def test_false_for_other_errors(self):
        throttle = ClientError(
            {"Error": {"Code": "Throttling", "Message": "x"},
             "ResponseMetadata": {"HTTPStatusCode": 400}}, "ListStacks")
        self.assertFalse(boto_common.is_access_denied_error(throttle))
        self.assertFalse(boto_common.is_access_denied_error(RuntimeError("plain")))


if __name__ == "__main__":
    unittest.main()
