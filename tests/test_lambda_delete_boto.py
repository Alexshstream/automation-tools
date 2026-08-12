import os
import sys
import unittest
from unittest.mock import MagicMock, patch

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
            {"Tags": {CFN_STACK_NAME_TAG: "LightlyticsStack-x"}} if Resource == "arn:B"
            else {"Tags": {}}
        )
        session = _session_with_lambda(client)

        # StreamSec_B's stack is live, so it stays protected (skipped_cfn) rather
        # than being reclassified as an orphan.
        with patch.object(boto_common, "list_live_stack_names",
                           return_value={"LightlyticsStack-x"}):
            to_delete, skipped, scan_errors = scan_lambdas_in_region(session, "us-east-1", "streamsec")

        self.assertEqual([d["function"] for d in to_delete], ["StreamSec_A"])
        self.assertEqual(len(skipped), 1)
        self.assertEqual(skipped[0]["function"], "StreamSec_B")
        self.assertEqual(skipped[0]["stack"], "LightlyticsStack-x")
        self.assertEqual(scan_errors, [])
        # list_tags must be called only for name-matched functions (A and B), not C
        self.assertEqual(client.list_tags.call_count, 2)

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

        to_delete, skipped, scan_errors = scan_lambdas_in_region(session, "us-east-1", "streamsec")

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

    def test_non_cfn_goes_to_delete_without_annotation(self):
        session = self._lambda_session([("MyCloudWatchColl", {})])
        with patch.object(boto_common, "list_live_stack_names") as lls:
            to_delete, skipped, errors = boto_common.scan_lambdas_in_region(
                session, "us-east-1", "cloudwatch")
        self.assertEqual(len(to_delete), 1)
        self.assertNotIn("orphaned_stack", to_delete[0])
        lls.assert_not_called()            # lazy: no CFN match -> never listed

    def test_cfn_with_live_stack_is_skipped(self):
        tags = {boto_common.CFN_STACK_NAME_TAG: "live-stack"}
        session = self._lambda_session([("MyCloudWatchColl", tags)])
        with patch.object(boto_common, "list_live_stack_names", return_value={"live-stack"}):
            to_delete, skipped, errors = boto_common.scan_lambdas_in_region(
                session, "us-east-1", "cloudwatch")
        self.assertEqual(to_delete, [])
        self.assertEqual(len(skipped), 1)
        self.assertEqual(skipped[0]["stack"], "live-stack")

    def test_cfn_with_missing_stack_is_orphan(self):
        tags = {boto_common.CFN_STACK_NAME_TAG: "gone-stack"}
        session = self._lambda_session([("MyCloudWatchColl", tags)])
        with patch.object(boto_common, "list_live_stack_names", return_value={"other"}):
            to_delete, skipped, errors = boto_common.scan_lambdas_in_region(
                session, "us-east-1", "cloudwatch")
        self.assertEqual(skipped, [])
        self.assertEqual(len(to_delete), 1)
        self.assertEqual(to_delete[0]["orphaned_stack"], "gone-stack")

    def test_liststacks_error_becomes_scan_gap_not_delete(self):
        tags = {boto_common.CFN_STACK_NAME_TAG: "gone-stack"}
        session = self._lambda_session([("MyCloudWatchColl", tags)])
        with patch.object(boto_common, "list_live_stack_names",
                          side_effect=RuntimeError("denied")):
            to_delete, skipped, errors = boto_common.scan_lambdas_in_region(
                session, "us-east-1", "cloudwatch")
        self.assertEqual(to_delete, [])
        self.assertEqual(skipped, [])
        self.assertEqual(len(errors), 1)

    def test_liststacks_fetched_once_for_multiple_cfn_matches(self):
        t1 = {boto_common.CFN_STACK_NAME_TAG: "gone-1"}
        t2 = {boto_common.CFN_STACK_NAME_TAG: "live-2"}
        session = self._lambda_session([("CloudWatchA", t1), ("CloudWatchB", t2)])
        with patch.object(boto_common, "list_live_stack_names",
                          return_value={"live-2"}) as lls:
            to_delete, skipped, errors = boto_common.scan_lambdas_in_region(
                session, "us-east-1", "cloudwatch")
        lls.assert_called_once()           # cached, not per-match
        self.assertEqual(len(to_delete), 1)   # gone-1 orphan
        self.assertEqual(len(skipped), 1)     # live-2 protected


if __name__ == "__main__":
    unittest.main()
