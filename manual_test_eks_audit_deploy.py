"""
THROWAWAY manual test script - not part of the CLI tool, safe to delete.

Exercises deploy_eks_audit_logs_stacks + sweep_stack_statuses directly
against real AWS, bypassing the org-wide account enumeration entirely (no
organizations:ListAccounts needed - just direct credentials to whatever
account you're running this in). Useful for testing the EKS audit-log
detect-and-deploy feature when you don't have org management-account access.

Usage: python3 manual_test_eks_audit_deploy.py
(run from the repo root, with AWS credentials for the target account active)

IMPORTANT: eks_audit_logs_regions is left unset (None) below, on purpose -
that's what makes deploy_eks_audit_logs_stacks actually run its own
auto-detection (get_active_eks_regions) internally, rather than being told
which region to use. CANDIDATE_REGIONS below is a deliberately broad list
mixing regions that do and don't have EKS clusters, so you can see the
detection logic correctly separate them.
"""
import sys
import boto3

sys.path.insert(0, '.')
from src.python.common.boto_common import deploy_eks_audit_logs_stacks, sweep_stack_statuses

# 5 candidate regions, 2 of which have EKS clusters (us-east-1, us-west-2 -
# should be detected and deployed-to/skipped-as-already-covered); the other
# 3 (us-east-2, eu-west-1, eu-central-1) have none and should be silently
# excluded by detection.
CANDIDATE_REGIONS = ["us-east-1", "us-west-2", "us-east-2", "eu-west-1", "eu-central-1"]

session = boto3.Session()
account_id = session.client('sts').get_caller_identity()['Account']
sub_account = (account_id, "manual-test-account")

sub_account_information = {
    "lightlytics_collection_token": "dummy-test-token-not-real",
    "cloud_regions": CANDIDATE_REGIONS,
}

print("=" * 70)
print(f"Candidate regions given to auto-detection: {CANDIDATE_REGIONS}")
print("=" * 70)
print("STEP 1: Submitting stacks for detected EKS regions")
print("=" * 70)
records = deploy_eks_audit_logs_stacks(
    environment_url="https://test.streamsec.io",
    sub_account_information=sub_account_information,
    sub_account_session=session,
    sub_account=sub_account,
    eks_audit_logs_regions=None,
    random_int=555555,
    custom_tags=None,
    wait=False,
)

print()
print("Records returned:", records)

if not records:
    print("No records to sweep (e.g. the audit-log lambda already exists there).")
else:
    print()
    print("=" * 70)
    print("STEP 2: Verifying final stack status")
    print("=" * 70)
    sts_client = session.client('sts')
    swept = sweep_stack_statuses(
        records, sts_client, management_account_id=account_id, poll_interval=10, timeout=180)

    print()
    print("Final swept records:")
    for r in swept:
        print(f"  {r['account']} | {r['region']} | {r['stack_type']} | "
              f"{r.get('final_status')} | reason: {r.get('status_reason')}")
