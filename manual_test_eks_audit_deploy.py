"""
THROWAWAY manual test script - not part of the CLI tool, safe to delete.

Exercises deploy_eks_audit_logs_stacks + sweep_stack_statuses directly
against real AWS, bypassing the org-wide account enumeration entirely (no
organizations:ListAccounts needed - just direct credentials to whatever
account you're running this in). Useful for testing the EKS audit-log
detect-and-deploy feature when you don't have org management-account access.

Usage: python3 manual_test_eks_audit_deploy.py
(run from the repo root, with AWS credentials for the target account active)
"""
import sys
import boto3

sys.path.insert(0, '.')
from src.python.common.boto_common import deploy_eks_audit_logs_stacks, sweep_stack_statuses

session = boto3.Session()
account_id = session.client('sts').get_caller_identity()['Account']
sub_account = (account_id, "manual-test-account")

sub_account_information = {
    "lightlytics_collection_token": "dummy-test-token-not-real",
}

print("=" * 70)
print("STEP 1: deploy_eks_audit_logs_stacks (fire-and-forget submission)")
print("=" * 70)
records = deploy_eks_audit_logs_stacks(
    environment_url="https://test.streamsec.io",
    sub_account_information=sub_account_information,
    sub_account_session=session,
    sub_account=sub_account,
    eks_audit_logs_regions=["us-west-2"],
    random_int=777777,
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
    print("STEP 2: sweep_stack_statuses (the honest end-of-run status check)")
    print("=" * 70)
    sts_client = session.client('sts')
    swept = sweep_stack_statuses(
        records, sts_client, management_account_id=account_id, poll_interval=10, timeout=180)

    print()
    print("Final swept records:")
    for r in swept:
        print(f"  {r['account']} | {r['region']} | {r['stack_type']} | "
              f"{r.get('final_status')} | reason: {r.get('status_reason')}")
