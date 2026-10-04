import argparse
import boto3
import json
import time
import zipfile
import shutil
import os
import tempfile

parser = argparse.ArgumentParser(description="Streamsec Organization Lambda Setup Script")
parser.add_argument("--environment", required=False, help="The environment name in which the Lambda function will operate.")
parser.add_argument("--user-name", required=False, help="The username for the environment. Ignored if --api-token is provided.")
parser.add_argument("--password", required=False, help="The password for the environment. Ignored if --api-token is provided.")
parser.add_argument("--api-token", required=False, help="Stream Security API token. If set, --user-name and --password are not required.")
parser.add_argument("--aws-profile", required=False, help="AWS profile name to use. If omitted, the default credential chain (env vars, SSO session, instance role, etc.) is used.")
parser.add_argument("--cleanup", action="store_true", help="Clean up the resources created by the script.")
parser.add_argument("--accounts", required=False, help="manually specify accounts to integrate.")
parser.add_argument("--ws-id", required=False, help="The workspace ID.")
parser.add_argument("--control-role", default="OrganizationAccountAccessRole", help="The control role name for assuming the role in the target account.", required=False)
parser.add_argument("--response", action="store_true", help="Enable creation of the response stack.")
parser.add_argument("--response-region", default="us-east-1", help="Region for the response stack only. Does not affect collection regions, see --regions.")
parser.add_argument("--response-exclude-runbooks", help="Comma separated list of runbooks to exclude from response stack.")
parser.add_argument("--regions", required=False, help="Comma separated list of regions to onboard each account to (collection stacks). If omitted, only regions with EC2 instances are detected, plus us-east-1, so new accounts without instances get us-east-1 only.")
parser.add_argument("--eks-audit-logs", action="store_true", help="Enable creation of the EKS audit logs.")
parser.add_argument("--eks-audit-logs-regions", required=False, help="Comma separated list of regions to enable EKS audit logs.")
parser.add_argument("--invoke-after-deploy", action="store_true", help="Invoke the Lambda asynchronously after deploy to onboard existing accounts immediately.")
parser.add_argument("--schedule-scan-days", type=int, required=False, help="Create a scheduled EventBridge rule to invoke the Lambda every X days (e.g. 1 for daily). Useful for syncing account name changes and other drift.")
args = parser.parse_args()

# Apply AWS profile before creating any boto3 clients. If not set, boto3 falls
# back to its default credential chain (env vars, SSO, instance role, etc.).
if args.aws_profile:
    os.environ['AWS_PROFILE'] = args.aws_profile


def _aws_clients():
    return (
        boto3.client('iam', region_name='us-east-1'),
        boto3.client('sts', region_name='us-east-1'),
        boto3.client('lambda', region_name='us-east-1'),
        boto3.client('events', region_name='us-east-1'),
    )


def _build_env_vars(args):
    env_vars = {
        "ENVIRONMENT": args.environment,
        "WS_ID": args.ws_id,
        "PARALLEL": "8",
        "CONTROL_ROLE": args.control_role,
        "RESPONSE": str(args.response).lower(),
        "RESPONSE_REGION": args.response_region,
        "EKS_AUDIT_LOGS": str(args.eks_audit_logs).lower(),
    }
    if args.api_token:
        env_vars["API_TOKEN"] = args.api_token
    else:
        env_vars["ENVIRONMENT_USER_NAME"] = args.user_name
        env_vars["ENVIRONMENT_PASSWORD"] = args.password
    if args.accounts is not None:
        env_vars["ACCOUNTS"] = args.accounts
        
    if args.response_exclude_runbooks is not None:
        env_vars["RESPONSE_EXCLUDE_RUNBOOKS"] = args.response_exclude_runbooks

    if args.eks_audit_logs_regions is not None:
        env_vars["EKS_AUDIT_LOGS_REGIONS"] = args.eks_audit_logs_regions

    if args.regions is not None:
        env_vars["REGIONS"] = ",".join(_parse_regions(args.regions))

    return env_vars


def _parse_regions(value):
    return [r.strip() for r in value.split(",") if r.strip()]


def _check_regions(requested, ec2_client):
    """Split --regions into names AWS doesn't know (typos) and real regions
    that aren't enabled in this account. Opt-in regions can be enabled per
    account, so the second group is a warning, not an error."""
    known = {r["RegionName"]: r.get("OptInStatus")
             for r in ec2_client.describe_regions(AllRegions=True)["Regions"]}
    unknown = [r for r in requested if r not in known]
    not_enabled = [r for r in requested if known.get(r) == "not-opted-in"]
    return unknown, not_enabled


def main():
    missing_args = []
    if not args.environment:
        missing_args.append("--environment")
    if not args.api_token:
        if not args.user_name:
            missing_args.append("--user-name")
        if not args.password:
            missing_args.append("--password")
    if not args.ws_id:
        missing_args.append("--ws-id")

    if missing_args:
        print(f"Missing required arguments: {', '.join(missing_args)}")
        print("Either --api-token, or both --user-name and --password, must be provided (unless --cleanup is specified).")
        return

    if args.regions is not None and not _parse_regions(args.regions):
        print("Error: --regions was given but contains no regions.")
        return

    iam_client, sts_client, lambda_client, events_client = _aws_clients()
    
    # verify the region is us-east-1
    region = boto3.Session().region_name
    if region != "us-east-1":
        print("The region must be us-east-1.")
        return

    if args.regions is not None:
        # REGIONS applies to every account in the organization, so one bad
        # name would fail every account on every run. Stop here instead.
        unknown, not_enabled = _check_regions(
            _parse_regions(args.regions), boto3.client('ec2', region_name='us-east-1'))
        if unknown:
            print(f"Error: unknown region(s) in --regions: {', '.join(unknown)}. "
                  f"Check for typos (e.g. us-west-2, not us-west2).")
            return
        if not_enabled:
            print(f"Warning: {', '.join(not_enabled)} not enabled in this account. "
                  f"Accounts without these regions enabled will skip them.")
    else:
        print("Note: --regions not set. Accounts will be onboarded to regions with EC2 instances, "
              "plus us-east-1, so new accounts without instances get us-east-1 only.")
    
    print("Welcome to the Streamsec Organization Lambda Setup Script!")
    print("This script will perform the following actions:")
    print("1. Create an IAM policy with permissions to list AWS accounts and describe EC2 regions.")
    print("2. Create an IAM role for the Lambda function with the necessary assume role policy.")
    print("3. Attach the created policy and the AWS Lambda basic execution role policy to the IAM role.")
    print("4. Create a Lambda function with the specified configurations.")
    print("5. Create an EventBridge rule to trigger the Lambda function when a new AWS account is created.")
    print("6. Add necessary permissions for EventBridge to invoke the Lambda function.")
    print("7. Set the EventBridge rule target to the Lambda function.")

    proceed = input("Do you want to proceed with these actions? (yes/no): ")
    if proceed.lower() != "yes":
        print("Script execution aborted by the user.")
        return

    print("Starting the setup process...")

    aws_account_id = sts_client.get_caller_identity()['Account']
    region = boto3.Session().region_name

    # Create IAM policy
    policy_name = "streamsec-organization-lambda-policy"
    policy_document = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "VisualEditor0",
                "Effect": "Allow",
                "Action": ["organizations:ListAccounts", "ec2:DescribeRegions"],
                "Resource": "*"
            },
            {
                "Sid": "VisualEditor1",
                "Effect": "Allow",
                "Action": "sts:AssumeRole",
                "Resource": f"arn:aws:iam::*:role/{args.control_role}"
            }
        ]
    }
    policy_response = iam_client.create_policy(
        PolicyName=policy_name,
        PolicyDocument=json.dumps(policy_document)
    )
    policy_arn = policy_response['Policy']['Arn']

    # Create IAM role
    role_name = "streamsec-organization-lambda-role"
    assume_role_policy_document = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "lambda.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }
        ]
    }
    role_response = iam_client.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=json.dumps(assume_role_policy_document)
    )
    role_arn = role_response['Role']['Arn']

    # Attach policies to role
    iam_client.attach_role_policy(
        RoleName=role_name,
        PolicyArn=policy_arn
    )
    iam_client.attach_role_policy(
        RoleName=role_name,
        PolicyArn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
    )

    time.sleep(10)  # Wait for role + cross-account propagation; 5s was too tight when --invoke-after-deploy hits AssumeRole right after deploy.

    # --- Create deployment package (zip) with dependencies ---
    zip_filename = "lambda_deploy.zip"
    with tempfile.TemporaryDirectory() as build_dir:
        # Install dependencies into build_dir
        os.system(f"pip install -r lambda/organization_integration/requirements.txt -t {build_dir}")
        # Copy app.py into build_dir
        shutil.copy("lambda/organization_integration/app.py", os.path.join(build_dir, "app.py"))
        # Copy src into build_dir/src
        shutil.copytree("src", os.path.join(build_dir, "src"))
        # Zip everything in build_dir
        with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for foldername, subfolders, filenames in os.walk(build_dir):
                for filename in filenames:
                    filepath = os.path.join(foldername, filename)
                    arcname = os.path.relpath(filepath, build_dir)
                    zipf.write(filepath, arcname=arcname)

    # Create Lambda function
    function_name = "streamsec-organization-lambda"
    env_vars = _build_env_vars(args)

    with open(zip_filename, 'rb') as f:
        zipped_code = f.read()

    lambda_client.create_function(
        FunctionName=function_name,
        Runtime="python3.12",
        Role=role_arn,
        Handler="app.lambda_handler",
        Code={
            'ZipFile': zipped_code
        },
        MemorySize=2048,
        Timeout=900,
        Environment={"Variables": env_vars},
        Publish=True
    )

    # Clean up zip file
    os.remove(zip_filename)

    # Create EventBridge rule
    rule_name = "streamsec-organization-newaccount-rule"
    # CreateAccountResult lands as a service event (async completion of CreateAccount),
    # not as an API call event. Including both detail-types so the rule matches regardless
    # of how AWS classifies the record.
    event_pattern = {
        "source": ["aws.organizations"],
        "detail-type": [
            "AWS API Call via CloudTrail",
            "AWS Service Event via CloudTrail",
        ],
        "detail": {"eventName": ["CreateAccountResult"]}
    }
    events_client.put_rule(
        Name=rule_name,
        EventPattern=json.dumps(event_pattern),
        Description="Rule to trigger Lambda when a new AWS account is created"
    )

    # Add permission for EventBridge to invoke Lambda
    lambda_client.add_permission(
        FunctionName=function_name,
        StatementId="EventBridgeInvokeLambda",
        Action="lambda:InvokeFunction",
        Principal="events.amazonaws.com",
        SourceArn=f"arn:aws:events:{region}:{aws_account_id}:rule/{rule_name}"
    )

    # Set EventBridge rule target to Lambda function
    events_client.put_targets(
        Rule=rule_name,
        Targets=[
            {
                "Id": "1",
                "Arn": f"arn:aws:lambda:{region}:{aws_account_id}:function:{function_name}"
            }
        ]
    )

    # Create scheduled EventBridge rule if --schedule-scan-days is set.
    # A scheduled scan is the only way to detect account name changes, because
    # the PutAccountName CloudTrail event only appears in the member account's
    # trail, not the management account where this EventBridge rule lives.
    if args.schedule_scan_days:
        if args.schedule_scan_days <= 0:
            print("Error: --schedule-scan-days must be a positive integer.")
            return
        scheduled_rule_name = "streamsec-organization-scheduled-scan"
        schedule_expr = f"rate({args.schedule_scan_days} day)" if args.schedule_scan_days == 1 else f"rate({args.schedule_scan_days} days)"
        events_client.put_rule(
            Name=scheduled_rule_name,
            ScheduleExpression=schedule_expr,
            Description=f"Scheduled rule to invoke the organization Lambda every {args.schedule_scan_days} day(s)"
        )
        try:
            lambda_client.add_permission(
                FunctionName=function_name,
                StatementId="ScheduledScanInvokeLambda",
                Action="lambda:InvokeFunction",
                Principal="events.amazonaws.com",
                SourceArn=f"arn:aws:events:{region}:{aws_account_id}:rule/{scheduled_rule_name}"
            )
        except lambda_client.exceptions.ResourceConflictException:
            pass
        events_client.put_targets(
            Rule=scheduled_rule_name,
            Targets=[
                {
                    "Id": "1",
                    "Arn": f"arn:aws:lambda:{region}:{aws_account_id}:function:{function_name}"
                }
            ]
        )
        print(f"Scheduled scan rule created: runs every {args.schedule_scan_days} day(s)")

    print("Setup completed successfully!")

    if args.invoke_after_deploy:
        print("Waiting for Lambda to become active...")
        waiter = lambda_client.get_waiter('function_active_v2')
        waiter.wait(FunctionName=function_name)
        print("Invoking the Lambda asynchronously to onboard existing accounts...")
        lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='Event',
            Payload=b'{}',
        )
        profile_flag = f" --profile {args.aws_profile}" if args.aws_profile else ""
        print(
            "Lambda invoked. Tail logs with:\n"
            f"  aws logs tail /aws/lambda/{function_name} --follow --since 1m"
            f" --region us-east-1{profile_flag}"
        )

def cleanup():
    proceed = input("Do you want to cleanup this script resources? (yes/no): ")
    if proceed.lower() != "yes":
        print("Script execution aborted by the user.")
        return
    # try to delete the resources created by the script
    # if resources are not found, ignore the exception

    iam_client, sts_client, lambda_client, events_client = _aws_clients()
    aws_account_id = sts_client.get_caller_identity()['Account']

    function_name = "streamsec-organization-lambda"

    try:
        print("Removing scheduled scan Lambda permission...")
        lambda_client.remove_permission(FunctionName=function_name, StatementId="ScheduledScanInvokeLambda")
    except lambda_client.exceptions.ResourceNotFoundException:
        pass

    try:
        # Delete the Lambda function
        print("Deleting the Lambda function...")
        lambda_client.delete_function(FunctionName=function_name)
    except lambda_client.exceptions.ResourceNotFoundException:
        print("Lambda function not found.")
        pass

    try:
        # Delete the EventBridge rule
        print("Deleting the EventBridge rule...")
        events_client.remove_targets(Rule="streamsec-organization-newaccount-rule", Ids=["1"])
        events_client.delete_rule(Name="streamsec-organization-newaccount-rule")
    except events_client.exceptions.ResourceNotFoundException:
        print("EventBridge rule not found.")
        pass

    try:
        # Delete the scheduled scan rule
        print("Deleting the scheduled scan rule...")
        events_client.remove_targets(Rule="streamsec-organization-scheduled-scan", Ids=["1"])
        events_client.delete_rule(Name="streamsec-organization-scheduled-scan")
    except events_client.exceptions.ResourceNotFoundException:
        print("Scheduled scan rule not found.")
        pass
    
    try:
        # Detach and delete the IAM role
        print("Deleting the IAM role...")
        role_name = "streamsec-organization-lambda-role"
        try:
            iam_client.detach_role_policy(RoleName=role_name, PolicyArn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole")
        except iam_client.exceptions.NoSuchEntityException:
            print("AWSLambdaBasicExecutionRole policy not attached.")
        try:
            iam_client.detach_role_policy(RoleName=role_name, PolicyArn="arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole")
        except iam_client.exceptions.NoSuchEntityException:
            print("AWSLambdaVPCAccessExecutionRole policy not attached.")
        try:
            iam_client.detach_role_policy(RoleName=role_name, PolicyArn=f"arn:aws:iam::{aws_account_id}:policy/streamsec-organization-lambda-policy")
        except iam_client.exceptions.NoSuchEntityException:
            print("Streamsec Organization Lambda policy not attached.")
        print("Detached the policies from the IAM role.")
        iam_client.delete_role(RoleName=role_name)
        print("Deleted the IAM role.")
    except iam_client.exceptions.NoSuchEntityException:
        print("IAM role not found.")
        pass
    
    try:
        # Delete the IAM policy
        print("Deleting the IAM policy...")
        iam_client.delete_policy(PolicyArn=f"arn:aws:iam::{aws_account_id}:policy/streamsec-organization-lambda-policy")
    except iam_client.exceptions.NoSuchEntityException:
        print("IAM policy not found.")
        pass
    
    print("Cleanup completed successfully!")

if __name__ == "__main__":
    if not args.cleanup:
        main()
    else:
        cleanup()