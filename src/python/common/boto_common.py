import concurrent.futures
import datetime
import time
from termcolor import colored as color
import os
from botocore.config import Config

def get_all_accounts(org_client):
    list_accounts = []
    next_token = None
    while True:
        if next_token:
            list_accounts_operation = org_client.list_accounts(NextToken=next_token)
        else:
            list_accounts_operation = org_client.list_accounts()
        list_accounts.extend(list_accounts_operation["Accounts"])
        if 'NextToken' in list_accounts_operation:
            next_token = list_accounts_operation["NextToken"]
        else:
            break
    return list_accounts


def wait_for_cloudformation(sub_account, cft_id, cf_client, timeout=240):
    """ Wait for stack to be deployed.
        :param sub_account (tup)    - Relevant account.
        :param timeout (int)        - Max waiting time; Defaults to 240.
        :param cft_id (str)         - Stack ID.
        :param cf_client (object)   - CF Session.
    """
    time.sleep(10)

    dt_start = datetime.datetime.utcnow()
    dt_diff = 0

    print(color(
        f"Account: {sub_account[0]} | Waiting for stack to finish creating, timeout is {timeout} seconds", "blue"))
    while dt_diff < timeout:
        stack_list = cf_client.list_stacks()
        status = [stack['StackStatus'] for stack in stack_list['StackSummaries'] if stack['StackId'] == cft_id][0]
        dt_finish = datetime.datetime.utcnow()
        dt_diff = (dt_finish - dt_start).total_seconds()

        if status == 'CREATE_COMPLETE':
            print(color(f'Account: {sub_account[0]} | Stack deployed successfully after {dt_diff} seconds', "green"))
            break
        elif status == 'ROLLBACK_IN_PROGRESS':
            err_msg = f"Account: {sub_account[0]} | Stack {cft_id} failed"
            print(color(err_msg, "red"))
            raise Exception(err_msg)
        else:
            time.sleep(1)
    if dt_diff >= timeout:
        print(color(f"Account: {sub_account[0]} | Timed out before stack has been created/deleted", "red"))
        return False
    return True


def create_stack_payload(stack_name, sub_account_template_url, custom_tags=None, params=None):
    stack_creation_payload = {
        "StackName": stack_name,
        "Capabilities": ['CAPABILITY_IAM', 'CAPABILITY_NAMED_IAM', 'CAPABILITY_AUTO_EXPAND'],
        "OnFailure": 'ROLLBACK',
        "EnableTerminationProtection": False,
        "TemplateURL": sub_account_template_url,
    }
    if custom_tags:
        stack_creation_payload['Tags'] = custom_tags
        
    if params:
        stack_creation_payload['Parameters'] = params
        
    return stack_creation_payload


def get_active_regions(sub_account_session, regions):
    active_regions = [sub_account_session.region_name]
    for region in regions:
        try:
            ec2_client = sub_account_session.client('ec2', region_name=region)
            instances = ec2_client.describe_instances()["Reservations"][0]["Instances"]
            if len(instances) > 0:
                active_regions.append(region)
        except:
            continue
    if "us-east-1" not in active_regions:
        active_regions.append("us-east-1")
    return list(set(active_regions))

def get_active_eks_regions(sub_account_session, regions):
    active_regions = []
    for region in regions:
        try:
            eks_client = sub_account_session.client('eks', region_name=region)
            eks_clusters = eks_client.list_clusters()
            if len(eks_clusters['clusters']) > 0:
                active_regions.append(region)
        except:
            continue
    return active_regions

def deploy_all_collection_stacks(
        active_regions, sub_account_session, random_int, account_information, sub_account, custom_tags=None):
    print(color(
        f"Account: {sub_account[0]} | Adding collection CFT stack for realtime events for each region in parallel "
        f"(Max 8 workers)", color="blue"))
    # List to hold the concurrent futures
    futures = []
    # Create a ThreadPoolExecutor with max_workers
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        # Iterate over active_regions and submit each task to the executor
        for region in active_regions:
            future = executor.submit(deploy_collection_stack, account_information,
                                     sub_account_session, sub_account, region, random_int, custom_tags, False)
            futures.append(future)
    # Wait for all the tasks to complete
    concurrent.futures.wait(futures)
    print(color(f"Account: {sub_account[0]} | Realtime enabled in regions: {active_regions}", "green"))
    return


def deploy_collection_stack(
        account_information, sub_account_session, sub_account, region, random_int, custom_tags, wait=True):
    # Existing code inside the for loop
    print(color(f"Account: {sub_account[0]} | Adding collection CFT stack for {region}", "blue"))
    region_client = sub_account_session.client('cloudformation', region_name=region)
    stack_creation_payload = create_stack_payload(
        f"LightlyticsStack-collection-{region}-{random_int}",
        account_information["collection_template_url"], custom_tags=custom_tags)
    collection_stack_id = region_client.create_stack(**stack_creation_payload)["StackId"]
    print(color(f"Account: {sub_account[0]} | Collection stack {collection_stack_id} deploying", "blue"))

    if wait:
        print(color(f"Account: {sub_account[0]} | Waiting for the stack to finish deploying successfully", "blue"))
        wait_for_cloudformation(sub_account, collection_stack_id, region_client)

def deploy_response_stack(
        environment_url, account_information, sub_account_session, sub_account, region, random_int, custom_tags, response_exclude_runbooks, wait=True):
    print(color(f"Account: {sub_account[0]} | Adding response CFT stack for {region}", "blue"))
    region_client = sub_account_session.client('cloudformation', region_name=region)
    
    params = [
        {
            "ParameterKey": "APIUrl",
            "ParameterValue": environment_url
        },
        {
            "ParameterKey": "APIToken",
            "ParameterValue": account_information["lightlytics_collection_token"]
        },
        {
            "ParameterKey": "ExternalId",
            "ParameterValue": account_information["external_id"]
        },
        {
            "ParameterKey": "TrustedAccountId",
            "ParameterValue": os.environ.get("STREAM_ACCOUNT_ID", "624907860825")
        }
    ]
    
    if response_exclude_runbooks != "":
        for runbook in response_exclude_runbooks.split(","):
            params.append({
                "ParameterKey": f"{runbook}Enabled",
                "ParameterValue": "false"
            })
    
    
    stack_creation_payload = create_stack_payload(
        f"LightlyticsStack-response-{region}-{random_int}",
        os.environ.get("STREAM_RESPONSE_CFT_URL", f"https://prod-lightlytics-public-cloudformation.s3.amazonaws.com/stream-security-remediation-latest-{region}.yaml"), custom_tags=custom_tags , params=params)
    response_stack_id = region_client.create_stack(**stack_creation_payload)["StackId"]
    print(color(f"Account: {sub_account[0]} | response stack {response_stack_id} deploying", "blue"))
    
    if wait:
        print(color(f"Account: {sub_account[0]} | Waiting for the stack to finish deploying successfully", "blue"))
        wait_for_cloudformation(sub_account, response_stack_id, region_client)
        print(color(f"Account: {sub_account[0]} | response stack deployed successfully", "green"))

def deploy_eks_audit_logs_stacks(
        environment_url, sub_account_information, sub_account_session, sub_account, eks_audit_logs_regions, random_int, custom_tags, wait=True):
    if not eks_audit_logs_regions:
        eks_audit_logs_regions = get_active_eks_regions(sub_account_session, sub_account_information["cloud_regions"])
    
    if not eks_audit_logs_regions:
        print(color(f"Account: {sub_account[0]} | No active EKS regions found, skipping EKS audit logs for {sub_account[0]}", "blue"))
        return
    
    params = [
        {
            "ParameterKey": "APIUrl",
            "ParameterValue": environment_url
        },
        {
            "ParameterKey": "APICollectionToken",
            "ParameterValue": sub_account_information["lightlytics_collection_token"]
        },
        {
            "ParameterKey": "EKSAuditCollectorPrefix",
            "ParameterValue": environment_url.split("//")[1].split(".")[0]
        }
    ]
    
    for region in eks_audit_logs_regions:
        # check if there is already a lambda function in the region
        region_client = sub_account_session.client('lambda', region_name=region)
        region_cloudformation_client = sub_account_session.client('cloudformation', region_name=region)
        try:
            region_client.get_function(FunctionName='StreamSec_EKSCloudWatchSubscriptionsFunction')
            print(color(f"Account: {sub_account[0]} | EKS audit logs lambda already exists in region {region}, skipping", "blue"))
            continue
        except region_client.exceptions.ResourceNotFoundException:
            pass
        
        print(color(f"Account: {sub_account[0]} | Adding EKS audit logs CFT stack for {region}", "blue"))
        stack_creation_payload = create_stack_payload(
            f"StreamSecurity-eks-audit-logs-{region}-{random_int}",
            os.environ.get("STREAM_EKS_AUDIT_LOGS_CFT_URL", f"https://public-lightlytics-cft.s3.amazonaws.com/eks-audit-collector-latest.yaml"), custom_tags=custom_tags, params=params)
        eks_audit_logs_stack_id = region_cloudformation_client.create_stack(**stack_creation_payload)["StackId"]
        print(color(f"Account: {sub_account[0]} | EKS audit logs stack {eks_audit_logs_stack_id} deploying", "blue"))
        
        if wait:
            print(color(f"Account: {sub_account[0]} | Waiting for the stack to finish deploying successfully", "blue"))
            wait_for_cloudformation(sub_account, eks_audit_logs_stack_id, region_cloudformation_client)
            print(color(f"Account: {sub_account[0]} | EKS audit logs stack deployed successfully", "green"))
        else:
            print(color(f"Account: {sub_account[0]} | EKS audit logs stack deployed successfully", "green"))

def deploy_init_stack(account_information, graph_client, sub_account, sub_account_session, random_int, wait=True,
                      custom_tags=None):
    sub_account_template_url = account_information["template_url"]
    print(color(f"Account: {sub_account[0]} | Finished fetching information", "green"))

    # Initializing "cloudformation" boto client
    cf = sub_account_session.client('cloudformation')

    print(color(f"Account: {sub_account[0]} | Creating the CFT stack using Boto", "blue"))
    stack_creation_payload = create_stack_payload(
        f"LightlyticsStack-{random_int}", sub_account_template_url, custom_tags=custom_tags)
    sub_account_stack_id = cf.create_stack(**stack_creation_payload)["StackId"]
    print(color(f"Account: {sub_account[0]} | {sub_account_stack_id} Created successfully", "green"))

    if wait:
        print(color(f"Account: {sub_account[0]} | Waiting for the stack to finish deploying successfully", "blue"))
        wait_for_cloudformation(sub_account, sub_account_stack_id, cf)

        print(color(f"Account: {sub_account[0]} | "
                    f"Waiting for the account to finish integrating with Lightlytics", "blue"))
        account_status = graph_client.wait_for_account_connection(sub_account[0])
        if account_status != "READY":
            print(color(
                f"Account: {sub_account[0]} | Account is in the state of {account_status}, integration failed", "red"))
            return False

    print(color(f"Account: {sub_account[0]} | Integrated successfully with StreamSecurity", "green"))
    return True


def delete_stacks_in_all_regions(sub_account, sub_account_session, regions, just_print=False, force_delete_failed=False, stack_name_contains=None):
    if force_delete_failed:
        print(color(f"Account: {sub_account[0]} | Force deleting DELETE_FAILED stacks from all regions", "blue"))
    else:
        print(color(f"Account: {sub_account[0]} | Deleting all stacks from all regions", "blue"))

    for region in regions:
        ll_stacks = filter_ll_stacks_by_name(sub_account_session, region, only_delete_failed=force_delete_failed, stack_name_contains=stack_name_contains)
        if len(ll_stacks) > 0:
            print(color(f"Account: {sub_account[0]} | Found {len(ll_stacks)} stacks in region: {region}", "blue"))
        for ll_stack in ll_stacks:
            if just_print:
                print(f"Account: {sub_account[0]} | Stack to be deleted: {ll_stack['StackName']} (region: {region})")
            else:
                print(color(f"Account: {sub_account[0]} | Deleting stack: {ll_stack['StackName']}", "blue"))
                delete_stack(sub_account_session, region, ll_stack["StackName"], force=force_delete_failed)
                print(color(f"Account: {sub_account[0]} | Stack began deleting!", "green"))


def filter_ll_stacks_by_name(sub_account_session, region, only_delete_failed=False, stack_name_contains=None):
    """Filter stacks by name. When stack_name_contains is provided, filter by that pattern (case-insensitive).
    Otherwise, filter by 'Lightlytics' or 'lightlytics'."""
    region_client = sub_account_session.client('cloudformation', region_name=region)
    try:
        stacks = region_client.describe_stacks()["Stacks"]

        KNOWN_PREFIXES = ["lightlytics", "streamsec"]

        def name_matches(stack_name):
            name_lower = stack_name.lower()
            is_known_stack = any(prefix in name_lower for prefix in KNOWN_PREFIXES)
            if not is_known_stack:
                return False
            if stack_name_contains:
                return stack_name_contains.lower() in name_lower
            return True

        if only_delete_failed:
            ll_stacks = [s for s in stacks if s["StackStatus"] == "DELETE_FAILED"
                         and name_matches(s["StackName"])]
        else:
            ll_stacks = [s for s in stacks if s["StackStatus"] != "DELETE_COMPLETE"
                         and name_matches(s["StackName"])]
        return ll_stacks
    except Exception:
        return []


def delete_stack(sub_account_session, region, stack_name, force=False):
    region_client = sub_account_session.client('cloudformation', region_name=region)
    if force:
        region_client.delete_stack(StackName=stack_name, DeletionMode='FORCE_DELETE_STACK')
    else:
        region_client.delete_stack(StackName=stack_name)


def filter_ll_stacks_from_url(sub_account_session, region, ll_url, return_only_names=False):
    ll_stacks_to_return = []
    region_client = sub_account_session.client('cloudformation', region_name=region)
    try:
        stacks = region_client.describe_stacks()["Stacks"]
        if len(stacks) > 0:
            ll_stacks = [s for s in stacks if s["StackStatus"] != "DELETE_COMPLETE"
                         and "Lightlytics" in s["StackName"]
                         and "Parameters" in s]
            for ll_stack in ll_stacks:
                stack_params_url = [p["ParameterValue"] for p in ll_stack["Parameters"]
                                    if p["ParameterKey"] == "LightlyticsApiUrl"][0]
                if stack_params_url in ll_url:
                    ll_stacks_to_return.append(ll_stack)
                    parent_stacks = [s for s in stacks if s["StackId"] == ll_stack["ParentId"]]
                    ll_stacks_to_return.extend(parent_stacks)
        if return_only_names:
            return [s["StackName"] for s in ll_stacks_to_return]
        else:
            return ll_stacks_to_return
    except Exception:
        return []


CFN_STACK_NAME_TAG = "aws:cloudformation:stack-name"
LAMBDA_MIN_PATTERN_LEN = 3


def validate_lambda_pattern(pattern):
    """Return the stripped pattern, or raise ValueError if it is missing or
    shorter than LAMBDA_MIN_PATTERN_LEN characters (guards against a typo like
    's' matching hundreds of functions)."""
    if pattern is None or len(pattern.strip()) < LAMBDA_MIN_PATTERN_LEN:
        raise ValueError(
            f"--lambda_name_contains must be at least {LAMBDA_MIN_PATTERN_LEN} characters")
    return pattern.strip()


def lambda_name_matches(function_name, pattern):
    """Case-insensitive substring match of pattern within function_name."""
    return pattern.lower() in function_name.lower()


def is_cfn_managed(tags):
    """True if the Lambda's tags mark it as CloudFormation-managed."""
    return CFN_STACK_NAME_TAG in (tags or {})


def build_account_rollup(results):
    """Group delete-plan results by account into (account_id, account_name, count)
    tuples, sorted by count descending then account id, so the accounts losing the
    most functions appear first."""
    counts = {}
    names = {}
    for r in results:
        counts[r["account"]] = counts.get(r["account"], 0) + 1
        names[r["account"]] = r.get("name", "")
    rollup = [(acc, names[acc], cnt) for acc, cnt in counts.items()]
    rollup.sort(key=lambda t: (-t[2], t[0]))
    return rollup


def format_plan_lines(results):
    """One 'account | region | function' line per result, sorted for stable output
    and easy diffing/grepping of the written plan file."""
    return [f"{r['account']} | {r['region']} | {r['function']}"
            for r in sorted(results, key=lambda r: (r["account"], r["region"], r["function"]))]


LAMBDA_CLIENT_CONFIG = Config(
    connect_timeout=15,
    read_timeout=60,
    retries={"max_attempts": 10, "mode": "adaptive"},
)


def list_live_stack_names(sub_account_session, region):
    """Return the set of CloudFormation stack names that currently exist (any
    status except DELETE_COMPLETE) in the region. Used to tell a live
    CloudFormation-managed Lambda from an orphan whose stack was already deleted.
    Raises on API failure so the caller can treat the region's stack state as
    unknown and skip rather than guess. Reuses LAMBDA_CLIENT_CONFIG only for its
    adaptive-retry/timeout settings (not Lambda-specific)."""
    client = sub_account_session.client(
        "cloudformation", region_name=region, config=LAMBDA_CLIENT_CONFIG)
    live = set()
    for page in client.get_paginator("list_stacks").paginate():
        for s in page["StackSummaries"]:
            if s["StackStatus"] != "DELETE_COMPLETE":
                live.add(s["StackName"])
    return live


def scan_lambdas_in_region(sub_account_session, region, pattern):
    """List Lambda functions in a region, keep those whose name matches pattern,
    and split them into (to_delete, skipped_cfn, scan_errors). A function tagged
    as CloudFormation-managed goes to skipped_cfn with its owning stack name; it
    is never deleted. list_tags is called only for name-matched functions, and a
    per-function tag failure never aborts the region scan: the function is left
    out of to_delete (we can't confirm it isn't CFN-managed, so deleting it would
    be unsafe) and recorded in scan_errors so the operator sees the gap."""
    client = sub_account_session.client("lambda", region_name=region, config=LAMBDA_CLIENT_CONFIG)
    to_delete, skipped_cfn, scan_errors = [], [], []
    for page in client.get_paginator("list_functions").paginate():
        for fn in page["Functions"]:
            name = fn["FunctionName"]
            if not lambda_name_matches(name, pattern):
                continue
            try:
                tags = client.list_tags(Resource=fn["FunctionArn"]).get("Tags", {})
            except Exception as e:
                scan_errors.append(
                    {"region": region, "function": name,
                     "reason": f"could not read tags, left out of plan: {str(e)[:150]}"})
                continue
            if is_cfn_managed(tags):
                skipped_cfn.append(
                    {"region": region, "function": name, "stack": tags[CFN_STACK_NAME_TAG]})
            else:
                to_delete.append({"region": region, "function": name})
    return to_delete, skipped_cfn, scan_errors


def delete_lambda_function(sub_account_session, region, function_name):
    """Delete a Lambda function. Returns 'deleted', or 'already gone' if it was
    already removed between the scan and now. Other errors propagate to the caller."""
    client = sub_account_session.client("lambda", region_name=region, config=LAMBDA_CLIENT_CONFIG)
    try:
        client.delete_function(FunctionName=function_name)
        return "deleted"
    except client.exceptions.ResourceNotFoundException:
        return "already gone"


def confirm_deletion(total, account_count, isatty_fn, input_fn):
    """Interactive safety gate. Returns True only on an interactive terminal when
    the operator types exactly 'delete'. On a non-TTY (nohup/pipe/CI) it refuses
    instead of blocking forever on input(); EOF/Ctrl+C also abort. isatty_fn and
    input_fn are injected so this is unit-testable."""
    if not isatty_fn():
        print(color(
            "No interactive terminal detected — refusing to delete. "
            "Run interactively, or use --just_print to preview.", "red"))
        return False
    try:
        answer = input_fn(
            f"About to delete {total} lambda functions across {account_count} accounts. "
            f"Type 'delete' to proceed: ")
    except (EOFError, KeyboardInterrupt):
        print(color("\nAborted — nothing was deleted.", "yellow"))
        return False
    if answer.strip() == "delete":
        return True
    print(color("Confirmation did not match 'delete' — nothing was deleted.", "yellow"))
    return False


def write_plan_file(results, path):
    """Write the full delete plan (one 'account | region | function' per line) to
    path so it can be grepped, diffed, shared, and kept as an audit record."""
    with open(path, "w") as f:
        f.write("\n".join(format_plan_lines(results)) + "\n")


def print_lambda_plan(results, skipped_cfn):
    """Print the delete plan: a per-account roll-up (biggest blast radius first),
    the grand total, the full flat table, and the CFN-managed functions skipped."""
    rollup = build_account_rollup(results)
    print(color("Lambda functions to delete (per account):", "blue"))
    for account_id, name, count in rollup:
        print(f"  {account_id} ({name})  {count} functions")
    print(color(
        f"Total: {len(results)} functions across {len(rollup)} accounts", "blue"))
    if results:
        print(color("Full list:", "blue"))
        for line in format_plan_lines(results):
            print(f"  {line}")
    if skipped_cfn:
        print(color(
            f"Skipped {len(skipped_cfn)} CloudFormation-managed function(s) "
            f"(not deleted):", "yellow"))
        for s in sorted(skipped_cfn, key=lambda x: (x["account"], x["region"], x["function"])):
            print(f"  {s['account']} | {s['region']} | {s['function']} "
                  f"(stack: {s['stack']})")


def format_assume_role_failure_lines(assume_role_failures):
    """Single source of the per-account 'ASSUME-ROLE FAILED' line format, shared
    by print_lambda_summary and the CF-mode summary so the two cannot drift."""
    return [f"  ASSUME-ROLE FAILED | account {account_id} ({name}) | {err}"
            for account_id, name, err in assume_role_failures]


def print_lambda_summary(deleted, already_gone, failed, skipped_cfn, assume_role_failures):
    """Print end-of-run counts with per-item detail for failures and an explicit
    list of accounts where assume-role failed (copy them into --accounts for a
    re-run). Returns len(failed) — the count of items that should drive a
    non-zero exit. Callers deliberately include scan gaps (regions/functions
    whose state couldn't be read) in `failed`, so those count too. Unreachable
    accounts are reported separately and do NOT affect the exit code (an account
    we merely couldn't reach is a reported gap, not an operation failure)."""
    print(color("=" * 60, "blue"))
    print(color(
        f"Run summary: {len(deleted)} deleted | {len(already_gone)} already gone | "
        f"{len(failed)} failed | {len(skipped_cfn)} skipped (CFN-managed) | "
        f"{len(assume_role_failures)} accounts unreachable (assume-role failed)", "blue"))
    for r in failed:
        print(color(
            f"  FAILED | account {r['account']} | {r['region']} | {r['function']} | "
            f"{r['reason']}", "red"))
    for line in format_assume_role_failure_lines(assume_role_failures):
        print(color(line, "yellow"))
    return len(failed)
