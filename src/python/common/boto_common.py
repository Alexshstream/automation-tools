import concurrent.futures
import datetime
import time
from termcolor import colored as color
import os
import boto3
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

def _stack_record(sub_account, region, stack_type, stack_name, stack_id,
                  final_status=None, status_reason=None):
    """Build the record shape shared by every deploy_* helper and consumed by
    sweep_stack_statuses / the CLI's summary. final_status/status_reason are
    only set here for a submit-time failure (create_stack itself never
    returned a stack_id) - a successfully-submitted stack's real status is
    always left for the end-of-run sweep to determine."""
    record = {
        "account": sub_account[0],
        "name": sub_account[1],
        "region": region,
        "stack_type": stack_type,
        "stack_name": stack_name,
        "stack_id": stack_id,
    }
    if final_status is not None:
        record["final_status"] = final_status
        record["status_reason"] = status_reason
    return record


def _try_create_stack(sub_account, region, stack_type, stack_name, cf_client, stack_creation_payload, dry_run=False):
    """Submit a CloudFormation stack. Returns (stack_id, None) on success, or
    (None, record) when nothing was actually submitted - either a
    submit_failed_record (final_status=SUBMIT_FAILED) or, when dry_run is
    set, a dry_run_record (final_status=DRY_RUN). Either way the caller
    should return/append that record and skip further processing (waiting,
    sweeping) for this stack. Shared by every deploy_* helper so this guard
    can't accidentally be missing from one of them (as happened with
    deploy_init_stack and deploy_collection_stack before this existed)."""
    if dry_run:
        print(color(f"Account: {sub_account[0]} | DRY RUN: would submit {stack_type} stack "
                    f"'{stack_name}' in {region}", "cyan"))
        return None, _stack_record(
            sub_account, region, stack_type, stack_name, None,
            final_status="DRY_RUN", status_reason=None)
    try:
        stack_id = cf_client.create_stack(**stack_creation_payload)["StackId"]
        print(color(f"Account: {sub_account[0]} | {stack_type} stack {stack_id} deploying", "blue"))
        return stack_id, None
    except Exception as e:
        print(color(f"Account: {sub_account[0]} | Failed to submit {stack_type} stack for "
                    f"{region}: {e}", "red"))
        return None, _stack_record(
            sub_account, region, stack_type, stack_name, None,
            final_status="SUBMIT_FAILED", status_reason=str(e)[:200])


def _try_wait_for_cloudformation(sub_account, region, stack_type, stack_id, cf_client):
    """Wait for a stack to finish, printing success/timeout/failure messages.
    Never raises - the caller's record (built with the real, already-known
    stack_id) is always returned regardless of outcome; the end-of-run sweep
    determines the actual final status."""
    try:
        if wait_for_cloudformation(sub_account, stack_id, cf_client):
            print(color(f"Account: {sub_account[0]} | {stack_type} stack deployed successfully", "green"))
        else:
            print(color(f"Account: {sub_account[0]} | {stack_type} stack for {region} did not "
                        f"confirm completion within the wait timeout", "red"))
    except Exception as e:
        print(color(f"Account: {sub_account[0]} | {stack_type} stack for {region} failed to "
                    f"finish deploying: {e}", "red"))


def deploy_all_collection_stacks(
        active_regions, sub_account_session, random_int, account_information, sub_account, custom_tags=None,
        dry_run=False):
    print(color(
        f"Account: {sub_account[0]} | Adding collection CFT stack for realtime events for each region in parallel "
        f"(Max 8 workers)", color="blue"))
    # List to hold the concurrent futures, paired with their region so a
    # failure can be attributed and reported without losing sibling regions'
    # results.
    futures = []
    # Create a ThreadPoolExecutor with max_workers
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        # Iterate over active_regions and submit each task to the executor
        for region in active_regions:
            future = executor.submit(deploy_collection_stack, account_information,
                                     sub_account_session, sub_account, region, random_int, custom_tags, False,
                                     dry_run)
            futures.append((region, future))
    # Collect the results (stack records) from all the tasks. A region whose
    # create_stack call itself failed must not discard the records of sibling
    # regions that already succeeded - those are real, live stacks that still
    # need to be swept and reported, not silently dropped.
    records = []
    for region, future in futures:
        try:
            records.append(future.result())
        except Exception as e:
            print(color(f"Account: {sub_account[0]} | Failed to submit collection stack for "
                        f"{region}: {e}", "red"))
            # Same deterministic name deploy_collection_stack would have used
            # (region + random_int) - preserved here too (not None) so a
            # submit failure is as identifiable in the sweep summary as the
            # equivalent eks_audit-stack failure already is.
            stack_name = f"LightlyticsStack-collection-{region}-{random_int}"
            records.append(_stack_record(
                sub_account, region, "collection", stack_name, None,
                final_status="SUBMIT_FAILED", status_reason=str(e)[:200]))
    print(color(f"Account: {sub_account[0]} | Collection stacks submitted for regions: {active_regions}", "blue"))
    return records


def deploy_collection_stack(
        account_information, sub_account_session, sub_account, region, random_int, custom_tags, wait=True,
        dry_run=False):
    # Existing code inside the for loop
    print(color(f"Account: {sub_account[0]} | Adding collection CFT stack for {region}", "blue"))
    region_client = sub_account_session.client('cloudformation', region_name=region)
    stack_name = f"LightlyticsStack-collection-{region}-{random_int}"
    stack_creation_payload = create_stack_payload(
        stack_name, account_information["collection_template_url"], custom_tags=custom_tags)

    # Self-guarded (like every sibling deploy_* function) rather than relying
    # solely on the caller's future.result() handling - a future direct
    # caller that bypasses deploy_all_collection_stacks still gets the
    # SUBMIT_FAILED protection instead of an unguarded raise.
    collection_stack_id, early_exit_record = _try_create_stack(
        sub_account, region, "collection", stack_name, region_client, stack_creation_payload, dry_run=dry_run)
    if early_exit_record:
        return early_exit_record

    if wait:
        print(color(f"Account: {sub_account[0]} | Waiting for the stack to finish deploying successfully", "blue"))
        _try_wait_for_cloudformation(sub_account, region, "collection", collection_stack_id, region_client)

    return _stack_record(sub_account, region, "collection", stack_name, collection_stack_id)

def deploy_response_stack(
        environment_url, account_information, sub_account_session, sub_account, region, random_int, custom_tags, response_exclude_runbooks, wait=True,
        dry_run=False):
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
    
    
    stack_name = f"LightlyticsStack-response-{region}-{random_int}"
    stack_creation_payload = create_stack_payload(
        stack_name,
        os.environ.get("STREAM_RESPONSE_CFT_URL", f"https://prod-lightlytics-public-cloudformation.s3.amazonaws.com/stream-security-remediation-latest-{region}.yaml"), custom_tags=custom_tags , params=params)

    response_stack_id, early_exit_record = _try_create_stack(
        sub_account, region, "response", stack_name, region_client, stack_creation_payload, dry_run=dry_run)
    if early_exit_record:
        return early_exit_record

    if wait:
        print(color(f"Account: {sub_account[0]} | Waiting for the stack to finish deploying successfully", "blue"))
        _try_wait_for_cloudformation(sub_account, region, "response", response_stack_id, region_client)

    return _stack_record(sub_account, region, "response", stack_name, response_stack_id)

def deploy_eks_audit_logs_stacks(
        environment_url, sub_account_information, sub_account_session, sub_account, eks_audit_logs_regions, random_int, custom_tags, wait=True,
        dry_run=False):
    records = []
    if not eks_audit_logs_regions:
        eks_audit_logs_regions = get_active_eks_regions(sub_account_session, sub_account_information["cloud_regions"])

    if not eks_audit_logs_regions:
        print(color(f"Account: {sub_account[0]} | No active EKS regions found, skipping EKS audit logs for {sub_account[0]}", "blue"))
        return records

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
        region_client = sub_account_session.client('lambda', region_name=region)
        region_cloudformation_client = sub_account_session.client('cloudformation', region_name=region)
        stack_name = f"StreamSecurity-eks-audit-logs-{region}-{random_int}"

        # Only ResourceNotFoundException means "the lambda doesn't exist,
        # proceed to deploy" - any OTHER exception from the existence check
        # itself (throttling, access denied) must not propagate out of this
        # function entirely and lose the records already collected for
        # earlier, successfully-deployed regions; treat it like a submit
        # failure for this region instead.
        try:
            region_client.get_function(FunctionName='StreamSec_EKSCloudWatchSubscriptionsFunction')
            print(color(f"Account: {sub_account[0]} | EKS audit logs lambda already exists in region {region}, skipping", "blue"))
            continue
        except region_client.exceptions.ResourceNotFoundException:
            pass
        except Exception as e:
            print(color(f"Account: {sub_account[0]} | Failed to check for an existing EKS audit "
                        f"logs lambda in {region}: {e}", "red"))
            records.append(_stack_record(
                sub_account, region, "eks_audit", stack_name, None,
                final_status="SUBMIT_FAILED", status_reason=str(e)[:200]))
            continue

        print(color(f"Account: {sub_account[0]} | Adding EKS audit logs CFT stack for {region}", "blue"))
        stack_creation_payload = create_stack_payload(
            stack_name,
            os.environ.get("STREAM_EKS_AUDIT_LOGS_CFT_URL", f"https://public-lightlytics-cft.s3.amazonaws.com/eks-audit-collector-latest.yaml"), custom_tags=custom_tags, params=params)

        eks_audit_logs_stack_id, early_exit_record = _try_create_stack(
            sub_account, region, "eks_audit", stack_name, region_cloudformation_client, stack_creation_payload,
            dry_run=dry_run)
        if early_exit_record:
            records.append(early_exit_record)
            continue

        if wait:
            print(color(f"Account: {sub_account[0]} | Waiting for the stack to finish deploying successfully", "blue"))
            _try_wait_for_cloudformation(
                sub_account, region, "eks_audit", eks_audit_logs_stack_id, region_cloudformation_client)

        records.append(_stack_record(sub_account, region, "eks_audit", stack_name, eks_audit_logs_stack_id))

    return records

def deploy_init_stack(account_information, graph_client, sub_account, sub_account_session, random_int, wait=True,
                      custom_tags=None, dry_run=False):
    """Returns (success: bool, record: dict) - always a 2-tuple, which is
    always truthy. A caller written against the old bare-bool return (e.g.
    `if not deploy_init_stack(...)`) must unpack it explicitly; a truthiness
    check on the tuple itself would silently stop detecting failures."""
    sub_account_template_url = account_information["template_url"]
    print(color(f"Account: {sub_account[0]} | Finished fetching information", "green"))

    # Initializing "cloudformation" boto client
    cf = sub_account_session.client('cloudformation')

    print(color(f"Account: {sub_account[0]} | Creating the CFT stack using Boto", "blue"))
    stack_name = f"LightlyticsStack-{random_int}"
    stack_creation_payload = create_stack_payload(
        stack_name, sub_account_template_url, custom_tags=custom_tags)

    # Self-guarded (like every sibling deploy_* function), matching this
    # function's own documented "always returns a 2-tuple" contract.
    sub_account_stack_id, early_exit_record = _try_create_stack(
        sub_account, sub_account_session.region_name, "init", stack_name, cf, stack_creation_payload,
        dry_run=dry_run)
    if early_exit_record:
        # DRY_RUN is not a failure - nothing was actually submitted, on
        # purpose, so the rest of the flow (region detection, response/EKS
        # previews) should still be allowed to run instead of aborting here
        # the way a genuine SUBMIT_FAILED does. There's also no real
        # stack/account-connection to wait on below, dry run or not.
        return dry_run, early_exit_record

    record = _stack_record(sub_account, sub_account_session.region_name, "init", stack_name, sub_account_stack_id)

    if wait:
        print(color(f"Account: {sub_account[0]} | Waiting for the stack to finish deploying successfully", "blue"))
        try:
            # wait_for_cloudformation returning False means its own internal
            # 240s wait timed out - NOT a confirmed failure (the stack may
            # still be creating in the background). Prior to this change set,
            # that return value was simply discarded and execution always
            # fell through to the account-connection check below, which is
            # the more authoritative (and more generous) signal for whether
            # the account is actually usable; a stack that took >240s but
            # still succeeded should not have its account setup aborted here
            # before that check even runs. Only a raised exception (e.g. CFN
            # itself reporting ROLLBACK_IN_PROGRESS) is a definitive enough
            # signal to stop early.
            cf_confirmed = wait_for_cloudformation(sub_account, sub_account_stack_id, cf)
            if not cf_confirmed:
                print(color(f"Account: {sub_account[0]} | Init stack did not confirm completion "
                            f"within the wait timeout, checking account status", "yellow"))

            print(color(f"Account: {sub_account[0]} | "
                        f"Waiting for the account to finish integrating with Lightlytics", "blue"))
            account_status = graph_client.wait_for_account_connection(sub_account[0])
        except Exception as e:
            # The stack_id is real and already known - let the end-of-run
            # sweep determine its actual terminal status rather than losing
            # it here. This is the default (non-"--parallel") code path, so an
            # unguarded exception here would have dropped the record on the
            # most common invocation of this tool. The reason is stashed on
            # the record (not raised) so the caller's generic "something went
            # wrong" message can still include it, instead of it only ever
            # appearing in this stdout print.
            print(color(f"Account: {sub_account[0]} | Init stack failed to finish deploying: {e}", "red"))
            record["status_reason"] = str(e)[:200]
            return False, record

        if account_status != "READY":
            print(color(
                f"Account: {sub_account[0]} | Account is in the state of {account_status}, integration failed", "red"))
            record["status_reason"] = f"account status: {account_status}"
            return False, record

        if not cf_confirmed:
            # The backend independently confirmed READY even though the
            # local CloudFormation wait timed out first - genuinely
            # successful, but worth a note on the record. Only takes effect
            # in the final summary if the sweep's own CloudFormation query
            # doesn't already have a more specific reason to report (see
            # _sweep_account's status_reason fallback).
            record["status_reason"] = "account reached READY; local CloudFormation wait had timed out first"
        print(color(f"Account: {sub_account[0]} | Integrated successfully with StreamSecurity", "green"))
    else:
        # Fire-and-forget (e.g. --parallel mode): the stack's real outcome is not
        # known yet here - claiming success now would repeat the exact false-
        # success pattern this change set exists to fix. The record above is
        # tracked and its real status is determined by the end-of-run sweep.
        print(color(f"Account: {sub_account[0]} | Init stack submitted, final status pending", "blue"))
    return True, record


def _terminal_stack_status(status):
    """True if a CloudFormation StackStatus is terminal (ends in _COMPLETE or
    _FAILED, e.g. CREATE_COMPLETE, CREATE_FAILED, ROLLBACK_COMPLETE,
    UPDATE_ROLLBACK_FAILED) - i.e. it will not change further on its own.
    Non-terminal statuses like CREATE_IN_PROGRESS or ROLLBACK_IN_PROGRESS keep
    polling."""
    return status.endswith("_COMPLETE") or status.endswith("_FAILED")


def _session_for_sweep_account(account_id, sts_client, management_account_id, control_role):
    """Fresh boto3 Session for the account, used only during the sweep - never
    reuse a session captured at deploy time, since deploy and sweep can be far
    enough apart in a large org run for 1-hour STS creds to expire. Mirrors the
    assume-role pattern in organization_delete_integration._session_for_account(),
    but (unlike that script) must use the SAME control_role the deploy phase
    actually used - organization_integration.py's --control_role is
    caller-configurable, so a hardcoded role name here would assume-role-fail
    (and falsely mark every stack "ERROR") for any org not using the default."""
    if account_id == management_account_id:
        return boto3.Session()
    assumed_role = sts_client.assume_role(
        RoleArn=f'arn:aws:iam::{account_id}:role/{control_role}',
        RoleSessionName='MySessionName')
    return boto3.Session(
        aws_access_key_id=assumed_role['Credentials']['AccessKeyId'],
        aws_secret_access_key=assumed_role['Credentials']['SecretAccessKey'],
        aws_session_token=assumed_role['Credentials']['SessionToken'])


def _sweep_account(account_id, records, sts_client, management_account_id, control_role,
                   poll_interval, timeout):
    """Poll one account's stack records to resolution or this account's own
    deadline, mutating each record's final_status/status_reason in place.
    Runs inside a worker thread submitted by sweep_stack_statuses.

    The deadline is computed HERE, the moment this account's own worker
    actually starts running - not once upfront in sweep_stack_statuses and
    shared across every account. With a worker pool capped at 32, an org
    with more accounts than that queues the rest; a shared deadline set
    before any worker even starts would silently eat into a queued
    account's real polling time before it gets its first chance to check
    anything, misreporting it TIMED_OUT purely because of queueing - not
    because anything was actually slow. Giving every account its own fresh
    `timeout`-second window from when it actually begins keeps this correct
    at any org size, with no scale-dependent tuning needed.

    A failure standing up the session is genuinely account-wide (nothing in
    this account can be polled at all), so it marks every still-unresolved
    record of THIS account as final_status="ERROR". A failure calling
    describe_stacks for one region is isolated to that region's
    still-unresolved records only - a transient problem in one region must
    not falsely mark another region's records that were resolving fine.
    Never raises out of this function, so one bad account can't take down
    the whole sweep."""
    deadline = time.time() + timeout
    try:
        session = _session_for_sweep_account(account_id, sts_client, management_account_id, control_role)
    except Exception as e:
        reason = str(e)[:200]
        for r in records:
            if r.get("final_status") is None:
                r["final_status"] = "ERROR"
                r["status_reason"] = reason
        return

    # Everything below is wrapped in a second, broad try/except as a safety
    # net: the per-region try/except further down handles the ONE expected
    # failure point (describe_stacks) with fine-grained per-region isolation,
    # but this function is documented and relied upon (by
    # sweep_stack_statuses, which never inspects these worker threads'
    # results) to NEVER raise - anything else unexpected here (e.g. building
    # region_clients itself) must still be converted into an account-wide
    # ERROR rather than silently escaping this thread and leaving records
    # with no final_status at all, which would later crash main()'s summary
    # loop on a status it assumes is always present.
    try:
        by_region = {}
        for r in records:
            by_region.setdefault(r["region"], []).append(r)
        # LAMBDA_CLIENT_CONFIG (defined further down this file) is reused here
        # for its adaptive-retry/timeout settings only, not anything
        # Lambda-specific - same reuse this file already does for
        # list_live_stack_names' cloudformation client. Up to 32 concurrent
        # workers each polling describe_stacks every poll_interval is exactly
        # the sustained-throttling scenario adaptive retries exist to absorb;
        # without it a ThrottlingException is more likely to exhaust boto3's
        # default retries and get bucketed as a false ERROR for a stack that
        # was actually deploying fine.
        region_clients = {region: session.client(
                              'cloudformation', region_name=region, config=LAMBDA_CLIENT_CONFIG)
                          for region in by_region}

        while True:
            # Always poll immediately on entry (no upfront sleep), so a
            # negative/expired deadline still gets one real check before falling
            # into the TIMED_OUT bucket below, rather than giving up without ever
            # having looked.
            unresolved_by_region = {}
            for region, recs in by_region.items():
                unresolved = [r for r in recs if r.get("final_status") is None]
                if unresolved:
                    unresolved_by_region[region] = unresolved
            if not unresolved_by_region:
                return

            for region, recs in unresolved_by_region.items():
                try:
                    # describe_stacks() call(s) per region per tick, no StackName filter -
                    # match every tracked record for this region locally against the
                    # results, instead of one call per stack (O(accounts x regions) per
                    # tick, not O(accounts x regions x stack_types)). DescribeStacks is
                    # itself paginated (NextToken) once an account/region has enough
                    # existing stacks to exceed one page - without following it, a
                    # tracked stack landing past page 1 would never be found and would
                    # be misreported as TIMED_OUT regardless of its real outcome, no
                    # matter how many ticks run.
                    stacks_by_id = {}
                    next_token = None
                    while True:
                        kwargs = {"NextToken": next_token} if next_token else {}
                        response = region_clients[region].describe_stacks(**kwargs)
                        for s in response.get("Stacks", []):
                            stacks_by_id[s["StackId"]] = s
                        next_token = response.get("NextToken")
                        if not next_token:
                            break
                    for r in recs:
                        stack = stacks_by_id.get(r["stack_id"])
                        if stack is None:
                            # Not present in this tick's results - keep it pending, it may
                            # resolve later, or fall into the timeout bucket.
                            continue
                        status = stack["StackStatus"]
                        if _terminal_stack_status(status):
                            r["final_status"] = status
                            # Prefer CloudFormation's own reason; fall back to
                            # whatever the deploy phase already stashed on the
                            # record (e.g. deploy_init_stack noting the
                            # backend account status separately from the CFN
                            # outcome) rather than silently discarding it -
                            # CFN itself rarely sets a reason for a clean
                            # *_COMPLETE, so this only fires when there's
                            # nothing more specific to prefer.
                            r["status_reason"] = stack.get("StackStatusReason") or r.get("status_reason")
                except Exception as e:
                    # Isolated to this region only - a throttling blip or a
                    # region-specific problem must not falsely fail other
                    # regions in this account that are resolving normally.
                    # Deliberately does NOT set final_status here: a single
                    # bad tick must not permanently lock these records out of
                    # being retried on the next tick (unresolved is exactly
                    # what lets them be picked up again above). If it never
                    # recovers before the deadline, they fall into the
                    # TIMED_OUT bucket below - an honest "could not confirm,
                    # check manually" rather than a false definitive ERROR
                    # from one transient failure.
                    print(color(f"Account: {account_id} | describe_stacks failed for region "
                                f"{region} (will retry next tick): {str(e)[:200]}", "yellow"))

            if all(r.get("final_status") is not None
                   for recs in unresolved_by_region.values() for r in recs):
                return
            if time.time() >= deadline:
                break
            time.sleep(poll_interval)

        # Deadline reached with records still unresolved - a distinct "still in
        # progress, check manually" bucket, never conflated with a real failure.
        # Preserve any pre-existing status_reason (e.g. deploy_init_stack's
        # note that the backend reached READY while the local CFN wait had
        # already timed out) the same way the terminal-status branch above
        # does - CloudFormation itself has nothing to report here (the
        # record never resolved), so there is no "real" reason to prefer
        # over it.
        for recs in by_region.values():
            for r in recs:
                if r.get("final_status") is None:
                    r["final_status"] = "TIMED_OUT"
                    # setdefault, not a plain assignment: preserves a
                    # pre-existing reason if one is already there, but still
                    # guarantees the key exists (matching the invariant every
                    # other final_status-setting branch in this function
                    # maintains) for a record that never had one.
                    r.setdefault("status_reason", None)
    except Exception as e:
        reason = str(e)[:200]
        for r in records:
            if r.get("final_status") is None:
                r["final_status"] = "ERROR"
                r["status_reason"] = reason


def sweep_stack_statuses(stack_records, sts_client, management_account_id,
                         control_role="OrganizationAccountAccessRole",
                         poll_interval=10, timeout=300):
    """Concurrently determine the final status of every stack this run created.

    stack_records: list of dicts, each shaped {"account", "name", "region",
    "stack_type", "stack_name", "stack_id"}. Returns the same list, each record
    augmented with "final_status" and "status_reason" (None when not
    applicable, e.g. for a clean *_COMPLETE).

    control_role must match whatever role the caller actually used to deploy
    these stacks (organization_integration.py's --control_role) - the sweep
    re-assumes it fresh per account rather than reusing any session captured at
    deploy time, since deploy and sweep can be far enough apart in a large org
    run for 1-hour STS creds to expire.

    A record that already carries a final_status (e.g. a submit-time failure
    the caller recorded because create_stack itself never returned a stack_id
    to poll) is left untouched and passed through as-is - only records with a
    stack_id and no final_status yet are actually swept.

    Every create_stack call stays fire-and-forget at submit time; this sweep runs
    once at the end of the run to determine what actually happened, instead of
    blocking per-stack during submission.
    """
    if not stack_records:
        return stack_records

    # Every record _sweep_account touches is guaranteed a real final_status
    # (terminal match, TIMED_OUT, or ERROR) before this function returns, so
    # there is nothing to pre-initialize here beyond selecting which records
    # need sweeping.
    to_sweep = [r for r in stack_records
               if r.get("stack_id") and r.get("final_status") is None]

    by_account = {}
    for r in to_sweep:
        by_account.setdefault(r["account"], []).append(r)

    if by_account:
        # max_workers capped at 32 bounds concurrent AWS API load for very
        # large orgs. Accounts beyond that cap queue behind the first 32 -
        # each one still gets its own full `timeout`-second window, computed
        # inside _sweep_account itself the moment its worker actually starts,
        # so time spent queued is never silently deducted from its real
        # polling time (see _sweep_account's docstring). This is what makes
        # the sweep correct at any org size without needing to tune timeout
        # based on account count.
        max_workers = min(32, len(by_account))

        futures = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            for account_id, records in by_account.items():
                future = executor.submit(
                    _sweep_account, account_id, records, sts_client,
                    management_account_id, control_role, poll_interval, timeout)
                futures[future] = records
            # Exiting the `with` block waits for every submitted task to finish.

        # _sweep_account is documented to never raise - it converts every
        # failure into a per-record final_status="ERROR" instead - so this is
        # a defense-in-depth backstop only, in case that contract is ever
        # violated by a future change. A plain executor.submit() with no
        # .result() call would otherwise silently discard such an exception,
        # leaving these records with no final_status at all (which would
        # later crash main()'s summary loop) - and re-raising it here
        # unguarded would crash the WHOLE script over one account's bug,
        # losing every other account's results too. Recover the same way
        # _sweep_account itself would: mark this account's records ERROR.
        for future, records in futures.items():
            try:
                future.result()
            except Exception as e:
                reason = str(e)[:200]
                for r in records:
                    if r.get("final_status") is None:
                        r["final_status"] = "ERROR"
                        r["status_reason"] = reason

    return stack_records


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
CFN_STACK_ID_TAG = "aws:cloudformation:stack-id"
LAMBDA_MIN_PATTERN_LEN = 3


def _stack_id_parts(stack_id):
    """Split a CloudFormation stack-id ARN into its fields, or None if it is
    missing or not a well-formed cloudformation ARN. Requires the service segment
    to be 'cloudformation' so a stray non-CFN ARN (e.g. arn:aws:lambda:...) is not
    mistaken for a stack reference."""
    if not stack_id:
        return None
    parts = stack_id.split(":")
    # arn:aws:cloudformation:<region>:<account>:stack/<name>/<uuid>  -> 6 fields,
    # with the resource segment being an actual stack (not e.g. :changeSet/... or a
    # non-stack resource). Anything else is treated as malformed -> protect.
    if (len(parts) < 6 or parts[0] != "arn" or parts[2] != "cloudformation"
            or not parts[5].startswith("stack/")):
        return None
    return parts


def region_from_stack_id(stack_id):
    """Region from a CloudFormation stack-id ARN, or None if missing/malformed.
    Used to verify an orphan's stack in the region that actually owns it rather
    than assuming the Lambda's own region."""
    parts = _stack_id_parts(stack_id)
    return (parts[3] or None) if parts else None


def account_from_stack_id(stack_id):
    """Account id from a CloudFormation stack-id ARN, or None if missing/malformed.
    Used together with the region so a stack-id pointing at a DIFFERENT account is
    never treated as owned by the account being scanned (which would list the wrong
    account's stacks and could misclassify a live-stack Lambda as an orphan)."""
    parts = _stack_id_parts(stack_id)
    return (parts[4] or None) if parts else None


# CloudFormation ListStacks surfaces authorization denials (IAM, SCP, permission
# boundary) as AccessDenied. Deliberately excludes transient/credential 403s like
# ExpiredToken (those must fall through to the scan-gap path, not skip clean).
_ACCESS_DENIED_CODES = ("AccessDenied", "AccessDeniedException")


def is_access_denied_error(e):
    """True if the exception is a botocore authorization-denied ClientError. Lets the scan
    fall back to the pre-orphan-detection behavior (skip CloudFormation-tagged
    functions) when the role simply lacks cloudformation:ListStacks, instead of
    turning every such function into a scan gap and a non-zero exit."""
    response = getattr(e, "response", None)
    if not isinstance(response, dict):
        return False
    # Match specific authorization-denial codes only — NOT raw HTTP 403. Transient
    # credential failures (ExpiredToken/ExpiredTokenException) are also HTTP 403, and
    # treating those as "role lacks ListStacks" would silently skip CFN-tagged
    # functions with a clean exit, hiding an incomplete scan. Those must fall through
    # to the scan-gap path instead.
    return response.get("Error", {}).get("Code") in _ACCESS_DENIED_CODES


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
    """True if the Lambda's tags mark it as CloudFormation-managed. Either the
    stack-name or the stack-id tag counts, so a function carrying only a stack-id
    is not mistaken for a plain (non-CFN) function and deleted unconditionally."""
    tags = tags or {}
    return CFN_STACK_NAME_TAG in tags or CFN_STACK_ID_TAG in tags


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
    and easy diffing/grepping of the written plan file. Orphaned functions (their
    CloudFormation stack no longer exists) are annotated so the plan makes the
    reason for deletion explicit."""
    lines = []
    for r in sorted(results, key=lambda r: (r["account"], r["region"], r["function"])):
        # Keep 'account | region | function' as the first three pipe-delimited
        # fields so the plan file stays machine-parseable (split on ' | ', take
        # field 2 for the function name). Orphan context goes in a 4th field.
        line = f"{r['account']} | {r['region']} | {r['function']}"
        if r.get("orphaned_stack"):
            line += f" | orphaned (stack {r['orphaned_stack']} gone)"
        lines.append(line)
    return lines


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


def scan_lambdas_in_region(sub_account_session, region, pattern, account_id):
    """List Lambda functions in a region, keep those whose name matches pattern,
    and split them into (to_delete, skipped_cfn, scan_errors). A CloudFormation-
    tagged function is checked against the live stacks in the region/account that
    own its stack (derived from the stack-id tag): if its stack still exists it is
    skipped (skipped_cfn, protected); if the stack is gone it is an orphan and goes
    to to_delete annotated with 'orphaned_stack'.

    Safety rules (we never guess 'delete' when we can't confirm the stack is gone):
    - A function whose stack lives in another region OR another account, or whose
      stack-id tag is missing/malformed, is protected (skipped_cfn) — we only list
      THIS account/region's stacks, so we can't confirm such a stack is gone.
    - If the role lacks cloudformation:ListStacks (AccessDenied), fall back to the
      pre-orphan-detection behavior: skip all CFN-tagged functions (skipped_cfn),
      exit clean — no orphan detection is possible without that permission — and
      print a warning so the operator knows detection was skipped for this region.
    - Any other stack-list failure, or a per-function tag-read failure, records a
      scan gap (scan_errors) and leaves the function out of to_delete.

    The live-stack set is fetched lazily (only when a same-account/region CFN-tagged
    match appears) and cached for the region."""
    client = sub_account_session.client("lambda", region_name=region, config=LAMBDA_CLIENT_CONFIG)
    account_id = str(account_id)   # compare like-for-like against the ARN's account field
    to_delete, skipped_cfn, scan_errors = [], [], []
    live_stacks = None          # lazily fetched set of live stack names
    live_stacks_error = None    # set once if listing stacks failed (don't retry)
    live_stacks_denied = False  # set once if ListStacks is not permitted (skip, don't gap)
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
            if not is_cfn_managed(tags):
                to_delete.append({"region": region, "function": name})
                continue
            stack_name = tags.get(CFN_STACK_NAME_TAG)
            if not stack_name:
                # CFN-managed (a CFN tag is present) but the stack-name value is
                # empty or absent, so we can't identify the owning stack. Protect it
                # rather than fall through and delete it as if it had no CFN tag.
                skipped_cfn.append(
                    {"region": region, "function": name,
                     "stack": "unknown - stack-id tag only"})
                continue
            # Only classify orphan status when the stack is owned by THIS account AND
            # region; otherwise we would be listing the wrong account/region's stacks
            # and could not confirm the stack is gone, so protect it.
            stack_id = tags.get(CFN_STACK_ID_TAG)
            if (region_from_stack_id(stack_id) != region
                    or account_from_stack_id(stack_id) != account_id):
                skipped_cfn.append(
                    {"region": region, "function": name, "stack": stack_name})
                continue
            if live_stacks is None and live_stacks_error is None and not live_stacks_denied:
                try:
                    live_stacks = list_live_stack_names(sub_account_session, region)
                except Exception as e:
                    if is_access_denied_error(e):
                        live_stacks_denied = True
                        print(color(
                            f"Account: {account_id} | {region}: cloudformation:ListStacks "
                            f"denied — orphan detection SKIPPED here; CFN-tagged functions "
                            f"are reported as skipped (not evaluated). Grant "
                            f"cloudformation:ListStacks to detect orphans.", "yellow"))
                    else:
                        live_stacks_error = str(e)[:150]
            if live_stacks_denied:
                # No permission to verify — behave as before orphan detection existed.
                skipped_cfn.append(
                    {"region": region, "function": name, "stack": stack_name})
            elif live_stacks_error is not None:
                scan_errors.append(
                    {"region": region, "function": name,
                     "reason": f"could not list stacks to verify orphan status, "
                               f"left out of plan: {live_stacks_error}"})
            elif stack_name in live_stacks:
                skipped_cfn.append(
                    {"region": region, "function": name, "stack": stack_name})
            else:
                to_delete.append(
                    {"region": region, "function": name, "orphaned_stack": stack_name})
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
    """Write the full delete plan to path so it can be grepped, diffed, shared, and
    kept as an audit record. Each line is 'account | region | function', with an
    optional 4th '| orphaned (...)' field for orphaned functions; splitting on
    ' | ' and taking field 2 always yields the bare function name."""
    with open(path, "w") as f:
        f.write("\n".join(format_plan_lines(results)) + "\n")


def print_lambda_plan(results, skipped_cfn):
    """Print the delete plan: a per-account roll-up (biggest blast radius first),
    the grand total, the full flat table, and the CFN-managed functions skipped."""
    rollup = build_account_rollup(results)
    print(color("Lambda functions to delete (per account):", "blue"))
    for account_id, name, count in rollup:
        print(f"  {account_id} ({name})  {count} functions")
    orphan_count = sum(1 for r in results if r.get("orphaned_stack"))
    total_line = f"Total: {len(results)} functions across {len(rollup)} accounts"
    if orphan_count:
        total_line += f" ({orphan_count} orphaned CFN — stack already deleted)"
    print(color(total_line, "blue"))
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
    orphan_deleted = sum(1 for d in deleted if d.get("orphaned_stack"))
    deleted_part = f"{len(deleted)} deleted"
    if orphan_deleted:
        deleted_part += f" ({orphan_deleted} orphaned CFN)"
    print(color(
        f"Run summary: {deleted_part} | {len(already_gone)} already gone | "
        f"{len(failed)} failed | {len(skipped_cfn)} skipped (CFN-managed) | "
        f"{len(assume_role_failures)} accounts unreachable (assume-role failed)", "blue"))
    for r in failed:
        print(color(
            f"  FAILED | account {r['account']} | {r['region']} | {r['function']} | "
            f"{r['reason']}", "red"))
    for line in format_assume_role_failure_lines(assume_role_failures):
        print(color(line, "yellow"))
    return len(failed)
