import boto3
import random
import os
import time
import concurrent.futures
from botocore.exceptions import BotoCoreError, ClientError
from src.python.common.boto_common import *
from src.python.common.graph_common import GraphCommon


def lambda_handler(event, context):
    # Extract parameters from environment variables
    environment = os.environ.get('ENVIRONMENT')
    domain = os.environ.get('ENVIRONMENT_DOMAIN', 'streamsec.io')
    ll_api_token = os.environ.get('API_TOKEN')
    ll_username = os.environ.get('ENVIRONMENT_USER_NAME')
    ll_password = os.environ.get('ENVIRONMENT_PASSWORD')
    accounts = os.environ.get('ACCOUNTS', None)
    parallel = int(os.environ.get('PARALLEL', 1))  # Convert to int, default to 1
    ws_id = os.environ.get('WS_ID', None)
    custom_tags = os.environ.get('CUSTOM_TAGS', None)
    regions_to_integrate = os.environ.get('REGIONS', None)
    control_role = os.environ.get('CONTROL_ROLE', "OrganizationAccountAccessRole")
    response = os.environ.get('RESPONSE', 'false').lower() == 'true'
    response_region = os.environ.get('RESPONSE_REGION', 'us-east-1')
    response_exclude_runbooks = os.environ.get('RESPONSE_EXCLUDE_RUNBOOKS', '')
    eks_audit_logs = os.environ.get('EKS_AUDIT_LOGS', 'false').lower() == 'true'
    eks_audit_logs_regions = os.environ.get('EKS_AUDIT_LOGS_REGIONS', None)
    if eks_audit_logs_regions:
        # Stripped like the CLI does: "us-east-1, eu-west-1" must not yield " eu-west-1".
        eks_audit_logs_regions = [r.strip() for r in eks_audit_logs_regions.split(",") if r.strip()]

    # Setting up variables
    random_int = random.randint(1000000, 9999999)
    if accounts:
        accounts = accounts.replace(" ", "").split(",")

    # Prepare tags if provided
    if custom_tags:
        custom_tags = [{'Key': k.split("|")[0], 'Value': k.split("|")[1]} for k in custom_tags.split(",")]

    # Prepare regions if provided
    if regions_to_integrate:
        # dict.fromkeys drops repeats (keeping order): a repeated region would
        # submit two collection stacks with the same name and fail the account.
        regions_to_integrate = list(dict.fromkeys(r.strip() for r in regions_to_integrate.split(",") if r.strip()))

    print(f"Trying to login into Stream Security environment: {environment}")
    ll_url = f"https://{environment}.{domain}/graphql"
    if ll_api_token:
        graph_client = GraphCommon(ll_url, token=ll_api_token, customer_id=ws_id)
    elif ll_username and ll_password:
        graph_client = GraphCommon(ll_url, ll_username, ll_password, ws_id)
    else:
        raise Exception("Missing Stream Security credentials: set API_TOKEN, or both ENVIRONMENT_USER_NAME and ENVIRONMENT_PASSWORD")
    print("Logged in successfully!")

    print("Creating Boto3 Session")
    
    # Set up the Organizations and STS clients
    org_client = boto3.client('organizations')
    sts_client = boto3.client('sts')

    # Set up org account variable
    org_account_id = sts_client.get_caller_identity().get('Account')

    # Get all activated regions from the Org account
    regions = [region['RegionName'] for region in boto3.client('ec2').describe_regions()['Regions']]

    print("Fetching all accounts connected to the organization")
    list_accounts = get_all_accounts(org_client)

    # Getting only the account IDs of the active AWS accounts
    sub_accounts = [(a["Id"], a["Name"]) for a in list_accounts if a["Status"] == "ACTIVE"]
    print(f"Found {len(sub_accounts)} accounts")

    if accounts:
        sub_accounts = [sa for sa in sub_accounts if sa[0] in accounts]

    # The Lambda can't onboard the management account: it reaches member
    # accounts through OrganizationAccountAccessRole, which AWS doesn't create
    # in the management account, and its own role can't deploy stacks.
    if any(sa[0] == org_account_id for sa in sub_accounts):
        sub_accounts = [sa for sa in sub_accounts if sa[0] != org_account_id]
        print(color(f"Skipping management account {org_account_id}: this Lambda only onboards member accounts "
                    f"(it reaches them through {control_role}, which doesn't exist in the management account, "
                    f"and its own role can't deploy stacks). Onboard the management account once from the "
                    f"Stream Security console, or with src/python/utilities/organization_integration.py "
                    f"using your own credentials", "yellow"))

    print(f"Accounts to-be integrated: {[sa[0] for sa in sub_accounts]}")

    failures = []
    if parallel:
        with concurrent.futures.ThreadPoolExecutor(max_workers=parallel) as executor:
            future_to_account = {
                executor.submit(
                    integrate_sub_account,
                    sub_account, sts_client, graph_client, regions, random_int, custom_tags, regions_to_integrate,
                    control_role, org_account_id, parallel,
                    response, response_region, response_exclude_runbooks, environment, domain,
                    eks_audit_logs, eks_audit_logs_regions
                ): sub_account for sub_account in sub_accounts
            }
            for future in concurrent.futures.as_completed(future_to_account):
                sub_account = future_to_account[future]
                try:
                    future.result()
                except Exception as e:
                    failures.append((sub_account[0], str(e)))
    else:
        for sub_account in sub_accounts:
            try:
                integrate_sub_account(
                    sub_account, sts_client, graph_client, regions, random_int,
                    custom_tags, regions_to_integrate, control_role, org_account_id,
                    parallel, response, response_region, response_exclude_runbooks, environment, domain,
                    eks_audit_logs, eks_audit_logs_regions
                )
            except Exception as e:
                failures.append((sub_account[0], str(e)))

    if failures:
        print(color(f"Integration finished with {len(failures)} failure(s):", "red"))
        for account_id, msg in failures:
            print(color(f"  {account_id}: {msg}", "red"))
        raise Exception(f"{len(failures)} account(s) failed to integrate")

    print("Integration finished successfully!")

def _raise_on_submit_failure(sub_account, records):
    # The deploy_* helpers return a SUBMIT_FAILED record instead of raising when
    # create_stack fails. Raise here so the handler still counts the account as
    # failed, as it did when create_stack raised directly.
    if isinstance(records, dict):
        records = [records]
    failed = [r for r in records or [] if r and r.get("final_status") == "SUBMIT_FAILED"]
    if failed:
        details = "; ".join(f"{r['stack_type']} in {r['region']}: {r.get('status_reason')}" for r in failed)
        raise Exception(f"Account: {sub_account[0]} | Failed to submit stack(s): {details}")


def integrate_sub_account(
        sub_account, sts_client, graph_client, regions, random_int, custom_tags, regions_to_integrate, control_role,
        org_account_id, parallel=False, response=False, response_region="us-east-1", response_exclude_runbooks="", environment=None, domain=None,
        eks_audit_logs=False, eks_audit_logs_regions=None):
    print(color(f"Account: {sub_account[0]} | Starting integration", color="blue"))
    # The response and EKS stacks take the base URL as APIUrl, without /graphql:
    # the response template's acknowledge call fails with it, and console-deployed
    # stacks use the base URL.
    api_base_url = f"https://{environment}.{domain}"
    try:
        if sub_account[0] == org_account_id:
            sub_account_session = boto3.Session()
        else:
            role_arn = f'arn:aws:iam::{sub_account[0]}:role/{control_role}'
            try:
                assumed_role = sts_client.assume_role(
                    RoleArn=role_arn,
                    RoleSessionName='MySessionName'
                )
            except ClientError as e:
                err_msg = f"Account: {sub_account[0]} | Failed to assume {role_arn}: {e}"
                print(color(err_msg, "red"))
                raise Exception(err_msg) from e
            print(color(f"Account: {sub_account[0]} | Initializing Boto session", "blue"))
            sub_account_session = boto3.Session(
                aws_access_key_id=assumed_role['Credentials']['AccessKeyId'],
                aws_secret_access_key=assumed_role['Credentials']['SecretAccessKey'],
                aws_session_token=assumed_role['Credentials']['SessionToken']
            )
            print(color(f"Account: {sub_account[0]} | Session initialized successfully", "green"))

        if regions_to_integrate:
            regions_to_integrate = _usable_regions(sub_account, sub_account_session, regions_to_integrate)

        print(color(f"Account: {sub_account[0]} | Checking if integration already exists", "blue"))
        ll_integrated = False
        # Only an empty lookup means "not in StreamSecurity yet": an IndexError elsewhere
        # below, or a failed API call, must fail the account rather than re-create it.
        matching_accounts = [acc for acc in graph_client.get_accounts(raise_on_error=True)
                             if sub_account[0] == acc["cloud_account_id"]]
        if matching_accounts:
            sub_account_information = matching_accounts[0]
            if sub_account_information["status"] == "UNINITIALIZED":
                ll_integrated = True
                print(color(f"Account: {sub_account[0]} | Integrated but uninitialized, continuing", "blue"))
            elif sub_account_information["status"] == "READY":
                print(color(f"Account: {sub_account[0]} | Integration exists and in READY state", "green"))
                # Check if account name has changed in AWS Organizations
                current_display_name = sub_account_information.get("display_name", "")
                aws_account_name = sub_account[1]  # Account name from AWS Organizations
                if aws_account_name and current_display_name != aws_account_name:
                    try:
                        print(color(f"Account: {sub_account[0]} | Display name changed from '{current_display_name}' to '{aws_account_name}', updating", "blue"))
                        graph_client.update_account_display_name(sub_account[0], aws_account_name)
                        print(color(f"Account: {sub_account[0]} | Display name updated successfully", "green"))
                    except Exception as e:
                        print(color(f"Account: {sub_account[0]} | Failed to update display name: {e}", "red"))
                # Response stack logic for READY state
                response_info = graph_client.get_account_response_config(sub_account_information["cloud_account_id"])
                if (response_info["remediation"] is None or response_info["remediation"]["status"] is None) and response:
                    _raise_on_submit_failure(sub_account, deploy_response_stack(
                        api_base_url, sub_account_information, sub_account_session, sub_account,
                        response_region, random_int, custom_tags, response_exclude_runbooks, wait=False))
                # Deploying EKS audit logs if enabled
                if eks_audit_logs:
                    _raise_on_submit_failure(sub_account, deploy_eks_audit_logs_stacks(
                        api_base_url, sub_account_information, sub_account_session, sub_account, eks_audit_logs_regions, random_int, custom_tags, wait=False))
                print(color(f"Account: {sub_account[0]} | Checking if regions are updated", "blue"))
                current_regions = sub_account_information["cloud_regions"]
                if regions_to_integrate:
                    # A copy: it gets extend()'d below.
                    potential_regions = list(regions_to_integrate)
                else:
                    potential_regions = get_active_regions(sub_account_session, regions)
                    # Regions already onboarded are kept (merged below), so only
                    # warn when the account really is on the defaults alone.
                    _warn_if_default_regions_only(
                        sub_account, sub_account_session, potential_regions + list(current_regions))
                if sorted(current_regions) != sorted(potential_regions):
                    potential_regions.extend(current_regions)
                    potential_regions = list(set(potential_regions))
                    print(color(
                        f"Account: {sub_account[0]} | Regions are different, updating to {potential_regions}", "blue"))
                    if not update_regions(graph_client, sub_account, potential_regions, not parallel):
                        err_msg = f"Account: {sub_account[0]} | Something went wrong with regions update"
                        print(color(err_msg, "red"))
                        raise Exception(err_msg)
                else:
                    print(color(f"Account: {sub_account[0]} | Regions are the same", "green"))
                print(color(f"Account: {sub_account[0]} | Checking if realtime regions are functioning", "blue"))
                realtime_regions = sub_account_information["realtime_regions"]
                if realtime_regions is None:
                    realtime_regions = []
                realtime_region_names = [r["region_name"] for r in realtime_regions]
                regions_to_integrate = [i for i in potential_regions if i not in realtime_region_names]
                if len(regions_to_integrate) > 0:
                    print(color(f"Account: {sub_account[0]} | Realtime is not enabled on all regions, "
                                f"adding support for {regions_to_integrate}", "blue"))
                    _raise_on_submit_failure(sub_account, deploy_all_collection_stacks(
                        regions_to_integrate, sub_account_session, random_int, sub_account_information, sub_account,
                        custom_tags=custom_tags))
                else:
                    print(color(f"Account: {sub_account[0]} | All regions are integrated to realtime", "green"))
                return
            else:
                err_msg = f"Account: {sub_account[0]} | Account is in {sub_account_information['status']} " \
                          f"status at StreamSecurity, remove it and try again"
                print(color(err_msg, "red"))
                raise Exception(err_msg)

        # If account is not already integrated to StreamSecurity
        if not ll_integrated:
            print(color(f"Account: {sub_account[0]} | Creating account in StreamSecurity", "blue"))
            if not graph_client.create_account(
                    sub_account[0], [sub_account_session.region_name], display_name=sub_account[1]):
                err_msg = f"Account: {sub_account[0]} | Failed to create account in StreamSecurity"
                print(color(err_msg, "red"))
                raise Exception(err_msg)
            print(color(f"Account: {sub_account[0]} | Account created successfully", "green"))

        print(color(f"Account: {sub_account[0]} | Fetching relevant account information", "blue"))
        account_information = [acc for acc in graph_client.get_accounts()
                               if acc["cloud_account_id"] == sub_account[0]][0]

        # Deploying the initial integration stack. deploy_init_stack now
        # returns (ok, record) instead of a bare bool (see boto_common.py) -
        # a 2-tuple is always truthy, so `if not deploy_init_stack(...)`
        # would never detect a real failure here without unpacking it.
        init_ok, _ = deploy_init_stack(
                account_information, graph_client, sub_account, sub_account_session, random_int, not parallel,
                custom_tags=custom_tags)
        if not init_ok:
            err_msg = f"Account: {sub_account[0]} | Something went wrong with init stack deployment"
            print(color(err_msg, "red"))
            raise Exception(err_msg)

        print(color(f"Account: {sub_account[0]} | Setting regions", "blue"))
        if regions_to_integrate:
            active_regions = regions_to_integrate
        else:
            print(color(f"Account: {sub_account[0]} | Getting active regions (Has EC2 instances)", "blue"))
            active_regions = get_active_regions(sub_account_session, regions)
            _warn_if_default_regions_only(sub_account, sub_account_session, active_regions)
        print(color(f"Account: {sub_account[0]} | Active regions are: {active_regions}", "blue"))

        # Response stack logic for new integrations
        if response:
            _raise_on_submit_failure(sub_account, deploy_response_stack(
                api_base_url, account_information, sub_account_session, sub_account,
                response_region, random_int, custom_tags, response_exclude_runbooks, wait=False))

        # Deploying EKS audit logs if enabled
        if eks_audit_logs:
            # account_information["cloud_regions"] is still the creation-time region
            # only; detect clusters across the active regions, as the CLI does.
            _raise_on_submit_failure(sub_account, deploy_eks_audit_logs_stacks(
                api_base_url, {**account_information, "cloud_regions": active_regions}, sub_account_session,
                sub_account, eks_audit_logs_regions, random_int, custom_tags, wait=False))

        if not update_regions(graph_client, sub_account, active_regions, not parallel):
            err_msg = f"Account: {sub_account[0]} | Something went wrong with regions update"
            print(color(err_msg, "red"))
            raise Exception(err_msg)

        _raise_on_submit_failure(sub_account, deploy_all_collection_stacks(
            active_regions, sub_account_session, random_int, account_information, sub_account, custom_tags=custom_tags))

        return

    except Exception as e:
        err_msg = f"Account: {sub_account[0]} | Something went wrong: {e}"
        print(color(err_msg, "red"))
        raise Exception(err_msg)


def _usable_regions(sub_account, sub_account_session, regions_to_integrate):
    """REGIONS is shared by every account in the organization. Drop regions
    this account hasn't enabled (opt-in regions are enabled per account),
    rather than failing the account on every run, and always keep us-east-1:
    edit_regions replaces the account's region list, and global-service
    events are only delivered there."""
    regions_to_integrate = list(dict.fromkeys(regions_to_integrate))
    try:
        enabled = {r["RegionName"] for r in sub_account_session.client("ec2").describe_regions()["Regions"]}
    except (ClientError, BotoCoreError) as e:
        print(color(f"Account: {sub_account[0]} | Could not list enabled regions ({e}), "
                    f"using REGIONS as is", "yellow"))
        enabled = set(regions_to_integrate)
    usable = [r for r in regions_to_integrate if r in enabled]
    skipped = [r for r in regions_to_integrate if r not in enabled]
    if skipped:
        print(color(f"Account: {sub_account[0]} | Warning: skipping {skipped} from REGIONS, "
                    f"not enabled in this account (or misspelled)", "yellow"))
    if "us-east-1" not in usable:
        usable.append("us-east-1")
    return usable


def _warn_if_default_regions_only(sub_account, sub_account_session, detected_regions):
    # Detection found nothing beyond the regions it always adds. A newly
    # vended account has no instances yet, so it stays there until instances
    # appear (then a scheduled scan adds those regions) or REGIONS is set.
    # get_active_regions swallows API errors, so a failed detection looks
    # the same.
    if set(detected_regions) <= {sub_account_session.region_name, "us-east-1"}:
        print(color(f"Account: {sub_account[0]} | Warning: no EC2 instances detected outside the default "
                    f"regions (or detection failed), onboarding to {sorted(set(detected_regions))} only. Set REGIONS "
                    f"(org_lambda.py --regions) to onboard accounts to more regions", "yellow"))


def update_regions(graph_client, sub_account, active_regions, wait=True):
    print(color(f"Account: {sub_account[0]} | Wait until account is initialized", "blue"))
    count = 0
    while True:
        sub_account_status = \
            [acc for acc in graph_client.get_accounts() if sub_account[0] == acc["cloud_account_id"]][0]['status']
        if sub_account_status == "UNINITIALIZED":
            time.sleep(1)
            count += 1
        else:
            break
        if count == 300:
            print(color(f"Account: {sub_account[0]} | Timed out after 5 minutes", "red"))
            raise Exception("Status change timed out, account still in uninitialized")
    print(color(f"Account: {sub_account[0]} | Account changed status to: {sub_account_status}", "blue"))

    print(color(f"Account: {sub_account[0]} | Updating regions in StreamSecurity according to active regions", "blue"))
    graph_client.edit_regions(sub_account[0], active_regions)
    print(color(f"Account: {sub_account[0]} | Updated regions to {active_regions}", "green"))

    if wait:
        print(color(f"Account: {sub_account[0]} | Waiting for the account to finish editing regions", "blue"))
        account_status = graph_client.wait_for_account_connection(sub_account[0])
        if account_status != "READY":
            print(color(
                f"Account: {sub_account[0]} | Account is in the state of {account_status}, integration failed", "red"))
            return False
    print(color(f"Account: {sub_account[0]} | Editing regions finished successfully", "green"))
    return True