import argparse
import boto3
import os
import sys
import uuid
from urllib.parse import urlparse

# Add the project root directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))
try:
    from src.python.common.boto_common import *
    from src.python.common.graph_common import GraphCommon
except ModuleNotFoundError:
    sys.path.append("../../..")
    from src.python.common.boto_common import *
    from src.python.common.graph_common import GraphCommon


def _classify_stack_status(status):
    """Bucket a swept stack's final_status for the summary: which counter to
    increment and which print color to use. Whitelists success rather than
    blacklisting failure - any terminal status not explicitly recognized as a
    clean success (an unexpected DELETE_COMPLETE from something external
    deleting the stack mid-sweep, a not-yet-seen CloudFormation status, the
    "UNKNOWN" fallback for a record that somehow reached here without a
    final_status) is reported failed, never silently defaulted to a green
    "succeeded" line."""
    if status == "TIMED_OUT":
        return "timed_out", "yellow"
    if status == "ERROR":
        return "errored", "red"
    if status in ("CREATE_COMPLETE", "UPDATE_COMPLETE"):
        return "succeeded", "green"
    return "failed", "red"


def main(environment_url, ll_username, ll_password, aws_profile_name, accounts, parallel,
         ws_id=None, custom_tags=None, regions_to_integrate=None, control_role="OrganizationAccountAccessRole", response=False, response_region="us-east-1", response_exclude_runbooks="", eks_audit_logs=False, eks_audit_logs_regions=None, api_token=None):

    try:
        if not environment_url:
            raise ValueError("The environment URL is required.")
        if api_token:
            if not ws_id:
                raise ValueError("--ws_id is required when using --api_token.")
        else:
            if not ll_username:
                raise ValueError("--environment_user_name is required (or use --api_token instead).")
            if not ll_password:
                raise ValueError("--environment_password is required (or use --api_token instead).")
    except Exception as e:
        print(color(f"Error: {e}", "red"))
        return

    if not ws_id:
        print(color("Warning: --ws_id not set; using the first workspace returned by the API.", "yellow"))

    # Setting up variables
    # A random suffix for this run's stack names, to avoid collisions across
    # separate runs against the same account/region. A short hex string has
    # far more entropy than a 7-digit int (~4 billion vs ~9 million).
    random_int = uuid.uuid4().hex[:8]
    if accounts:
        accounts = accounts.replace(" ", "").split(",")

    # Prepare tags if provided
    if custom_tags:
        custom_tags = [{'Key': k.split("|")[0], 'Value': k.split("|")[1]} for k in custom_tags.split(",")]

    # Prepare regions if provided
    if regions_to_integrate:
        regions_to_integrate = [r.strip() for r in regions_to_integrate.split(",") if r.strip()]

    if eks_audit_logs_regions:
        eks_audit_logs_regions = [r.strip() for r in eks_audit_logs_regions.split(",") if r.strip()]

    print(color("Trying to login into Stream Security", "blue"))
    try:
        parsed_url = urlparse(environment_url)
        if parsed_url.scheme and parsed_url.netloc:
            ll_url = f"https://{parsed_url.netloc}/graphql"
        elif '.' in environment_url:
            ll_url = f"https://{environment_url}/graphql" if environment_url.count('.') >= 2 else f"https://{environment_url}.streamsec.io/graphql"
        else:
            raise ValueError("The environment should be a valid URL or a subdomain.")
        print(color(f"Stream Security URL: {ll_url}", "blue"))
        if api_token:
            graph_client = GraphCommon(ll_url, token=api_token, customer_id=ws_id)
        else:
            graph_client = GraphCommon(ll_url, ll_username, ll_password, ws_id)
        print(color("Logged in successfully!", "green"))
    except Exception as e:
        print(color(f"Error: {e}", "red"))
        return
    
    try:
        print(color("Creating Boto3 Session", "blue"))
        if aws_profile_name:
            os.environ['AWS_PROFILE'] = aws_profile_name
            print(color(f"Using AWS profile: {aws_profile_name}", "blue"))
        else:
            print(color("No AWS profile specified, using default credential chain", "blue"))
        # Set up the Organizations client
        org_client = boto3.client('organizations')

        # Set up the STS client
        sts_client = boto3.client('sts')

        # Set up org account variable
        org_account_id = sts_client.get_caller_identity().get('Account')

        # Get all activated regions from Org account
        regions = [region['RegionName'] for region in boto3.client('ec2').describe_regions()['Regions']]

        print(color("Fetching all accounts connected to the organization", "blue"))
        list_accounts = get_all_accounts(org_client)

        # Getting only the account IDs of the active AWS accounts
        sub_accounts = [(a["Id"], a["Name"]) for a in list_accounts if a["Status"] == "ACTIVE"]
        print(color(f"Found {len(sub_accounts)} accounts", "blue"))
    except Exception as e: 
        print(color(f"Error: {e}", "red"))
        return

    if accounts:
        sub_accounts = [sa for sa in sub_accounts if sa[0] in accounts]

    print(color(f"Accounts to-be integrated: {[sa[0] for sa in sub_accounts]}", "blue"))
       # Confirm with the user to continue
    confirmation = input("Do you want to continue? Type 'yes' to proceed: ")
    if confirmation.lower() != 'yes':
        print("Operation canceled.")
        return

    failures = []
    all_deployed_stacks = []
    if parallel:
        with concurrent.futures.ThreadPoolExecutor(max_workers=parallel) as executor:
            # `parallel` is passed positionally on purpose: integrate_sub_account uses
            # `not parallel` to skip per-account blocking waits in parallel mode (the
            # sequential path below omits it -> defaults to False -> waits per account).
            future_to_account = {
                executor.submit(
                    integrate_sub_account,
                    environment_url, sub_account, sts_client, graph_client, regions, random_int, custom_tags, regions_to_integrate,
                    control_role, org_account_id, parallel, response, response_region, response_exclude_runbooks, eks_audit_logs, eks_audit_logs_regions
                ): sub_account for sub_account in sub_accounts
            }
            for future in concurrent.futures.as_completed(future_to_account):
                sub_account = future_to_account[future]
                account_id = sub_account[0]
                try:
                    account_deployed_stacks = future.result()
                    if account_deployed_stacks:
                        all_deployed_stacks.extend(account_deployed_stacks)
                except Exception as e:
                    failures.append((account_id, str(e)))
                    # Even though this account's run raised, it may have already
                    # created some real stacks before the failure (e.g. the init
                    # stack succeeded, then a later step raised) - preserve those
                    # so they still get swept/reported instead of silently lost.
                    all_deployed_stacks.extend(getattr(e, "deployed_stacks", None) or [])
    else:
        for sub_account in sub_accounts:
            account_id = sub_account[0]
            try:
                account_deployed_stacks = integrate_sub_account(
                    environment_url, sub_account, sts_client, graph_client, regions, random_int,
                    custom_tags, regions_to_integrate, control_role, org_account_id, response=response, response_region=response_region, response_exclude_runbooks=response_exclude_runbooks,
                    eks_audit_logs=eks_audit_logs, eks_audit_logs_regions=eks_audit_logs_regions)
                if account_deployed_stacks:
                    all_deployed_stacks.extend(account_deployed_stacks)
            except Exception as e:
                failures.append((account_id, str(e)))
                all_deployed_stacks.extend(getattr(e, "deployed_stacks", None) or [])

    if failures:
        print(color(f"Integration finished with {len(failures)} failure(s):", "red"))
        for account_id, msg in failures:
            print(color(f"  {account_id}: {msg}", "red"))

    # Every create_stack call above was fire-and-forget (submitted, not waited on).
    # Sweep every stack this run created to find out what actually happened, then
    # print one consolidated, honest summary instead of assuming success.
    if all_deployed_stacks:
        print(color(f"Checking final status of {len(all_deployed_stacks)} stack(s)...", "blue"))
        swept_stacks = sweep_stack_statuses(
            all_deployed_stacks, sts_client, org_account_id, control_role=control_role)

        counts = {"succeeded": 0, "failed": 0, "timed_out": 0, "errored": 0}
        for r in swept_stacks:
            # Defensive .get() with a fallback, not direct indexing: every
            # record reaching here is SUPPOSED to carry a final_status (either
            # set directly for a submit-time failure, or guaranteed by
            # sweep_stack_statuses/_sweep_account otherwise), but that
            # invariant is enforced only by convention across several
            # functions - a future change that violates it should degrade to
            # an honest "unknown, check manually" line instead of crashing
            # this loop and losing every other account's already-printed
            # results.
            status = r.get("final_status") or "UNKNOWN"
            reason = r.get("status_reason")
            line = f"{r['account']} ({r['name']}) | {r['region']} | {r['stack_type']} | {status}"
            if reason:
                line += f" | reason: {reason}"
            bucket, print_color = _classify_stack_status(status)
            counts[bucket] += 1
            print(color(line, print_color))

        succeeded_count = counts["succeeded"]
        failed_count = counts["failed"]
        timed_out_count = counts["timed_out"]
        errored_count = counts["errored"]

        summary = (
            f"Stack sweep summary ({len(swept_stacks)} stack(s) total): "
            f"{succeeded_count} succeeded, {failed_count} failed, "
            f"{timed_out_count} timed out, {errored_count} errored")
        if failed_count or errored_count:
            print(color(summary, "red"))
        elif timed_out_count:
            print(color(summary, "yellow"))
        else:
            print(color(summary, "green"))
    elif not failures:
        # Nothing new was deployed this run (e.g. every targeted account was
        # already fully READY in every region) - there is nothing to sweep.
        print(color("Integration finished successfully!", "green"))


def integrate_sub_account(
        environment_url, sub_account, sts_client, graph_client, regions, random_int, custom_tags, regions_to_integrate, control_role,
        org_account_id, parallel=False, response=False, response_region="us-east-1", response_exclude_runbooks="", eks_audit_logs=False, eks_audit_logs_regions=None):
    print(color(f"Account: {sub_account[0]} | Starting integration", color="blue"))
    deployed_stacks = []
    try:
        if sub_account[0] == org_account_id:
            sub_account_session = boto3.Session()
        else:
            # Assume the role in the sub_account[0]
            assumed_role = sts_client.assume_role(
                RoleArn=f'arn:aws:iam::{sub_account[0]}:role/{control_role}',
                RoleSessionName='MySessionName'
            )
            print(color(f"Account: {sub_account[0]} | Initializing Boto session", "blue"))
            sub_account_session = boto3.Session(
                aws_access_key_id=assumed_role['Credentials']['AccessKeyId'],
                aws_secret_access_key=assumed_role['Credentials']['SecretAccessKey'],
                aws_session_token=assumed_role['Credentials']['SessionToken']
            )
            print(color(f"Account: {sub_account[0]} | Session initialized successfully", "green"))

        print(color(f"Account: {sub_account[0]} | Checking if integration already exists", "blue"))
        ll_integrated = False
        try:
            sub_account_information = \
                [acc for acc in graph_client.get_accounts() if sub_account[0] == acc["cloud_account_id"]][0]
            if sub_account_information["status"] == "UNINITIALIZED":
                ll_integrated = True
                print(color(f"Account: {sub_account[0]} | Integrated but uninitialized, continuing", "blue"))
            elif sub_account_information["status"] == "READY":
                print(color(f"Account: {sub_account[0]} | Integration exists and in READY state", "green"))
                
                # Deploying response stack if enabled. wait=False here (this
                # branch used to pass wait=True) - matches the fire-and-forget
                # pattern every other deploy call in this file already uses;
                # the end-of-run sweep determines the real outcome instead of
                # blocking here.
                response_info = graph_client.get_account_response_config(sub_account_information["cloud_account_id"])
                remediation = response_info.get("remediation")
                if (remediation is None or remediation.get("status") is None) and response:
                    response_record = deploy_response_stack(
                        environment_url ,sub_account_information, sub_account_session, sub_account, response_region, random_int, custom_tags, response_exclude_runbooks, wait=False)
                    if response_record:
                        deployed_stacks.append(response_record)

                # Deploying EKS audit logs if enabled
                if eks_audit_logs:
                    eks_records = deploy_eks_audit_logs_stacks(
                        environment_url, sub_account_information, sub_account_session, sub_account, eks_audit_logs_regions, random_int, custom_tags, wait=False)
                    if eks_records:
                        deployed_stacks.extend(eks_records)
                
                print(color(f"Account: {sub_account[0]} | Checking if regions are updated", "blue"))
                current_regions = sub_account_information["cloud_regions"]
                if regions_to_integrate:
                    potential_regions = regions_to_integrate
                else:
                    potential_regions = get_active_regions(sub_account_session, regions)
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
                    collection_records = deploy_all_collection_stacks(
                        regions_to_integrate, sub_account_session, random_int, sub_account_information, sub_account,
                        custom_tags=custom_tags)
                    if collection_records:
                        deployed_stacks.extend(collection_records)
                else:
                    print(color(f"Account: {sub_account[0]} | All regions are integrated to realtime", "green"))
                return deployed_stacks
            else:
                err_msg = f"Account: {sub_account[0]} | Account is in {sub_account_information['status']} " \
                          f"status at StreamSecurity, remove it and try again"
                print(color(err_msg, "red"))
                raise Exception(err_msg)
        except IndexError:
            pass

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

        # Deploying the initial integration stack
        init_ok, init_record = deploy_init_stack(
                account_information, graph_client, sub_account, sub_account_session, random_int, not parallel,
                custom_tags=custom_tags)
        if init_record:
            deployed_stacks.append(init_record)
        if not init_ok:
            reason = init_record.get("status_reason") if init_record else None
            err_msg = f"Account: {sub_account[0]} | Something went wrong with init stack deployment"
            if reason:
                err_msg += f": {reason}"
            print(color(err_msg, "red"))
            raise Exception(err_msg)

        print(color(f"Account: {sub_account[0]} | Setting regions", "blue"))
        if regions_to_integrate:
            active_regions = regions_to_integrate
        else:
            print(color(f"Account: {sub_account[0]} | Getting active regions (Has EC2 instances)", "blue"))
            active_regions = get_active_regions(sub_account_session, regions)
        print(color(f"Account: {sub_account[0]} | Active regions are: {active_regions}", "blue"))

        if response:
            response_record = deploy_response_stack(
                environment_url, account_information, sub_account_session, sub_account, response_region, random_int, custom_tags, response_exclude_runbooks, wait=False)
            if response_record:
                deployed_stacks.append(response_record)

        if eks_audit_logs:
            eks_records = deploy_eks_audit_logs_stacks(
                environment_url, account_information, sub_account_session, sub_account, eks_audit_logs_regions, random_int, custom_tags, wait=False)
            if eks_records:
                deployed_stacks.extend(eks_records)

        # Updating the regions in StreamSecurity and waiting
        if not update_regions(graph_client, sub_account, active_regions, not parallel):
            err_msg = f"Account: {sub_account[0]} | Something went wrong with regions update"
            print(color(err_msg, "red"))
            raise Exception(err_msg)

        # Deploying collections stacks for all regions
        collection_records = deploy_all_collection_stacks(
            active_regions, sub_account_session, random_int, account_information, sub_account, custom_tags=custom_tags)
        if collection_records:
            deployed_stacks.extend(collection_records)

        return deployed_stacks

    except Exception as e:
        err_msg = f"Account: {sub_account[0]} | Something went wrong: {e}"
        print(color(err_msg, "red"))
        wrapped = Exception(err_msg)
        # Preserve whatever stacks were already created before this failure (e.g.
        # the init stack succeeded but a later step raised) so main() can still
        # sweep/report them instead of silently losing track of real AWS resources.
        wrapped.deployed_stacks = deployed_stacks
        raise wrapped


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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='This script will integrate StreamSecurity environment with every account in the organization.')
    parser.add_argument(
        "--environment_url", help="The StreamSecurity environment URL", required=True)
    parser.add_argument(
        "--api_token", help="Stream Security API token. If set, --environment_user_name and --environment_password are not required.", required=False)
    parser.add_argument(
        "--environment_user_name", help="The StreamSecurity environment user name. Ignored if --api_token is provided.", required=False)
    parser.add_argument(
        "--environment_password", help="The StreamSecurity environment password. Ignored if --api_token is provided.", required=False)
    parser.add_argument(
        "--aws_profile_name", help="AWS profile name to use. If omitted, the default credential chain (env vars, SSO session, instance role, etc.) is used.",
        default=None)
    parser.add_argument(
        "--accounts", help="Accounts list to iterate when creating the report (e.g '123123123123,321321321321')",
        required=False)
    parser.add_argument(
        "--parallel", help="Number of threads for parallel integration", type=int, required=False)
    parser.add_argument(
        "--ws_id", help="ID of the WS to deploy to", required=False)
    parser.add_argument(
        "--custom_tags", help="Add custom tags to CFT Stacks and all resources, format: Name|Test,Env|Dev",
        required=False)
    parser.add_argument(
        "--regions", help="Force select specific regions to integrate, separated by comma", required=False)
    parser.add_argument(
        "--control_role", help="Specify a role for control", default="OrganizationAccountAccessRole", required=False)
    parser.add_argument(
        "--response", help="Create response stack", action="store_true", required=False)
    parser.add_argument(
        "--response_region", help="Region for response stack", required=False, default="us-east-1")
    parser.add_argument(
        "--response_exclude_runbooks", help="Comma separated list of runbooks to exclude from response stack",
        required=False, default="")
    parser.add_argument(
        "--eks_audit_logs", help="Enable EKS audit logs", action="store_true", required=False)
    parser.add_argument(
        "--eks_audit_logs_regions", help="Regions for EKS audit logs, separated by comma", required=False)
    args = parser.parse_args()
    main(args.environment_url, args.environment_user_name, args.environment_password,
         args.aws_profile_name, args.accounts, args.parallel,
         ws_id=args.ws_id, custom_tags=args.custom_tags, regions_to_integrate=args.regions,
         control_role=args.control_role, response=args.response, response_region=args.response_region, response_exclude_runbooks=args.response_exclude_runbooks,
         eks_audit_logs=args.eks_audit_logs, eks_audit_logs_regions=args.eks_audit_logs_regions, api_token=args.api_token)
