import argparse
import concurrent.futures
import os
import sys
import time

# Add the project root directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))
try:
    from src.python.common.boto_common import *
except ModuleNotFoundError:
    sys.path.append("../../..")
    from src.python.common.boto_common import *

import boto3


class _DeleteIntegrationParser(argparse.ArgumentParser):
    """Parser that rejects mixing lambda-only mode with the stack-only flags,
    regardless of the order the flags appear in."""
    def parse_args(self, args=None, namespace=None):
        ns = super().parse_args(args, namespace)
        if ns.lambda_name_contains and (ns.force_delete_failed or ns.stack_name_contains):
            self.error(
                "--lambda_name_contains cannot be combined with --force_delete_failed "
                "or --stack_name_contains (lambda-only mode does not touch stacks).")
        if ns.lambda_name_contains:
            try:
                validate_lambda_pattern(ns.lambda_name_contains)
            except ValueError as e:
                self.error(str(e))
        return ns


def build_arg_parser():
    parser = _DeleteIntegrationParser(
        description="Delete Stream Security integration from organization accounts. "
                    "Default mode deletes the CloudFormation stacks (which removes the "
                    "integration). Lambda-only mode (--lambda_name_contains) deletes only "
                    "matching Lambda functions and does NOT remove the integration.")
    parser.add_argument(
        "--accounts", help="Account IDs to target, e.g. '111111111111,222222222222' "
        "(default: all ACTIVE org accounts)", required=False)
    parser.add_argument(
        "--regions", help="Regions to target, e.g. 'us-east-1,eu-west-1' "
        "(default: all enabled regions)", required=False)
    parser.add_argument(
        "--aws_profile_name", help="AWS profile with admin permissions on the org account. "
        "Omit to use the default credential chain / SSO.", default=None)
    parser.add_argument(
        "--just_print", action="store_true",
        help="Dry run - list what would be deleted, delete nothing")
    # Stack-only flags (CF mode)
    parser.add_argument(
        "--force_delete_failed", action="store_true",
        help="CF mode only: target DELETE_FAILED stacks with FORCE_DELETE_STACK")
    parser.add_argument(
        "--stack_name_contains",
        help="CF mode only: filter stacks by name (case-insensitive)", required=False)
    # Lambda-only mode
    parser.add_argument(
        "--lambda_name_contains",
        help="LAMBDA-ONLY MODE: delete only Lambda functions whose name contains this "
        "string (case-insensitive, min 3 chars). Does NOT touch CloudFormation stacks and "
        "does NOT remove the Stream Security integration. Mutually exclusive with the "
        "stack-only flags.", required=False)
    return parser


def _session_for_account(sub_account, sts_client, management_account_id):
    """Return a boto3 Session for the account, or raise on assume-role failure.
    The management account uses the current session (no assume-role)."""
    if sub_account[0] == management_account_id:
        return boto3.Session()
    assumed_role = sts_client.assume_role(
        RoleArn=f'arn:aws:iam::{sub_account[0]}:role/OrganizationAccountAccessRole',
        RoleSessionName='MySessionName')
    return boto3.Session(
        aws_access_key_id=assumed_role['Credentials']['AccessKeyId'],
        aws_secret_access_key=assumed_role['Credentials']['SecretAccessKey'],
        aws_session_token=assumed_role['Credentials']['SessionToken'])


def _scan_account_lambdas(sub_account, session, regions, pattern):
    """Scan all regions of one account in parallel; return
    (to_delete, skipped, scan_errors) with account id+name stamped on each result
    dict. scan_errors carries regions that could not be scanned and functions
    whose tags could not be read, so an incomplete scan is surfaced rather than
    silently dropping target functions."""
    to_delete, skipped, scan_errors = [], [], []
    if not regions:
        return to_delete, skipped, scan_errors

    def _stamp(items):
        for item in items:
            item.update({"account": sub_account[0], "name": sub_account[1]})

    # Warm the session's client-creation caches single-threaded; boto3 Session
    # is not thread-safe for concurrent first-time client creation. The region
    # threads below create both a lambda client (the scan) and a cloudformation
    # client (the orphan stack-existence check), so warm both services here.
    session.client("lambda", region_name=regions[0])
    # Also force the cloudformation client AND its list_stacks paginator model to
    # load single-threaded here — the orphan check calls get_paginator inside the
    # region threads, and first-loading the paginator config concurrently races.
    session.client("cloudformation", region_name=regions[0]).get_paginator("list_stacks")
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        future_to_region = {
            executor.submit(scan_lambdas_in_region, session, region, pattern): region
            for region in regions}
        for future in concurrent.futures.as_completed(future_to_region):
            region = future_to_region[future]
            try:
                region_delete, region_skipped, region_errors = future.result()
            except Exception as e:
                # The whole region could not be scanned. Record it so its target
                # functions are surfaced as a gap and counted, not silently lost.
                print(color(f"Account: {sub_account[0]} | Failed to scan region "
                            f"{region}: {e}", "red"))
                scan_errors.append({"region": region, "function": "(entire region)",
                                    "reason": f"region scan failed: {str(e)[:150]}"})
                _stamp([scan_errors[-1]])
                continue
            _stamp(region_delete); _stamp(region_skipped); _stamp(region_errors)
            to_delete.extend(region_delete)
            skipped.extend(region_skipped)
            scan_errors.extend(region_errors)
    return to_delete, skipped, scan_errors


def _reverify_orphans(session, account_id, items, failed):
    """Guard against a scan→delete race: an orphan classified because its stack was
    gone at scan time could have had that stack recreated before this delete phase
    reached it. Re-list the live stacks (per region) for this account's orphans and,
    for each orphan, either keep it (still gone), re-protect it (stack came back), or
    fail it (couldn't re-verify). Non-orphan items (no CloudFormation tag) pass
    through untouched.

    Returns (kept, reprotected):
      - kept:        still safe to delete (stack confirmed still gone) + non-orphans.
      - reprotected: stack exists again — a CORRECT protection, not an operation
                     failure, so it is reported as skipped and does NOT drive a
                     non-zero exit.
    Orphans we can no longer verify are appended to `failed` (a real gap that drives
    a non-zero exit) so they are never deleted on an unverified guess.

    Relies on the scan-time invariant that an orphan's region equals its stack's
    owning region (scan_lambdas_in_region only classifies an orphan when
    region_from_stack_id(stack) == the scan region), so re-listing by it["region"]
    checks the correct region."""
    orphan_regions = {it["region"] for it in items if it.get("orphaned_stack")}
    if not orphan_regions:
        return items, []
    live_now = {}
    for rg in orphan_regions:
        try:
            live_now[rg] = list_live_stack_names(session, rg)
        except Exception as e:
            # Can't re-verify — refuse to delete this region's orphans (safe default),
            # and record them as failed (a real gap), matching the bookkeeping below.
            live_now[rg] = None
            print(color(f"Account: {account_id} | Could not re-list stacks in {rg} to "
                        f"re-verify orphans; those are recorded as failed (not deleted): "
                        f"{e}", "yellow"))
    kept, reprotected = [], []
    for it in items:
        stack = it.get("orphaned_stack")
        if not stack:
            kept.append(it)
            continue
        live = live_now.get(it["region"])
        if live is None:
            failed.append(dict(it, reason="could not re-verify stack still gone at "
                                          "delete time - not deleted"))
        elif stack in live:
            print(color(f"Account: {account_id} | Stack {stack} exists again - NOT "
                        f"deleting {it['function']} ({it['region']}) (protected, not a "
                        f"failure)", "yellow"))
            reprotected.append(it)
        else:
            kept.append(it)
    return kept, reprotected


def _run_lambda_mode(sub_accounts, sts_client, management_account_id, regions,
                     pattern, just_print):
    pattern = validate_lambda_pattern(pattern)
    print(color(
        "LAMBDA-ONLY MODE: only Lambda functions will be deleted. CloudFormation "
        "stacks are NOT touched — to remove the Stream Security integration itself, "
        "run without --lambda_name_contains.", "yellow"))

    all_to_delete, all_skipped, all_scan_errors, assume_role_failures = [], [], [], []

    # Scan phase
    try:
        for sub_account in sub_accounts:
            try:
                session = _session_for_account(sub_account, sts_client, management_account_id)
            except Exception as e:
                print(color(f"Account: {sub_account[0]} | Can't assume role, skipping. "
                            f"Error: {e}", "red"))
                assume_role_failures.append((sub_account[0], sub_account[1], str(e)))
                continue
            print(color(f"Account: {sub_account[0]} | Scanning {len(regions)} regions", "blue"))
            try:
                to_delete, skipped, scan_errors = _scan_account_lambdas(
                    sub_account, session, regions, pattern)
            except Exception as e:
                # One account's unexpected scan error must not discard every other
                # account's results — record it as a gap and carry on.
                print(color(f"Account: {sub_account[0]} | Unexpected error scanning "
                            f"account: {e}", "red"))
                all_scan_errors.append(
                    {"account": sub_account[0], "name": sub_account[1], "region": "-",
                     "function": "(entire account)",
                     "reason": f"account scan failed: {str(e)[:150]}"})
                continue
            all_to_delete.extend(to_delete)
            all_skipped.extend(skipped)
            all_scan_errors.extend(scan_errors)
    except KeyboardInterrupt:
        # Match the delete phase: abort cleanly instead of dumping a traceback.
        # The scan is incomplete, so do not proceed to deletion — but still show
        # what was already discovered so the operator isn't misled into thinking
        # nothing was found.
        print(color("\nInterrupted during scan - aborting before any deletion.", "yellow"))
        print_lambda_plan(all_to_delete, all_skipped)
        if all_to_delete:
            print(color(f"({len(all_to_delete)} function(s) were already identified but "
                        f"will NOT be deleted because the scan was interrupted.)", "yellow"))
        print_lambda_summary([], [], all_scan_errors, all_skipped, assume_role_failures)
        return 1

    # Listing + plan file
    print_lambda_plan(all_to_delete, all_skipped)
    if all_scan_errors:
        print(color(
            f"WARNING: {len(all_scan_errors)} region(s)/function(s) could not be fully "
            f"scanned — the plan below may be INCOMPLETE (these are reported in the run "
            f"summary and make the run exit non-zero):", "yellow"))
        for e in all_scan_errors:
            print(color(f"  SCAN GAP | account {e['account']} | {e['region']} | "
                        f"{e['function']} | {e['reason']}", "yellow"))
    plan_path = os.path.join(
        os.getcwd(), f"lambda_delete_plan_{time.strftime('%Y%m%d-%H%M%S')}.txt")
    if all_to_delete:
        try:
            write_plan_file(all_to_delete, plan_path)
            print(color(f"Full plan written to: {plan_path}", "blue"))
        except Exception as e:
            print(color(f"Could not write plan file to {plan_path}: {e} "
                        "(continuing - the plan above is still valid)", "yellow"))

    # Scan gaps are surfaced as failures so they show in the summary and drive a
    # non-zero exit code, even on paths that delete nothing.
    def _surface_early():
        return print_lambda_summary([], [], all_scan_errors, all_skipped, assume_role_failures)

    if just_print:
        print(color("Dry run (--just_print) - nothing deleted.", "green"))
        return _surface_early()
    if not all_to_delete:
        print(color("No matching lambda functions found - nothing to delete.", "green"))
        return _surface_early()

    account_count = len({d["account"] for d in all_to_delete})
    if not confirm_deletion(len(all_to_delete), account_count, sys.stdin.isatty, input):
        early = _surface_early()
        if not sys.stdin.isatty():
            # Non-interactive and we had functions pending: the tool refused to
            # delete because it couldn't confirm. That is NOT success — exit
            # non-zero so a cron/CI caller doesn't record it as a completed run.
            print(color("Refused to delete without an interactive confirmation while "
                        f"{len(all_to_delete)} function(s) were pending — exiting non-zero.",
                        "red"))
            return max(early, 1)
        # Interactive operator deliberately declined: a clean abort, exit reflects
        # only real gaps (scan errors), which _surface_early already returned.
        return early

    # Delete phase - re-assume roles fresh (the scan can outlive 1h STS creds)
    deleted, already_gone, failed, reprotected = [], [], [], []
    interrupted = False
    by_account = {}
    for d in all_to_delete:
        by_account.setdefault((d["account"], d["name"]), []).append(d)

    try:
        for (account_id, name), items in by_account.items():
            sub_account = (account_id, name)
            try:
                session = _session_for_account(sub_account, sts_client, management_account_id)
            except Exception as e:
                print(color(f"Account: {account_id} | Can't assume role for delete, "
                            f"skipping. Error: {e}", "red"))
                # These functions were planned but couldn't be deleted — record each
                # as failed (a real operation failure) so the summary and exit code
                # reflect the miss. Do NOT also add the account to the 'unreachable'
                # list: that would double-report the same failure across two counters.
                for it in items:
                    failed.append(dict(it, reason=f"delete skipped - could not assume "
                                                  f"role: {str(e)[:120]}"))
                continue
            try:
                # Re-verify orphans before deleting: a stack could have been
                # (re)created between the scan and now (the delete phase re-assumes
                # roles because the scan can outlive 1h STS creds). Never delete a
                # Lambda whose stack came back. Single-threaded, before the pool, and
                # inside this try so an unexpected error here can't crash the whole
                # run — the except below records this account's items as failed.
                items, reprotected_now = _reverify_orphans(session, account_id, items, failed)
                reprotected.extend(reprotected_now)
                if not items:
                    continue
                # Warm the session's client-creation caches single-threaded; boto3
                # Session is not thread-safe for concurrent first-time client creation.
                session.client("lambda", region_name=items[0]["region"])
                with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
                    future_to_item = {
                        executor.submit(delete_lambda_function, session, it["region"],
                                        it["function"]): it for it in items}
                    try:
                        for future in concurrent.futures.as_completed(future_to_item):
                            it = future_to_item[future]
                            try:
                                outcome = future.result()
                                if outcome == "already gone":
                                    already_gone.append(it)
                                else:
                                    deleted.append(it)
                                    print(color(f"Account: {account_id} | Deleted "
                                                f"{it['function']} ({it['region']})", "green"))
                            except Exception as e:
                                it2 = dict(it, reason=str(e)[:200])
                                failed.append(it2)
                                print(color(f"Account: {account_id} | Failed to delete "
                                            f"{it['function']} ({it['region']}): {e}", "red"))
                    except KeyboardInterrupt:
                        try:
                            executor.shutdown(wait=False, cancel_futures=True)
                        except TypeError:
                            # cancel_futures was added in Python 3.9; on 3.8 fall back
                            # to a plain shutdown so Ctrl+C still aborts cleanly.
                            executor.shutdown(wait=False)
                        raise
            except Exception as e:
                # An unexpected error for this account (e.g. the warm-up client call
                # or executor setup) must not crash the whole run — record whatever
                # of this account's functions weren't handled as failed and continue.
                print(color(f"Account: {account_id} | Unexpected error during delete, "
                            f"skipping remaining: {e}", "red"))
                done = {(x["account"], x["region"], x["function"])
                        for x in deleted + already_gone + failed}
                for it in items:
                    if (it["account"], it["region"], it["function"]) not in done:
                        failed.append(dict(it, reason=f"delete error: {str(e)[:120]}"))
                continue
    except KeyboardInterrupt:
        interrupted = True
        print(color("\nInterrupted - stopping. Summary of work done so far:", "yellow"))

    # Account for functions that were planned but never attempted (e.g. the run
    # was interrupted before reaching them), so an aborted run isn't mistaken for
    # a completed one.
    attempted = {(x["account"], x["region"], x["function"])
                 for x in deleted + already_gone + failed + reprotected}
    not_attempted = [x for x in all_to_delete
                     if (x["account"], x["region"], x["function"]) not in attempted]
    if not_attempted:
        print(color(f"{len(not_attempted)} planned function(s) were NOT attempted "
                    f"(run interrupted before reaching them); re-run to finish them.",
                    "yellow"))

    if reprotected:
        print(color(f"{len(reprotected)} planned function(s) were protected at delete "
                    f"time (their CloudFormation stack was recreated) - not deleted, not "
                    f"a failure.", "yellow"))
    # Re-protected functions are now CloudFormation-managed again, so they count as
    # skipped (not failures) and do not drive a non-zero exit.
    rc = print_lambda_summary(deleted, already_gone, failed + all_scan_errors,
                              all_skipped + reprotected, assume_role_failures)
    # An interrupted run left work undone — never report it as a clean success.
    return max(rc, 1) if interrupted else rc


def _run_cf_mode(sub_accounts, sts_client, management_account_id, regions,
                 just_print, force_delete_failed, stack_name_contains):
    assume_role_failures = []
    for sub_account in sub_accounts:
        try:
            session = _session_for_account(sub_account, sts_client, management_account_id)
        except Exception as e:
            print(color(f"Account: {sub_account[0]} | Can't assume role, skipping. "
                        f"Error: {e}", "red"))
            assume_role_failures.append((sub_account[0], sub_account[1], str(e)))
            continue
        print(color(f"Account: {sub_account[0]} | Session initialized", "green"))
        delete_stacks_in_all_regions(sub_account, session, regions, just_print,
                                     force_delete_failed, stack_name_contains)
    if assume_role_failures:
        print(color("=" * 60, "blue"))
        print(color(f"{len(assume_role_failures)} account(s) unreachable "
                    f"(assume-role failed):", "yellow"))
        for line in format_assume_role_failure_lines(assume_role_failures):
            print(color(line, "yellow"))
    # Unreachable accounts are reported but do not fail the exit code (they are a
    # gap, not an operation failure). CF mode does not compute a per-stack failure
    # count, and its stack helpers are not resilient: filter_ll_stacks_by_name
    # swallows list errors (a region that can't be listed reports zero stacks and
    # is silently skipped), while delete_stack is unguarded (an error initiating a
    # delete aborts the run with a traceback). Hardening that legacy path is out of
    # scope here; this returns 0 because there is no tracked failure count to report.
    return 0


def main(accounts, aws_profile_name, regions=None, just_print=False,
         force_delete_failed=False, stack_name_contains=None, lambda_name_contains=None):
    # Adaptive retries for every client created below (both modes)
    os.environ["AWS_RETRY_MODE"] = "adaptive"
    os.environ["AWS_MAX_ATTEMPTS"] = "10"

    if aws_profile_name:
        os.environ['AWS_PROFILE'] = aws_profile_name
        print(color(f"Using AWS profile: {aws_profile_name}", "blue"))
    else:
        print(color("No AWS profile specified - using default credential chain", "blue"))

    account_filter = accounts.replace(" ", "").split(",") if accounts else None

    org_client = boto3.client('organizations', region_name='us-east-1')
    sts_client = boto3.client('sts', region_name='us-east-1')

    if regions:
        regions = regions.replace(" ", "").split(",")
    else:
        regions = [r['RegionName'] for r in
                   boto3.client('ec2', region_name='us-east-1').describe_regions()['Regions']]
    print(color(f"Targeting {len(regions)} region(s)", "blue"))

    print("Fetching all accounts connected to the organization")
    list_accounts = get_all_accounts(org_client)
    # Used to route the management account through the current session instead of
    # assume-role (it cannot assume OrganizationAccountAccessRole into itself). If
    # DescribeOrganization is not permitted, fall back to the caller's own account
    # id: when the tool is run from the management account (the usual case) this
    # still identifies it correctly, avoiding both a crash and a doomed assume-role.
    try:
        management_account_id = org_client.describe_organization()['Organization']['MasterAccountId']
    except Exception as e:
        try:
            management_account_id = sts_client.get_caller_identity()['Account']
            print(color(f"Warning: DescribeOrganization not permitted ({e}); assuming the "
                        f"current account {management_account_id} is the management "
                        f"account.", "yellow"))
        except Exception as e2:
            management_account_id = None
            print(color(f"Warning: could not determine the management account "
                        f"(DescribeOrganization: {e}; caller identity: {e2}); every "
                        f"account will be accessed via assume-role.", "yellow"))
    sub_accounts = [(a["Id"], a["Name"]) for a in list_accounts if a["Status"] == "ACTIVE"]
    if account_filter:
        active_ids = {sa[0] for sa in sub_accounts}
        unknown = [aid for aid in account_filter if aid not in active_ids]
        if unknown:
            # Don't silently ignore a typo'd or inactive account id — that would let
            # a run "succeed" while never targeting the account the operator meant.
            print(color(f"Warning: {len(unknown)} requested account(s) are not ACTIVE "
                        f"org accounts and will be skipped: {unknown}", "red"))
        sub_accounts = [sa for sa in sub_accounts if sa[0] in account_filter]
        if not sub_accounts:
            print(color("None of the requested --accounts match an ACTIVE org account; "
                        "nothing to do. Exiting non-zero.", "red"))
            return 1
    print(color(f"Accounts to process: {[sa[0] for sa in sub_accounts]}", "blue"))

    if lambda_name_contains:
        return _run_lambda_mode(sub_accounts, sts_client, management_account_id,
                                regions, lambda_name_contains, just_print)
    return _run_cf_mode(sub_accounts, sts_client, management_account_id, regions,
                        just_print, force_delete_failed, stack_name_contains)


if __name__ == "__main__":
    parser = build_arg_parser()
    args = parser.parse_args()
    exit_code = main(
        args.accounts, args.aws_profile_name, regions=args.regions,
        just_print=args.just_print, force_delete_failed=args.force_delete_failed,
        stack_name_contains=args.stack_name_contains,
        lambda_name_contains=args.lambda_name_contains)
    sys.exit(1 if exit_code else 0)
