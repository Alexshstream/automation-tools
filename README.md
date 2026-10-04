# automation-tools
This repository provides various Automation Tools for the Stream Security platform

## Installation
To install the dependencies, run the following command:

```pip install -r requirements.txt```
This will install all the required libraries listed in the `requirements.txt` file.

## "organization based integration" tool usage
You need to have "aws-cli" installed.

 
To run the script, simply execute:

```python src/python/utilities/organization_integration.py --environment_sub_domain <ENV_NAME> --environment_user_name <ENV_USERNAME> --environment_password <ENV_PASSWORD>```

Note that the script will use the `staging` AWS profile by default - make sure that it has the proper organization level IAM permissions, if you want to point to another AWS profile, you can add the `aws_profile_name` flag.

Script execution with the AWS profile flag:

```python src/python/utilities/organization_integration.py --environment_sub_domain <ENV_NAME> --environment_user_name <ENV_USERNAME> --environment_password <ENV_PASSWORD> --aws_profile_name <AWS_PROFILE_NAME>```

## Organization Lambda (auto onboarding of new accounts)
`lambda/organization_integration/org_lambda.py` deploys the `streamsec-organization-lambda` function in the organization management account. The Lambda onboards new accounts as they are created.

```python lambda/organization_integration/org_lambda.py --environment <ENV_NAME> --ws-id <WS_ID> --api-token <API_TOKEN> --regions us-east-1,us-west-2,eu-west-1```

Regions:
- `--regions` sets the regions each account is onboarded to, meaning the regions that get a collection stack. It is stored as the Lambda's `REGIONS` environment variable.
- Without it, the Lambda detects regions that have EC2 instances, plus us-east-1. A newly created account has no instances, so it is onboarded to us-east-1 only. Set `--regions` if new accounts should be onboarded to more regions.
- `--response-region` only sets where the response stack is deployed. It does not affect collection regions.

To change the regions of an existing deployment, either edit the `REGIONS` environment variable on the Lambda, or run the script with `--cleanup` and deploy it again with `--regions`.

Use `--schedule-scan-days <N>` to run the Lambda periodically. Each run also adds collection stacks in regions that are missing them for already onboarded accounts.

## Prerequisites
- Python 3.9 or higher
- pip
- All dependencies listed in the `requirements.txt` file