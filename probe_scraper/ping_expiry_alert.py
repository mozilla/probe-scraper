import argparse
import re
import sys
from collections import defaultdict
from datetime import date, datetime
from typing import Any, Dict, List, Tuple

import requests
from google.cloud import bigquery

from probe_scraper.emailer import send_ses

EMAIL_SUBJECT_TEMPLATE = "Glean Pings Expiring for {app_name}"

EMAIL_TEMPLATE = """
The following BigQuery tables are set to start expiring collected data soon based on their retention policies.
Note that this expiration will only delete data from the raw telemetry ("ping") tables listed below.
Aggregated/analytics tables derived from the telemetry data, such as "clients_daily" and "clients_last_seen" tables, will not be affected and will continue to retain all historical data.

{tables}

What to do about this:

1. If this is expected and you do not need data for any of these tables past the listed dates, then no action is needed.

2. If you wish to continue collecting data for a longer period of time for any of these tables, please file Jira ticket for data engineering, requesting the changes to retention settings at https://mozilla-hub.atlassian.net/secure/CreateIssue.jspa?issuetype=10007&pid=10056

Requests should be triaged at least weekly but if there's urgency or if you have any questions, please ask on the #data-help Slack channel. We'll give you a hand.

Retention policies are defined in probe-scraper [1], with options to either stop collecting data after a certain or delete data older than the specified number of days.

Your Friendly Neighborhood Data Team

[1] - https://github.com/mozilla/probe-scraper/blob/main/repositories.yaml

This is an automated message sent from probe-scraper.  See https://github.com/mozilla/probe-scraper for details.
"""  # noqa

APP_GROUP_TEMPLATE = """
{app_name}:
{messages}
"""

RETENTION_DAYS_MESSAGE_TEMPLATE = '\t- The "{table_name}" table for will start expiring data older than {retention_days} days starting on {expiry_date} ({num_weeks} week{plural_weeks} from now)'  # noqa

# emails in this list will receive alerts for all pings
DEFAULT_EMAILS = ["telemetry-alerts@mozilla.com", "dataops+alerts@mozilla.com"]

NOTIFICATION_DAYS_MAX = 25
NOTIFICATION_DAYS_MIN = 5

EXPIRATIONS_QUERY_TEMPLATE = """
WITH actual_expiration_days AS (
  SELECT
    table_schema AS dataset_id,
    table_name AS table_id,
    CAST(REGEXP_EXTRACT(option_value, "^[0-9]+") AS INT) AS actual_partition_expiration_days,
  FROM
    `moz-fx-data-shared-prod.region-us.INFORMATION_SCHEMA.TABLE_OPTIONS`
  WHERE
    option_name = "partition_expiration_days"
)
SELECT
  project_id,
  dataset_id,
  ARRAY_AGG(
    STRUCT(
        table_id, partition_expiration_days,
        actual_partition_expiration_days,
        next_deletion_date,
        expiration_changed
    ) ORDER BY table_id
  ) AS tables,
FROM
  `moz-fx-data-shared-prod.monitoring_derived.table_partition_expirations_v1`
FULL JOIN
  actual_expiration_days
USING
  (dataset_id, table_id)
WHERE
  run_date = "{run_date}"
  AND project_id = "{project}"
  AND ENDS_WITH(dataset_id, "_stable")
GROUP BY
  project_id,
  dataset_id
ORDER BY
  dataset_id
"""


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--run-date",
        "--run_date",
        type=date.fromisoformat,
        required=True,
        help="The date to use to check for expiring pings",
    )
    parser.add_argument(
        "--dry-run",
        "--dry_run",
        "--dryrun",
        action="store_true",
        help="Whether emails should be sent, used for testing",
    )
    parser.add_argument(
        "--bigquery-project",
        "--bigquery_project",
        default="moz-fx-data-shared-prod",
        help="Bigquery project in which ping tables are located",
    )
    return parser.parse_args()


def request_get(url: str) -> Dict:
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


def send_emails(messages_by_email: Dict[str, Dict[str, List[str]]], dry_run: bool):
    for email, messages_by_app in messages_by_email.items():
        combined_messages = [
            APP_GROUP_TEMPLATE.format(app_name=app, messages="\n".join(messages))
            for app, messages in messages_by_app.items()
        ]
        email_body = EMAIL_TEMPLATE.format(tables="".join(combined_messages))
        email_subject = EMAIL_SUBJECT_TEMPLATE.format(
            app_name=(
                f"{len(messages_by_app)} apps"
                if len(messages_by_app) > 1
                else list(messages_by_app.keys())[0]
            )
        )
        send_ses(
            fromaddr="telemetry-alerts@mozilla.com",
            subject=email_subject,
            body=email_body,
            recipients=[email],
            dryrun=dry_run,
        )


def send_error_email(message: str, run_date: date, dry_run: bool):
    email_subject = f"Ping expiry alert errors on {run_date.isoformat()}"
    send_ses(
        fromaddr="telemetry-alerts@mozilla.com",
        subject=email_subject,
        body=message,
        recipients=["telemetry-alerts@mozilla.com"],
        dryrun=dry_run,
    )


def table_name_to_doctype(table_name: str) -> str:
    """Convert a bigquery table name to the associated telemetry document type."""
    return re.sub("_v[0-9]+$", "", table_name).replace("_", "-")


def validate_retention_settings(
    dataset_info: bigquery.Row, app_info: Dict[str, Any]
) -> List[Tuple[str, int, int]]:
    """Return a list of tables that have retention settings that do not match metadata.

    :param dataset_info: Row from the query on monitoring_derived.table_partition_expirations_v1.
    :param app_info: Entry from probeinfo app listings

    :return: List of tuples of (table_id, retention set in metadata, retention set in bigquery)
    """
    errors = []

    pipeline_metadata = app_info.get("moz_pipeline_metadata", {})

    default_retention_days = (
        app_info.get("moz_pipeline_metadata_defaults", {})
        .get("expiration_policy", {})
        .get("delete_after_days")
    )
    for table_info in dataset_info["tables"]:
        document_type = table_name_to_doctype(table_info["table_id"])
        applied_retention_days = table_info["actual_partition_expiration_days"]

        if (
            delete_after_days := pipeline_metadata.get(document_type, {})
            .get("expiration_policy", {})
            .get("delete_after_days")
        ) is not None:
            metadata_retention_days = delete_after_days
        else:
            metadata_retention_days = default_retention_days

        if (
            metadata_retention_days != applied_retention_days
            and table_info["expiration_changed"] is False
        ):
            errors.append(
                (
                    f"{dataset_info['dataset_id']}.{table_info['table_id']}",
                    metadata_retention_days,
                    applied_retention_days,
                )
            )

    return errors


def get_expiring_pings(
    run_date: datetime.date,
    project_id: str,
) -> Tuple[Dict[str, Dict[str, List[str]]], Dict[str, List]]:
    """
    Get expiring pings across all apps and a tuple of expiring pings and errors encountered.

    Expiring pings are stored in a dict where the key is an email to send to and the value
    is a dict with list of messages per app.

    Errors are stored in a dict where the key is a bigquery table name and the value is a list of
    errors related to the associated ping.

    Ping expiration is based on the most recent data in the
    moz-fx-data-shared-prod.monitoring_derived.table_partition_expirations_v1 bigquery table.
    """
    client = bigquery.Client()

    app_listings = request_get(
        "https://probeinfo.telemetry.mozilla.org/v2/glean/app-listings"
    )
    bq_dataset_to_app_listing = {app["bq_dataset_family"]: app for app in app_listings}

    errors = defaultdict(list)

    # dict structure: {email: {app_name: [messages]}}
    expiring_pings_by_email = defaultdict(lambda: defaultdict(list))

    current_expirations = list(
        client.query_and_wait(
            EXPIRATIONS_QUERY_TEMPLATE.format(run_date=run_date, project=project_id)
        )
    )

    for dataset_info in current_expirations:
        document_namespace = re.sub("_stable$", "", dataset_info["dataset_id"])
        app_info = bq_dataset_to_app_listing.get(document_namespace)

        # check if retention settings in metadata match applied settings (glean apps only)
        if app_info is not None:
            for (
                table_id,
                metadata_retention_days,
                applied_retention_days,
            ) in validate_retention_settings(dataset_info, app_info):
                errors[f"{project_id}.{table_id}"].append(
                    f"Retention period in metadata ({metadata_retention_days} days) "
                    f"does not match period applied to table ({applied_retention_days} days)"
                )

            app_pings = request_get(
                f"https://probeinfo.telemetry.mozilla.org/glean/{app_info['v1_name']}/pings"
            )

        # Find expiring pings and create list of emails to send
        for table_info in dataset_info["tables"]:
            # Send to app and ping owners for glean apps
            if app_info is not None:
                app_name = app_info["app_name"]
                document_type = table_name_to_doctype(table_info["table_id"])

                email_list = {
                    *(
                        app_pings[document_type]["history"][-1]["notification_emails"]
                        if document_type in app_pings
                        else []
                    ),
                    *app_info["notification_emails"],
                    *DEFAULT_EMAILS,
                }
            # send to telemetry-alerts@mozilla.com for legacy telemetry
            else:
                app_name = "legacy telemetry"
                email_list = {
                    "telemetry-alerts@mozilla.com",
                    *DEFAULT_EMAILS,
                }

            expires_in_days = (
                (table_info["next_deletion_date"] or run_date) - run_date
            ).days

            if NOTIFICATION_DAYS_MIN <= expires_in_days <= NOTIFICATION_DAYS_MAX:
                message = RETENTION_DAYS_MESSAGE_TEMPLATE.format(
                    table_name=f"{document_namespace}."
                    f"{re.sub('_v[0-9]+$', '', table_info['table_id'])}",
                    retention_days=table_info["partition_expiration_days"],
                    expiry_date=table_info["next_deletion_date"],
                    num_weeks=expires_in_days // 7,
                    plural_weeks="" if expires_in_days // 7 == 1 else "s",
                )
                for email in email_list:
                    expiring_pings_by_email[email][app_name].append(message)

    return expiring_pings_by_email, errors


def main():
    args = parse_args()

    expiring_pings_by_email, errors = get_expiring_pings(
        run_date=args.run_date,
        project_id=args.bigquery_project,
    )

    # Only send emails on Wednesday, dry run on other days for error checking
    dry_run = args.dry_run or args.run_date.weekday() != 2

    if len(errors) > 0:
        error_string = "\n".join([f"{ping}: {msg}" for ping, msg in errors.items()])
        full_message = f"Encountered {len(errors)} errors: \n{error_string}"
        send_error_email(
            message=full_message,
            run_date=args.run_date,
            dry_run=args.dry_run,  # send error emails regardless of day
        )
    else:
        full_message = None

    send_emails(expiring_pings_by_email, dry_run)

    if full_message is not None:
        print(full_message, file=sys.stderr)


if __name__ == "__main__":
    main()
