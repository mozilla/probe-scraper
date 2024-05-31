import argparse
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Dict, List, Tuple

import requests
from dateutil.tz import tzutc
from dateutil.utils import today
from google.cloud import bigquery
from google.cloud.exceptions import Forbidden

from probe_scraper.emailer import send_ses

EMAIL_SUBJECT_TEMPLATE = "Glean Pings Expiring for {app_name}"

EMAIL_TEMPLATE = """
The following Glean pings are set to start deleting collected data soon based on the retention policy.
{pings}
What to do about this:
1. If this is expected and you do not need data for any of these pings past the retention dates, then no action is needed.
2. If you wish to continue collecting data for a longer period of time for any of these pings, TODO: create a jira ticket?

Retention policies are defined in probe-scraper [1], with options to either stop collecting data after a certain or delete data older than the specified number of days.

If you have any problems, please ask for help on the #glean Slack channel. We'll give you a hand.

Your Friendly, Neighborhood Glean Team

[1] - https://github.com/mozilla/probe-scraper/blob/main/repositories.yaml

This is an automated message sent from probe-scraper.  See https://github.com/mozilla/probe-scraper for details.
"""  # noqa

APP_GROUP_TEMPLATE = """
{app_name}:
{messages}
"""

RETENTION_DAYS_MESSAGE_TEMPLATE = '\t- The "{ping_name}" ping for will start deleting data older than {retention_days} days starting on {expiry_date}'  # noqa

COLLECT_THROUGH_MESSAGE_TEMPLATE = (
    '\t- The "{ping_name}" ping for will stop collecting data after {end_date}'
)

DEFAULT_EMAILS = ["glean-team@mozilla.com"]

NOTIFICATION_DAYS = 17


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


def get_oldest_partition_date(
    client: bigquery.Client, full_table_id: str, partition_fallback: bool
) -> date:
    """Get the date of the oldest partition in the given table.

    This attempts to get the partitions using the partition summary which requires read
    access on the table data.  If the client does not have the necessary permissions and
    `partition_fallback` is set to true, the oldest partition is inferred from the number of
    partitions in the table.  This can be used for local testing with restricted-access tables.
    """

    first_partition_date = None
    try:
        partitions = [
            partition
            for partition in client.list_partitions(full_table_id)
            if partition != "__NULL__"
        ]
        if len(partitions) > 0:
            first_partition_date = datetime.strptime(min(partitions), "%Y%m%d").date()
    except Forbidden:
        if partition_fallback:
            print(f"Inferring partition from number of partitions for {full_table_id}")
            table = client.get_table(full_table_id)
            first_partition_date = (
                today(tzinfo=tzutc())
                - timedelta(days=int(table._properties["numPartitions"]) - 1)
            ).date()

    return first_partition_date


def is_reaching_retention_limit(
    run_date: date, retention_days: int, oldest_partition_date: date
) -> bool:
    return (
        retention_days is not None
        and oldest_partition_date is not None
        and 3
        < retention_days - (run_date - oldest_partition_date).days
        <= NOTIFICATION_DAYS
    )


def is_reaching_collect_through_date(run_date: date, collect_through_date: str) -> bool:
    return (
        collect_through_date is not None
        and 3
        < (date.fromisoformat(collect_through_date) - run_date).days
        <= NOTIFICATION_DAYS
    )


def send_emails(messages_by_email: Dict[str, Dict[str, List[str]]], dryrun: bool):
    for email, messages_by_app in messages_by_email.items():
        combined_messages = [
            APP_GROUP_TEMPLATE.format(app_name=app, messages="\n".join(messages))
            for app, messages in messages_by_app.items()
        ]
        email_body = EMAIL_TEMPLATE.format(pings="".join(combined_messages))
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
            dryrun=dryrun,
        )


def get_expiring_pings(
    run_date: datetime.date,
    project_id: str,
    partition_fallback: bool = False,
) -> Tuple[Dict[str, Dict[str, List[str]]], Dict[str, List]]:
    """
    Get expiring pings across all apps and a tuple of expiring pings and errors encountered.

    Expiring pings are stored in a dict where the key is an email to send to and the value
    is a dict with list of messages per app.

    Errors are stored in a dict where the key is a bigquery table name and the value is a list of
    errors related to the associated ping.

    If partition_fallback is true, calculate the oldest partition date using the number of
    partitions in a table if missing read permissions on the table.  This is used for local testing.
    """
    client = bigquery.Client(project=project_id)

    app_listings = request_get(
        "https://probeinfo.telemetry.mozilla.org/v2/glean/app-listings"
    )
    errors = defaultdict(list)
    # dict structure: {email: {app_name: [messages]}}
    expiring_pings_by_email = defaultdict(lambda: defaultdict(list))

    for app in app_listings:
        if app.get("deprecated", False) is True:
            continue

        app_name = app["v1_name"]
        dataset_id = app["bq_dataset_family"] + "_stable"

        # get active tables to include pings defined in dependencies are included
        tables = client.list_tables(dataset_id)

        pings = request_get(
            f"https://probeinfo.telemetry.mozilla.org/glean/{app_name}/pings"
        )
        default_ping_metadata = {
            "moz_pipeline_metadata": {},
            "history": [{"notification_emails": app["notification_emails"]}],
        }

        for table in tables:
            table_id = str(table.reference)
            ping_name = table.table_id.replace("_v1", "").replace("_", "-")
            ping_metadata = pings.get(ping_name, default_ping_metadata)

            # merge pipeline metadata from /pings and /app-listings to verify metadata is propagated
            pipeline_metadata = {
                **ping_metadata["moz_pipeline_metadata"],
                **app.get("moz_pipeline_metadata", {}).get(ping_name, {}),
            }

            expiration_policy = pipeline_metadata.get("expiration_policy", {})
            retention_days_metadata = expiration_policy.get("delete_after_days")
            collect_through_date = expiration_policy.get("collect_through_date")

            retention_days_applied = (
                int(table.time_partitioning.expiration_ms / 1000 / 60 / 60 / 24)
                if table.time_partitioning.expiration_ms is not None
                else None
            )

            # If retention is defined in probe scraper, the expiration on the table must match
            # retention_days_metadata may be null if retention is set in the app defaults
            if (
                retention_days_metadata is not None
                and retention_days_applied != retention_days_metadata
            ):
                errors[table_id].append(
                    f"Retention period in metadata ({retention_days_metadata} days) "
                    f"does not match period applied to table ({retention_days_applied} days)"
                )

            # no retention policy
            if retention_days_applied is None and collect_through_date is None:
                continue

            oldest_partition_date = get_oldest_partition_date(
                client, table_id, partition_fallback
            )
            if oldest_partition_date is None:
                errors[table_id].append(
                    f"Could not get earliest partition for {table.table_id}"
                )

            # Use emails for ping if they exist, otherwise use emails for app
            # e.g. for pings from dependencies
            email_list = set(
                ping_metadata["history"][-1].get(
                    "notification_emails", app["notification_emails"]
                )
                + DEFAULT_EMAILS
            )

            if is_reaching_retention_limit(
                run_date, retention_days_applied, oldest_partition_date
            ):
                message = RETENTION_DAYS_MESSAGE_TEMPLATE.format(
                    ping_name=ping_name,
                    retention_days=retention_days_applied,
                    expiry_date=oldest_partition_date
                    + timedelta(days=retention_days_applied),
                )
                for email in email_list:
                    expiring_pings_by_email[email][app_name].append(message)

            # Approaching collect through date
            if is_reaching_collect_through_date(run_date, collect_through_date):
                message = COLLECT_THROUGH_MESSAGE_TEMPLATE.format(
                    ping_name=ping_name,
                    end_date=collect_through_date,
                )
                for email in email_list:
                    expiring_pings_by_email[email][app_name].append(message)

    return expiring_pings_by_email, errors


if __name__ == "__main__":
    args = parse_args()

    expiring_pings_by_email, errors = get_expiring_pings(
        run_date=args.run_date,
        project_id=args.bigquery_project,
        partition_fallback=args.dry_run,
    )

    # Only send emails on Wednesday, dry run on other days for error checking
    dry_run = args.dry_run or args.run_date.weekday() != 2

    send_emails(expiring_pings_by_email, dry_run)

    if len(errors) > 0:
        error_string = "\n".join([f"{ping}: {msg}" for ping, msg in errors.items()])
        raise RuntimeError(f"Encountered {len(errors)} errors: \n{error_string}")
