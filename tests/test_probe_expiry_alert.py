import datetime
import json
from dataclasses import dataclass
from unittest import mock

from requests.exceptions import HTTPError

from probe_scraper import probe_expiry_alert
from probe_scraper.probe_expiry_alert import ProbeDetails


@dataclass
class ResponseWrapper:
    text: str

    def text(self):
        return self.text


def test_bugzilla_prod_urls():
    assert probe_expiry_alert.BUGZILLA_BUG_URL.startswith(
        "https://bugzilla.mozilla.org/"
    )
    assert probe_expiry_alert.BUGZILLA_USER_URL.startswith(
        "https://bugzilla.mozilla.org/"
    )
    assert probe_expiry_alert.BUGZILLA_BUG_LINK_TEMPLATE.startswith(
        "https://bugzilla.mozilla.org/"
    )


def test_find_expiring_probes_no_expiring():
    probes = {"p1": {"expiry_version": "never"}}
    expiring_probes = probe_expiry_alert.find_expiring_probes(probes, "75", "")
    expected = []
    assert expiring_probes == expected


def test_find_expiring_probes_channel_no_probes():
    probes = {}
    expiring_probes = probe_expiry_alert.find_expiring_probes(probes, "75", "")
    expected = []
    assert expiring_probes == expected


@mock.patch("probe_scraper.probe_expiry_alert.get_bug_component")
def test_find_expiring_probes_expiring(mock_get_bug_component):
    mock_get_bug_component.return_value = "prod", "comp"

    probes = {
        "p1": {
            "expiry_version": "74",
            "notification_emails": ["test@email.com"],
            "bug_numbers": [],
        },
        "p2": {
            "expiry_version": "75",
            "notification_emails": ["test@email.com"],
            "bug_numbers": [1],
        },
        "p3": {"expiry_version": "75", "bug_numbers": []},
    }
    expiring_probes = probe_expiry_alert.find_expiring_probes(probes, "75", "")
    expected = [
        {
            "name": "p2",
            "product": "prod",
            "component": "comp",
            "emails": ["test@email.com"],
            "previous_bug": 1,
        },
        {
            "name": "p3",
            "product": "Firefox",
            "component": "General",
            "emails": [],
            "previous_bug": None,
        },
    ]
    assert [probe.__dict__ for probe in expiring_probes] == expected


@mock.patch("probe_scraper.parsers.events.EventsParser.parse")
@mock.patch("probe_scraper.parsers.histograms.HistogramsParser.parse")
@mock.patch("probe_scraper.parsers.scalars.ScalarsParser.parse")
@mock.patch("probe_scraper.probe_expiry_alert.download_file")
@mock.patch("probe_scraper.probe_expiry_alert.get_latest_nightly_version")
@mock.patch("probe_scraper.probe_expiry_alert.file_bugs")
@mock.patch("probe_scraper.probe_expiry_alert.send_emails")
def test_not_dryrun_only_once_per_week(
    mock_send_emails,
    mock_file_bugs,
    mock_get_version,
    mock_download_file,
    mock_scalars_parser,
    mock_histograms_parser,
    mock_events_parser,
):
    mock_file_bugs.return_value = {}
    mock_events_parser.return_value = {}
    mock_histograms_parser.return_value = {}
    mock_scalars_parser.return_value = {}
    mock_get_version.return_value = "75"
    for weekday in range(7):
        base_date = datetime.date(2020, 1, 1)
        probe_expiry_alert.main(base_date + datetime.timedelta(days=weekday), False, "")

    mock_file_bugs.assert_has_calls(
        [mock.call([], "75", "", dryrun=False)]
        + [mock.call([], "75", "", dryrun=True)] * 6,
        any_order=True,
    )
    assert mock_file_bugs.call_count == 7
    mock_send_emails.assert_has_calls(
        [mock.call({}, {}, "75", dryrun=False)]
        + [mock.call({}, {}, "75", dryrun=True)] * 6,
        any_order=True,
    )
    assert mock_send_emails.call_count == 7


@mock.patch("probe_scraper.probe_expiry_alert.find_existing_bugs")
@mock.patch("probe_scraper.probe_expiry_alert.create_bug")
def test_no_bugs_created_on_dryrun(mock_create_bug, mock_find_bugs):
    mock_find_bugs.return_value = {}
    expiring_probes = [
        ProbeDetails("p1", "", "", [], 1),
        ProbeDetails("p2", "", "", ["a@test.com"], 1),
    ]

    bug_ids = probe_expiry_alert.file_bugs(expiring_probes, "76", "", dryrun=True)

    assert mock_create_bug.call_count == 0
    assert len(bug_ids) == 0


@mock.patch("probe_scraper.probe_expiry_alert.find_existing_bugs")
@mock.patch("probe_scraper.probe_expiry_alert.create_bug")
def test_bugs_created_not_dryrun(mock_create_bug, mock_find_bugs):
    mock_find_bugs.return_value = {}
    expiring_probes = [
        ProbeDetails("p1", "1", "2", [], 1),
        ProbeDetails("p2", "3", "4", ["a@test.com"], 1),
    ]

    bug_ids = probe_expiry_alert.file_bugs(expiring_probes, "76", "", dryrun=False)

    assert mock_create_bug.call_count == 2
    assert len(bug_ids) == 2


@mock.patch("boto3.client")
def test_no_email_sent_on_dryrun(mock_boto_client):
    probes_by_email = {
        "a@test.com": ["p1", "p2"],
        "b@test.com": ["p1", "p2"],
        "c@test.com": ["p3"],
    }
    probe_to_bug_id = {"p1": 1, "p2": 2, "p3": 3}

    probe_expiry_alert.send_emails(probes_by_email, probe_to_bug_id, "75", dryrun=True)

    assert mock_boto_client.call_count == 0


@mock.patch("boto3.client")
def test_send_email_not_dryrun(mock_boto_client):
    probes_by_email = {
        "a@test.com": ["p1", "p2"],
        "b@test.com": ["p1", "p2"],
        "c@test.com": ["p3"],
    }
    probe_to_bug_id = {"p1": 1, "p2": 2, "p3": 3}

    probe_expiry_alert.send_emails(probes_by_email, probe_to_bug_id, "75", dryrun=False)

    assert mock_boto_client.call_count == 4


@mock.patch("probe_scraper.parsers.events.EventsParser.parse")
@mock.patch("probe_scraper.parsers.histograms.HistogramsParser.parse")
@mock.patch("probe_scraper.parsers.scalars.ScalarsParser.parse")
@mock.patch("probe_scraper.probe_expiry_alert.download_file")
@mock.patch("probe_scraper.probe_expiry_alert.get_latest_nightly_version")
@mock.patch("probe_scraper.probe_expiry_alert.check_bugzilla_user_exists")
@mock.patch("probe_scraper.probe_expiry_alert.file_bugs")
@mock.patch("probe_scraper.probe_expiry_alert.send_emails")
def test_main_run(
    mock_send_emails,
    mock_file_bugs,
    mock_user_exists,
    mock_get_version,
    mock_download_file,
    mock_scalars_parser,
    mock_histograms_parser,
    mock_events_parser,
):
    mock_user_exists.return_value = False
    mock_events_parser.return_value = {
        "p1": {
            "expiry_version": "76",
            "notification_emails": ["test@email.com"],
            "bug_numbers": [],
        }
    }
    mock_histograms_parser.return_value = {
        "p2": {
            "expiry_version": "75",
            "notification_emails": ["test@email.com"],
            "bug_numbers": [],
        }
    }
    mock_scalars_parser.return_value = {
        "p3": {
            "expiry_version": "77",
            "notification_emails": ["test@email.com"],
            "bug_numbers": [],
        }
    }
    mock_get_version.return_value = "75"

    probe_expiry_alert.main(datetime.date(2020, 1, 8), False, "")

    expected_expiring_probes = [ProbeDetails("p1", "Firefox", "General", [], None)]
    mock_file_bugs.assert_called_once_with(
        expected_expiring_probes, "75", "", dryrun=False
    )


@mock.patch("probe_scraper.probe_expiry_alert.find_existing_bugs")
@mock.patch("probe_scraper.probe_expiry_alert.create_bug")
def test_bugs_created_only_for_new_probes(mock_create_bugs, mock_find_bugs):
    mock_find_bugs.return_value = {"p2": 2, "p3": 3}
    mock_create_bugs.side_effect = [1, 4]
    probes = [
        ProbeDetails("p1", "", "", ["email1"], 1),
        ProbeDetails("p2", "", "", [], 1),
        ProbeDetails("p3", "", "", [], 1),
        ProbeDetails("p4", "", "", ["email2"], 1),
    ]
    bug_ids = probe_expiry_alert.file_bugs(probes, "1", "", dryrun=False)

    assert bug_ids == {"p2": 2, "p3": 3, "p1": 1, "p4": 4}

    assert mock_create_bugs.call_count == 2
    mock_create_bugs.has_calls(
        [
            mock.call(ProbeDetails("p1", "", "", ["email1"], 1), "1", mock.ANY),
            mock.call(ProbeDetails("p4", "", "", ["email2"], 1), "1", mock.ANY),
        ],
        any_order=True,
    )


@mock.patch("requests.post")
def test_create_bug(mock_post):
    mock_response = mock.MagicMock()
    mock_response.json = mock.MagicMock(return_value={"id": 2})
    mock_post.return_value = mock_response

    probes = [
        ProbeDetails("p1", "prod", "comp", ["a@test.com", "b@test.com"], 1),
        ProbeDetails("p1", "prod", "comp", ["a@test.com", "b@test.com"], 1),
    ]
    bug_id = probe_expiry_alert.create_bug(
        probes,
        "76",
        probe_expiry_alert.BUG_WHITEBOARD_TAG,
        probe_expiry_alert.BUG_SUMMARY_TEMPLATE,
        probe_expiry_alert.BUG_DESCRIPTION_TEMPLATE,
        "",
    )

    assert bug_id == 2


@mock.patch("requests.post")
def test_create_bug_try_on_needinfo_blocked(mock_post):
    error_response = mock.MagicMock()
    error_response.text = json.dumps(
        {"error": 'a <a@test.com> is not currently accepting "needinfo" requests.'}
    )
    good_response = mock.MagicMock()
    good_response.json = mock.MagicMock(return_value={"id": 2})

    def raise_for_status():
        mock_post.return_value = good_response
        raise HTTPError()

    mock_post.return_value = error_response
    error_response.raise_for_status = raise_for_status

    probes = [ProbeDetails("p1", "prod", "comp", ["a@test.com"], 1)]

    assert (
        probe_expiry_alert.create_bug(
            probes,
            "76",
            probe_expiry_alert.BUG_WHITEBOARD_TAG,
            probe_expiry_alert.BUG_SUMMARY_TEMPLATE,
            probe_expiry_alert.BUG_DESCRIPTION_TEMPLATE,
            "",
            needinfo=True,
        )
        == 2
    )

    assert mock_post.call_count == 2

    call_args_1 = mock_post.call_args_list[0][1]["json"]
    call_args_2 = mock_post.call_args_list[1][1]["json"]

    assert len(call_args_1["flags"]) == 1

    assert len(call_args_2["flags"]) == 0


@mock.patch("requests.post")
def test_create_bug_try_on_requestee_inactive(mock_post):
    error_response = mock.MagicMock()
    error_response.text = json.dumps(
        {
            "error": "You can't ask Not active! Use a@test.com instead! <inactive@test.com> because that account is disabled."  # noqa
        }
    )
    good_response = mock.MagicMock()
    good_response.json = mock.MagicMock(return_value={"id": 2})

    def raise_for_status():
        mock_post.return_value = good_response
        raise HTTPError()

    mock_post.return_value = error_response
    error_response.raise_for_status = raise_for_status

    probes = [ProbeDetails("p1", "prod", "comp", ["a@test.com"], 1)]

    assert (
        probe_expiry_alert.create_bug(
            probes,
            "76",
            probe_expiry_alert.BUG_WHITEBOARD_TAG,
            probe_expiry_alert.BUG_SUMMARY_TEMPLATE,
            probe_expiry_alert.BUG_DESCRIPTION_TEMPLATE,
            "",
            needinfo=True,
        )
        == 2
    )

    assert mock_post.call_count == 2

    call_args_1 = mock_post.call_args_list[0][1]["json"]
    call_args_2 = mock_post.call_args_list[1][1]["json"]

    assert len(call_args_1["flags"]) == 1

    assert len(call_args_2["flags"]) == 0


@mock.patch("requests.get")
def test_bug_description_parser(mock_get):
    """
    Checking if current expiring probes have already had bugs filed uses regex on the bug
    description.  So if the bug description template changes such that the regex fails, this test
    should fail.
    """
    search_results = {
        "bugs": [
            {
                "summary": "",
                "id": 1,
                "description": probe_expiry_alert.BUG_DESCRIPTION_TEMPLATE.format(
                    version="76", probes="\np1\np2 \n", notes=""
                ),
            },
            {
                "summary": "",
                "id": 2,
                "description": probe_expiry_alert.BUG_DESCRIPTION_TEMPLATE.format(
                    version="77", probes="\n p3 p4", notes=""
                ),
            },
            {
                "summary": "",
                "id": 3,
                "description": probe_expiry_alert.BUG_DESCRIPTION_TEMPLATE.format(
                    version="76", probes="\np5 p6\n", notes=""
                ),
            },
        ]
    }
    mock_response = mock.MagicMock()
    mock_response.json = mock.MagicMock(return_value=search_results)
    mock_get.return_value = mock_response

    probes_with_bugs = probe_expiry_alert.find_existing_bugs(
        "76", "", probe_expiry_alert.BUG_WHITEBOARD_TAG
    )

    assert probes_with_bugs == {"p1": 1, "p2": 1, "p5": 3, "p6": 3}


@mock.patch("requests.get")
def test_bug_description_invalid(mock_get):
    """
    If a bug has the probe expiry whiteboard tag but the Firefox version or
    probes can't be parsed, it should be ignored.
    """
    search_results = {
        "bugs": [
            {
                "summary": "",
                "id": 1,
                "description": "release: ver 123",
            },
            {
                "summary": "",
                "id": 2,
                "description": "```\nprobe\n```",
            },
            {
                "summary": "",
                "id": 3,
                "description": probe_expiry_alert.BUG_DESCRIPTION_TEMPLATE.format(
                    version="76", probes="\np5 p6\n", notes=""
                ),
            },
        ]
    }
    mock_response = mock.MagicMock()
    mock_response.json = mock.MagicMock(return_value=search_results)
    mock_get.return_value = mock_response

    probes_with_bugs = probe_expiry_alert.find_existing_bugs(
        "76", "", probe_expiry_alert.BUG_WHITEBOARD_TAG
    )

    assert probes_with_bugs == {"p5": 3, "p6": 3}


def test_get_longest_prefix():
    values = [
        "FX_PICTURE_IN_PICTURE_WINDOW_OPEN_DURATION",
        "pictureinpicture.opened_method",
        "pictureinpicture.closed_method",
        "",
    ]
    assert probe_expiry_alert.get_longest_prefix(values, 0) == values[0]
    assert probe_expiry_alert.get_longest_prefix(values, 1) == values[0]
    assert probe_expiry_alert.get_longest_prefix(values, 2) == "pictureinpicture.*"
    assert probe_expiry_alert.get_longest_prefix([]) == ""
    assert probe_expiry_alert.get_longest_prefix(["abc"]) == "abc"


@mock.patch("requests.get")
def test_check_bugzilla_user_account_not_found(mock_get):
    mock_response = mock.MagicMock()
    mock_response.status_code = 400
    mock_response.json = mock.MagicMock(return_value={"code": 51})

    def user_not_found():
        raise HTTPError(response=mock_response)

    mock_response.raise_for_status.side_effect = user_not_found
    mock_get.return_value = mock_response

    assert not probe_expiry_alert.check_bugzilla_user_exists("test@test.com", "")


@mock.patch("requests.get")
def test_check_bugzilla_user_account_not_found_200(mock_get):
    mock_response = mock.MagicMock()
    mock_response.status_code = 200
    mock_response.json = mock.MagicMock(
        return_value={
            "message": None,
            "code": 100500,
            "documentation": "https://bmo.readthedocs.io/en/latest/api/",
            "error": True,
        }
    )

    mock_get.return_value = mock_response

    assert not probe_expiry_alert.check_bugzilla_user_exists("test@test.com", "")


@mock.patch("requests.get")
def test_check_bugzilla_user_account_inactive(mock_get):
    users = {
        "users": [
            {
                "can_login": False,
                "is_new": False,
                "real_name": "test",
                "email": "test@test.com",
                "id": 123,
                "name": "test",
            }
        ]
    }

    mock_response = mock.MagicMock()
    mock_response.json = mock.MagicMock(return_value=users)
    mock_get.return_value = mock_response

    assert not probe_expiry_alert.check_bugzilla_user_exists("test@test.com", "")


@mock.patch("requests.get")
def test_check_bugzilla_user_account_active(mock_get):
    users = {
        "users": [
            {
                "can_login": True,
                "is_new": False,
                "real_name": "test",
                "email": "test@test.com",
                "id": 123,
                "name": "test",
            }
        ]
    }

    mock_response = mock.MagicMock()
    mock_response.json = mock.MagicMock(return_value=users)
    mock_get.return_value = mock_response

    assert probe_expiry_alert.check_bugzilla_user_exists("test@test.com", "")
