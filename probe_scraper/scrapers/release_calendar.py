# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from collections import defaultdict
from datetime import datetime

from bs4 import BeautifulSoup
import requests

from probe_scraper.parsers.utils import get_major_version

RELEASE_CALENDAR_URL = ("https://wiki.mozilla.org/api.php"
                        "?action=parse&format=json&page=Release_Management/Calendar")


def get_version_table_dates(table):
    """
    Given a version table, obtains a dictionary mapping release channel to a dictionary mapping
    Firefox version numbers to their intended release date.
    e.g.:
    {
      "beta": {
        "72": 2019-12-03
      }
    }

    The table is expected to be in the following form:
    <table>
      (...other rows...)
      <tr>
        (..other td columns...)
        <th>(expected merge date as YYYY-MM-DD)</th>
        <td>Firefox (Firefox nightly)</td>
        <td>Firefox (Firefox beta)</td>
        <th>(expected release date as YYYY-MM-DD)</th>
        <td>Firefox (Firefox release)</td>
        <td>Firefox (Firefox ESR)</td>
      </tr>
      (...other rows...)
    </table>
    """
    def extract_version_string(version_object):
        return get_major_version(version_object.string.strip().replace("Firefox ", ""))

    result = defaultdict(dict)
    for row in table.find_all("tr"):
        fields = list(row.find_all("td"))
        if len(fields) < 4:
            continue  # not enough fields in the row, probably a header row
        for i in range(1, 5):  # ensure that each column represents a Firefox version
            field = fields[-i]
            if field.find("a") is not None:  # unnest nested hyperlink
                field = field.find("a")
                fields[-i] = field
            if "Firefox" not in field.string:
                # likely cause for error is that html structure changed; fail so it can be fixed
                raise Exception(f"Could not parse {field.string} as a Firefox version")

        nightly_version = extract_version_string(fields[-4])
        beta_version = extract_version_string(fields[-3])
        release_version = extract_version_string(fields[-2])

        release_date_string = list(row.find_all("th"))[-1].string.strip(" \t\r\n*")
        release_date = datetime.strptime(release_date_string, "%Y-%m-%d").date()

        result["nightly"][nightly_version] = release_date
        result["beta"][beta_version] = release_date
        result["release"][release_version] = release_date

    return result


def get_release_dates():
    """
    Obtain a dictionary mapping future Firefox version numbers to their intended release date.
    Takes data from the RapidRelease page of the Mozilla Wiki.
    https://wiki.mozilla.org/Release_Management/Calendar
    There are multiple past release tables, only the first is used.

    The page is expected to be in the following form:
    (...beginning of document...)
    <h2><span id="Future_branch_dates">(TITLE)</span></h2>
    (...anything other than a table...)
    (...version table...)
    (...more content...)
    <h2><span id="Past_branch_dates">(TITLE)</span></h2>
    (...anything other than a table...)
    (...latest past version table...)
    (...older past version tables...)
    (...rest of document...)
    """
    response = requests.get(RELEASE_CALENDAR_URL).json()
    page_html = BeautifulSoup(response["parse"]["text"]["*"], "html.parser")

    # scrape for future release date tables
    table = page_html.find(id="Future_branch_dates").find_parent("h2").find_next_sibling("table")
    result = get_version_table_dates(table)

    # scrape for past release date tables
    table = page_html.find(id="Past_branch_dates").find_parent("h2").find_next_sibling("table")
    past_release_dates = get_version_table_dates(table)
    for channel, value in past_release_dates.items():
        result[channel].update(value)
    return result
