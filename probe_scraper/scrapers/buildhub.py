import pprint
import re
from datetime import datetime

import requests


class NoDataFoundException(Exception):
    pass


class Buildhub(object):

    search_url = "https://buildhub.moz.tools/api/search"
    default_window = 1000

    date_formats = ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%f")

    def _paginate_revision_dates(
        self,
        iteration,
        channel,
        min_version,
        product,
        locale,
        platform,
        max_version,
        verbose,
        window,
    ):
        query_str = [
            {"term": {"source.product": product}},
            {"term": {"target.channel": channel}},
            {"term": {"target.locale": locale}},
            {"term": {"target.platform": platform}},
        ]

        # See: "99" > "65" == True, "100" > "65" == False
        # FIXME: This breaks if we get to v200
        # If we only need versions above 99 we restrict it to versions below 200,
        # then we're good for a bunch of versions.
        if min_version >= 100:
            query_str.append({"range": {"target.version": {"gte": str(min_version)}}})
            if max_version is None:
                # This works because the minimum we ever ask for is v30.
                query_str.append({"range": {"target.version": {"lt": "200"}}})
        else:
            # If the user didn't set a max version we need to explicitly include v100..v200 here.
            if max_version is None:
                query_str.append(
                    {
                        "bool": {
                            "should": [
                                {
                                    "range": {
                                        "target.version": {"gte": str(min_version)}
                                    }
                                },
                                {
                                    "bool": {
                                        "must": [
                                            {
                                                "range": {
                                                    "target.version": {"gte": "100"}
                                                }
                                            },
                                            {
                                                "range": {
                                                    "target.version": {"lt": "200"}
                                                }
                                            },
                                        ]
                                    }
                                },
                            ]
                        }
                    }
                )
            else:
                # Otherwise we only check the min version,
                # the max version check will be appended
                query_str.append(
                    {"range": {"target.version": {"gte": str(min_version)}}}
                )

        if max_version is not None:
            query_str.append(
                {
                    "bool": {
                        "should": [
                            {"range": {"target.version": {"lte": str(max_version)}}},
                            {"prefix": {"target.version": str(max_version)}},
                        ]
                    }
                }
            )

        body = {"query": {"bool": {"filter": query_str}}, "size": window}

        if iteration != 0:
            body["from"] = iteration * window

        if verbose:
            print("------QUERY STRING------\n")
            pprint.pprint(body)

        response = requests.post(url=Buildhub.search_url, json=body)
        data = response.json()

        if verbose:
            print("------QUERY RESULTS------\n")
            pprint.pprint(data)

        return data

    def _distinct_and_clean(self, records):
        """
        For more information on the schema of the records,
        see the Buildhub API documentation:
        https://buildhub.readthedocs.io/en/latest/api.html#more-about-the-data-schema
        """
        cleaned_records = {}

        for record in records:
            # %:z not supported, see https://bugs.python.org/msg169952
            # Drop the tz portion entirely
            d = record["_source"]["download"]["date"]
            if re.search(r"\+\d{2}:\d{2}$", d):
                d = d[:-6]

            date = None
            try:
                date = datetime.strptime(d, self.date_formats[0])
            except ValueError:
                pass

            if date is None:
                date = datetime.strptime(d, self.date_formats[1])

            entry = {
                "date": date,
                "revision": record["_source"]["source"]["revision"],
                "version": record["_source"]["target"]["version"],
                "tree": record["_source"]["source"]["tree"],
            }

            revision = entry["revision"]
            min_entry = entry

            if revision in cleaned_records:
                if cleaned_records[revision] != entry:
                    min_entry = min(
                        (entry, cleaned_records[revision]), key=lambda x: x["date"]
                    )

            cleaned_records[revision] = min_entry

        return sorted(cleaned_records.values(), key=lambda x: x["date"])

    def get_revision_dates(
        self,
        channel,
        min_version,
        product="firefox",
        locale="en-US",
        platform="win64",
        max_version=None,
        verbose=False,
        window=500,
    ):
        """
        Retrieve the revisions and publish-dates for a given filter set.
        The combination of channel, product, local, and platform almost
        gives a set of unique (revision, publication-dates). For example,
        `win64` includes x86 and arm-64 builds. As such we de-duplicate
        the result set and include the build with the earliest publication
        date.

        Tree is the source tree, usually one of:
            - mozilla-central
            - mozilla-beta
            - mozilla-release

        :param channel: The release channel
        :param min_version: The minimum version to include
        :param product: Defaults to firefox
        :param locale: Defaults to en-US
        :param platform: Defaults to win64
        :param max_version: Optional maximum version to include
        :param verbose: Verbose output of query string and results
        :param window: Number of records to retrieve at a time

        returns a list of records of type
        {
            "date": <date>
            "revision": <revision>,
            "version": <version>,
            "tree": <tree>
        }
        """

        # Because "100" > "99" == False we special-case v100 to v199.
        # v200 is far out, so we just ignore that for now.
        assert min_version < 200, "Only versions below 200 are supported"

        total_hits = 0
        results = []

        for i in range(2**20):
            data = self._paginate_revision_dates(
                i,
                channel,
                min_version,
                product,
                locale,
                platform,
                max_version,
                verbose,
                window,
            )

            # hits/total gives total number of records, including
            # those outside the window. We need to know the number
            # inside the window.
            hits = len(data["hits"]["hits"])

            if hits:
                total_hits += hits
                results.append(data)

            # optimization, removes the last no-result window
            if hits < window:
                break

        if total_hits == 0:
            raise NoDataFoundException(
                "No data found for channel {} and minimum \
                                       version {}".format(
                    channel, min_version
                )
            )

        all_records = [
            record for result in results for record in result["hits"]["hits"]
        ]
        return self._distinct_and_clean(all_records)
