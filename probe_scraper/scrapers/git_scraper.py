# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import re
import tempfile
import traceback
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from functools import cached_property
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple, Union

import git

from probe_scraper.exc import ProbeScraperInvalidRequest
from probe_scraper.parsers.repositories import Repository

GIT_HASH_PATTERN = re.compile("([A-Fa-f0-9]){40}")

# WARNING!
# Changing these dates can cause files that had metrics to
# stop being scraped. When the probe-info-service
# stops reporting those files, the schema-generator
# will not add them to the schemas, resulting in a
# schema-incompatible change that breaks the pipeline.
FENIX_DATE = "2019-06-04 00:00:00"
MIN_DATES = {
    # Previous versions of the file were not schema-compatible
    "glean": "2019-04-11 00:00:00",
    "fenix": FENIX_DATE,
    "fenix-nightly": FENIX_DATE,
    "firefox-android-nightly": FENIX_DATE,
    "firefox-android-beta": FENIX_DATE,
    "firefox-android-release": FENIX_DATE,
    "reference-browser": "2019-04-01 00:00:00",
    "firefox-desktop": "2020-07-29 00:00:00",
    "glean-js": "2020-09-21 13:35:00",
    "mozilla-vpn": "2021-05-25 00:00:00",
    "mozilla-vpn-android": "2021-05-25 00:00:00",
    "rally-markup-fb-pixel-hunt": "2021-12-04 00:00:00",
    "rally-citp-search-engine-usage": "2022-04-15 00:00:00",
    "relay-backend": "2024-05-09 00:00:00",
}

# Some commits in projects might contain invalid metric files.
# When we know these problems are fixed in later commits we can skip them.
SKIP_COMMITS = {
    "engine-gecko": [
        "9bd9d7fa6c679f35d8cbeb157ff839c63b21a2e6"  # Missing schema update from v1 to v2
    ],
    "engine-gecko-beta": [
        "9bd9d7fa6c679f35d8cbeb157ff839c63b21a2e6"  # Missing schema update from v1 to v2
    ],
    "gecko": [
        "43d8cf138695faae2fca0adf44c94f47fdadfca8",  # Missing gfx/metrics.yaml
        "340c8521a54ad4d4a32dd16333676a6ff85aaec2",  # Missing toolkit/components/glean/pings.yaml
        "4520632fe0664572c5f70688595b7721d167e2d0",  # Missing toolkit/components/glean/pings.yaml
        "c5d5f045aaba41933622b5a187c39da0d6ab5d80",  # Missing toolkit/components/glean/tags.yaml
        "2c475db0ffe5df6010ded8ccb9784b0645d92ebb",  # Missing netwerk/protocol/http/metrics.yaml
        "b6dbdfec41221b0a80cc211f85abbb01e33f8692",  # Missing netwerk/protocol/http/metrics.yaml
        "da2b7986d0b26f26cd1ac2d8c5b78b70009a24b6",  # Missing netwerk/protocol/http/metrics.yaml
        "74a43f86ea999ec985d77eb6c3c7f766b570dd9d",  # Missing dom/media/webrtc/metrics.yaml
        # Missing toolkit/components/pdfjs/metrics.yaml
        "d1d0b69871e3d38ead989d73f30563a501a448b6",
        "66d41da90f85d19fef2d5249c8f3058433ec4bd5",  # Missing dom/pings.yaml
        "1e64234ac7f7303d5942deb6d90dd85cd4eb6e12",  # Missing xpcom/metrics.yaml
        "cf06f2778f48c7f92d908dae73d48268db454e72",  # Missing ipc/ipdl/metrics.yaml
        "bb188d821a6b3d27951ed05526ec7010d3ec0c52",  # Missing ipc/ipdl/metrics.yaml
        "0e55b6d34c8fac3144f10f9aa450e33e4b55d520",  # Missing ipc/ipdl/metrics.yaml
        "0b1543e85d13c30a13c57e959ce9815a3f0fa1d3",  # Missing ipc/ipdl/metrics.yaml
        "9bc20993bc6960762ed281201e9cff437a88ca6c",  # Missing ipc/ipdl/metrics.yaml
        "3f6ba0d4adbdf9d3e81b7047ff4c21384abbd234",  # Missing dom/base/use_counter_metrics.yaml
        "1a7724cfd6b3cce2c599e323afb14f31430e5acd",  # Missing dom/base/use_counter_metrics.yaml
        "02731904bba2c2f4e1c043e45a492bb21b33a930",  # Missing security/manager/ssl/metrics.yaml
        "b16c6e1f04e563c916fb43b62661fdc0d354a925",  # Missing security/manager/ssl/metrics.yaml
        # Missing toolkit/components/reportbrokensite/metrics.yaml
        "42acdc9cd5ae89222bdceeeaed7bacac755be48f",
        # Missing toolkit/components/reportbrokensite/metrics.yaml
        "c76093316c58ae74a21e854b8035c91d0c75df6e",
        # Missing toolkit/components/translations/metrics.yaml
        "b80d1b362960cef8ee389ed54cdc41702ca832d9",
        # Broken yaml in toolkit/components/translations/metrics.yaml, fixed in subsequent commit
        "3ac10c73a280b1f9bba82bb08082db7bcfd5d2de",
        "01a75161fac9acfc5a603bc2256245e914591e5e",  # Missing dom/security/metrics.yaml
        "cdb47e79cd499b67d5de2804cbfb70eb2ab29796",  # Missing parser/html/metrics.yaml
        "ed40307b32b221322505a86ebd33a322c64820bb",  # Missing security/ct/metrics.yaml
        # Missing toolkit/components/antitracking/bouncetrackingprotection/metrics.yaml
        "32aceda20e3960fae23b3959be179693ec825599",
        # Missing toolkit/components/antitracking/bouncetrackingprotection/metrics.yaml
        "189fed694934b8cde47c83fa9fb56ae76b93092c",
        # Missing toolkit/components/antitracking/imageinputmetadatastripper/metrics.yaml
        "3b9744aaa5694b1c633acb0d0ea1fe8ec31c9d28",
        # Missing toolkit/components/reader/metrics.yaml
        "5bd2d84327d9385a4f4a0fbc4f55e4e0a0302bb2",
        "abbfb0e92e37e68d008ba0af29dbe199651fd2f3",  # Missing toolkit/profile/metrics.yaml
        # Missing toolkit/components/antitracking/bouncetrackingprotection/metrics.yaml
        "84748d4bd6523268d905b0bc78cc7773a37bbca9",
        # Missing toolkit/components/antitracking/bouncetrackingprotection/metrics.yaml
        "7b49203aee2818b96242b4746fed722844619760",
        # Missing toolkit/components/resistfingerprinting/pings.yaml
        "de714a36bce1431b1332b52c48106fedb2d4142a",
        # Missing toolkit/components/resistfingerprinting/pings.yaml
        "2df76493a78a6cc21c37b699fa4ae3eb91f87218",
        # Missing toolkit/components/captchadetection/metrics.yaml
        "1fc70947ff229c30ba1b5c54f884a503af13ccda",
        # pdfjs metrics missing `description`s
        "35ed43c92f51ee67e66f2a8d2814d57acbb4063e",
        # Around this point we switched from gecko-dev to mozilla-firefox/firefox
        # meaning that prior SHAs no longer apply : |
        "34092faf48616f4f9dfc351e9d8176b3ac54c121",  # Missing services/common/metrics.yaml
        # Missing devtools/client/webconsole/metrics.yaml
        "40e17f076bef05a0ea4f85d2e47a566037d59dad",
        # Missing dom/base/use_counter_metrics.yaml
        "aeac4d77e188608115b323888a0e7f4ab771d593",
        # Missing dom/base/use_counter_metrics.yaml
        "7f8a3ccc5ea8efe09c87b36b27563e7c7239e643",
        "d01e8d9f996cab64c99d4871de5c2096df0a9f3d",  # Missing dom/media/webrtc/metrics.yaml
        "0e02a53995929ece449457cc60b5a7ed461b3376",  # Missing dom/security/metrics.yaml
        "e61c5ecc4d75444051c805cd5e6c660047df8edb",  # Missing gfx/metrics.yaml
        "f3bc89cb1fd9732c951f4f55e580a845c1feb00d",  # Missing intl/locale/metrics.yaml
        "379594ec0cb8003005a24ae833e08426ebefde7c",  # Missing netwerk/protocol/http/metrics.yaml
        "21a149a784310241f4f3d1db46f855e9a0b5e336",  # Missing netwerk/protocol/http/metrics.yaml
        "b903ab5d69ce3e13d405b950ef9ebea8c39364ed",  # Missing netwerk/protocol/http/metrics.yaml
        "b80eb24c664e6baa15bd9c5fc2413b176b96e629",  # Missing security/ct/metrics.yaml
        "17360b21e0f09bf2e5fff13338ce6fe35ffca6b7",  # Missing security/manager/ssl/metrics.yaml
        "cbdeaba2c139bf9c369a3445f824de7af0eb88ad",  # Missing security/manager/ssl/metrics.yaml
        # Missing toolkit/components/antitracking/bouncetrackingprotection/metrics.yaml
        "02a2978f8047c21aff54c2941e964aadd2eea758",
        # Missing toolkit/components/antitracking/bouncetrackingprotection/metrics.yaml
        "7cdea11ce1ce783ab2ef6fb6d497613bb49f5fef",
        # Missing toolkit/components/captchadetection/metrics.yaml
        "48bbabbc358143032f3bdf0b89eaeb792f0ca390",
        "cc33cd20bb9efd8895f57236f8d3b19c65c6a608",  # Missing toolkit/components/doh/metrics.yaml
        "1d64fae3b4027244ffa603ea5a075ae5b35aca43",  # Missing toolkit/components/ml/metrics.yaml
        "a4c382ea54e5ff84183230f1ae6173e8af182962",  # Missing toolkit/components/pdfjs/metrics.yaml
        # Missing toolkit/components/reader/metrics.yaml
        "806eacf8c594522a3de2b171c276cf9f6e2968d0",
        # Missing toolkit/components/translations/metrics.yaml
        "b971c9609e34ff8fe76145ae5059d8503fd06583",
        "afe3db6bb23e76bbf925d61c8e0c5db84ee9391f",  # Missing toolkit/mozapps/update/metrics.yaml
        "aee1a52659567c4d08f6dcc9d173b6ff1eea9223",  # Missing toolkit/profile/metrics.yaml
        "df36a8d502166bdfc9ad9f34353a8d967bbd5ff9",  # Missing xpcom/metrics.yaml
        "7cade7af82b2fa1c9c908f0154934e7990d89643",  # Missing dom/pings.yaml
        "eb5d60087707d1acaf663a8ea2bbf14602e88d2f",  # Missing netwerk/pings.yaml
        # Missing toolkit/components/antitracking/bouncetrackingprotection/pings.yaml
        "7422c4eea96ee39d39acd8f953403c29a2a53d9a",
        # Missing toolkit/components/antitracking/bouncetrackingprotection/pings.yaml
        "88e54822a53a68a74616930098d1a6a23966c308",
        # Missing toolkit/components/captchadetection/pings.yaml
        "48bbabbc358143032f3bdf0b89eaeb792f0ca390",
        "ed7c6247ed0f2821d411453a3f07b5ae300c3416",  # Missing toolkit/components/glean/pings.yaml
        "744c67d2de320fe19f127039f290395a8d826bb1",  # Missing toolkit/components/glean/pings.yaml
        # Missing toolkit/components/resistfingerprinting/pings.yaml
        "d534c92fc42671af1ba2480aa626fcbedb622418",
        # Missing toolkit/components/resistfingerprinting/pings.yaml
        "db3d57b115a2eb86f5cbc62a7a060a1c6e782169",
        "22d98c8bb4325c2e2fb32f9fd06394a6e19b8dce",  # Missing toolkit/components/glean/tags.yaml
        "775705d9e5e29f9b81899080d20b3ecbdd4e244b",  # Bad ping 'gecko_trace'
        "29e167130c00b71907e8eb64d2dd95efafaf44dc",  # pdfjs metrics missing descriptions
        # Bad yaml alias in toolkit/components/translations/metrics.yaml
        "7ddead757c64f11521f663448e5171aaaae2f8ad",
    ],
    "firefox-desktop": [
        "c5d5f045aaba41933622b5a187c39da0d6ab5d80",  # Missing toolkit/components/glean/tags.yaml
        "3e81d4efd88a83e89da56b690f39ca2a78623810",  # No browser/components/newtab/metrics.yaml
        "d556b247aaec64b3ab6a033d40f2022f1213101e",  # No toolkit/components/nimbus/metrics.yaml
        "d1d0b69871e3d38ead989d73f30563a501a448b6",  # No toolkit/components/nimbus/metrics.yaml
        "642be079c4465445ab42b55d18e0a4d644c19c36",  # No toolkit/components/telemetry/pings.yaml
        # Missing toolkit/components/telemetry/dap/metrics.yaml
        "c5c002f81f08a73e04868e0c2bf0eb113f200b03",
        # Missing browser/components/backup/metrics.yaml
        "4d4322e829aa7ba8a4abd00fca0dcd3b10e127a3",
        # Missing browser/components/privatebrowsing/metrics.yaml
        "47da40cec7bb1235bd9dc597a26f7b69b48fc2a7",
        # Missing dom/media/platforms/wmf/metrics.yaml
        "41edcdf7fe44678c5913a603a286b1fc3979d540",
        # Missing toolkit/components/contentrelevancy/metrics.yaml
        "856ef9e3e5132cf536dc5662e220c0e0e5127a7e",
        # Missing toolkit/components/contentrelevancy/metrics.yaml
        "c7f67706fcdac6a6198d8867cb102546213dbaf8",
        # Missing toolkit/components/places/metrics.yaml
        "bc739eb4ae15600f5eb668a060de8732e34e7e26",
        # Missing toolkit/components/shopping/metrics.yaml
        "f03abd1c7bf9f721afd0df7e36023f4ea925afd2",
        "c9bbde88a4e816950372d1647827491902f62af4",  # Missing widget/windows/metrics.yaml
        "21001e9ab793daf750ad988ce86cc7eefd29b856",  # Missing toolkit/components/nimbus/pings.yaml
        "514742c4bda3c0a5ea5c631029929efa8fd6f855",  # Missing toolkit/components/nimbus/pings.yaml
        # Missing toolkit/components/reportbrokensite/metrics.yaml
        "42acdc9cd5ae89222bdceeeaed7bacac755be48f",
        # Missing toolkit/components/reportbrokensite/metrics.yaml
        "c76093316c58ae74a21e854b8035c91d0c75df6e",
        # Around this point we switched from gecko-dev to mozilla-firefox/firefox
        # meaning that prior SHAs no longer apply : |
        # Missing browser/components/asrouter/metrics.yaml
        "695882bbbfdcb8c5cd9a613ced7c6b6a86a29afb",
        # Missing browser/components/attribution/metrics.yaml
        "efcf0aaa0af9644c5213f2acc8b33cff0f0c7fd7",
        # Missing browser/components/attribution/metrics.yaml
        "280680c5c455ccb18ecf3615dc3b72ec549bae63",
        # Missing browser/components/attribution/metrics.yaml
        "ed01a1d8b2d1cc0339b424fc35e4c5ee6639b71b",
        # Missing browser/components/backup/metrics.yaml
        "013051b63c532875b99d47157926350af3f28128",
        # Missing browser/components/contextualidentity/metrics.yaml
        "dc64a7e82ff4e2e31b7dafaaa0a9599640a2c87c",
        # Missing browser/components/contextualidentity/metrics.yaml
        "257efb0851f403d6f49aeb9adeb142f899710a2a",
        # Missing browser/components/contextualidentity/metrics.yaml
        "fdd7267509d268df2f11e60d338be0afd1296c6b",
        # Missing browser/components/contextualidentity/metrics.yaml
        "dd94fbfd7a6bc94a89df928f583c9eae9fef83ee",
        # Missing browser/components/customkeys/metrics.yaml
        "d277e7f5b98c953c914c13a522ea767a7ceda2c4",
        # Missing browser/components/newtab/metrics.yaml
        "b967cfd2bb58fecbaab44e2e1db43a0bf81e35fe",
        # Missing browser/components/privatebrowsing/metrics.yaml
        "a389d668fb89ff4a74d6a1c4389a24e8a2423b0c",
        # Missing browser/components/search/metrics.yaml
        "b967cfd2bb58fecbaab44e2e1db43a0bf81e35fe",
        # Missing browser/components/tabnotes/metrics.yaml
        "bb3d5095b9b93306feb11edf6953e6aa65731ba4",
        "dbc4a6f9dd59e24cac39d2f360d17e27064be646",  # Missing dom/media/platforms/wmf/metrics.yaml
        # Missing toolkit/components/contentanalysis/metrics.yaml
        "abfcb76cf97c59ab3e55523e63cec18ec370adfe",
        # Missing toolkit/components/contentrelevancy/metrics.yaml
        "4d64600494aaacff9d18e3546366a61c98ef023b",
        # Missing toolkit/components/contentrelevancy/metrics.yaml
        "28b0abdfac3b5be8cca4a9236a23cc1a36246551",
        # Missing toolkit/components/nimbus/metrics.yaml
        "4e2cd0f7c2ba33d3cfc474e4a9aefa2bd8a75eab",
        # Missing toolkit/components/places/metrics.yaml
        "aef39cc559ae3837341ec01cc0a47b05845e052f",
        # Missing toolkit/components/reportbrokensite/metrics.yaml
        "f0be2f011927a48c1c135463eb7a3ea57db29b27",
        # Missing toolkit/components/reportbrokensite/metrics.yaml
        "8e6281d821149c1ea7e968272b66cbb6c096ab52",
        "6a27bbefde56fff7e1f63b5653686497c01c834b",  # Missing widget/windows/metrics.yaml
        # Missing browser/components/asrouter/pings.yaml
        "695882bbbfdcb8c5cd9a613ced7c6b6a86a29afb",
        "b967cfd2bb58fecbaab44e2e1db43a0bf81e35fe",  # Missing browser/components/newtab/pings.yaml
        "4af280a39685908b2947cae3057adabf748e12a2",  # Missing services/sync/pings.yaml
        "91f71ab9b13d3d44541fe40f1b8752c36e053f58",  # Missing toolkit/components/nimbus/pings.yaml
        "3dee923b49228c5fbba22354de6c5650b0a26720",  # Missing toolkit/components/nimbus/pings.yaml
        # Missing toolkit/components/reportbrokensite/pings.yaml
        "f0be2f011927a48c1c135463eb7a3ea57db29b27",
        # Missing toolkit/components/reportbrokensite/pings.yaml
        "8e6281d821149c1ea7e968272b66cbb6c096ab52",
        # Missing toolkit/components/telemetry/pings.yaml
        "6c64ffc8e1fcb59b48533e22eca8f426e72e6ce2",
        "22d98c8bb4325c2e2fb32f9fd06394a6e19b8dce",  # Missing toolkit/components/glean/tags.yaml
        # Bad yaml alias in toolkit/components/nimbus/metrics.yaml
        "12175724abfa7ae282cb27e308f551d116aede1b",
    ],
    "firefox-desktop-background-update": [
        "c5d5f045aaba41933622b5a187c39da0d6ab5d80",  # Missing toolkit/components/glean/tags.yaml
        # Around this point we switched from gecko-dev to mozilla-firefox/firefox
        # meaning that prior SHAs no longer apply : |
        "22d98c8bb4325c2e2fb32f9fd06394a6e19b8dce",  # Missing toolkit/components/glean/tags.yaml
    ],
    "firefox-desktop-background-tasks": [
        # Missing toolkit/components/backgroundtasks/metrics.yaml
        "0caa2f1940d744d1154f47c242bc5c119cf453f8",
    ],
    "firefox-translations": [
        # Invalid extension/model/telemetry/metrics.yaml
        "02dc27b663178746499d092a987ec08c026ee560",
    ],
    "pine": [
        "c5d5f045aaba41933622b5a187c39da0d6ab5d80",  # Missing toolkit/components/glean/tags.yaml
        "3e81d4efd88a83e89da56b690f39ca2a78623810",  # No browser/components/newtab/metrics.yaml
        "642be079c4465445ab42b55d18e0a4d644c19c36",  # No toolkit/components/telemetry/pings.yaml
    ],
    "rally-core": [
        "4df4dc23317e155bf1b605d04b466c27d78537fa",  # Missing web-platform/glean/metrics.yaml
        "69559324f775b79c9a39c6a95fdb3657c184ed0e",  # Bug 1769579 omit deleted onboarding ping
        "f633df7676b6ef64e496fea1b3687eff22680d49",  # Missing web-platform/glean/pings.yaml
    ],
    "rally-attention-stream": [
        "9fd0b2aeb82ca37f817dcda51bd2f34b6925b487",  # `bugs`/`data_reviews` is not of type `string`
        "a3dacb30e198c5c19159678c6617064cf4ae1d77",  # Bug 1783960 omit deleted meta-pixel ping
    ],
    "support-migration": [
        "2e05b2b7d775ea726e035a7a7f16d889d63fc09a",  # No components/support/migration/metrics.yaml
    ],
    "viu-politica": [
        "e41967f92f40dd36729939cf67bcf680352ec1a4",  # Removed all data collection
    ],
    "moso-mastodon-backend": [
        "cd5c69456d88b7023366fd50806855086a039dba",  # No .glean/metrics.yaml
    ],
    "tiktokreporter-android": [
        "96bf78fbde4dc1eddd8fc7de175d6c58fe82e23e",  # Improperly named metric
    ],
    "accounts-backend": [
        "095b4e47cebaa8a2ca54d1d496814f0620dcf8b1",  # Wrong schema spec used
    ],
    "glean-server-metrics-compat": [
        "6fe8a8f8a4026f389a8f697669d56673e0817a29",  # Wrong schema spec used
    ],
}


def _file_in_commit(repo: git.Repo, filename: Path, ref: str) -> bool:
    # adapted from https://stackoverflow.com/a/25961128
    subtree = repo.commit(ref).tree
    for path_element in filename.parts[:-1]:
        try:
            subtree = subtree[path_element]
        except KeyError:
            return False  # subdirectory not in tree
    return str(filename) in subtree


@dataclass(eq=True, frozen=True)
class Commit:
    hash: str
    # only compare hash when checking if commits are equal
    timestamp: int = field(compare=False)
    # Since commits from the same PR may have the same timestamp, we also record
    # an index representing its position in the git log so the correct ordering
    # of commits can be preserved.
    reflog_index: int = field(compare=False)
    is_head: bool = field(compare=False)

    def sort_key(self) -> Tuple[int, int]:
        # git log returns newest commits first, so use negative reflog_index
        return self.timestamp, -self.reflog_index

    @cached_property
    def pretty_timestamp(self):
        return datetime.utcfromtimestamp(self.timestamp).isoformat(" ")


def get_commits(
    repo: git.Repo,
    filename: Path,
    ref: str,
    only_ref: bool = False,
    deprecated: bool = False,
    branch_head_hash: Optional[str] = None,
) -> Iterable[Commit]:
    sep = ":"
    log_format = f"%H{sep}%ct"
    commits = set()
    if not only_ref:
        # include "--" to prevent error for filename not in current tree
        log = repo.git.log(ref, "--", filename, format=log_format)
        # filter out empty strings
        change_commits = filter(None, log.split("\n"))
        commits |= set(enumerate(change_commits))
    if (only_ref and not deprecated) or _file_in_commit(repo, filename, ref):
        # include ref when it contains filename
        log = repo.git.log(ref, format=log_format, max_count=1)
        # filter out empty strings
        change_commits = filter(None, log.split("\n"))
        commits |= set(enumerate(change_commits))

    # Store the index in the ref-log as well as the timestamp, so that the
    # ordering of commits will be deterministic and always in the correct
    # order.
    for reflog_index, entry in commits:
        hash_, timestamp = entry.split(sep)
        yield Commit(
            hash=hash_,
            timestamp=int(timestamp),
            reflog_index=reflog_index,
            is_head=hash_ == branch_head_hash,
        )


def get_file_at_hash(repo: git.Repo, _hash: str, filename: Path) -> str:
    return repo.git.show(f"{_hash}:{filename}")


def utc_timestamp(d: datetime) -> float:
    # See https://docs.python.org/3/library/datetime.html#datetime.datetime.timestamp
    # for why we're calculating this UTC timestamp explicitly
    return (d - datetime(1970, 1, 1)) / timedelta(seconds=1)


def retrieve_files(
    repo_info: Repository,
    cache_dir: Path,
    glean_commit: Optional[str] = None,
    glean_commit_branch: Optional[str] = None,
    limit_date: Optional[date] = None,
) -> Tuple[Dict[Commit, List[Path]], bool]:
    commits = defaultdict(list)
    base_path = cache_dir / repo_info.name
    org_name, repo_name = repo_info.url.rstrip("/").split("/")[-2:]
    repo_path = cache_dir / org_name / f"{repo_name}.git"

    min_date = None
    if repo_info.name in MIN_DATES:
        min_date = utc_timestamp(datetime.fromisoformat(MIN_DATES[repo_info.name]))

    skip_commits = SKIP_COMMITS.get(repo_info.name, [])

    if repo_path.exists():
        print(f"Pulling commits into {repo_path}")
        repo = git.Repo(repo_path)
        actual_urls = set(repo.remote("origin").urls)
        if actual_urls != {repo_info.url}:
            raise Exception(
                f"invalid cache: git repo at {repo_path} should be for "
                f"{repo_info.url} but got {actual_urls}"
            )
    else:
        print(f"Cloning {repo_info.url} into {repo_path}")
        repo = git.Repo.clone_from(
            repo_info.url,
            repo_path,
            bare=True,
            depth=1 if glean_commit or limit_date else None,
        )

    repo_is_shallow = repo.git.rev_parse(is_shallow_repository=True) == "true"
    branch = repo_info.branch or repo.active_branch
    if glean_commit is None:
        if limit_date is not None:
            shallow_since = utc_timestamp(datetime.combine(limit_date, time.min))
            try:
                repo.git.fetch(
                    "origin",
                    f"{branch}:{branch}",
                    force=True,
                    shallow_since=shallow_since,
                )
            except git.GitCommandError as e:
                if any(
                    log in e.stderr
                    for log in (
                        # github error
                        "\n  stderr: 'fatal: error processing shallow info: 4'",
                        # local git dir error
                        "\n  stderr: 'fatal: no commits selected for shallow requests\n",
                    )
                ):
                    # no commits, don't upload
                    return {}, False
                raise
        else:
            repo.git.fetch(
                "origin",
                f"{branch}:{branch}",
                force=True,
                unshallow=repo_is_shallow,
            )
        # pass ref around to avoid updating repo.active_branch, so that it
        # can be preserved for other glean repos with the same git url
        ref = f"refs/heads/{branch}"
        branch_head_hash = repo.commit(ref).hexsha
        upload_repo = True
    elif GIT_HASH_PATTERN.fullmatch(glean_commit) is None:
        raise ProbeScraperInvalidRequest(
            f"commit must be full length git hash, but got {glean_commit!r}"
        )
    else:
        repo.git.fetch(
            "origin", glean_commit, force=True, depth=1 if repo_is_shallow else None
        )
        ref = glean_commit
        upload_repo = str(branch) == glean_commit_branch
        # When commit_branch is the branch for this repo, verify that commit_hash is on that branch
        if upload_repo:
            print(f"Verifying that {glean_commit} is in {branch}")
            # doesn't change depth
            repo.git.fetch("origin", f"{branch}:{branch}", force=True)
            branch_ref = f"refs/heads/{branch}"
            branch_head_hash = repo.commit(branch_ref).hexsha
            if glean_commit != branch_head_hash:
                if repo_is_shallow:
                    repo.git.fetch(
                        "origin", f"{branch}:{branch}", force=True, unshallow=True
                    )
                try:
                    # when commit != branch, check if it's in the history for branch
                    repo.git.merge_base(glean_commit, branch_ref, is_ancestor=True)
                except git.GitCommandError:
                    raise ProbeScraperInvalidRequest(
                        f"Commit {glean_commit} not found in branch {branch} of {repo_info.url}"
                    )
        else:
            branch_head_hash = None

    for rel_path in map(Path, repo_info.get_change_files()):
        for commit in get_commits(
            repo,
            rel_path,
            ref,
            only_ref=glean_commit is not None,
            deprecated=repo_info.deprecated,
            branch_head_hash=branch_head_hash,
        ):
            if min_date and commit.timestamp < min_date:
                continue
            if commit.hash in skip_commits:
                continue

            probe_file = base_path / commit.hash / rel_path
            if not probe_file.exists():
                try:
                    contents = get_file_at_hash(repo, commit.hash, rel_path)
                except git.GitCommandError as e:
                    if "does not exist" in str(e):
                        raise ProbeScraperInvalidRequest(
                            f"{rel_path} not found in commit {commit.hash} for {repo_info.app_id}"
                        )
                    raise

                probe_file.parent.mkdir(parents=True, exist_ok=True)
                probe_file.write_bytes(contents.encode("UTF-8"))

            commits[commit].append(probe_file)

    return commits, upload_repo


def scrape(
    folder: Optional[Path] = None,
    repos: Optional[List[Repository]] = None,
    glean_commit: Optional[str] = None,
    glean_commit_branch: Optional[str] = None,
    limit_date: Optional[date] = None,
) -> Tuple[
    Dict[str, Dict[Commit, List[Path]]],
    Dict[str, Dict[str, List[Union[Dict[str, str], str]]]],
    List[str],
]:
    """
    Returns three data structures. The first is commits_by_repo:
    {
      <repo-name>: {
        <Commit>: [<path>, ...]
      }
    }

    The second is emails:
    {
      <repo-name>: {
        "addresses": [<email>, ...].
        "emails": [
          {
            "subject": <str>,
            "message": <str>,
          },
        ]
      },
    }

    The third is the names of repos that are authorized to be uploaded, based on
    whether commit_branch matches the configured branch for that repo. When commit is
    not None but commit_branch is None, this is empty. When commit and commit_branch are
    both None, this includes all repos:
    [<repo-name>, ...]

    Raises InvalidCommitError when commit is not None or a 40 character hex sha.

    Also raises InvalidCommitError when commit and commit_branch are both specified and
    commit_branch matches the configured branch for a repo and commit is not part of the
    history of commit_branch for that repo. This ensures that return values correctly
    indicate repos where commits are authorized to be uploaded.
    """
    if folder is None:
        folder = Path(tempfile.mkdtemp())

    commits_by_repo = {}
    emails = {}
    upload_repos = []

    for repo_info in repos:
        print("Getting commits for repository " + repo_info.name)

        commits_by_repo[repo_info.name] = {}
        emails[repo_info.name] = {
            "addresses": repo_info.notification_emails,
            "emails": [],
        }

        if not (
            repo_info.metrics_file_paths
            or repo_info.ping_file_paths
            or repo_info.tag_file_paths
        ):
            print(
                f"Skipping commits for repository {repo_info.name}"
                " because it has no metrics/ping/tag files."
            )
            continue

        try:
            commits, upload_repo = retrieve_files(
                repo_info,
                folder,
                glean_commit,
                glean_commit_branch,
                limit_date,
            )
            print("  Got {} commits".format(len(commits)))
            commits_by_repo[repo_info.name] = commits
            if upload_repo:
                upload_repos.append(repo_info.name)
        except Exception:
            raise
            emails[repo_info.name]["emails"].append(
                {
                    "subject": "Probe Scraper: Failed Probe Import",
                    "message": traceback.format_exc(),
                }
            )

    return commits_by_repo, emails, upload_repos
