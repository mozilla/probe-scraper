Thank you for your interest in contributing to probe-scraper!
This document tries to codify some best practices for contribution to this
repository.

## Participation guidelines

All communication is expected to follow the
[Mozilla Community Participation Guidelines](https://www.mozilla.org/about/governance/policies/participation/).
For more information, see the [code of conduct document](./CODE_OF_CONDUCT.md)
in the root of this repository.

## Filing issues

File an issue if you have a bug report or feature request that you (personally)
do not intend to work on right away _or_ you would like additional feedback on
your approach before starting implementation work. If you found a bug (or small
missing feature) and you want to start implementing it immediately (or already
have a solution), go ahead and skip straight to making a pull request (see
below).

To help with triage, issues should have a descriptive title. Examples of good
issue titles:

- Require channels be unique for applications
- "Telemetry Probe Expiry" emails sometimes don't include list of filed bugs

In the issue itself, provide as much information as necessary to help someone
reading it understand the nature of the problem (and provide feedback). For
examples of this, look at some of the
[fixed issues](https://github.com/mozilla/probe-scraper/issues?q=is%3Aissue+is%3Aclosed)
filed by the project maintainers.

Occasionally probe-scraper bugs are tracked inside Bugzilla, especially for issues
which might affect other parts of the pipeline.

## Opening pull requests

Like issues, pull requests should have a descriptive title to help with triage.
However there are two things that are different:

- Instead of pointing out a problem, they should describe the solution
- If a pull request fixes a specific issue, the title should specify
  `(fixes #X)` (where X refers to the issue number)

For example, a pull request to fix an issue entitled `"Telemetry Probe Expiry" emails sometimes don't include list of filed bugs` could be named `Include list of filed bugs in "Telemetry Probe Expiry" emails (fixes #1234)`.

When a pull request fixes a bug in Bugzilla, prepend the bug number to the title with
the keyword `Bug ` in the format `Bug XXXX - <one-line description>`.
This allow the [Bugzilla PR Linker] to link to this PR automatically in bugzilla.
For example, `Bug 1234 - Include list of filed bugs in "Telemetry Probe Expiry" emails`.

As much as possible, each pull request should attempt to solve _one problem_.
For logically separate changes, file multiple PRs.

Make sure that the pull request passes continuous integration (including linter
errors) and that there are no merge conflicts before asking for review. If you
want some feedback on a work-in-progress (where these conditions are not yet
met), mark your pull request as a draft.

[bugzilla pr linker]: https://github.com/mozilla/github-bugzilla-pr-linker

## Dangerous changes

This repository is central to how ingestion and processing Telemetry data at
Mozilla: in particular, adding new Glean repositories (`repositories.yaml` at the root
of this repository) needs to be done with some care.

Things to bear in mind:

- Once probe scraper has successfully run, there is no changing or rewriting history of the metrics files, as this will cause problems downstream with [mozilla-schema-generator].
- There is currently no provision for deleting a repository once added.

As such, testing works in progress should happen locally with a probe-scraper checkout (see the "dry run" instructions in the README) and/or evaluating test pings via the [Glean Debug Ping Viewer].
Under no circumstances should a testing application be added to this repository to "see what happens".
If you only want part of the history of a repository to be processed by probe-scraper, you can set a "start
date" in `probe_scraper/scrapers/git_scraper.py` _before_ the first successful run of probe-scraper
against it (i.e. the changes to `git_scraper.py` and `repositories.yaml` should land as a unit).

To try and prevent problems from occurring, changes to these files must go through people who have extensive
experience debugging and reasoning about the schema generation portions of the data pipeline, documented in `.github/CODEOWNERS`.
If you submit a pull request, these people will automatically be flagged for review.

[mozilla-schema-generator]: https://github.com/mozilla/mozilla-schema-generator
[glean debug ping viewer]: https://mozilla.github.io/glean/book/user/debugging/index.html#glean-debug-view
