# Documentation-only CI selection

`perl scripts/ci/classify-ci-change.pl --event pull_request --ref refs/heads/main
--base <base-sha> --head <tested-sha> --policy ci/docs-only-paths.txt` prints `docs`
or `full`. Pass full commit identities through quoted arguments, not shell source.
For pushes use `--event push` and the before/after SHAs; a tag or release ref
always selects full qualification. PR comparison uses the merge base; pushes
require ancestry. Missing history, invalid input and failed Git observations
select `full` without fetching. An empty comparison also selects `full`.

Consumers own an exact, reviewed list of Markdown paths that are prose only.
There are no globs. The classifier reads identical policy bytes from both
compared commits, never an uncommitted list. Unknown paths, policy/workflow/code
changes, executable files and symlinks select full CI. Renames are checked as
both deletion and addition. Do not list Markdown used by `include_str!`, doctests,
generation, packaging or other executable/build contracts; the extension alone
is not proof. Keep the classifier and workflow under normal code review: this
is an optimization, not a security boundary against workflow authors.

Submodule identity changes always select full CI, even when local Git settings
normally hide them. Relative-diff settings cannot narrow the compared paths.

Use a cheap classification job with complete comparison history. Keep stable
required job names and run a real documentation check on the docs path. Avoid
workflow-level path filters that leave required checks pending. If classification
fails as a job, dependent checks must fail or run full qualification, never report
unverified success. Full code/tooling checks keep all required native hosts;
release gates remain complete and independent of this selection.

Shared Tooling's initial allowlist covers governance prose only. Each existing
portable matrix check retains its name, runs on Linux for docs-only changes and
checks local documentation links and whitespace. Normal changes retain native
Linux and both macOS jobs. The lint/security job still runs, including the
repository secret scan. A docs-only pass does not claim native qualification.
