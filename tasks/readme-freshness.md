# Advisory README freshness review

Apply the [common run contract](README.md). Review the selected repositories'
root README and directly linked setup/usage instructions every three days as
part of the maintenance pass. Bound the review to maintained claims and examples;
record any unreviewed scope.

Compare commands, tool prerequisites, feature claims, package examples and MSRV
statements with their existing owners: manifests, supported-host documentation,
command implementations, adopted snapshots and matching validation evidence.
For claims about the latest published version, observe the owning registry or
release directly and record its identity and observation time. A local version,
undated changelog draft or worktree rollback does not establish publication.
Missing credentials, ambiguous package identities or unavailable evidence are
explicit limitations; do not guess the correct replacement.

Treat differences as evidence to assess, not automatic defects. Supported older
examples, version ranges and deliberately historical instructions can be valid.
Multiple examples, absent sections and repository-specific layouts are allowed.
Report an outdated example when its documented command or compatibility claim
is demonstrably wrong, explaining the supported replacement and its source.
Do not require an exact latest patch number, one canonical example or a uniform
README structure.

This task is advisory only. It never rewrites or stages prose, changes source,
manifests or lockfiles, or runs builds. It adds no CI, release, preflight,
publication or deployment gate. Formatting, missing sections and prose version
differences cannot block delivery; actual package/release identity validation
remains with its existing owner.

Return the reviewed commit, README locations, authoritative comparisons and
material findings using the catalog's result vocabulary. Search existing issues
before creating or updating an owning issue, and do not repeat unchanged
findings on each pass. A documented valid older example is not a finding;
uncertain or missing evidence is not proof that the README is current.
