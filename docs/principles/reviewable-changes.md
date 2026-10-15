# Reviewable Changes

## Purpose

A change should deliver one bounded outcome while including all direct proof
and propagation required to make that outcome complete. File count is a signal,
not the unit of work.

This principle combines IcyDB's landing-slice guidance with Canic's distinction
between implementation slices and release batches. The shared baseline owns
continuation, validation authority and effect authorization. Consumers define
their accepted outcomes, qualification gates and release inputs within it.
Release command names and sequencing follow the [common contract](../releases.md).

## Coherent outcome

A reviewable change names:

- one bounded outcome and its canonical owner;
- the delivery areas it expects to touch;
- its focused validation boundary;
- relevant public, persisted-format, performance, or artifact-size impact; and
- the directly required tests, diagnostics, documentation, fixtures, and
  mechanical propagation.

Those direct consequences belong with the outcome. Do not create micro-changes
for compiler fallout, exhaustive matches, or a regression test that proves the
same behavior.

Conversely, do not combine independent owners or independently reviewable
outcomes merely to reduce the number of handoffs or validation runs.

## Width and coupling

A wide change can remain coherent when every touched file is required by the
same authority and outcome. A small diff can still be incoherent when it mixes
unrelated behavior.

At handoff, report:

- files touched and scoped metrics under the [change-reporting rules](../../rules/change-reporting.md);
- primary delivery areas crossed;
- whether width came from semantics or mechanical propagation; and
- whether the implementation became simpler, stayed neutral, or became more
  complex.

Stop and split or update the plan when work reveals another production
behavior, canonical owner, or independently useful result. The cost of another
compile or review is not sufficient reason to hide that outcome in the current
change.

## Cleanup reporting

Apply the [canonical reporting contract](../../rules/change-reporting.md): separate
genuine deletions, moves and renames under **symbol changes**, with counts and
exact identities. Keep edited declarations distinct and keep the full inventory
only in the final Codex app handoff. Physical line churn and semantic deletion
counts answer different questions; neither substitutes for the other. The same
rule owns the required Rust/type/documentation metrics and their counting units.

## Validation

Run the narrowest evidence that matches the changed risk automatically while
developing. Before delivering a completed code change as ready, run the owning
repository's documented full validation suite under the baseline's standing
authority. Reuse passing results for unchanged inputs; further changes require
the affected checks again. Documentation-only changes need link, instruction
consistency and diff checks. Inspection alone does not authorize a full suite,
and release execution still requires its own explicit request. Record what
passed and what was not run; a progress update is not a completed delivery.

Tests protect maintained behavior and trust boundaries, not incidental
implementation shape. Avoid repeating equivalent assertions through every
surface unless the interaction itself can change behavior, recovery, routing,
or cost.

## Consumer-owned policy

Each repository retains authority over:

- its planned outcomes and how they map to pull requests and releases;
- exact focused-check commands and complete qualification gates;
- explicit release execution and version overrides within the
  [common compatibility and automatic changelog rules](../../rules/changelogs.md); and
- closeout, publication, and deployment gates.

Ordinary continuation completes the accepted coherent batch, including its direct
evidence and propagation, without a compulsory one-slice-per-turn stop. New
independent scope and release boundaries still require direction. Authorized PR
delivery includes its scoped commits and branch push under the
[contribution rules](../../rules/contributions.md). Merging, direct integration-branch
pushes, releases, publication and deployment retain their own explicit authority.
