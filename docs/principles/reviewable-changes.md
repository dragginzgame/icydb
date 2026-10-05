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

- files touched and approximate line delta;
- primary delivery areas crossed;
- whether width came from semantics or mechanical propagation; and
- whether the implementation became simpler, stayed neutral, or became more
  complex.

Stop and split or update the plan when work reveals another production
behavior, canonical owner, or independently useful result. The cost of another
compile or review is not sufficient reason to hide that outcome in the current
change.

## Cleanup reporting

Build the final cleanup report from the actual diff, including declarations in
deleted files. List every removed function, method and type, public or private,
with its former file or module, removal reason and replacement when applicable.
Use exact names so the maintainer can identify what disappeared and why; a
summary such as "removed legacy helpers" or a symbol count is insufficient.
Group entries with the same reason if useful, while enumerating every name.
Label moves and renames explicitly so they do not imply behavior was deleted.

## Validation

Run the narrowest evidence that matches the changed risk automatically while
developing. Broad workspace, full CI and release gates require an explicit request
or their configured CI pipeline. Record what passed and what was not run.

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
independent scope and release boundaries still require direction. Publication and
deployment retain explicit authority, and agents never create or amend commits.
