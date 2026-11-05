> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

# Facet Rust library audit for IcyDB

Facet could help IcyDB build better host diagnostics, inspect application types,
and share traversal code at typed conversion boundaries. Its strongest potential
is outside the canister runtime. This audit recommends keeping the current
schema, value, storage, and execution authorities, and evaluating Facet only
against a concrete tooling need. A general replacement of IcyDB's macros or
Serde is not justified by the evidence collected here.

The most consequential blockers are Facet's experimental security support,
unstable reflection identities, the audited prerelease's Rust 1.92 requirement,
domain-type integration work, and unmeasured canister costs. One concrete
generator mismatch is verified in source: the audited TypeScript generator maps
64-bit and 128-bit Rust integers to JavaScript `number`.

## Scope and evidence

Requested scope: a full adoption audit of the Rust reflection library and its
relevance to IcyDB. This investigation covers reflection architecture, derives,
reading and construction, invariant preservation, formats, dynamic values,
diagnostics, code generation, dependency boundaries, portability, maintenance,
and the IcyDB integration points those capabilities could affect.

The evidence is source inspection and published API documentation. This is an
adoption assessment, not an exhaustive memory-safety certification of every
Facet crate. No exploit reproduction, dependency advisory scan, build, behavioral
test, fuzz campaign, or IC measurement was executed. Unrelated ecosystem
projects were assessed for relevance rather than audited internally.

| Input | Exact evidence boundary |
| --- | --- |
| IcyDB | Workspace `0.261.19`, HEAD `c16083c1da5ebe7c12b974d07b4f30374e287ca1`, including the worktree observed during this audit |
| Facet core source | Commit `65bae5c31a7ce401bc44630fb96250ea884cfd3e`, dated 2026-08-20; principal packages identify themselves as `0.50.0-rc.7` |
| Facet format source | Commit `4279debff780ae1cd5b028201f446c26594b1120`, dated 2026-08-20; principal packages identify themselves as `0.50.0-rc.7` |
| Published documentation | docs.rs serves Facet and facet-reflect `0.46.5`; its release list also includes `0.50.0-rc.7` |
| Working source copies | Temporary checkouts under `/tmp/icydb-facet-audit` and `/tmp/icydb-facet-format-audit` |

The published release list dates `0.46.5` to 2026-05-26 and `0.50.0-rc.7` to
2026-08-20. Those are release dates, not the audit date. Prerelease source findings
must not be silently treated as properties of `0.46.5`.
[Published release list](https://docs.rs/crate/facet/latest).

## Adoption findings

These severities describe risk to an IcyDB adoption proposal. They are not
confirmed vulnerabilities in Facet.

## Recommended follow up

The current decision is to keep implementation unchanged. The audit found
plausible uses, but no demonstrated unmet requirement or measured runtime benefit
that justifies a general adoption.

If richer artifact diagnostics are wanted, the smallest useful experiment is a
host-only comparison of one existing diagnostic artifact. Preserve its accepted
identity and current JSON contract, compare Facet pretty/diff output with a
renderer over the existing Serde representation, and choose the simpler option
that solves the concrete problem. Keep any mirror projection temporary; a
permanent duplicate DTO would weaken the proposed simplification.

No-build gate for that candidate: the need is a specific unreadable or difficult
comparison; the existing artifact remains canonical; the alternative is a small
renderer over existing data; the owner is host diagnostics; the only proposed
new mechanism is reflection for display. It should add no canister execution
route, persisted state, wire format, or schema authority. If the simpler renderer
is sufficient, do not add Facet.

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/investigations/2026/09/30/facet-adoption/01/report.md).
