# C37 — Replicated Accepted-Runtime Preparation

## Decision And Scope

Retain preparation of the existing accepted-schema runtime root in the existing
replicated startup/convergence driver, after recovery and schema readiness and
before optional cardinality work or quiescence. The normal accepted-snapshot
compiler, budget, atomic cache publication and invalidation remain authoritative.
There is no generated-model fallback, new cache, timer, format, configuration,
public method or readiness state.

The earlier [read investigation](i1-read-investigation.md) identified repeated
query-only preparation. Queries cannot retain heap changes. Merely restarting
therefore did not remove the cost. This change prepares that same cache in a
replicated callback. Deterministic errors use the existing schema-failure
classification and durable receipt; retryable errors stay retryable.

`Ready` is still a durable admission condition, not a cache-warm guarantee.
It may be observed before the callback. This qualification covers the generated
Item/Holder rename lifecycle, including restart before and after publication;
it does not qualify every schema size, later DDL invalidation, custom lifecycle,
or downstream application. Whole-application savings remain unmeasured.

## Matched Inputs

Measured 2026-09-23 with Rust 1.98.1 and PocketIC 16.0.0 using the maintained
`schema_migration_closeout` target's two `entity_rename` cases. The baseline and
retained candidate share source inputs except the five-line driver change.
Host-only instrumentation separately records upgrade/watchdog cycles and query
delivery cycles; it does not alter the actor workload or projection assertions.

Both builds use the existing user-owned Candid 0.10.36 lockfile, not the tagged
0.261.6 dependency graph. Cargo.lock SHA-256:
`51faebfd5df4a49b66446630de53a78cd4c9664af62a8e55ee80024faba0aa73`.
Cargo versions remain 0.261.6; this is a matched experiment, not a release gate.

| Actor | Baseline raw Wasm bytes | Retained raw Wasm bytes | Delta |
| --- | ---: | ---: | ---: |
| Rename source | 8,943,144 | 8,943,212 | +68 |
| Rename successor | 8,975,047 | 8,975,115 | +68 |

Artifact BLAKE3 identities:

- Baseline source: `e629545e4e0a1e8cc6172e6f8496b102ef53a406a86a8b8319df8920b3698d91`
- Baseline successor: `783f914bd16a9b23a86956149813c26c5eb66cdda0e4f3f23d6b911df664dfdc`
- Retained source: `7cfd0b44d0aef0f5a913c905c341fbf838efe06f3f1c23ebb36fbd9c74d2d127`
- Retained successor: `ab9a7745bf040d5c2cba04abb079e3ab17a8351e959f65d1b76fe43c6ac7776f`

The encoded seed input BLAKE3 is
`6755e93b2ea267fb63a1ca43907378ed3923f6416bab2555c3c82aa060f41cae`;
the exact `Advance` input is
`0b92fe0e0f5a4015c482fa2bd0c05c80040b4f9bc44ee3e7394f3bf718c824c5`.
Source projection BLAKE3 is
`bedf56c3343890fc91f4a9a9b3b9f7ea763145fa8e41a7c5d62a39ceac121ee0`;
all successor/restarted projections retain
`234fe17427faf7b0cd1445bbc0ef0ae7d2d7900d887a0d16092974d6a1fa99ea`.

## Query Instructions

The interval is the existing actor-local SQL measurement. Readiness delivery is
outside that interval and is accounted separately below. Repeated query-only
reads have the same measured instruction count as the first successor read.

| Lifecycle | Observation | Baseline | Retained | Change |
| --- | --- | ---: | ---: | ---: |
| Direct publication | First/repeated successor read | 34,297,419 | 2,027,468 | −94.09% |
| Direct publication | Read after restart, before updates | 35,309,890 | 2,023,462 | −94.27% |
| Restart before publication | First/repeated successor read | 34,392,132 | 1,946,007 | −94.34% |
| Restart before publication | Read after final restart, before updates | 35,399,967 | 1,942,418 | −94.51% |

Already-warm observations are mixed, not uniformly cheaper. The direct
post-update read changes from 2,070,171 to 2,231,982 instructions (+7.82%);
its final restarted warm read changes from 1,899,915 to 2,059,861 (+8.42%).
The restart-before-publication final warm read changes from 1,980,523 to
1,980,213. These are measured whole-query intervals, not an attribution of every
instruction difference to cache preparation. The retained benefit is avoiding
repeated cold compilation, not a general warm-query optimization.

## Replicated Cycle Cost

Cycle balance differences cover the complete named host-call envelope, including
delivered watchdog work. They are not isolated compiler instruction counts and
must not be added to query instructions as if the units were interchangeable.

| Direct-publication envelope | Baseline cycles | Retained cycles | Delta |
| --- | ---: | ---: | ---: |
| Source-to-successor upgrade and watchdog | 52,286,554,661 | 52,287,880,478 | +1,325,817 |
| First successor read delivery | 457,258,951 | 488,993,873 | +31,734,922 |
| Post-publication restart and watchdog | 20,376,151,792 | 20,428,895,024 | +52,743,232 |
| Exact command retry after restart | 152,173,085 | 135,118,973 | −17,054,112 |

The restart-before-publication case similarly adds 31,689,691 cycles to first
successor read delivery and 51,057,055 to post-publication restart/watchdog.
Its extra pre-publication restart changes from 20,345,028,784 to 20,345,166,884
cycles. `Advance` and its immediate retry are unchanged in both cases:
405,883,032 / 132,678,546 cycles for direct publication and
405,865,307 / 132,490,838 with the preceding restart. Repeated query-only delivery
reports zero canister cycle-balance change in both variants; that is not a claim
that query execution consumes zero instructions.

## Validation And Complexity

Both matched rename cases preserve rows, relations, rejected delete behavior,
ordinary updates and exact terminal-command replay. The retained code also
uses the existing typed startup-failure and atomic accepted-root tests.
The broader focused migration target qualifies physical-migration controls as
well as the metadata-only rename; full release validation remains user-owned.

Production grows by five driver lines and one clarified enum documentation
line. Measurement code adds 14 net test lines. This adds one call and failure
handoff to an existing flow, not a parallel execution system. The documentation
maintenance cleanup removes substantially more duplicated prose/source scans
than it adds. Temporary PocketIC fixtures were used; no persistent network was
restarted. No wall-clock performance measures were used.
