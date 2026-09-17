# Lifecycle instruction-ceiling investigation

Date: 2026-09-17. The user chose to retain the 12,750,000-instruction ceiling.
Verdict: published `ic-memory 0.14.3` now passes that ceiling in IcyDB. The
prototype below is historical evidence; no local dependency override is adopted.

## Cause and experiment

`CommittedGenerationBytes::payload` in `ic-memory::physical` is an already-encoded
ledger byte vector. Derived serde encodes it again as a CBOR integer array inside
the stable-cell record. The outer codec and bounded syntax walk therefore visit
each byte as a CBOR value. The fallible cell preflight and `Cell::init` both
decode that record; capacity admission and `Cell::set` both encode it. Those
checks cannot simply be removed: they protect typed recovery and safe writes.

Temporary IC instruction probes localized 14,485,757 instructions to bootstrap
and 10,869 to IcyDB declaration adoption in the converged upgrade. A separate
instrumented dependency run attributed 6,864,703 instructions to cell loading and
3,533,755 to persistence, versus 539,221 to resolution. Instrumentation perturbs
code generation: those phase samples are localization evidence, not additive
measurements of the uninstrumented release artifact.

The [experimental patch](opaque-payload-prototype.patch) stores the opaque payload
as a CBOR byte string with a byte-string-only bounded visitor. It preserves the
logical payload, checksum, dual slots, generation history, recovery limits and
commit flow. All probes were removed before the candidate qualification run.
Only a disposable copy of registry 0.14.2 was changed; no upstream checkout was
edited. No compatibility decoder, alternative runtime mode or cache was added.

## Results

Same maintained lifecycle fixture and test sequence, Rust 1.98.1, PocketIC 16,
existing local/production build profiles and canonical post-link pipeline.
The published run is the retained release-failure log; candidate results come
from all three lifecycle tests against the temporary dependency override.

| Phase | Published 0.14.2 instructions | Byte-string prototype |
| --- | ---: | ---: |
| Init | 5,756,243 | 4,170,766 |
| Empty post-upgrade | 11,217,284 | 4,989,358 |
| Populated post-upgrade | 14,473,114 | 5,086,348 |
| Converged post-upgrade | 14,985,588 | 5,127,761 |

Worst-phase reduction: 9,857,827 instructions (65.782%). All three lifecycle
tests pass without raising the ceiling, including rollback/retry, ingress
surface, synchronous ordering, deferred activation and retained-row checks.
Twelve memory-admission tests and the default-manager test also pass. Allocated
stable extent remains 23,134,208 bytes in the lifecycle fixture. Raw Wasm and
cycle deltas were not measured; no native timing is used.

## Required upstream follow-up

Filed as [ic-memory #7](https://github.com/dragginzgame/ic-memory/issues/7),
including the measurements, prototype and safety qualification requirements.

This changes the outer persisted CBOR representation and needs an explicitly
qualified pre-1.0 hard cut, retaining format version 1 and requiring recreation
of earlier data. Do not ship the prototype as an IcyDB patch override. The
upstream owner must qualify malformed/oversized byte strings, current-form
fixtures, checksum/slot recovery, interrupted/refused writes and public DTO
serialization implications before publication. The experiment does not replace
that qualification, and does not prove how much of the original regression was
introduced by each earlier dependency change.

After the experiment, the workspace override, probes and temporary lockfile
changes were removed, restoring published 0.14.2 pending upstream release. Temporary native
lock resolution changed host-only edges during probing; no such change is retained.
Local PocketIC instances were used; no application network or database was changed.

## Published 0.14.3 adoption

IcyDB now resolves registry 0.14.3, checksum
`b9368f37df84f896d04da5b980e0e038302c4da24892f89e00124c6d7cb953c7`.
Only that package's version/checksum changed in Cargo.lock. The published codec
uses an owned bounded byte-string visitor, allocation-free length preflight and
human-readable DTO array support. Earlier stable data must be recreated; the
current format remains version 1 with no predecessor reader.

Measured with Rust 1.98.1, PocketIC 16 and the unchanged maintained lifecycle
fixture/build profiles; baseline is the retained 0.14.2 release-failure run.

| Phase | Published 0.14.2 instructions | Published 0.14.3 instructions |
| --- | ---: | ---: |
| Init | 5,756,243 | 4,170,116 |
| Empty post-upgrade | 11,217,284 | 4,992,970 |
| Populated post-upgrade | 14,473,114 | 5,089,747 |
| Converged post-upgrade | 14,985,588 | 5,131,230 |

Worst phase falls 9,854,358 instructions (65.759%) and passes the unchanged
12,750,000 ceiling. Stable extent remains 23,134,208 bytes. Consumer raw Wasm
and cycle deltas are unmeasured; no timing metric is substituted.

Lifecycle ordering/preservation and rollback/retry tests pass. The initial
combined target failed only its endpoint-surface test during post-link input
hashing with a missing-file error; that test passes when run alone. This suggests
a concurrent build-artifact lifetime issue, not a proven root cause or repaired
test harness. Investigate the shared Cargo-output to post-link handoff separately.
Filed as [ic-testkit #2](https://github.com/dragginzgame/ic-testkit/issues/2), with
pinned-source lifetime findings and deterministic concurrency/pruning acceptance
cases. The subsequent 0.10.0 adoption is recorded below.
All 12 memory-admission tests and the default-manager test pass; focused lint,
formatting and dependency/memory/durability invariants pass. The instruction
blocker is resolved; full release validation remains user-owned. A disposable
local PocketIC server was started; no application database/network was changed.

IcyDB execution shape is unchanged: dependency and documentation edits only,
without a local codec, new runtime state, compatibility path or raised limit.

## Retained build-artifact adoption

Published `ic-testkit 0.10.0` gives build/cache records ownership of exact read-only
artifacts through their last clone's lifetime. IcyDB retains those records across
single/batch post-link processing, then through caller reads and staging. Batch
summaries preserve records rather than only successful indexes; public integration
build helpers return `BuiltCanisterArtifacts` rather than ownerless paths.

Configured paths are only mutable publication destinations. Consumers read the
upstream-owned exact artifacts, without reconstructing private cache paths,
adding locks, disabling pruning or retrying races. Regression coverage exercises
warm acquisition, concurrent destination replacement/removal, pruning and eventual
release, plus retained successes in a partially failed post-link batch.
All five focused cache tests and all three concurrent lifecycle tests pass against
the published dependency. All four lifecycle instruction samples above are
unchanged, including the 5,131,230 worst phase and 23,134,208 stable bytes.
Raw Wasm/cycle deltas for this host-side harness change are unmeasured.
Repository lint, affected-target compilation, formatting, shell lint and
post-link/dependency invariants pass. The initial underscore-field lint finding
was repaired before these final checks. No production execution flow is changed.

A disposable local PocketIC server was started for this qualification. The
whole-fleet artifact target and full release suite remain user-owned validation;
the exact original missing-file interleaving has not been reproduced. Ownership
is now explicit in the existing flow, with no additional cache or locking owner.
