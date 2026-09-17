# Lifecycle instruction-ceiling investigation

Date: 2026-09-17. The user chose to retain the 12,750,000-instruction ceiling.
Verdict: a small upstream encoding prototype passes that ceiling; published
`ic-memory 0.14.2` still fails it. The prototype is not adopted by IcyDB.

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

This changes the outer persisted CBOR representation and needs an explicitly
qualified pre-1.0 hard cut, retaining format version 1 and requiring recreation
of earlier data. Do not ship the prototype as an IcyDB patch override. The
upstream owner must qualify malformed/oversized byte strings, current-form
fixtures, checksum/slot recovery, interrupted/refused writes and public DTO
serialization implications before publication. The experiment does not replace
that qualification, and does not prove how much of the original regression was
introduced by each earlier dependency change.

The workspace override, probes and all temporary lockfile changes were removed;
IcyDB still resolves published 0.14.2. The previous release failure therefore
remains a blocker until an upstream fix is released and adopted. Temporary native
lock resolution changed host-only edges during probing; no such change is retained.
Local PocketIC instances were used; no application network or database was changed.
