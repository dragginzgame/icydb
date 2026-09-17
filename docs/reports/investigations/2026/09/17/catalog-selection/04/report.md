# Borrowed fingerprint normalization — allocation experiment

Date: 2026-09-17. Original experiment was isolated; implementation status follows.

## Implementation follow-through — 2026-09-17

The borrowed encoding is now implemented. The raw runtime fingerprint requires
a valid original snapshot, including a nonzero declared version, before encoding
the canonical hash version. Invalid zero-version input returns the existing
store-invariant category. This is not a change to valid hash bytes, accepted
schema versions, persisted encoding or admission-name normalization.

Current-format projection tests cover complete temporal, check, candidate-index,
identity, relation and expression-index fixtures at versions 1, 7 and u32::MAX.
All 158 focused test runs pass: 42 codec tests with all features, the same 42
SQL-free, 51 schema-store tests and 23 index-domain tests. The first test build's
incorrect import was corrected before the successful runs. Repository Clippy,
formatting and invariants also pass; full-suite validation remains user-owned.

The landed audit actor was rebuilt with the same toolchain/profile/post-link
flags and is **byte-identical** to the experimental candidate below: SHA-256
`b5dc543fae61164b771a385b279f8a77d39343f8a69df71938d0b34d677bb773`.
The archived complete-query/cycle results therefore apply to the landed actor;
the cold-selection deltas below remain the prior isolated-probe qualification.
No PocketIC network changes or repeat query measurements were needed.

The experiment and its original implementation prerequisites below are retained
as historical evidence; the source and active status tracker own the current
completion verdict. The 192-KiB allocated-heap increase remains unresolved.

## Recommendation

Remove the runtime fingerprint's temporary deep schema copy in a focused follow-up.
The experiment reduces cold entity-selection instructions by 1.3–7.1%, with
essentially flat complete warmed-query instructions and charged update cycles.
It does **not** recover the additional 192 KiB of allocated heap or explain the
small complete-query regression recorded in [receipt 03](../03/report.md).

The current fingerprint builder reconstructs a complete owned schema solely to
normalize the declared schema version to one. The experiment instead passes
that scalar to the existing bounded encoder. Field, layout, constraint, relation
and index collections remain borrowed. One encoding traversal remains; there is
no new cache, configuration, persisted form or runtime execution route.

This experiment only changes runtime/commit fingerprint normalization. Admission
fingerprints also normalize generated index names and retain their existing
owned path; converting those is not required to land this narrower improvement.

## Measured result

The baseline is the current shared-snapshot handoff, **not** published 0.257.20.
The baseline selection samples are reused from [receipt 01](../01/candidate.csv).
The same SQL-free selection probe and runner supply 27 new candidate samples;
all three repeats per fixture agree, including snapshot and identity assertions.

| Schema | Baseline cold instructions | Candidate | Change |
| --- | ---: | ---: | ---: |
| 2 scalar fields | 149,403 | 138,882 | −7.042% |
| 17 scalar fields | 615,583 | 574,791 | −6.627% |
| 65 scalar fields | 4,294,063 | 4,149,537 | −3.366% |
| 255 scalar fields | 40,136,601 | 39,600,433 | −1.336% |
| 255 scalar fields, long names | 43,532,026 | 42,853,844 | −1.558% |
| 2 fields, depth-10 composite reference | 149,333 | 139,326 | −6.701% |
| 2 fields, nested record leaf | 161,290 | 149,801 | −7.123% |

All warm single-selection counts are unchanged. Four warm selections are
unchanged except the nested-record sample (+1 instruction). These intervals
exclude initial fixture construction, publication and bundle loading, so they
do not measure complete startup. [All candidate selection samples](selection.csv).

Complete warmed-query instructions use the unchanged six-query fixture and
four-epoch query-statistics protocol from receipt 03, with 624 executed queries
and 468 reported calls. Every result matches its warm-up and the other artifact.

| Query | Baseline per call | Candidate | Change |
| --- | ---: | ---: | ---: |
| Primary key | 8,525,120 | 8,525,818 | +0.0082% |
| Indexed equality | 9,298,660 | 9,298,421 | −0.0026% |
| Indexed range | 9,241,839 | 9,242,315 | +0.0052% |
| Primary-key `IN` | 8,767,364 | 8,765,998 | −0.0156% |
| Count | 8,770,941 | 8,769,757 | −0.0135% |
| Grouped count | 9,143,476 | 9,139,572 | −0.0427% |

[Whole-query totals](whole-query.csv). Charged update-cycle deltas across the
18 paired updates range from −0.0482% to +0.0069%; [phase/cycle samples](phases.csv).
Local instruction intervals still move much more and must not replace the
complete-call result. No wall-clock performance measurements were used.

[Allocated memory](heap.csv) is identical before and throughout five sampled
primary-key query/update pairs: 4,784,128 Wasm heap bytes and 39,911,424 stable
bytes. Removing this temporary copy does not cross an allocation-page boundary
in the fixture. This is not a measurement of live retained bytes.

Raw maintained audit actor: **4,483,015 → 4,482,953 bytes (−62)**; defined
functions remain 10,490. Compiler-emitted bytes: 5,127,549 → 5,127,454.
Isolated selection probe: **564,371 → 563,764 bytes (−607)**, functions
1,500 → 1,496. Probe sizes do not predict application actor savings.

## Compatibility and next implementation boundary

The temporary native build asserts complete encoded-byte equality against the
existing owned normalization whenever the runtime fingerprint is exercised.
All 51 focused schema-store tests and 41 codec tests pass. These are experimental
qualification checks, not a replacement for permanent current-contract tests.

Before landing, explicitly settle zero declared-version behavior: the old
fingerprint normalizes it before validation, whereas this prototype validates
the original snapshot. Accepted versions are nonzero, but the raw fingerprint
helper also has schema-publication, recovery and mutation consumers. Do not
silently change their validation/error-precedence contract. Add direct rich
snapshot and valid-version identity coverage without retaining an old runtime
implementation. The existing persisted decoder and byte limits must stay intact.

The broader retained-snapshot duplication between the decoded bundle and entity
selection remains a separate investigation. Sharing at that boundary could have
a larger scope than removing this scratch copy; no such redesign was attempted.

## Reproduction and scope

Apply the [experimental diff](experiment.patch.txt) to the receipt-03 candidate
in a separate source copy. Build the audit actor with the exact Rust 1.98.1,
locked dependencies, profile and pinned Binaryen transform in receipt 02. Reuse
receipt 03's archived native probe unchanged. For cold selection, temporarily
wire receipt 01's schema fixture probe, using `selected.snapshot()`, and build
the SQL-free core as a Wasm cdylib with the same profile. Remove that wiring
before further actor builds. No unsafe production boundary was added.

Audit actor SHA-256: baseline
`052f2a36c9ab60643b11d958f79dff0fa54a404255330199d1969c6caf6cba61`;
candidate `b5dc543fae61164b771a385b279f8a77d39343f8a69df71938d0b34d677bb773`.

All three query/heap/cycle probes and the selection probe pass. Twenty-seven
disposable PocketIC fixtures were released; shared networks were untouched.
Temporary root test wiring was removed. Full-suite validation remains user-owned.
This handoff adds evidence and tracker notes only, with no production complexity
change. The experimental two-file runtime change replaces an owned normalization
handoff with a borrowed one; it is not yet a landed optimization.
