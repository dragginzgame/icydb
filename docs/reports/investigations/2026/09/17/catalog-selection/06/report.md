# Shared snapshot payload — isolated ownership preflight

Date: 2026-09-17. Baseline: current 0.257.21 candidate (shared accepted
selection plus borrowed runtime fingerprinting), not published 0.257.20.
The original preflight is recorded below; production landing confirmation
appears at the end of this receipt.

## Verdict

Recommend landing the shared payload at its existing snapshot owner. In this
fixture it removes retained duplication, reduces allocated Wasm memory and
shrinks raw Wasm. Five of six complete queries use fewer instructions; grouped
count is slightly higher. All paired warm updates use fewer charged cycles.
This is a small ownership change, not a new cache or execution flow.

The [prototype patch](prototype.patch) wraps the existing persisted snapshot
fields in one private `Rc` payload. Existing builders detach with
`Rc::make_mut` before mutation; accepted wrappers and decoded bundles share
unchanged payloads. Validation, encoding, runtime roots and normalized hash
ownership remain unchanged. There is no interior mutability or per-field
reference graph. Two cache-identity constructors lose `const` because snapshot
access now dereferences the shared payload.

Demonstrated need: the bundle-to-selection deep copy documented in
[receipt 05](../05/report.md). Simplest alternative: keep that copy. Canonical
owner: `PersistedSchemaSnapshot`. State-space delta: one private storage type,
no new mode, cache, configuration, authority, public API or persisted format.
The prototype changes three files: approximately +77 net lines including a
38-line test; production adds 39 lines across two files. Read-side ownership
is simpler; schema edits gain explicit standard-library copy-on-write handling.

## Complete-query and cycle measurements

Use the unchanged six-query fixture and [receipt 03 runner](../03/probe.rs.txt).
The baseline artifact is the byte-verified landed artifact from receipt 04.
Both artifacts use the same Rust 1.98.1, feature configuration and Binaryen 132
post-link flags. Uninstrumented artifacts supply every instruction/cycle result.
Query statistics report 39 calls per case/artifact; all 624 executed query
results match their warm-up and the opposite artifact.

| Query | Baseline instructions/call | Prototype | Change |
| --- | ---: | ---: | ---: |
| Primary key | 8,525,818 | 8,439,772 | −1.0092% |
| Indexed equality | 9,298,421 | 9,289,987 | −0.0907% |
| Indexed range | 9,242,315 | 9,158,555 | −0.9063% |
| Primary-key IN | 8,765,998 | 8,684,027 | −0.9351% |
| Count | 8,769,757 | 8,685,784 | −0.9575% |
| Grouped count | 9,139,572 | 9,141,699 | +0.0233% |

[Whole-query totals](whole-query.csv). Across 18 paired updates, charged cycles
fall 0.4587–1.4010%; [all phase/cycle samples](phases.csv). The harness brackets
each update with cycle balances after settling setup/deferred charges. Do not
substitute its narrower local counters for complete-query statistics.

This does not eliminate every earlier regression against published 0.257.20:
indexed equality remains about 0.89% higher in complete-query instructions,
and grouped count about 0.17% higher. Those are separate from the favorable
incremental comparison here. No general production-workload speedup is claimed.

## Memory and Wasm

The [same temporary allocator probe](../05/allocator-probe.rs.txt) and
[runner](../05/runner.rs.txt) measure requested payload in a separate build.
The allocator forwards to System unchanged and only observes successful Rust
global allocations. Counter limitations from receipt 05 still apply: these
are not allocator bookkeeping, stack, free-list or exact physical-live bytes.
Instrumented instructions/cycles are not used.

| Stage | Baseline live requested bytes | Prototype | Wasm pages, baseline → prototype |
| --- | ---: | ---: | ---: |
| Installed | 27,024 | 27,024 | 22 → 22 |
| Startup settled | 30,333 | 30,333 | 29 → 29 |
| Fixtures reset | 510,890 | 440,998 | 31 → 29 |
| Fixtures loaded | 658,488 | 588,596 | 73 → 70 |
| First warm update | 696,080 | 626,188 | 73 → 70 |
| Second through fifth warm updates | 710,597 | 640,705 | 73 → 70 |

After reset, live requested payload falls 69,892 bytes and live allocation
count falls by 1,121. Loaded lifetime peak requested bytes fall
2,703,663 → 2,633,771. Peak block count falls 18,099 → 16,718.
Cumulative successful allocation demand at the final sample falls
85,903,704 → 84,402,148 bytes. All 18 stage observations repeat exactly and ten
updates preserve results. [Complete allocation observations](allocations.csv).

The uninstrumented [heap probe](heap.csv) independently confirms
4,784,128 → 4,587,520 allocated Wasm bytes (−196,608 / three pages),
with stable memory unchanged at 39,911,424 bytes. It remains flat across five
sampled query/update pairs per artifact. This removes the earlier three-page
increase in this fixture; allocator thresholds do not guarantee the same
page saving in other applications.

| Uninstrumented artifact | Baseline | Prototype | Delta |
| --- | ---: | ---: | ---: |
| Raw post-link Wasm bytes | 4,482,953 | 4,478,067 | −4,886 |
| Defined Wasm functions | 10,490 | 10,487 | −3 |

## Reproduction and validation

Apply the archived patch to the current implementation containing receipt 04's
landed borrowed fingerprint encoder. Build the maintained SQL audit actor with
`wasm-release`, `wasm32-unknown-unknown` and
`test-admin-api,candid-export`; use the pinned Binaryen flags from receipt 04.
Reuse the linked runners, supplying baseline/candidate Wasm files in their
measurement directories. Add the allocator module only to the separate
instrumented actor build. Invalidate core/actor source mtimes when switching
source copies sharing a Cargo target directory, and freeze artifacts before
building the next variant.

SHA-256:

- Uninstrumented baseline:
  `b5dc543fae61164b771a385b279f8a77d39343f8a69df71938d0b34d677bb773`
- Uninstrumented prototype:
  `fd018a90dfafab265c639aca70700eebd5c8e575718cf439fdc0811a9fc2019b`
- Instrumented baseline:
  `4459c1a2b16a0b1235e122a26bc522a4973a84e26bd1a863cccf60681cd12dd9`
- Instrumented prototype:
  `8fbc65e32e3c29854eba1cdfe6bcb3e1bc50f2cea8d6d93f64982a3fb0dc9f0b`

The prototype passes 164 focused tests: 26 snapshot, 41 codec, 51 store and 46
mutation tests. The new test verifies initial sharing, detached version/catalog/
allocator edits, current byte round-trip and unchanged accepted bytes. Existing
tests cover failed promotion, root-keyed selection and journal/recovery selection.
Initial compiler errors identified the two required non-const callers; both
were fixed before these successful runs. SQL-free core checking also passes.

All four PocketIC probes pass. Their 28 disposable fixtures are released; no
shared network was restarted. Temporary root test modules and isolated actor
instrumentation were removed. Root core/actor mtimes were invalidated so a
subsequent root build cannot reuse the instrumented scratch artifact.
No wall-clock performance metrics were taken. Full release tests remain
user-owned; production Clippy and focused SQL-free tests belong to the landing
slice, not a claim established by this isolated preflight.

Next bounded slice: land this ownership change with maintained mutation-isolation
tests, run focused SQL-enabled/SQL-free gates and Clippy, and confirm its
uninstrumented artifact against this receipt. Keep broader catalog sharing,
allocator tuning and outstanding query-preparation work separate.

This preflight handoff changes nine evidence/documentation files, adding about
730 net lines, mostly the archived prototype patch and measurements. Production
runtime complexity is unchanged in this handoff. Formatting, diff checks and
the archived patch's non-mutating application check pass.

## Production landing confirmation

The shared payload is now implemented at the same owner. Production differs
from the prototype only in documentation; maintained tests also cover successful
unique promotion/abort while retaining the original accepted snapshot. The
shared fixture is reused for failed-promotion coverage. No instrumentation,
new cache, mutable shared payload or persisted-format change is landed.

The root audit actor was rebuilt using the same configuration and post-link
flags. Its optimized Wasm is byte-for-byte identical to the prototype:
4,478,067 bytes, 10,487 defined functions, SHA-256
`fd018a90dfafab265c639aca70700eebd5c8e575718cf439fdc0811a9fc2019b`.
The recorded uninstrumented whole-query instructions and update-cycle results
therefore apply to the landed artifact. Allocation-counter observations above
remain measurements of the isolated instrumented build. No additional local
network lifecycle actions were needed.

All 285 focused production test runs pass: 27 snapshot, 42 codec, 51 store and
46 mutation tests with all features; 26 snapshot, 42 codec and 51 store tests
with no default features. Full repository tests remain user-owned.

Repository Clippy, invariants, formatting and diff checks pass. This landing
changes eight source/test/documentation files with approximately 150 net added
lines, including 40 production lines. Read ownership is simpler; mutation uses
explicit standard-library copy-on-write rather than adding a second flow.
