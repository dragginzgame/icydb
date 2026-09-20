# W1 — Canic evidence reconciliation

Date: 2026-09-20. **W1 is complete for the six controlled composition subjects.**
The Canic prerequisite is delivered. W2 remains a separate, conditional slice;
no safe reduction or application-wide ICYDB-033 closure is established.

## Evidence and identity

Canic owns the [returned report](../../../../canic/docs/audits/working/icydb033-composed-wasm/w1.md)
and [structured evidence](../../../../canic/docs/audits/working/icydb033-composed-wasm/w1-measurements.json).
The original [handoff](canic-handoff.md) defines the comparison contract.

- Canic base: `eaace2777e9dfb4601c1af86f609c946a25dbc5d` (0.110.29), plus
  the archived dirty/untracked source. The fixture extension is uncommitted;
  HEAD alone is not the measured source identity.
- Frozen source-map SHA-256:
  `ee3558e2c6539d2aada53de4c71cde5679c0c953fecdc1b30d31e8de44336236`.
- Returned evidence JSON SHA-256:
  `9a5ad7ce9cb9801e82554d02d9c75408737aca606d4130dd8572155f7d2641b7`.
- All subjects use published IcyDB 0.259.6 and ic-memory 0.14.3. They do not
  contain IcyDB's dirty 0.260 C1/R1/D1 changes. The standalone lock synchronises
  Canic path packages to .29; it is not IcyDB's workspace lock.
- Rust 1.98.1, Release z/LTO/one codegen unit, panic abort, overflow checks off,
  compiler symbols retained; canonical ic-wasm 0.11.1 shrink and Binaryen 132
  `-Oz`, sidecar-only Candid. SQL off, participant metrics on, 16-page memory
  buckets, admitted IcyDB memory range 100–106 and synchronous Canic lifecycle.
- Retained artifacts and source archive:
  `/home/adam/projects/canic/target/icydb033-w1/evidence`. These are local audit
  inputs, not published release qualification.

## Contract reconciliation

The runner and feature chain select host, empty participant, one binding,
one page, one page plus insert, and ten matching entities with all operations.
The entities share the same two Nat64 fields and journaled store. Fixed audit
wire adapters exist in every subject, including the host; operation inputs are
ingress values so the selected operations remain reachable.

The page uses the existing typed binding and prepared cursor's **trusted**
page with limit 16, followed by generated row decoding. The insert uses the
trusted typed-write owner and decodes its returned row. This satisfies the
fixed typed-page/write reachability contract; it is not a public-read admission
comparison or a populated Toko Miner workload. Matching simple entity growth
cannot predict richer schemas, indexes, relations or payloads.

IcyDB independently checked, without rebuilding or executing the actors:

- Hashes and byte lengths for 83 retained artifacts, including compiler/final
  and named Wasm, Candid, feature trees, mapping/reachability records, source
  archive, build/lint logs and tool records.
- All twelve compiler/final raw sizes, code/data section sizes and defined
  function counts, parsed directly from the retained Wasm bytes.
- All five adjacent raw/code/data/function deltas and byte equality of the
  five participant Candid sidecars.
- The nine fixture/runner source hashes plus Canic's root manifest and lock:
  eleven current inputs match the frozen record. Feature trees retain metrics
  in participant cases and no IcyDB SQL feature.

Canic's retained mapping records report a bijection and exact body matches for
every defined function in each named/canonical pair, with no failures; its
reachability records identify the intended binding/page/insert paths, including
all ten entities. IcyDB inspected and hash-checked these records, not reran the
body-mapping algorithm. Canic reports six successful canonical builds and scoped
Clippy checks; those builds/lints were not repeated during this reconciliation.

## Reconciled raw Wasm measurements

| Subject | Raw bytes | Code bytes | Data bytes | Defined functions | Adjacent raw delta |
| --- | ---: | ---: | ---: | ---: | ---: |
| Host | 2,920,668 | 2,698,385 | 213,351 | 4,753 | — |
| Empty participant | 4,337,881 | 4,076,781 | 248,326 | 7,897 | +1,417,213 |
| One binding | 4,377,586 | 4,115,867 | 248,814 | 8,002 | +39,705 |
| One page | 5,270,399 | 4,979,428 | 275,414 | 10,061 | +892,813 |
| One page and insert | 5,386,411 | 5,091,931 | 278,557 | 10,288 | +116,012 |
| Ten entities, same operations | 5,394,613 | 5,099,019 | 279,650 | 10,309 | +8,202 |

These are adjacent reachability increments under one matched build contract,
not isolated additive ownership partitions or savings. The earlier 0.257.9
empty pair and IcyDB's differently configured catalogue actors are not valid
subtraction baselines. No instruction/cycle deltas were measured; runtime,
duplicate-insert, invalid-slot and lifecycle/recovery behavior was not executed.

## W2 handoff and exclusions

The first page is the largest operation increment. Its named diagnostic points
to the existing
[`execute_structural_projection_page`](../../../crates/icydb-core/src/db/executor/projection/facade.rs)
owner and prepared page route. The named closure has 43,282 shallow / 330,541
retained bytes; the audit-page export retains 788,181 bytes. Those retained
figures overlap and are not removable-byte estimates. Reachable predicate and
covering-scan machinery warrants source inspection; reachability alone does not
prove that the fixed query executes those branches or that they can be removed.

W2 should first establish one concrete simplification at this owner and a
matched counterfactual using the existing measurement owners. Retain a change
only with a raw-Wasm reduction and relevant semantic and IC instruction/cycle
evidence; otherwise record no-build. Do not add a second route, remove recovery,
weaken accepted-schema authority or repeat the discarded startup-outlining
experiment. W2 implementation and measurements have not started in this slice.

Full ICYDB-033 application attribution remains open. Final 0.260 closeout still
needs W2's disposition and reconciliation of current-candidate qualification;
the existing IcyDB lockfile churn and D1 are not measured by these artifacts.
This reconciliation changes documentation only, with no runtime state-space
change, sibling edits, version changes, network lifecycle actions or new build
framework. Full repository validation and publication remain user-owned.

Documentation validation: all 24 local links in the five detailed documents
resolve and the diff whitespace check passes. Six documentation files change,
approximately +125 net lines; runtime implementation shape is unchanged.
