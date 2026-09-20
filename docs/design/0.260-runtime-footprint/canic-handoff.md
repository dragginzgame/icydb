# Canic handoff: ICYDB-033 composed reachability attribution

Delivered and [reconciled](w1-reconciliation.md) on 2026-09-20. The original
request below is satisfied for its six controlled subjects; full application
attribution remains open. No further Canic fixture extension is pending for W1.

Please extend the existing `canisters/audit/icydb_composed` fixture and
`crates/canic-host/examples/icydb_composed_audit.rs` for IcyDB's authorised
0.260 attribution work. Keep all changes in Canic; Toko Miner and IcyDB remain
read-only references. Do not change production pins, versions or release state.

## Starting evidence

Read-only preflight on 2026-09-19 at Canic
`522ea1894e9adaa7489baffd54537bad5207e1b5`: fixture pin IcyDB 0.259.6;
runner variants only `host` and `participant`; empty schema. The earlier
1,423,492 raw-byte / 1,387,426 code-byte participant increment measures IcyDB
0.257.9. It is not a current baseline or removable-byte target. The later
startup-outlining experiment produced identical binaries and was discarded.

## One bounded outcome

Freeze one source snapshot, dependency lock, Rust 1.98.1 and the maintained
canonical post-link settings. Extend the existing controls into these six
matched subjects, all under the existing build/retention owner:

1. Canic host only.
2. Host plus empty metrics-enabled IcyDB participant.
3. One generated entity with its typed binding reachable.
4. The same entity with a fixed typed page query reachable.
5. The same entity with that query and typed insert reachable.
6. Ten matching entities with the same operations reachable for each.

Use the current exact IcyDB pin consistently; if any dependency/source changes,
refreeze all compared inputs. Keep SQL off, metrics on for all participants,
the existing synchronous lifecycle owner, memory profile and export policy.
Ensure intended operations really remain reachable after optimisation. Reuse
the existing schema/build machinery; no runtime feature or new measurement
framework is requested. Fixture feature selections are measurement subjects,
not proposed product configuration.

## Return evidence

- Compiler/final artifact hashes, raw non-gzipped Wasm bytes, code/data section
  bytes, defined functions and optional gzip context for each subject.
- Exact source inputs (including relevant dirty/untracked files), lock identity,
  features, profile, tool versions and finalisation settings. Keep artifact
  owners alive through reads; serialize variants if their build outputs are
  mutable. Do not add a second cache, lock or persisted receipt protocol.
- Adjacent comparisons separating fixed participant, binding, query, write
  and entity-scaling increments. Do not add overlapping retained symbol sizes
  or treat synthetic one-to-ten growth as a universal per-entity constant.
- Named diagnostic attribution to one concrete owner worth investigating, or
  an explicit no-proven-reduction conclusion. No runtime optimisation is part
  of this task; do not remove recovery, metrics, types or constraints.
- Focused compilation/build qualification and any semantic checks actually
  executed, with unexecuted runtime/lifecycle checks clearly distinguished.
  No wall-clock/native timing measurements; cycle/instruction measurements
  remain unmeasured unless actually collected. No deployment is requested.

State that these are controlled subjects, not complete Toko Miner qualification.
The full ICYDB-033 application attribution remains open until separately
qualified or explicitly narrowed by the maintainer. Return results to IcyDB's
`docs/design/0.260-runtime-footprint/0.260-status.md` owner; do not edit that
sibling file from Canic.
