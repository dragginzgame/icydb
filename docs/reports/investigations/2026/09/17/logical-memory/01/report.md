# 0.258 — Bootstrap identity and omitted-journal preflight

Date: 2026-09-17. IcyDB baseline `f3bd969be` plus the pending registry dependency
update to `ic-memory 0.14.0`, checksum
`de56a3830c6506e0498cc8900a8a45d01a60057a419f51479e8a880e00e2caf4`.
This completes the investigation part of L2, not its admission-contract decision.

## Executed evidence

The [driver](driver.rs) ran as a temporary `icydb` Cargo example with the workspace
lockfile, offline, on Rust 1.98.1. It used `ic-memory`'s production
`MemoryRuntime<VectorMemory>` rather than IcyDB's test-only memory lookup.
The temporary example was moved here after execution; no runtime code changed.
To reproduce, temporarily place this source under `crates/icydb/examples/` and
run that example with the maintained locked dependency graph.

All three assertions passed:

| Sequence | Observed result |
| --- | --- |
| Commit control A and journal B, write `debt` to B, recreate runtime with only A | Opening B rejects with `StableKeyNotCommitted`. |
| Recreate again, explicitly declaring A and B before bootstrap | B opens with its original `debt` marker intact. |
| Change the owner/namespace and grant its new authority the same pool | A new control key receives fresh slot 102 and opens at zero pages; the old slots remain claimed. |

These are functional boundary probes, not performance measurements or a complete
generated-actor upgrade test. The namespace result requires a newly matching
host grant: an unchanged grant for the prior authority would reject instead.
It demonstrates that keys plus grants do not distinguish a typo from an
intentional new database. It does not demonstrate overwritten or lost data.

## Why current generated IcyDB cannot supply the journal list automatically

1. `icydb-model/src/build/actor/db/store.rs::ensure_memory_bootstrap` commits the
   memory declarations before constructing the database session/store registry.
2. `icydb-core/src/db/database_format/convergence.rs::ensure_current_convergence_format`
   reads the persisted store registry from the opened commit-control memory.
3. `reconcile_current_registry` needs each omitted journal to check debt, including
   retained retired entries. The production `store_memory_owned` opens through
   current committed `ic-memory` authority; its unit-test substitute does not.
4. Neither the generated declaration list nor the current canister model contains
   a separately retained manifest of previously removed journal keys.

The dependency's default doctor helper seals declarations before producing its
report. Its ordinary ledger export requires bootstrap. Using a diagnostic DTO
or a separately constructed raw manager as allocation authority would introduce
a second recovery/authority flow and is not an acceptable workaround.

## Decision required before L3

The published explicit-manifest contract is safe but insufficient for automatic
IcyDB reconciliation as currently generated. There are two honest directions:

- **Recommended:** request an upstream design for bounded recovered-metadata
  admission/declaration completion inside the existing recover → resolve →
  validate → commit boundary. It must support explicit current host policy,
  known-only historical requests and rejecting unintended owner-set changes
  before commitment. No pre-commit memory handles, second ledger, second commit
  or arbitrary historical opens. The precise API and sufficiency for IcyDB must
  be reviewed, not assumed. IcyDB retains namespace/store intent and journal checks.
- Require an independently supplied historical journal manifest, or restrict
  store removal/identity changes. Both change the intended application contract;
  neither is silently adopted here. A forever-maintained application list works
  against the goal of reducing manual storage configuration.

Demonstrated need: persisted identity becomes available after the current static
declaration boundary. Simplest alternative: explicit retained declarations.
Canonical owner: `ic-memory` for recovered allocation evidence and commitment;
IcyDB for database identity/lifecycle admission. Proposed state-space delta:
one bounded preparation step in the existing bootstrap, not a persistent mode,
new identity ledger or independent open route. No such API is implemented here.

The namespace/store replacement rule still needs an explicit contract: new
identities are not provably typos. Any fail-closed policy must distinguish allowed
additions and separately authorized removal/replacement, not claim inference of
application intent. Until the owner-set evidence is available at the correct
boundary, this rule cannot be safely implemented by the macro alone.

Verdict: retain the qualified 0.14.0 update; **L2 remains open and L3 does not
start**. Seek approval for the recommended upstream design follow-up rather
than introduce an IcyDB workaround. Wasm/cycle/instruction deltas are unmeasured;
this handoff changes documentation/evidence only.
