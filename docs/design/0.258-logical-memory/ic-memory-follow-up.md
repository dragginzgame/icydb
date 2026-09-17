# Upstream issue: recovered-metadata admission within bootstrap

Publication status: filed as [ic-memory #5](https://github.com/dragginzgame/ic-memory/issues/5)
on 2026-09-17 after confirming IcyDB and the referenced source are public.

Dependency disposition: released in `ic-memory 0.14.1` through the existing
bootstrap policy's `prepare_bootstrap` hook and bounded `BootstrapAdmission`.
IcyDB role/identity policy and generated end-to-end qualification are complete;
the [status tracker](0.258-status.md) records the executed checks, coverage limits
and subsequent adoption of `ic-memory 0.14.2`. Full release validation remains
user-owned; this note does not claim the GitHub issue has been closed.

Target: `dragginzgame/ic-memory` (public GitHub repository).
Title: **Design recovered-metadata admission within bootstrap for automatic historical declaration completion**.

The archived issue body starts below. Its original requests and unchecked
acceptance checklist are retained as filed, not as the current delivery status.

---

## Outcome

Design a small, bounded way for a consumer to admit and complete declarations
from recovered allocation metadata inside the existing recover → resolve →
validate → commit flow, before allocation-open capabilities are published.

Follow-up to #4, using key-only placement from #2 and recovery limits from #3.
This is the remaining IcyDB 0.258 integration prerequisite, not a request to
replace the allocator or weaken historical-open restrictions. Review the
contract and an IcyDB-shaped example before implementing a new public surface.

## Confirmed evidence on 0.14.0

Baseline: release `33a28e15598fb63d3e7b426abb37d4a0d474dfd8`, registry checksum
`de56a3830c6506e0498cc8900a8a45d01a60057a419f51479e8a880e00e2caf4`.

A Rust 1.98.1 probe used production `MemoryRuntime<VectorMemory>` with explicit
grants for IDs 100–110:

1. Commit control A and journal B; write a debt marker to B. Recreate with only A:
   opening B returns `StableKeyNotCommitted`.
2. Recreate with A and B explicitly declared before bootstrap: B opens with its
   original marker intact.
3. Change namespace/authority and supply a matching new host grant: the new
   control key receives fresh slot 102 with zero pages; old slots remain claimed.

These results confirm the documented contract, not data loss or a policy bypass.
Without a matching new grant, the changed authority rejects.

IcyDB's ordering gap is source-traced: generated memory bootstrap commits
declarations first; database convergence then opens commit-control storage and
discovers omitted journal keys from the persisted store registry. Debt checks
require those journals. The generator has no independently retained historical
journal manifest. Its unit-test memory substitute does not prove the production
bootstrap sequence.

## Why current alternatives are insufficient

- The explicit-manifest contract is safe, but assumes historical keys are known
  before sealing.
- Default doctor reports seal declarations before reporting; ordinary ledger
  export requires bootstrap.
- Using a diagnostic DTO or another raw manager as allocation authority creates
  an independent recovery flow.
- Requiring applications to maintain removed-store lists adds manual configuration;
  silently disabling store removal changes the application contract.

## Requested contract

Choose the smallest extension at the existing bootstrap owner. API names and
callback/trait shape are deliberately unspecified.

- Supply bounded, validated recovered metadata for consumer identity admission
  and declaration completion before commitment. Metadata grants no memory access.
- Permit explicitly selected known-only historical keys in the final request set
  under current host authority/policy. Unknown or retired selections must reject,
  not become fresh allocations. Do not admit all historical allocations automatically.
- Let consumer policy reject a disallowed owner/key-set transition before advancing
  an existing ledger or publishing capabilities. Namespace/store intent remains
  IcyDB/host policy; ic-memory cannot infer whether a new key is a typo.
- Preserve one recovery authority, one resolved snapshot and one commit boundary.
  Final requests still undergo collision, range, retirement and bound checks.
- Cover default and explicitly owned runtimes. Explain how generated libraries
  participate in host-owned bootstrap without replacing host policy/profile,
  replaying admission on warm adoption, or adding another commit.
- Define rejection/retry and already-bootstrapped behavior, including whether an
  empty initial runtime can acquire its bootstrap root before admission. Existing
  committed mappings must remain unchanged on rejection.

If recovered allocation metadata is insufficient without additional persistent
identity state, report that limitation and alternatives before implementing it.

## Acceptance evidence

- [ ] An IcyDB-shaped example discovers authorized historical journal-role keys
  from recovered metadata, completes declarations before one commit, and opens
  journals only afterward. No unopened control-store reads or external manifest.
- [ ] Production-runtime tests preserve B's marker, reject foreign/revoked/unknown/
  retired selections, and leave existing mappings unchanged after rejection.
- [ ] Consumer-policy tests distinguish allowed additions from disallowed owner/
  key-set replacement; no claimed automatic typo detection.
- [ ] Journal-debt and pending-commit checks remain consumer responsibilities;
  the API never retires a database or clears its data.
- [ ] Corrupt history, exhaustion, failed persistence and interrupted/retried
  bootstrap fail closed without a partial capability.
- [ ] Warm host adoption preserves policy/profile without another initialization path.
- [ ] Document bounded work and files/lines/flow complexity. Measure raw Wasm and
  IC instructions/cycles where available, otherwise say unmeasured. No native timing.

## Scope control

Need: persisted identity is available too late for declaration completion.
Simplest alternative: explicit retained declarations, safe but insufficient for
automatic generated reconciliation. Canonical owners: ic-memory for allocation
evidence and commitment; IcyDB for database identity and lifecycle.

Intended state-space delta: one bounded preparation/admission step, not another
ledger, persisted mode, arbitrary historical-open API or budget framework.

No allocator-strategy configuration, migration, reclamation, journal interpretation,
pre-commit handles, compatibility bridge or automatic application changes.
Pre-1.0 changes remain hard cuts; versioned integration formats stay at version 1.

## Public source references

- [Generated memory bootstrap](https://github.com/dragginzgame/icydb/blob/f3bd969be9dd7087a0c00e2655d703c397756616/crates/icydb-model/src/build/actor/db/store.rs#L766-L809)
- [Persisted store reconciliation](https://github.com/dragginzgame/icydb/blob/f3bd969be9dd7087a0c00e2655d703c397756616/crates/icydb-core/src/db/database_format/convergence.rs#L43-L86)
- [ic-memory 0.14.0 integration contract](https://github.com/dragginzgame/ic-memory/blob/33a28e15598fb63d3e7b426abb37d4a0d474dfd8/docs/key-only-recovery.md)
