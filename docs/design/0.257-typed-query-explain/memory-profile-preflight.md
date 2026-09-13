# Canister memory profiles — preflight

Status: requested on 2026-09-13; published `ic-memory 0.13.3` is adopted and
the IcyDB profile wiring is implemented in active 0.257.11. IcyDB-owned
initialization now defaults to 16-page buckets. Qualification is recorded below.

## One configuration owner

Add `memory_profile = "general"` to the existing `#[canister(...)]` schema
macro, with omission selecting `general`. Carry the selection through the
build-time `icydb_model::node::Canister` into generated memory bootstrap.
Do not add a second override to `start!`, per-store settings, endpoint arguments,
or fields in the accepted schema/catalog. This is physical canister configuration,
not query authority or a schema migration.

| Profile | Bucket pages | Bucket size | Shared manager capacity |
| --- | ---: | ---: | ---: |
| `compact` | 4 | 256 KiB | 8 GiB |
| `general` (default) | 16 | 1 MiB | 32 GiB |
| `high_headroom` | 128 | 8 MiB | 256 GiB |

Capacity includes every region using the manager, including indexes and journals;
it is not payload capacity or a platform guarantee. The
[experiment](../../reports/investigations/2026/09/13/bucket-sizing/01/report.md)
supports 16 pages as a starting point, not a universal optimum. Lifecycle and
mutation qualification accompany this default change; full-scale application
growth and aggregate background costs are not established by that experiment.

Need: ordinary applications should select measured sizing without writing their
own memory bootstrap policy. The simplest alternative is documentation around
the existing host-owned configuration; it does not give generated lifecycle
users a configurable default. State-space delta: three closed sizing choices
at one existing bootstrap boundary, with one mapping to upstream page counts;
no additional execution route, adaptive mode, or persisted profile tag.

## Bootstrap contract and prerequisite

Keep `db::bootstrap::ensure_default_memory_manager` as the integration owner.
For IcyDB-owned initialization, apply the selected setting before any operation
can construct the runtime. Reopening requires the persisted bucket size to
match: changing profiles requires recreation/reinstall, not resizing or fallback.
Changing the omitted default from 128 to 16 therefore also affects existing
IcyDB-owned deployments; selecting `high_headroom` explicitly retains 128.

For a host that already bootstrapped the shared runtime, preserve the maintained
adoption contract: validate IcyDB's committed declarations without reasserting
the host's policy. The host owns bucket size in this case; the schema profile
is an IcyDB-bootstrap setting, not an override. Allocation diagnostics report
the effective persisted size. Document this beside the macro setting.

Initial `ic-memory 0.13.2` source inspection established two API gaps, now
resolved by the published 0.13.3 dependency:

- `committed_allocations()` and `is_default_memory_manager_bootstrapped()` both
  construct an absent runtime using the upstream default (128 pages). Neither
  can safely precede configured initialization of fresh memory.
- `bootstrap_default_memory_manager_with_config` accepts an explicit policy;
  upstream's generic range policy is private. Calling it also reasserts policy
  identity, so it cannot replace adoption of an already committed host runtime.

The upstream prerequisite is a nonconstructing committed-capability lookup and
a supported way to bootstrap with a bucket configuration under its existing
generic policy. Final API shape belongs to `ic-memory`; converge on its existing
bootstrap implementation. Do not copy that policy into IcyDB, use a physical
allocation report as a lifecycle probe, or add a second runtime.

## One implementation handoff after the prerequisite

Wire macro parsing, model configuration and generated bootstrap together, with
unknown-profile rejection and omission/default coverage. Test fresh settings,
matching reopen, mismatched reopen rejecting before writes, repeated startup,
and adoption of a host runtime with a different policy and bucket size. Exercise
generated lifecycle, normal writes/updates/deletes and recovery using the default.
Measure matched raw Wasm and IC instructions; do not substitute native timings.
Include user guidance and active release notes in that same handoff.

## Upstream prerequisite handoff — 2026-09-13

The user authorized the sibling-repository change. Existing default-runtime
status and capability lookups now share nonconstructing access with allocation
diagnostics. Absence does not choose bucket size; cached construction failures
and TLS access errors remain typed failures. The existing built-in policy is
public as `GenericRangePolicy`, so configured bootstrap needs no copied policy
or new bootstrap function. Its identity and range enforcement remain unchanged.

Focused validation passes: 30 runtime tests and four public integration tests,
including fresh 4/16/128-page settings after observation, exact configuration
matching, repeated bootstrap, configured custom host policy, and macro startup.
All-target Clippy, formatting and whitespace checks pass. An initial integration
fixture used a constant where the declaration macro requires a literal; corrected
before the passing run. Upstream has no `make clippy` target, so its direct
all-target lint command was used. Full release validation remains user-owned.

Matched upstream probes use Rust 1.98.1, the unchanged lockfile and `wasm-size`
profile: core raw Wasm remains 240,603 bytes; diagnostics falls from 289,638 to
289,605 bytes (-33). These are existing upstream probes, not a measurement of
future generated IcyDB profile wiring. IC instructions/cycles are unmeasured.

The upstream edit keeps one bootstrap implementation and one policy authority;
no extra configuration mode, durable format or unsafe code is added. Cargo
versions, dependency pins and application code were untouched in that upstream
handoff. The user has since published it; IcyDB now selects 0.13.3.

## IcyDB integration handoff — 2026-09-13

The macro carries an optional closed profile into the model's
`Canister::with_memory_profile`. Omission uses the model constructor's `General`
default; only `CanisterMemoryProfile::bucket_size_pages` maps profiles to pages.
Generated wiring passes that constant to the existing bootstrap helper. No
runtime profile enum, new endpoint, alternate bootstrap or catalog field is added.

Native qualification passes 32 selected tests covering macro parsing and actual
macro-to-model registration, configuration defaults, generated bootstrap arguments,
unchanged schema-submission identity, declaration validation, all three fresh
sizes, repeated initialization, conflicting unbootstrapped layout and host adoption.
Full Clippy and repository invariants pass. Initial fixture errors (private model
accessor, lock lifetime and the changed hidden helper signature) were corrected;
the affected tests and lint were rerun successfully.

PocketIC qualification passes generated lifecycle ordering, empty/populated and
converged upgrades, trapped-upgrade rollback and controller-only read-only
allocation reporting. Populated 2,048-row recovery with a trapped complete batch
rolls back and then succeeds through the canonical watchdog. The normal SQL
update test also covers deleting populated rows and querying the emptied index.
Full-suite/release validation remains user-owned.

### Matched cost evidence

Same lifecycle-participant actor, Rust 1.98.1, unchanged 0.13.3 dependency graph,
`wasm-release` with no added actor features, and canonical Binaryen 132 pipeline:

| Measurement | Before profile wiring (128 pages) | After (16 pages) | Delta |
| --- | ---: | ---: | ---: |
| Raw optimized Wasm bytes | 2,024,807 | 2,024,945 | +138 |
| Defined Wasm functions | 5,205 | 5,205 | 0 |
| Synchronous init participant instructions | 3,868,633 | 3,868,676 | +43 (+0.0011%) |
| Physical bytes immediately after init | 8,454,144 | 1,114,112 | -7,340,032 |

The baseline already selects 0.13.3 but predates IcyDB profile source edits.
Artifact SHA-256 values:

- Before: `b8fa41d6b2a8b49e6746638ff387114bd6fcbf3c435386dae794261ec0276078`.
- After: `fbfec89850c46b06dbe2efe48be167a021502b8d68b6ac4dca5f6baf39a225b2`.

A temporary native probe installed each saved artifact through
`install_prebuilt_fixture_canister_without_startup_delivery`, decoded the
`participant_instructions` field from `lifecycle_composition_snapshot`, and read
back stable memory outside the measured interval. It did not deliver deferred
startup callbacks. The probe was removed after recording the results; this is
one matched sample, not a whole-request, steady-state or background-cost claim.
Artifacts remain at `/tmp/icydb-memory-profile-lifecycle-{before,after}-opt.wasm`.

The separate maintained lifecycle test reports 3,544,487 init, 9,844,573 empty
post-upgrade, 12,161,570 populated post-upgrade and 12,572,867 converged
post-upgrade instructions, all below its unchanged 12,750,000 ceiling. It mixes
the maintained local/production build shapes, so these are qualification
observations, not deltas against the matched optimized pair above. Its converged
empty and one-row states both occupy 23,134,208 physical bytes (22.06 MiB).
Aggregate background cost, steady-state instruction deltas and separate cycle
deltas remain unmeasured in this integration; the earlier sweep remains the
bounded workload evidence for write/read trade-offs.

Complexity: 16 profile-integration files including direct tests, dependency,
guidance and status/release notes; about +280 Rust lines, mostly tests and roughly
90 production lines. There is one new three-choice configuration dimension, but
one retained runtime flow and one page mapping. Previously retained experiment
snapshots are separate evidence, not added runtime machinery. No application,
workspace/package version, upstream source, commit or publication changes were
made. Qualification used disposable PocketIC instances, without restarting an
application network.
