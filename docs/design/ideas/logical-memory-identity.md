# Logical Memory Identity For Canisters And Stores

Status: idea intake only; not design or implementation authority

Recorded: 2026-09-06

## Problem

A journaled store currently asks application authors to assign four physical
memory IDs: data, index, schema, and journal. The canister also declares a memory
range and IDs for commit, startup, and integrity-progress storage. Authors must
coordinate these implementation details across stores and other libraries.

A range would shorten the store declaration but leave physical placement with
the application. The candidate here removes physical IDs from both canister
and store declarations, while retaining explicit durable logical identity.

## Candidate Surface

Illustrative syntax only; these attributes are not implemented:

```rust
#[canister(memory_namespace = "translation")]
pub struct TranslationCanister {}

/// The durable global editorial catalogue owned by the Translation canister.
#[store(
    canister = "TranslationCanister",
    storage(journaled(key = "editorial"))
)]
pub struct TranslationStore {}
```

Keep the existing explicit canister namespace as the durable owner identity.
Add one store key scoped to that namespace. The `canister` Rust reference still
selects the generated owner; it does not define durable storage identity.
The namespace is local to the concrete canister's memory runtime, not a claim
of uniqueness across deployed canisters or an identity derived from a principal.

IcyDB would derive role keys with distinct store and canister domains, for example:

| Owner | Derived allocation key |
| --- | --- |
| Editorial store data | `icydb.translation.store.editorial.data.v1` |
| Editorial store index | `icydb.translation.store.editorial.index.v1` |
| Editorial store schema | `icydb.translation.store.editorial.schema.v1` |
| Editorial store journal | `icydb.translation.store.editorial.journal.v1` |
| Canister commit control | `icydb.translation.canister.commit.control.v1` |
| Canister startup control | `icydb.translation.canister.startup.control.v1` |
| Canister integrity progress | `icydb.translation.canister.integrity.progress.v1` |

These names are proposed, not an instruction to rewrite existing keys.
Explicit namespace and store key values remain stable when Rust types, modules,
or packages are renamed. Changing either durable value changes identity.

`memory = 1` could serve as a logical store key, but its spelling suggests a
physical slot and loses descriptive diagnostics. Inferring identity from Rust
store or canister names makes ordinary refactors storage changes. Prefer one
explicit string identity at each ownership level, with no inference mode.

## Current Dependency Boundary

The repository uses `ic-memory` 0.13.1. Its declarations require a stable key
and a physical slot. Its durable ledger validates that an existing key has not
moved and that a slot has not been reused for a different key; it does not
currently choose free slots for key-only declarations.

Version 0.13.0 adds bounded physical allocation reports and explicit bucket
configuration. Reports expose current bindings, unknown allocations and bucket
slack, which can inform the ownership audit below. Neither capability supplies
automatic slot placement, reclamation, data migration or payload occupancy.

IcyDB currently derives store keys from the explicit memory ID and role,
deliberately avoiding source-name identity. Generated startup wiring passes
both the key and ID to `ic-memory`. Automatic placement therefore requires a
dependency capability and changes to generated opening/registration, not just
attribute shorthand.

Relevant local sources:

- [Canister declarations](../../../crates/icydb-model-macros/src/node/canister.rs)
- [Store declarations](../../../crates/icydb-model-macros/src/node/store.rs)
- [Current allocation keys](../../../crates/icydb-model/src/node/store.rs)
- [Generated memory wiring](../../../crates/icydb-model/src/build/actor/db/store.rs)

## Required ic-memory Ownership Audit

Before promoting this idea, audit the pinned `ic-memory` implementation and
IcyDB's integration together. The audit must establish what the dependency
already owns, what it should own, and which IcyDB responsibilities can be
deleted or reduced. The ownership proposal below is a hypothesis to verify,
not a completed audit or a requirement to add every candidate capability.

Trace declarations through validation, policy admission, ledger recovery,
allocation commitment, and memory opening. Include the default and explicitly
owned runtimes, generated IcyDB wiring, and composition with other libraries.
Record source evidence and distinguish enforced invariants from conventions.

| Area | Ownership question to resolve |
| --- | --- |
| Logical identity | Which key validation and uniqueness rules are generic to `ic-memory`, and which namespace/role rules belong to IcyDB? |
| Physical placement | Can key-only allocation extend the existing ledger and committed capability without a second registry or allocation route? |
| Host composition | Who supplies allowed ranges and reservations, and how does one allocation authority reconcile automatic requests with fixed external claims? |
| Bootstrap and recovery | Does `ic-memory` own the complete recover/validate/commit/open boundary, including its fixed bootstrap root and interrupted persistence? |
| Old allocations | What currently happens to absent, reintroduced, reserved, and retired keys; which lifecycle guarantees need strengthening? |
| Retirement and reclamation | Which generic claim transitions belong in `ic-memory`, and what database evidence must IcyDB require before requesting them? |
| Migration | Where does allocation bookkeeping end and schema validation, data copying, relation repair, or cross-canister transfer begin? |
| Diagnostics | Can the dependency report key, owner, role context, slot, and conflict/history facts without understanding database semantics? |
| Bounds and cost | Are declaration admission, persisted decoding, allocation search, history growth, and slot exhaustion bounded and fallible? |

Inspect duplicated checks and state in both projects. Prefer retaining one
enforcing owner and narrowing callers; preserve early macro diagnostics only
where they provide useful author feedback without becoming a second semantic
authority. `ic-memory` should not learn about entities, accepted database
schemas, SQL, relations, or database journal replay to perform allocation.

The audit deliverable must include:

- A current-versus-proposed responsibility map with source references and a
  single canonical owner for each invariant.
- Concrete gaps, duplication, and failure scenarios, including populated
  deployments and old stores; separate observed behavior from proposed behavior.
- A disposition for each finding: retain, delete, simplify, move, or add, with
  the smallest alternative and state-space cost of each proposed addition.
- Required dependency API changes, IcyDB code/configuration that can disappear,
  and focused recovery, lifecycle, and composition validation requirements.
- A verdict on whether automatic allocation belongs in `ic-memory` as proposed,
  and any blockers that must be resolved before choosing syntax or a deployment
  transition. Do not create new lifecycle or migration machinery merely to
  complete an ownership table.

## Candidate Ownership And Allocation

- Application declarations supply durable owner and store identities.
- IcyDB defines required storage roles and their key construction.
- `ic-memory` owns physical placement, reservations, collision checks, durable
  allocation history, and recovery within the concrete memory runtime.
- Accepted schema snapshots remain authority for database semantics. Allocation
  lookup must not reconstruct accepted schema from generated models.

The candidate bootstrap flow recovers the existing ledger, resolves declared
keys to existing slots, assigns eligible slots only to genuinely new keys, and
commits the complete allocation result before exposing memory handles. Opening
storage resolves through the committed allocation capability by key.

Existing assignments must never be recomputed from declaration order, sorted
store names, hashes reduced to a slot ID, or the currently present stores.
Deterministic ordering can choose among new allocations; durable history owns
all existing assignments. Insufficient space or conflicts must produce typed
errors before any partial set of new allocations becomes usable.

The ledger's own bootstrap storage needs a fixed, dependency-owned root; it
cannot depend on looking up its location in itself. Physical IDs still exist
inside `ic-memory` even when application declarations no longer expose them.

Composition with other users of stable memory needs an explicit dependency
contract: existing allocations, fixed external claims, and reserved ranges must
exclude slots from automatic placement. Removing IcyDB's per-canister range
arguments must not silently claim the whole canister or bypass host policy.
All participants must register or reserve their storage before allocation;
unregistered raw MemoryManager use cannot be made safe by inference.

## Migration And Old Stores

Migration is a prerequisite design question, not follow-up polish. Allocation
history, database schema transitions, and moving stored data have different
owners and must not be conflated.

| Change | Required interpretation |
| --- | --- |
| Rename a Rust store or canister type | Preserve explicit namespace/key and reuse existing allocations. |
| Reorder declarations or add another store | Reuse existing mappings; allocate only the new store's roles. |
| Change a store's accepted schema | Use the existing catalog-native schema transition authority; allocation identity stays stable. |
| Remove a store declaration | Do not infer deletion, retirement, or permission to reuse its slots. |
| Reintroduce an absent but unretired store | Resolve its existing identity, subject to accepted-schema and lifecycle checks. |
| Explicitly retire a store | Require a defined lifecycle operation; preserve allocation history and reject reopening retired identity. |
| Change a store key or canister namespace | Treat as different durable identity, not an inferred rename or permission to adopt old bytes. |
| Replace a store or move it between owners | Require a separately designed data-transfer outcome, including cutover and recovery. |
| Move to another deployed canister | Requires explicit data export/import or transfer; a matching namespace does not move data. |

Absence is especially dangerous: the linked declarations describe this binary,
not every store ever present in durable memory. Missing stores must retain their
claims. Reclamation is not implied by absence, schema deletion, or an empty
store. The initial candidate should not support reuse of retired slots.

Before retirement can be designed, account for outstanding journal/recovery
work, shared commit-control state, active migrations, relation dependencies,
and accepted catalog references. A new allocator must not bypass these owners.
Inspect the dependency's existing retirement semantics before adding lifecycle
states; reuse its allocation history rather than create an IcyDB shadow ledger.

An accidental new namespace or store key could look like a valid new empty
database. Decide how startup reports unmatched durable stores and how an
intentional replacement is distinguished from a typo. Do not assume every
unmatched old store is an error: binaries can intentionally omit declarations.
The admission rule remains open pending concrete upgrade scenarios.

If data movement is later authorized, its design must specify bounded copying,
accepted-schema validation, relations and indexes, write coordination, durable
progress, interruption recovery, atomic authority cutover, and source disposal.
Moving a mapping alone is not a data migration. Do not introduce a generic
transfer engine or a new persisted job in this idea without a demonstrated need.

### Transition From Today's Explicit-ID Layout

The proposed keys differ from current ID-derived store keys. Existing ledgers
will not automatically recognize them as the same allocations. The initial
transition must be treated as an incompatible representation change.

Under the repository's pre-1.0 hard-cut rules, the default is reinstall,
recreation, or explicit regeneration of the current representation. This idea
does not propose old-format decoders, key aliases, automatic adoption of old
slots, dual opening routes, or an in-place compatibility bridge. Versioned
internal representations remain in their current version-1 form.

For applications that must preserve data, establish the concrete preservation
requirement before promotion. Assess a separately authorized export before
replacement and import through maintained accepted-schema admission afterward,
including preservation of identities and relationships. No such tooling or
lossless round trip is assumed to exist. An in-place bridge would require an
explicit exception to the current rules and is outside this proposal.

Routine upgrades after adopting this design are a separate concern: they must
recover unchanged durable mappings, tolerate declaration reordering and
additions, and respect absent/retired store history without reassigning slots.

## Need, Alternative, And Complexity Delta

- Demonstrated need: four coordinated physical IDs per store plus canister
  control IDs and ranges make routine application declarations brittle.
- Simplest alternative: one starting ID or exact four-ID range per store.
  This needs no automatic allocator but retains application-owned placement
  and canister-level coordination.
- Canonical owner: extend `ic-memory`'s existing durable allocation authority;
  IcyDB supplies logical declarations and consumes committed allocations.
- State-space delta: replace explicit placement with one automatic resolution
  path for IcyDB. Add no explicit/automatic selection mode, inferred-name mode,
  second ledger, or IcyDB allocation format. New-slot assignment and composition
  rules add dependency complexity that must be justified against deleted
  configuration and validation work. Retirement and data transfer remain
  separate outcomes unless evidence requires them for safe adoption.

## Evidence Needed Before Promotion

- Complete the required `ic-memory` ownership audit above and resolve its
  ownership and adoption blockers before defining implementation slices.
- Demonstrate upgrades with reordered declarations, added stores, unchanged
  source-independent identities, and absent stores whose slots remain claimed.
- Exercise duplicate keys, fixed external allocations, reserved ranges, slot
  exhaustion, failed allocation persistence, and interrupted bootstrap recovery.
- Define diagnostics and admission for namespace/key mistakes on populated
  memory, including intentional replacement and explicitly retired stores.
- Choose the initial deployment transition and identify any application data
  that must survive it before committing to the new key construction.
- Measure startup instructions, allocation metadata growth, and raw Wasm bytes,
  alongside files/lines changed and the resulting implementation complexity.

No minor version or implementation tracker is assigned. Promotion requires a
bounded design and meaningful landing slices, with dependency allocation work,
IcyDB declaration/wiring changes, and any separately justified migration work
given explicit ownership and scope.
