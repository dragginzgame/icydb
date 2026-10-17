# Host-owned IcyDB endpoints

Status: proposed, unscheduled. Implementation and follow-up tracking belong to
[IcyDB #373](https://github.com/dragginzgame/icydb/issues/373) and
[Canic #590](https://github.com/dragginzgame/canic/issues/590).
This proposal grants no release or deployment authority.

## Problem and observed boundary

IcyDB supplies native database/session APIs, but also owns canister export
macros, controller authorization, endpoint envelopes and lifecycle exports.
`icydb::endpoints!` explicitly selects methods; schemas do not automatically
expose per-entity CRUD. `start!` can emit init/post-upgrade exports or a
synchronous participant for a host-owned lifecycle.

The inspected main is `27035c56c02641a84626b8631e843a2a69e85624`.
The separately uncommitted Wire migration replaces Candid endpoint encoding
with IC Wire. Moving that encoding alone does not remove host coupling. That
migration and the local schema dependency cleanup are prerequisites to qualify,
not code included or claimed delivered by this proposal.

## Proposed ownership

| Concern | Canonical owner |
| --- | --- |
| Accepted schema, storage, queries, writes and recovery semantics | IcyDB |
| Native operation inputs, results and typed database errors | IcyDB |
| Execution scope, resource budgets and precommit reply admission | IcyDB |
| Component memory requirements and synchronous database restoration | IcyDB |
| Public IC exports, caller/controller policy and transport refusal | Canic adapter |
| Endpoint method identities, Wire envelopes and artifact declarations | Canic adapter contract |
| Host memory admission, lifecycle ordering and dispatch instrumentation | Canic |
| Concrete application grants, hooks and deployment | Consuming application |

Dependency direction is `canic-icydb -> {canic, icydb}`. IcyDB must acquire no
Canic dependency. Its generated native binding exposes the existing operations
as ordinary Rust capabilities; preserve the current database/schema binding
instead of adding a second service runtime or a general platform abstraction.
Generated entity types and schema proposals remain in IcyDB.

Move the public endpoint macros, raw request/reply serving and endpoint-specific
authorization into the adapter. Host-independent serialization/resource helpers
may remain with their existing native type owner. Pure IcyDB callers need no
mounted endpoint. A standalone canister supplies its own thin host composition.

## Maintained endpoint contract

The existing 12 selectable names are:

- `icydb_query`, `icydb_ddl`, `icydb_update`, `icydb_integrity`;
- `icydb_fixtures_reset`, `icydb_fixtures_load`;
- `icydb_metrics`, `icydb_metrics_reset`, `icydb_snapshot`;
- `icydb_schema`, `icydb_schema_migrate`, `icydb_schema_migration`.

Preserve query/update modes, feature and fixture gating, native input/result
meaning, declared IC Wire identities and guard/default-controller behavior.
The adapter's endpoint ABI is Candid-free. One canonical contract must feed
the server, Rust/CLI callers and artifact inspection; do not duplicate DTOs,
method lists or response budgets in framework-specific mirrors.

SQL RETURNING must still validate its complete response projection before
mutation commits. This remains a database guarantee, even when the adapter
owns framing. Unexpected encoding failure after a synchronous handler must
trap so the current message rolls back; explicit rejection is insufficient.
[Issue #371](https://github.com/dragginzgame/icydb/issues/371) owns that proof.
Earlier messages and external effects across await boundaries need their own
operation recovery; a trap cannot undo them.

## Delivery boundaries and hard cut

1. Qualify the existing Wire migration and expose the minimal native binding.
2. Qualify Canic's protocol-neutral entrypoint capability and the adapter against
   that binding. Keep Canic's single memory/lifecycle owner and restore IcyDB
   synchronously before deferred application work.
3. Migrate actual endpoint declarations, callers, CLI and artifact contracts
   together, then delete superseded IcyDB export/lifecycle-root paths and their
   retired fixtures. No aliases, fallback codec or optional legacy owner remain.

These are review boundaries, not new release versions. Same-contract restore
coverage does not authorize cross-release Canic upgrade compatibility. Remaining
non-endpoint CDK/management dependencies require their separate runtime audit.

## Required evidence

Qualify the native binding without endpoint mounting, then an actual composed
canister covering each maintained endpoint family and its nearest refusal:
authorization, startup readiness, malformed/wrong-method/direction/trailing and
noncanonical payloads, bounds before copying or effects, and valid calls after
refusal. Retain SQL precommit refusal and installed post-handler rollback/retry
controls, one memory runtime and synchronous restoration ordering.

Compare dependency graphs and matched raw deployable Wasm; record available IC
cycle/instruction deltas and source/type/documentation churn. Mark unavailable
measurements explicitly. Browser codecs remain with
[#372](https://github.com/dragginzgame/icydb/issues/372), private dependency CI
access with [#370](https://github.com/dragginzgame/icydb/issues/370), and full
endpoint drift qualification with
[#194](https://github.com/dragginzgame/icydb/issues/194).

The simplest alternative is retaining IcyDB's current export macros. That leaves
the coupling this proposal removes. The intended state-space delta is one host
endpoint owner replacing one library endpoint owner, with no new database
format, persisted mode, fallback reader or execution route.
