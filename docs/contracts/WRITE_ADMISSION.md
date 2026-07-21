# IcyDB Write Admission Contract

This document defines the normative admission boundary for IcyDB row writes.
It answers which mutation surfaces may create or replace persisted row state and
which schema guarantees every such surface must enforce.

Atomic commit ordering is defined in `ATOMICITY.md`. Batch transaction scope is
defined in `TRANSACTION_SEMANTICS.md`. Durable replay and import trust boundaries
are defined in `DURABILITY.md`. SQL syntax and exposure policy are defined in
`SQL_SUBSET.md`, and relation-specific guarantees are defined in
`REF_INTEGRITY.md`.

## Core Rule

Every accepted persisted field has exactly one accepted field kind and one
schema-derived absence policy. Every supported logical mutation ingress that
produces a row after-image must canonicalize and admit that after-image against
the current accepted schema before publishing its commit marker.

IcyDB has no non-strict entity or table mode. There is no trusted row-write
bypass that disables accepted-schema validation.

Every declared relation participates in referential-integrity checks. Ordinary
key-typed fields may store identifiers without declaring a relation.

## Accepted Field Contract

The accepted schema snapshot is runtime authority. Generated entity and index
models may propose or reconcile schema, but they are not a fallback runtime
authority for row admission.

Each accepted field contributes:

- one exact accepted persisted kind;
- one physical row slot;
- one absence policy: `Required`, `NullIfMissing`, or `DefaultIfMissing`;
- any database-owned generation or managed-write policy;
- any scalar bounds, decimal scale, text limit, collection encoding, enum or
  exact-composite catalog identity, index, or relation facts that apply to the
  field.

An accepted composite field resolves to one store-local nominal type ID and a
complete accepted record, tuple, or newtype definition. Record member names,
tuple arity, nested kinds, and nested nullability are admission facts. Generated
Rust codecs may confirm that contract but may not reconstruct it at runtime.

The absence policy is derived exhaustively from accepted nullability and
database-default metadata. Rust `Default`, generated construction values, and
the physical length of an older row do not independently authorize omission.

Omission and explicit `NULL` are distinct. Omission is admitted only when the
accepted absence or write policy can materialize the field. Explicit `NULL` is
admitted only when the accepted field is nullable.

Normalization is allowed only where the accepted field contract explicitly
owns it. Incompatible values must reject; they must not be coerced through a
different field kind or stored as an alternate persisted representation.

## Required Pre-Commit Work

Before an applicable mutation can enter a commit window, the write path must
complete all fallible work required by that mutation, including:

- accepted catalog, entity, row-layout, and schema-fingerprint resolution;
- required-field authorship and omission/default/generated-value handling;
- accepted input encoding and complete canonical after-image construction;
- sanitization and user validation;
- primary-key shape, type, and row-identity validation;
- field-kind, nullability, scalar-bound, decimal, text, enum, exact-composite,
  collection, and deterministic-encoding validation;
- relation target-existence or delete-safety validation;
- uniqueness, index, reverse-relation, and commit-row preparation;
- request and response bounds that are part of the mutation's atomic result.

Some paths can prove a requirement while constructing the canonical row rather
than in a separate validation pass. The guarantee is that every requirement is
complete before marker publication, not that all implementations use the same
function or repeat the same decode.

After marker publication, apply is mechanical. It may apply only row, index,
relation, journal, and schema effects that were already prepared under the
accepted authority. No new coercion, validation, or mutation planning may begin
in the apply phase.

## Current Surface Inventory

| Surface | Write-admission rule |
| --- | --- |
| Typed `create`, `insert`, `update`, and `replace` | Materialize through the accepted row contract, run the typed preflight, then prepare the commit from the canonical row. |
| Public structural mutation | Resolve field names and slots through the accepted layout, construct a complete canonical after-image, materialize it through the generated-compatible boundary, and run the same typed preflight. |
| SQL `INSERT` and `UPDATE` | Decode literals and omissions against accepted field contracts, then enter the structural mutation pipeline. Trusted or generated SQL exposure policy never bypasses row admission. |
| Typed, fluent, and SQL `DELETE` | Resolve selected rows through accepted authority, validate relation delete safety, and prepare row/index/relation removals before the marker. Deletes have no row after-image. |
| Atomic single-entity batches | Admit and stage every item before opening one commit window. One rejected item rejects the entire batch. |
| Non-atomic single-entity batches | Apply the complete admission contract independently to each item. A previously committed prefix is not rolled back when a later item rejects. |
| Defaults, generated values, and managed fields | Materialize only from accepted absence/write policy and pass normal canonical row admission. |
| SQL DDL and schema row rewrites | Derive an accepted-after catalog candidate first. Any physical row rewrite must preserve or construct values admitted by that candidate before accepted-schema publication becomes authoritative. |
| Index and reverse-relation projection | Derived state only. Projection must consume row images already decoded through the accepted contract; it is not an independent row ingress. |
| Recovery replay | Not a fresh mutation ingress. It may replay only internally produced marker/journal state that passes the durable replay checks in `DURABILITY.md`. |

There is no `trusted_write_unchecked` equivalent to the trusted read bypass.
The word `trusted` on a SQL mutation API describes caller-owned authorization
and exposure policy; it does not weaken schema admission.

## Schema Mutation And Backfill Rules

Schema mutation remains catalog-native. SQL DDL is a frontend, not the source
of mutation semantics.

Current additive fields use explicit accepted missing-slot behavior rather than
a general physical backfill executor. Nullable fields without defaults
materialize older rows as `NULL`; fields with accepted database defaults
materialize the accepted default. Required additions without a database default
reject.

`SET NOT NULL` must validate existing rows through the accepted contract before
publication. A row-layout rewrite such as `DROP COLUMN` must validate the
accepted-before row, construct the constrained accepted-after row, retain the
rollback image, and publish the row effects with the accepted schema candidate.

Any future general backfill surface must be treated as row ingress. It must
either use the normal structural write-admission pipeline or provide an
equivalent accepted-after proof before publishing row or schema effects.

## Recovery Is Replay, Not Admission

Recovery completes an internally produced commit; it does not accept new user
values. Replay must fail closed on invalid marker or journal envelopes, store or
entity mismatches, invalid keys, incompatible schema fingerprints, malformed
rows, rejected primary-key identity, or inconsistent derived index state.

Recovery does not rerun mutation-time sanitizers or user validators. Those may
depend on authoring context and are required before the original marker is
published. Recovery also does not turn arbitrary external stable-memory bytes
into an admitted write.

The current durability contract covers internally produced interrupted state,
not hostile import or arbitrary logical-corruption repair. A future promise to
detect structurally valid but dangling relations after recovery would
require a separate global integrity check; it must not be described as replaying
the original mutation validators.

## Unsupported And Future Ingress

IcyDB does not currently expose a supported raw backup, import, or restore
surface. Custom stable-memory injection is outside this contract.

Exact and resumable bulk update is not a current production ingress. Any future
bulk implementation must admit every produced after-image under this contract.
Resumable work must additionally pin or revalidate accepted-schema identity for
each chunk and must never treat a partial window as proof of a complete
mutation.

External import, restore, general data-migration, and bulk APIs must not be
documented as supported until their trust, resource, schema-version, failure,
and write-admission rules are normative and implemented.

Those absent surfaces reject by non-exposure rather than sharing an unchecked
internal write path.

## Mandatory Invariants

- Accepted schema is the only runtime row-admission authority.
- No supported mutation ingress may persist an incompatible field value or
  unapproved omission.
- No generated-model fallback may reconstruct missing accepted row authority.
- No commit marker may be published before applicable row admission and commit
  preparation complete.
- No index or relation projection may derive persisted state from an
  unvalidated row image.
- Rejection before marker publication must not make that mutation's row effects
  visible. Explicit non-atomic batches may retain only their already committed
  prefix.
- Adding a new mutation surface requires adding it to this inventory or marking
  it unsupported.

Violating any of these invariants is a correctness bug.

## Non-Goals

Always-strict write admission does not imply:

- SQL compatibility beyond `SQL_SUBSET.md`;
- joins, cascades, or deferred constraints;
- referential-integrity guarantees for ordinary key-typed fields;
- rerunning mutation validators during reads or recovery;
- transaction scope wider than one documented IcyDB mutation operation;
- acceptance of raw or cross-version persisted bytes as an import format.
