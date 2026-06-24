# Chunked Mutation Pipeline

## Purpose

H6 / D7 / F6 identified three write paths that still stage complete candidate
sets before mutation:

- `UPDATE` runs a structural selector, projects primary keys, then builds a
  mutation batch.
- `DELETE` resolves a scalar key stream, materializes candidate rows, then
  applies delete-owned filtering and commit preparation.
- `INSERT ... SELECT` executes the compiled source query and turns every source
  row into an insert patch before commit.

The 0.184 write-path work intentionally added bounds, metrics, and public
policy checks first. This note scopes the next shape without changing write
semantics yet.

## Current State

Public generated write surfaces now have explicit staged-row and RETURNING
bounds for the supported bounded `UPDATE` and `DELETE` shapes. Broad
session/admin SQL writes are still allowed where the existing surface allows
them, but their staged-row pressure is measured instead of hidden.

The mutation commit window still expects a fully prepared batch. It preflights
row operations, records touched index-store generations, prepares journal
appends, and only then applies the mutation. That full-batch preflight is a
correctness boundary, not just an implementation detail: uniqueness checks,
index updates, journaling, RETURNING rows, and rollback safety all depend on
knowing whether the complete mutation can be prepared before durable apply.

## Target Contract

Introduce a shared mutation-candidate contract before introducing chunked
durable commits. The first contract can still materialize bounded candidates;
its job is to make candidate selection, bounds, ordering, and diagnostics
uniform across write families.

The contract should represent:

- write family: `UPDATE`, `DELETE`, or `INSERT_SELECT`;
- source plan identity and route diagnostics;
- candidate payload shape: primary key only, row for filtering/RETURNING, or
  insert patch source row;
- staged-row bound and RETURNING-row bound;
- deterministic ordering proof when a public policy requires primary-key order;
- row-store reads, index-entry reads, filtered rows, staged rows, and returned
  rows;
- atomicity mode: full preflight today, future chunked preflight only after the
  commit model supports it.

This should be an executor/session handoff contract, not another SQL-only DTO.
SQL may build it first, but the shape should not encode SQL parser details.

## Migration Order

1. Keep the current full-batch commit behavior.
2. Add a shared bounded candidate collector for public `UPDATE` and `DELETE`
   shapes only.
3. Move staged-row and RETURNING diagnostics to that collector while preserving
   the existing SQL write attribution surface.
4. Add parity tests proving bounded `UPDATE` and `DELETE` still return the same
   affected rows, RETURNING rows, rejection codes, and failure atomicity.
5. Extend the collector to `INSERT ... SELECT` only when the source payload
   contract can reuse the same diagnostics without forcing insert values into
   the selector path.
6. Design chunked preflight after the shared collector exists. A chunked apply
   must still prove the entire mutation can succeed before any durable row op
   is applied, or it must introduce an explicit staging overlay with rollback
   semantics.

## Invariants

- A failed multi-row write must not partially mutate storage.
- Public bounded write policies must reject over-limit candidate sets before
  opening the mutation commit window.
- RETURNING row counts must remain bounded separately from staged row counts.
- `UPDATE` and `DELETE` must preserve their current residual-filter semantics.
- Candidate ordering must be explicit when a public surface depends on
  deterministic primary-key order.
- Commit-window generation guards remain the authority for detecting index
  store changes between preflight and apply.
- INSERT generation, managed fields, uniqueness checks, and accepted-schema
  row contracts must stay catalog-native.

## Non-Goals For The First Code Slice

- Do not stream durable commits.
- Do not widen public generated write exposure.
- Do not make broad session/admin SQL writes silently bounded unless the API
  contract says so.
- Do not merge `INSERT VALUES` into the selector-style candidate path.
- Do not change RETURNING result shape or row order.
- Do not introduce cost-based write planning.

## First Code Slice Candidate

Start with a shared bounded candidate collector for public `UPDATE` and
`DELETE` execution:

- `UPDATE` supplies selected primary keys plus one structural patch.
- `DELETE` supplies candidate rows through the existing scalar key-stream
  resolver and delete post-access filter.
- Both flows report the same candidate metrics and enforce staged-row bounds
  before commit-window open.
- Both flows keep the current full-batch preflight and apply path.

This should remove duplication in candidate bounds and diagnostics without
changing the mutation commit model.

The first cleanup slice started one level smaller: SQL `UPDATE` and `DELETE`
now share candidate-row bound/accounting helpers at the SQL write boundary.
That preserves the current collectors and full-batch commit model while making
the next row-collection cleanup less ambiguous.

The next cleanup slice moved DELETE scanned-row attribution into the shared
candidate resolver used by typed DELETE, count-only structural DELETE, and
DELETE RETURNING. Count-only structural DELETE and DELETE RETURNING also share
accepted-layout candidate row decoding, then keep their distinct rollback and
response packaging.

A later cleanup slice replaced the remaining typed-only and structural-only
delete leaf/prepared payloads with one generic delete output contract. Typed
DELETE, count-only structural DELETE, and DELETE RETURNING now share selected
row packaging, rollback-row ownership, row-count propagation, empty-result
handling, and commit row-op preparation while keeping their outward response
shaping separate. Structural delete helpers now hand prepared outputs back to
the `DeleteExecutor` wrapper, so final commit-window application is also owned
at the same boundary for typed, count-only, and RETURNING deletes.

## Deferred

- Real chunked preflight/apply.
- Broad session/admin write policy changes.
- Shared `INSERT ... SELECT` candidate collection.
- Chunk-level journal and rollback design.
- Cost/selectivity-based write routing.
