# Streaming Recovery Follow-Up

This note tracks the streaming index fold/rebuild work that 0.191 deliberately
does not implement.

## Current Decision

0.191 does not claim production-scale recovery-size guarantees.

The current line has:

- executable recovery correctness proof for 256-row and 1,024-row host
  secondary-index rebuild windows;
- executable mixed ordinary, conditional, and expression index rebuild proof
  over a 128-row-per-shape host window;
- a PocketIC same-WASM upgrade/reentry instruction probe over a 32-row
  journaled `sql_perf` fixture;
- no PocketIC instruction-budget ceiling for arbitrary production indexes;
- no streaming recovery runtime;
- fail-closed guarded access if recovery cannot complete.

Because 0.191 stops at this documented boundary, streaming recovery is not
required to close the 0.191 productization slice. It becomes required before
IcyDB claims production recovery support for index sizes that cannot be proven
to fit one IC update-message budget.

## Trigger

Design and implement streaming recovery before any release claims one of:

- a broad production recovery-size guarantee above the measured single-message
  budget;
- online recovery progress across multiple IC messages;
- backup/import validation for large external stable-memory images;
- large-index rebuild/fold support without a conservative row/index bound.

## Required Model

Streaming recovery must be an explicit recovery mode, not ordinary query
execution.

Required properties:

- guarded reads and writes must fail closed or report recovery-in-progress
  until the stream reaches a fully valid durable state;
- no query may observe partially rebuilt derived indexes;
- progress state must be durable and idempotent;
- each chunk must be safe to repeat after interruption;
- readiness must be restored only after canonical rows, derived indexes,
  journal tail, fold watermark, and marker state agree;
- chunk boundaries must be bounded by explicit row/index budgets;
- direct raw-store access remains outside contract during streaming recovery.

## Durable Progress State

A future design must define persisted progress for each streamed recovery
family:

- row scan cursor or last processed data-store key;
- index being rebuilt or folded;
- journal batch/fold watermark progress;
- derived-index readiness state;
- marker or recovery-domain authority that keeps guarded access closed.

Progress bytes are persisted format and must be classified under
`docs/contracts/PERSISTED_FORMAT_POLICY.md` before implementation.

## Validation Requirements

Executable proof must cover:

- interruption before the first chunk writes progress;
- interruption after chunk progress persists;
- interruption after derived index entries are written but before readiness;
- repeated interruption across at least two chunks;
- stale pre-existing index entries removed or ignored deterministically;
- guarded reads/writes reject or report in-progress state until completion;
- final state matches the existing one-shot recovery oracle.

## Non-Goals

This follow-up does not require:

- POSIX file-database recovery semantics;
- background threads;
- canister calls from recovery;
- query access during partial recovery;
- backup/import product support without its own threat model.
