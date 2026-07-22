# IcyDB Persisted Format Policy

This document defines how IcyDB classifies changes to bytes that may survive
in Internet Computer stable memory.

It is normative for the current line and complements
`docs/contracts/DURABILITY.md`.

The active persisted-surface checklist lives in
`docs/contracts/PERSISTED_FORMAT_INVENTORY.md`.

## Scope

Persisted format includes any byte layout or durable identity that can be read
after an upgrade or guarded recovery pass, including:

- commit markers;
- journal-tail batches and fold watermarks;
- raw row envelopes;
- accepted schema snapshots;
- data-store keys;
- secondary-index keys and entries;
- structural field and value-storage payloads;
- stable-memory memory ID allocation roles;
- cursor tokens when they can be stored or returned across calls.

Pure in-heap caches, diagnostic rendering strings, test-only failpoints, and
non-persisted execution plans are not persisted format.

## Current Compatibility Posture

Before `1.0.0`, IcyDB keeps one active internal persisted format for each
durable surface. Every active format uses version 1.

The default posture is a pre-1.0 hard cut:

- an incompatible format change replaces the current version-1 form instead
  of introducing a version 2 reader or writer;
- the current form either decodes exactly or fails closed; old internal forms
  do not receive compatibility decoders;
- unknown future versions fail closed;
- format drift must be intentional, documented, and tested;
- generated model reconstruction is not runtime authority for accepted schema
  or row layout.

Application schema revisions and entity-local row-layout revisions are domain
history, not wire-format versions. They may advance beyond 1 while the sole
current schema and row codecs remain version 1.

This policy matches the repository rule that internal protocols/formats can be
hard-cut before `1.0.0` instead of carrying legacy fallback machinery.

## Required Classification

Every change to persisted bytes must be classified before implementation as
exactly one of these:

### Test-only harness change

The change affects only test fixtures, failpoint payloads, mocked bytes, or
audit corpus data. It cannot alter production persisted bytes.

Required evidence:

- production encoding/decoding code is unchanged; or
- test-only `cfg(test)` containment is explicit.

### Internal pre-1.0 hard cut

The active persisted format changes and older internal bytes are no longer
accepted.

Required evidence:

- release notes call out the hard cut;
- malformed or old-version bytes fail closed;
- upgrade/import expectations are updated;
- no silent fallback reconstructs authority from generated models.

### Backward-compatible reader extension

The reader accepts both old and new bytes while writers produce the new format.

This classification is unavailable before `1.0.0` unless the user explicitly
authorizes an exception to the repository hard-cut rule.

Required evidence:

- version discrimination is explicit;
- old and new decode paths have malformed-input coverage;
- the compatibility window has an owner and removal plan.

### Format-breaking migration

The change requires an explicit migration step or external operator action.

Required evidence:

- migration order is documented;
- failure and rollback behavior is defined;
- recovery behavior during an interrupted migration is tested or explicitly out
  of scope.

## Checksums

Checksums are persisted format.

Adding marker, journal, row, schema, index, structural-value, or cursor
checksums requires one of the classifications above. The checksum design must
state whether the checksum is meant to detect accidental corruption, hostile
imports, incompatible versions, or all of those cases.

The current line does not add checksum bytes.

## Backup, Restore, And Import

Raw stable-memory backup, restore, or import is not a supported product surface
in the current line.

Any future import contract must state:

- whether inputs are trusted operator artifacts or hostile external bytes;
- which persisted versions are accepted;
- whether checksums or full integrity scans are required before serving reads;
- whether unknown versions are incompatible format, corruption, or migration
  inputs;
- what resource limits apply during validation and recovery.

Until such a contract lands, externally copied stable-memory images are outside
IcyDB's supported compatibility guarantee.

## Fail-Closed Rule

Persisted decoders must be bounded and fallible.

Malformed or unsupported persisted bytes must not:

- panic;
- silently produce wrong logical state;
- allocate unbounded memory before validation;
- loop indefinitely;
- reconstruct missing authority from generated compile-time models.

The correct behavior is a typed incompatibility/corruption/recovery error at a
guarded boundary, or an explicit pre-1.0 hard-cut rejection.

## Documentation Rule

Release notes for persisted-format changes must identify the classification.

If a change is intentionally not a persisted-format change, the relevant design
or changelog should say so when the surrounding work touches commit, recovery,
row, schema, index, cursor, or structural-value code.
