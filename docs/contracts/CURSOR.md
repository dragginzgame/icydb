# IcyDB Cursor Pagination Contract

This document describes the maintained continuation boundary. Normative query
semantics also live in [`QUERY_CONTRACT.md`](QUERY_CONTRACT.md).

## External Boundary

Continuations are opaque and authenticated, but not encrypted. Applications
must pass them back unchanged, authorize every ordered field represented by a
cursor, and must not treat cursor contents as a field-level secrecy boundary.

IcyDB emits lowercase hexadecimal text over a bounded binary token. Decode
accepts either hex case. Empty, odd-length, non-hexadecimal, oversized,
truncated, modified, wrong-database, and unsupported-version tokens fail
closed before execution. Binary scalar tokens are capped at 8 KiB.

## Current Wires

Grouped continuation retains the sole current bounded version-1 wire. Scalar
live and exhaustive pages use the sole current authenticated version-2 wire.
No legacy scalar decoder or translation path exists.

The scalar MAC covers the current payload before semantic fields are used. Its
contract binds:

- live or exhaustive mode;
- canonical query shape and bound parameter identity;
- database incarnation, accepted runtime root, entity, and access authority;
- every explicit and hidden order term with its own direction and canonical
  null/comparison semantics;
- total query window and immutable page-envelope identity;
- last emitted logical boundary, consumed physical progress, and bounded
  unconsumed lookahead state; and
- in exhaustive mode, the complete `ReadSetRevisionProof` identity.

Changing any bound fact rejects the token. Ordered boundary values are capped
at 4 KiB across at most 32 terms; a single unrepresentable boundary fails with
the typed terminal page-unit error rather than returning a looping cursor.

## Ordering And Progress

Continuation is strict, deterministic, and forward-only in the canonical
mixed-direction order. IcyDB appends missing primary-key components as hidden
tie breakers while preserving explicitly supplied primary-key terms and their
directions. Null and value comparison use the frozen canonical comparison
contract rather than locale collation.

A non-null continuation means traversal has not been proven exhausted. It
does not guarantee another matching row exists. Page-envelope exhaustion may
therefore return an empty page with continuation after consuming only
nonmatching physical entries.

Lookahead never consumes an unreturned match. The cursor either remains before
that match or retains enough bounded state to return it on the next page. If
lookahead proves physical exhaustion, continuation is null even when the page
is exactly full.

## Live And Exhaustive Modes

Live pages are revision-tolerant keyset traversal for ordinary UI browsing.
Concurrent writes may change which rows remain after the validated boundary;
live pages do not claim snapshot completeness.

Exhaustive pages are revision-strict. The first page captures or accepts one
canonical bounded proof for all participating physical stores. Every resume
must supply that proof beside the continuation. IcyDB compares it before and
after page execution; a protected row, accepted-root, database-incarnation,
or access-state change returns a typed revision failure. Completion is only a
null continuation under one unchanged proof.

## Non-Goals

The cursor contract does not provide backward/random-page traversal,
confidential cursor fields, compatibility decoding, or automatic application
job authorization. Durable multi-call accumulation uses the separate
idempotent resumable-job boundary.
