# IcyDB Cursor Pagination Contract

This document describes the maintained continuation boundary. Normative query
semantics also live in [`QUERY_CONTRACT.md`](QUERY_CONTRACT.md).

## External Boundary

Continuations are opaque and authenticated, but not encrypted. Applications
must pass them back unchanged, authorize every ordered field represented by a
cursor, and must not treat cursor contents as a field-level secrecy boundary.

IcyDB emits canonical unpadded URL-safe Base64 over a bounded binary token.
Decode trims surrounding whitespace and rejects invalid lengths, alphabet,
padding, or nonzero unused trailing bits. Empty, oversized, truncated, modified,
wrong-database, and unsupported-version tokens fail closed before execution.
Binary tokens are capped at 8 KiB; external text is capped at 10,923 bytes.
Applications must regenerate saved continuations after a representation hard cut.

## Current Wires

Grouped continuation and scalar live/exhaustive pages retain their sole current
bounded version-1 wires. No compatibility decoder or translation path exists.

Both token variants share the authenticated envelope: magic, version, variant,
payload length, payload, and a 32-byte HMAC-SHA256. The existing durable cursor
key seals framing and payload; verification precedes value decoding. Grouped
tokens bind their accepted-schema/query signature, direction, initial offset,
and every group-key value. This grouped hard cut requires restarting saved
grouped pagination; scalar token bytes and stored database formats are unchanged.

Big-integer values carry a u32 byte count and minimal little-endian magnitude;
signed integers prefix sign 0/1/2 for zero/positive/negative. Zero has no
magnitude bytes. Redundant high zero bytes and inconsistent signs reject.
Account retains its fixed 62-byte payload directly after the value tag; Decimal
retains its full i128 mantissa and one scale byte (0–28). The same value codec
owns stored mutation-job literals, so affected job records require recreation.

Value nesting is limited to 128 edges on both encode and decode. Each root
value starts at depth zero; a list item, map key/value or enum payload adds
one edge. Tuple entries and siblings do not accumulate depth. Empty containers
at the limit remain valid, and this limit does not restrict the width of a
shallow list beyond existing byte/size limits. Excessive nesting fails with
the existing token encode/decode error before further value recursion. The
same guard applies to stored mutation-job literals, independently of their
expression-depth limit. Depth admission itself does not change value encoding;
over-depth saved continuations must be discarded and affected jobs recreated.

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

## Grouped Request Policy

After authentication, grouped resume validates the current signature, direction,
offset, tuple arity and accepted key types before row execution. Direct fields
reuse accepted-value validation with the group-key representation: decimals,
including those nested in collections, may use a normalized scale only when
exactly representable at the accepted field scale. Stored-value validation and
opaque enum bodies retain their strict scale requirement. Scalar record paths
use the accepted query-type check and retain missing/null path semantics.
Resume never rewrites a boundary. Malformed boundaries use the existing
invalid-cursor code and `CursorTokenDecode` reason; missing required plan/schema
metadata remains an internal invariant rather than a caller rejection.

Each outgoing grouped cursor is encoded/authenticated once within the execution
budget. Its binary length is charged as temporary bytes and its exact unpadded
Base64 length as result bytes. The response consumes the same encoded bytes;
it does not read the authentication key or encode the binary token again.
Absent continuations incur no cursor-byte or cursor-step charge.

Grouped continuation requires a row `LIMIT` and is not supported for global
DISTINCT aggregation without group keys. Supplying a decoded cursor in either
case rejects before row execution with `QUERY_INVALID_CONTINUATION_CURSOR` at
`Cursor` origin, not an internal invariant error. The `DecodeReason` fact is
`CursorGroupedContinuationRequiresLimit` (11) or
`CursorGlobalDistinctContinuationUnsupported` (12), respectively. Eligible
bounded grouped queries without a cursor remain supported; these policy
rejections do not change cursor encoding or relax internal invariant checks.

## Non-Goals

The cursor contract does not provide backward/random-page traversal,
confidential cursor fields, compatibility decoding, or automatic application
job authorization. Durable multi-call accumulation uses the separate
idempotent resumable-job boundary.
