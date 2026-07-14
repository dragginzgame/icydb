# IcyDB Cursor Pagination Contract

This document describes the current cursor representation and implementation
boundary. The normative pagination semantics are defined in
[`QUERY_CONTRACT.md`](QUERY_CONTRACT.md).

The current source owners are:

- `crates/icydb-core/src/db/cursor/token/codec.rs` for bounded binary encoding;
- `crates/icydb-core/src/db/cursor/string.rs` for external hex text;
- `crates/icydb-core/src/db/cursor/spine.rs` for decode and validation;
- executor continuation planning for route-specific resume behavior.

## External Token Boundary

Cursor tokens are opaque to callers, but they are not confidential. Applications
must pass them back unchanged and must not parse fields or depend on their wire
layout.

IcyDB emits lowercase hexadecimal text over the binary token. Decode accepts
lowercase or uppercase hexadecimal digits. Empty, whitespace-only, odd-length,
non-hexadecimal, and oversized tokens fail closed with typed cursor errors.

The binary token is bounded to 8 KiB. The external hex form is bounded to twice
that size before decode, so untrusted input cannot force an unbounded allocation.

## Current Binary Wire

There is one accepted binary wire: the `ICYQ` magic followed by
`TOKEN_WIRE_VERSION = 1`. IcyDB does not retain a decoder or translate older
cursor formats. A magic or version mismatch, wrong scalar/grouped variant,
truncated field, invalid value tag, invalid direction, invalid optional-field
marker, or trailing byte is rejected.

Both scalar and grouped tokens begin with the magic, wire version, and token
variant. The current payloads then carry:

- a continuation signature bound to the canonical query shape;
- traversal direction;
- the initial offset owned by the first page;
- scalar canonical boundary slots and an optional index-range anchor, or the
  grouped continuation key tuple.

The wire version is an internal protocol discriminator. Public APIs promise an
opaque token, not a stable field layout.

## Validation and Continuation

Decode is only the first gate. The cursor validation spine checks the token
against the current entity and canonical query shape before execution. A cursor
cannot be reused after changing the predicate, access shape, ordering, entity,
direction, or other signature-owned pagination facts.

Continuation is strict and forward-only:

`next page := rows whose canonical order is greater than the boundary`

The canonical order is the requested order plus the primary-key tie-breaker.
Cursor and non-zero offset modes cannot be mixed.

## Execution Model

Cursor semantics do not depend on the selected physical route. Every route must
preserve the same canonical ordering, residual filtering, strict boundary, page
limit, and lookahead behavior.

When planning proves that an ordered access route can resume safely, execution
may seek or stream from the continuation boundary and stop at the bounded page.
Index-range routes may carry the validated raw-key anchor needed for that resume.
When the proof is unavailable, execution uses the admitted materialize/filter/
order/cursor/window path. A broader runtime fallback cannot bypass public-read
admission.

Diagnostics report the route and continuation mode chosen for the execution;
they do not reconstruct a second cursor contract.

## Live-State Semantics

Pagination is best-effort and forward-only over live state. It is not snapshot
isolated across requests. With a fixed query shape and stable ordered keys,
pages do not overlap. Concurrent inserts, deletes, or updates to ordered fields
may change which rows remain after the boundary.

## Non-Goals

The current cursor contract does not provide:

- backward or random-page traversal;
- snapshot isolation across page requests;
- encryption, authentication, or server-side cursor storage;
- decoding or translation of retired wire formats.
