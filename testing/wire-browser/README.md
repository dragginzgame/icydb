# Native IcyDB browser contracts

The browser uses the same native Postcard payloads and IC Wire method/direction
headers as IcyDB's Rust callers. There is no Candid actor or alternate RPC format.
The host-only `icydb-testing-build` example generates TypeScript types, contracts
and exact native frames under `target/wire-browser/`. Wire collects the concrete
endpoint graph; opaque/incomplete metadata fails generation. Serde reflection
independently supplies native samples covering custom scalar decoders. Scalar
byte widths, calendar bounds and canonical Decimal text remain explicit at their
owning types. Metadata dependencies enter native libraries, but not canister Wasm.

`client.ts` passes raw requests to the caller's ICP SDK agent. The SDK owns
identity, signatures, certificates and update polling. The adapter does not fetch
root keys or disable verification. Only the disposable local browser fixture
explicitly trusts its local replica key and uses public deterministic identities.

## Prepare and qualify

Select Node 24.21.0 and npm 12.2.0, then prepare dependencies explicitly:

```sh
npm --prefix testing/wire-browser ci --ignore-scripts
npm --prefix testing/wire-browser run prepare-browser
make fetch
make check-wire-browser
make install-testkit
make test-wire-browser
```

IC tooling and the Rust Wasm target must already be prepared using the ordinary
workstation setup. No validation command installs npm packages, browsers or a
PocketIC binary. Testkit owns admission, startup and cleanup of the disposable
PocketIC instance/server; its instance also owns the browser's temporary gateway.
The installed test builds the existing SQL fixture with query, fixture-admin and
migration endpoints. It verifies generated query/update modes against actual Wasm
exports and decodes the real browser request/reply frames again with native Rust.

The codec corpus covers all twelve endpoint families and all SQL result variants,
public errors, recursive values, wide integers, finite floats including signed
zero/subnormals, UTF-8 byte lengths, exact retry bytes and malformed frames.
Grouped display cells remain strings: `NULL` and `"NULL"` differ. Cursors remain
opaque. Browser admission refuses oversized pages without editing or truncating
rows. The installed Chromium caller exercises every method, signed updates,
controller refusal, typed failures and successful calls after malformed ingress.
The fixture does not establish production login/delegation or update idempotency.
Repeated query bytes are stable; updates have their existing database semantics.

## Reviewed contract baseline

`contract-baseline.json` is the reviewed current endpoint contract. The normal
browser check compares it with freshly generated metadata before running codecs.
It records method identities/modes, budgets, named types, field/variant order and
scalar bounds/check names. Generation never updates this file. For an intentional
change, review the semantic and browser API impact, coordinate incompatible method
identity/client changes, then update the baseline in the same change. Preserve
array order; JSON object formatting is not significant. This is a current-contract
gate, not a compatibility reader or proof that custom validator bodies are unchanged.

The installed fixture also sends empty, nonempty, maximum-length and oversized
byte-buffer migration cursors. The current status API rejects every supplied
cursor and retains its status; it has no successful cursor pagination path.
Historical constraint validation supplies real nonempty finding keys and tests
exact replay of an unacknowledged finding page through Chromium and native decoding.
Populated physical-integrity and rejected-migration finding pages still need
installed browser fixture coverage ([#448](https://github.com/dragginzgame/icydb/issues/448));
generated/native vectors alone do not qualify those installed paths.

## Synchronous reply failures

Canic owns the raw serving boundary. A request admission failure rejects before
calling the handler. If encoding a synchronous handler's reply fails, the serving
boundary traps so the current message's mutations roll back. Typed application
errors remain normal encoded replies. This does not undo earlier messages or
external effects across an await boundary, or make arbitrary updates idempotent.
The maintained `query_canister` rollback subject checks failed encoding,
identical retries, successful writes, typed errors and malformed request controls
([#371 — Roll back mutations after reply-encoding failure](https://github.com/dragginzgame/icydb/issues/371)).

## Shared codec ownership

Registry IC Wire supplies `SchemaRegistry`, `typescript::generate_registry` and
`typescript::RUNTIME` from the same package used by native Rust. DTOs derive
native-only Wire metadata alongside Serde; custom scalar serializers have explicit
metadata implementations at their owning types. Wire owns recursive collection,
type rendering, reference validation and typed method objects. IcyDB retains
public browser names, endpoint identities and native scalar validation. Metadata
dependencies are native-only and are not enabled for canister Wasm.

Serde reflection remains only as the independent native sample/vector oracle.
It no longer translates production contract metadata. The reviewed baseline
retains the complete collected registry; generated aliases are representation
details, while the existing public browser names remain available. Map entries
are two-element tuples, matching Rust `(PublicValue, PublicValue)`: valid payloads
and traversal charges are unchanged, and wrong-length entries fail with a value
error. TypeScript now rejects those malformed pairs at compile time too.

The schema wire policy owns native allowances and the intentionally narrower
browser 32,768-value/32-level allowances. Wire's `method_limits` projects those
constants; Rust allocation admission is not a JavaScript heap limit.
`native-runtime.ts` binds the shared codec to IcyDB's Decimal validator.

Wire 0.7.9's `lint_bulk_roots` reviews explicit endpoint request/reply roots,
following shared registry references without using synthetic names as policy
identities ([Wire #25](https://github.com/dragginzgame/ic-wire/issues/25)). The
consumer regression protects independent sibling fields and registration order.

## Bulk-field review

Contract generation runs the published Wire bulk lint over every endpoint
request/response root and its reachable types, including error replies. New candidates and stale
acknowledgements fail generation before output files are written. The existing
browser checks and installed fixture therefore enforce the same review.

Key and cursor byte fields use `serde_bytes`, including optional bytes, and generate
`Uint8Array` client values. Regenerate clients and replace numeric arrays at these
fields; there is no compatibility fallback. Payload bytes remain unchanged, but
byte buffers no longer consume one traversal node per byte. Byte and application
length limits still apply. The main public Blob uses the same representation.
Only the diagnostic field-ID list is acknowledged in the generator's `bulk`
module: compact integer varints remain preferable without measured evidence for
`Packed<u32>`. The three SQL endpoint response paths are acknowledged explicitly;
other fields sharing the same container remain subject to review.
When a field is removed or changes representation, remove its stale acknowledgement.
Wire owns candidate detection; this repository owns the reasons and client rollout.

## Admission and scope

The browser admits at most 32,768 visited values and 32 nesting levels, within the
native request/reply byte ceilings (2/3 MiB including the 11-byte envelope). These
accommodate the maintained 76-entity fixture schema (11,174 visited values),
while remaining deliberately smaller browser limits, not inferred static bounds on arbitrary
Serde types. Collection counts are admitted before retaining their elements.
Strings count UTF-8 bytes; integers wider than 32 bits use `bigint`; byte strings
use `Uint8Array`. Unknown tags, invalid primitive encodings, nonminimal varints,
wrong method/direction and trailing bytes refuse. Rust retains domain validation,
accepted-schema authority and the native node/heap/depth admission. Browser limits
do not measure or guarantee JavaScript engine heap use; SDK transport buffers are
allocated before reply decoding. Request smaller pages after a limit refusal.
Storage, ordered keys, journals and authenticated cursor construction are excluded.

The normal check compares all generated method budgets with a separate projection
from the schema policy, rejecting both lower and higher mutations of each of the
six request/reply settings. Existing real-codec refusal cases retain the smaller
browser contract. This checks configuration drift, not equal Rust/JS traversal
charges or complete below/at/above cross-runtime boundary qualification.

Shared policy ownership and browser-budget qualification are tracked in
[Shared Tooling #307](https://github.com/dragginzgame/shared-tooling/issues/307);
typed budget projection and cross-runtime admission qualification are tracked in
[Wire #21](https://github.com/dragginzgame/ic-wire/issues/21). Equal numeric limits
alone do not establish identical Rust/JavaScript traversal admission.

Generated files, bundles and reports stay ignored under `target/wire-browser/`.
The installed report records the browser and actual frames. Linux qualification
does not qualify native macOS. Raw Wasm/cycles/instructions are not measured by
these correctness checks. Tracking: [#372 — Qualify native Wire browser codecs](https://github.com/dragginzgame/icydb/issues/372).
