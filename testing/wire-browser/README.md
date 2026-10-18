# Native IcyDB browser contracts

The browser uses the same native Postcard payloads and IC Wire method/direction
headers as IcyDB's Rust callers. There is no Candid actor or alternate RPC format.
The host-only `icydb-testing-build` example generates TypeScript types, contracts
and exact native frames under `target/wire-browser/`. Serde reflection must finish
the entire concrete endpoint graph; opaque/incomplete metadata fails generation.
Valid samples cover custom scalar decoders. Native scalar byte widths, calendar
bounds and canonical Decimal text are retained explicitly at this projection.
These host development dependencies do not enter the IcyDB library/Wasm graph.

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

## Shared codec ownership

Registry IC Wire 0.4 supplies `typescript::RUNTIME`; generated browser files
copy that exact dependency payload from the same package used by native Rust. IcyDB retains concrete Serde metadata,
endpoint identities, its 32,768-value/32-level allowances and native scalar rules.
`native-runtime.ts` only binds the shared codec to IcyDB's Decimal validator.
The finite IC Wire generator still refuses opaque native Rust metadata.

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

Generated files, bundles and reports stay ignored under `target/wire-browser/`.
The installed report records the browser and actual frames. Linux qualification
does not qualify native macOS. Raw Wasm/cycles/instructions are not measured by
these correctness checks. Tracking: [#372](https://github.com/dragginzgame/icydb/issues/372).
