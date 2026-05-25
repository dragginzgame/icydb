### LLMs PLEASE IGNORE THIS FILE
### It's just here so I can manage multiple prompts without scrolling up and down constantly

Hard-Cut Persisted Big-Integer Field Storage to Candid LEB128 Bytes

Goal:
Change persisted DB storage for `int_big(max_bytes=N)` and `nat_big(max_bytes=N)` fields from the current structural limb/list representation to compact Candid-compatible LEB128/SLEB128 bytes.

This is a storage-density hard cut.

Important:
- Keep `int_big` / `nat_big` schema support.
- Keep `types::IntBig` / `types::NatBig` wrappers Candid-backed.
- Keep max_bytes semantics based on the exact stored Candid byte length.
- Do not change fixed-width `int128` / `nat128`.
- Do not change primary-key admissibility: `int_big` / `nat_big` remain rejected as primary-key components.

Rationale:
Current structural storage is wasteful for common small big integers:

```text
Candid Nat/Int LEB128:
- 0 / 1 can be 1 byte

Current structural field layout:
- NatBig(0): roughly 5 bytes
- NatBig(1): roughly 14 bytes
- IntBig(0): roughly 19 bytes
- IntBig(1): roughly 28 bytes

The current code already computes to_leb128() for max_bytes validation. This patch should make those exact canonical bytes the persisted payload for int_big / nat_big.

Required end state:

Persisted int_big field payload = canonical Candid signed LEB128 bytes
Persisted nat_big field payload = canonical Candid unsigned LEB128 bytes

Expected APIs:

impl IntBig {
    pub(crate) fn to_leb128(&self) -> Vec<u8>;
    pub(crate) fn from_leb128(bytes: &[u8]) -> Result<Self, Error>;
}

impl NatBig {
    pub(crate) fn to_leb128(&self) -> Vec<u8>;
    pub(crate) fn from_leb128(bytes: &[u8]) -> Result<Self, Error>;
}

If exact function names differ, use project naming conventions.

Locate all current persisted big-int storage paths

Audit and update:

structural field encode/decode
by-kind persisted row codec
default value encoding
DDL default encoding
derive default encoding
accepted snapshot/default payload round-trips
value storage decode/encode
generic Value::IntBig / Value::NatBig wire/cursor paths if they reuse structural persisted field encoding

Search for:

IntBig
NatBig
to_leb128
encode_int_big
encode_nat_big
decode_int_big
decode_nat_big
int_big
nat_big
limb
limbs
BigInt
BigUint

Classify every big-int encoding path as:

persisted DB field storage
generic runtime value wire
cursor token
ordered index key
max_bytes validation
display/rendering
test helper

Only change persisted DB field storage unless a path intentionally shares that codec.

Hard-cut persisted field payload encoding

For FieldKind::IntBig { max_bytes } / PersistedFieldKind::IntBig { max_bytes }:

encode:
  bytes = value.to_leb128()
  reject if bytes.len() > max_bytes
  persist bytes directly

decode:
  read persisted bytes
  reject if bytes.len() > max_bytes
  decode with IntBig::from_leb128(bytes)
  reject malformed/non-canonical signed LEB128 if the wrapper/API can detect it

For FieldKind::NatBig { max_bytes } / PersistedFieldKind::NatBig { max_bytes }:

encode:
  bytes = value.to_leb128()
  reject if bytes.len() > max_bytes
  persist bytes directly

decode:
  read persisted bytes
  reject if bytes.len() > max_bytes
  decode with NatBig::from_leb128(bytes)
  reject malformed/non-canonical unsigned LEB128 if the wrapper/API can detect it

Do not store:

structural list headers
limb lists
fixed-width limb values
sign tuples
generic structural Value envelopes

The persisted field payload should be the canonical byte payload itself.

Canonical decode requirements

Decoding must be fail-closed.

Check for:

empty byte payload
overlong/non-minimal encodings
unterminated LEB128 continuation bytes
signed/unsigned mismatch
NatBig negative encodings
payload longer than max_bytes
excessive allocation / denial-of-service risk

If the underlying Candid decode API accepts non-canonical encodings, add a canonicality check:

decoded = from_leb128(bytes)
reencoded = decoded.to_leb128()
require reencoded == bytes

Do this for both IntBig and NatBig unless the Candid API already guarantees canonical minimal decoding.

max_bytes semantics

After this patch, max_bytes means:

maximum canonical Candid LEB128 payload length

not:

structural payload length
limb count
decimal digit length
generic Value wire length

Update code comments, diagnostics, docs, and tests accordingly.

Examples:

nat_big(max_bytes = 1) admits 0..=127 if using unsigned LEB128.
nat_big(max_bytes = 1) rejects 128.
int_big(max_bytes = 1) admits signed values representable in one signed LEB128 byte.
int_big(max_bytes = 1) rejects values requiring two or more signed LEB128 bytes.
Preserve unrelated encodings

Do not accidentally change:

ordered index key encoding for IntBig/NatBig if it has separate ordering semantics
cursor token encoding unless it intentionally uses persisted field storage
external SQL rendering
Candid boundary behavior
Value wire encoding unless currently identical to persisted field storage by design
int128/nat128 native storage
compact primary-key encoding
schema snapshot structural codecs except where they store encoded defaults for big integers

If a generic helper is shared by persisted fields and cursor/wire values, split the helper instead of broadening the storage hard-cut unintentionally.

Defaults and schema snapshots

Update default payloads for:

DDL defaults
derive-generated defaults
accepted schema snapshot defaults
schema check/show/describe paths if they inspect encoded defaults

A default for int_big / nat_big should now be encoded as the same compact canonical LEB128 persisted payload used for row slots.

Ensure old structural default payload paths are removed, not left as compatibility fallbacks.

Tests to add or update

Add focused tests proving compact payload size and behavior:

NatBig:

nat_big(0) stores exactly [0x00]
nat_big(1) stores exactly [0x01]
nat_big(127) stores one byte
nat_big(128) stores two bytes
nat_big(max_bytes = 1) accepts 127 and rejects 128
malformed/unterminated unsigned LEB128 rejects
overlong non-canonical zero rejects if canonicality is enforced

IntBig:

int_big(0) stores exactly [0x00]
int_big(1) stores exactly [0x01]
int_big(-1) stores exactly [0x7f] if using canonical signed LEB128
positive/negative one-byte boundary cases
int_big(max_bytes = 1) accepts one-byte signed range and rejects first two-byte values
malformed/unterminated signed LEB128 rejects
overlong non-canonical signed values reject if canonicality is enforced

Round-trips:

structural persisted field encode/decode roundtrip for small and large values
DDL default roundtrip
derive default roundtrip
accepted snapshot default roundtrip
row save/load roundtrip
generic Value::IntBig/Value::NatBig unaffected if separate
ordered index key semantics unaffected if separate

Regression:

int_big/nat_big remain rejected as primary-key fields
int128/nat128 support remains unchanged
no schema aliases int / nat reintroduced
Audit after implementation

Run searches:

rg "limb|limbs|BigUint|BigInt|to_u32_digits|from_bytes_le|to_bytes_le" crates/icydb-core/src/db/data crates/icydb-core/src/db/schema crates/icydb-schema-derive/src
rg "encode_int_big|decode_int_big|encode_nat_big|decode_nat_big|to_leb128|from_leb128" crates
rg "IntBig|NatBig|max_bytes|int_big|nat_big" crates docs

Classify remaining limb/list structural code:

Acceptable:

ordered key encoding if intentionally limb-based for sort semantics
wrapper internals
tests explicitly checking non-storage behavior

Unacceptable:

persisted row field payloads
DDL/default payloads
accepted default payloads
structural value storage for int_big/nat_big if that is the persisted row storage path
Validation

Run:

cargo fmt --all
cargo check --workspace
cargo clippy -p icydb-core --all-targets -- -D warnings
cargo clippy -p icydb-schema-derive --all-targets -- -D warnings
git diff --check

Run focused tests for:

structural big-int field encode/decode
persisted row big-int save/load
DDL big-int defaults
derive big-int defaults
max_bytes acceptance/rejection
malformed LEB128 decode rejection
ordered key tests to prove no accidental ordering regression
primary-key rejection for int_big/nat_big
Output format

Return:

Summary
Files changed
Confirmation that persisted big-int field payloads now use canonical Candid LEB128 bytes
Confirmation that max_bytes now measures canonical stored LEB128 payload length
Confirmation that int_big/nat_big remain non-primary-key-admissible
Confirmation that int128/nat128 were not changed
Remaining structural limb/list big-int code, classified
Tests added/updated
Validation passed
Skipped checks or failures

Strict non-goals:

No legacy structural big-int decode fallback unless explicitly requested.
No mixed old/new persisted field format.
No primary-key admission for int_big/nat_big.
No Candid wrapper rewrite.
No hand-rolled LEB128 if the existing Candid-backed wrappers already provide canonical encode/decode.
No unrelated numeric terminology cleanup.
ok so this makes max_bytes mean exactly what it should mean regardless of the size on disk?

Yes — and that is the strongest argument for the hard-cut.

Right now your max_bytes is semantically fuzzy because it measures:

size of IcyDB’s current structural representation

which includes:

list headers
tuple headers
limb framing
fixed-width limb payloads
sign wrappers

So:

NatBig(1)

can consume ~14 bytes despite the actual integer payload being representable in one byte.

That means the current max_bytes is really:

max_structural_storage_bytes

not:

max_integer_encoding_bytes

After the LEB128 cutover:

max_bytes == exact canonical persisted byte length

which is much cleaner.

Then the contract becomes precise and intuitive:

nat_big(max_bytes = 1)

literally means:

the stored canonical unsigned integer encoding must fit in 1 byte

Likewise:

int_big(max_bytes = 4)

means:

the canonical signed LEB128 representation must fit in 4 bytes

That gives you several benefits:

1. Storage semantics become honest

Before:

max_bytes ≠ actual integer payload size

After:

max_bytes == exact persisted payload size
2. Deterministic economic semantics

On the IC, bytes matter economically.

After the cut:

max_bytes directly bounds:
- stable memory use
- replication cost
- serialization cost
- backup/export size

without hidden framing overhead.

3. Better developer intuition

People can reason about it naturally:

u64-ish values fit in small max_bytes
huge arbitrary values require larger max_bytes

instead of:

why does the number 1 consume 14 bytes?
4. Cleaner validation model

Current model:

value
-> structural encoding
-> measure structural bytes
-> compare to max_bytes

New model:

value
-> canonical LEB128
-> bytes.len() <= max_bytes

Very easy to audit.

5. Canonical representation

This is the biggest long-term win.

If:

stored bytes == canonical bytes

then:

hashing is cleaner
replication/export is cleaner
corruption detection is easier
canonicality checking is simpler
debugging is easier
future interop is easier

You eliminate:

multiple equivalent encodings
structural overhead ambiguities
limb-layout policy questions

The important caveat:

After this change, max_bytes becomes tied specifically to:

canonical Candid LEB128 length

not:

decimal digit count
magnitude
BigInt heap size
current Rust allocation size

That is good, but it should be documented very explicitly.

I would literally document it as:

For int_big/nat_big fields, max_bytes limits the canonical persisted Candid LEB128 payload length.

That is an excellent invariant.