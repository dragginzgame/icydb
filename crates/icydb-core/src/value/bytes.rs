use crate::value::Value;
use canic_core::utils::hash::Xxh3;

///
/// ValueTag
///
/// Can we remove ValueTag?
/// Yes, technically.
///
/// Should we?
/// Almost certainly no, unless you control all serialization + don’t need hashing + don’t care about stability.
///
/// Why keep it?
/// Binary stability, hashing, sorting, versioning, IC-safe ABI, robustness.
///

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ValueTag {
    Account = 1,
    Blob = 2,
    Bool = 3,
    Date = 4,
    Decimal = 5,
    Duration = 6,
    Enum = 7,
    E8s = 8,
    E18s = 9,
    Float32 = 10,
    Float64 = 11,
    Int = 12,
    Int128 = 13,
    IntBig = 14,
    List = 15,
    None = 16,
    Principal = 17,
    Subaccount = 18,
    Text = 19,
    Timestamp = 20,
    Uint = 21,
    Uint128 = 22,
    UintBig = 23,
    Ulid = 24,
    Unit = 25,
    Unsupported = 26,
}

impl ValueTag {
    #[must_use]
    pub const fn to_u8(self) -> u8 {
        self as u8
    }
}

impl Value {
    ///
    /// HASHING
    ///

    #[must_use]
    pub const fn tag(&self) -> u8 {
        match self {
            Self::Account(_) => ValueTag::Account,
            Self::Blob(_) => ValueTag::Blob,
            Self::Bool(_) => ValueTag::Bool,
            Self::Date(_) => ValueTag::Date,
            Self::Decimal(_) => ValueTag::Decimal,
            Self::Duration(_) => ValueTag::Duration,
            Self::Enum(_) => ValueTag::Enum,
            Self::E8s(_) => ValueTag::E8s,
            Self::E18s(_) => ValueTag::E18s,
            Self::Float32(_) => ValueTag::Float32,
            Self::Float64(_) => ValueTag::Float64,
            Self::Int(_) => ValueTag::Int,
            Self::Int128(_) => ValueTag::Int128,
            Self::IntBig(_) => ValueTag::IntBig,
            Self::List(_) => ValueTag::List,
            Self::None => ValueTag::None,
            Self::Principal(_) => ValueTag::Principal,
            Self::Subaccount(_) => ValueTag::Subaccount,
            Self::Text(_) => ValueTag::Text,
            Self::Timestamp(_) => ValueTag::Timestamp,
            Self::Uint(_) => ValueTag::Uint,
            Self::Uint128(_) => ValueTag::Uint128,
            Self::UintBig(_) => ValueTag::UintBig,
            Self::Ulid(_) => ValueTag::Ulid,
            Self::Unit => ValueTag::Unit,
            Self::Unsupported => ValueTag::Unsupported,
        }
        .to_u8()
    }
}

///
/// Canonical Byte Representation
///

#[inline]
fn feed_i32(h: &mut Xxh3, x: i32) {
    h.update(&x.to_be_bytes());
}
#[inline]
fn feed_i64(h: &mut Xxh3, x: i64) {
    h.update(&x.to_be_bytes());
}
#[inline]
fn feed_i128(h: &mut Xxh3, x: i128) {
    h.update(&x.to_be_bytes());
}
#[inline]
fn feed_u8(h: &mut Xxh3, x: u8) {
    h.update(&[x]);
}
#[inline]
fn feed_u32(h: &mut Xxh3, x: u32) {
    h.update(&x.to_be_bytes());
}
#[inline]
fn feed_u64(h: &mut Xxh3, x: u64) {
    h.update(&x.to_be_bytes());
}
#[inline]
fn feed_u128(h: &mut Xxh3, x: u128) {
    h.update(&x.to_be_bytes());
}

#[inline]
fn feed_bytes(h: &mut Xxh3, b: &[u8]) {
    h.update(b);
}

#[allow(clippy::cast_possible_truncation)]
impl Value {
    ///
    /// Compute a **canonical, deterministic 128-bit fingerprint** of this `Value`.
    ///
    /// This is *not* the same as serializing the value (e.g. with CBOR or Serde) and hashing:
    /// - CBOR is not canonical by default (ints can have multiple encodings, maps can reorder keys, NaN payloads differ, etc.).
    /// - Rust's internal layout is not stable across versions or platforms.
    ///
    /// Instead, we define our own **canonical byte representation**:
    /// - Prefix with a fixed `VERSION` byte to allow evolution of the format.
    /// - Prefix with a `ValueTag` to distinguish enum variants (`Int(1)` vs `Uint(1)`).
    /// - Encode each variant deterministically (e.g. Decimal as sign/scale/mantissa).
    /// - Recurse through lists element-by-element in order.
    ///
    /// ### Why?
    /// - **Stable across upgrades / canisters**: the same logical value always yields the same hash.
    /// - **Indexing**: provides a fixed-size `[u8; 16]` fingerprint for use in secondary indexes
    ///   and fast equality lookups.
    /// - **Canonicalization**: ensures semantically equal values hash identically, avoiding
    ///   “same value, different bytes” bugs.
    ///
    /// Use this in query planning, index scans, and anywhere you need a compact,
    /// reproducible identity for a `Value`.
    ///
    fn write_to_hasher(&self, h: &mut Xxh3) {
        feed_u8(h, self.tag());

        match self {
            Self::Account(a) => {
                feed_bytes(h, &a.to_bytes());
            }
            Self::Blob(v) => {
                feed_u8(h, 0x01);
                feed_bytes(h, v);
            }
            Self::Bool(b) => {
                feed_u8(h, u8::from(*b));
            }
            Self::Date(d) => feed_i32(h, d.get()),
            Self::Decimal(d) => {
                // encode (sign, scale, mantissa) deterministically:
                feed_u8(h, u8::from(d.is_sign_negative()));
                feed_u32(h, d.scale());
                feed_bytes(h, &d.mantissa().to_be_bytes());
            }
            Self::Duration(t) => {
                feed_u64(h, t.get());
            }
            Self::Enum(v) => {
                match &v.path {
                    Some(path) => {
                        feed_u8(h, 0x01); // path present
                        feed_u32(h, path.len() as u32);
                        feed_bytes(h, path.as_bytes());
                    }
                    None => feed_u8(h, 0x00), // path absent → loose match
                }

                feed_u32(h, v.variant.len() as u32);
                feed_bytes(h, v.variant.as_bytes());

                match &v.payload {
                    Some(payload) => {
                        feed_u8(h, 0x01); // payload present
                        payload.write_to_hasher(h); // include nested value
                    }
                    None => feed_u8(h, 0x00),
                }
            }
            Self::E8s(v) => {
                feed_u64(h, v.get());
            }
            Self::E18s(v) => {
                feed_bytes(h, &v.to_be_bytes());
            }
            Self::Float32(v) => {
                feed_bytes(h, &v.to_be_bytes());
            }
            Self::Float64(v) => {
                feed_bytes(h, &v.to_be_bytes());
            }
            Self::Int(i) => {
                feed_i64(h, *i);
            }
            Self::Int128(i) => {
                feed_i128(h, i.get());
            }
            Self::IntBig(v) => {
                feed_bytes(h, &v.to_leb128());
            }
            Self::List(xs) => {
                feed_u32(h, xs.len() as u32);
                for x in xs {
                    feed_u8(h, 0xFF);
                    x.write_to_hasher(h); // recurse, no sub-hash
                }
            }
            Self::Principal(p) => {
                let raw = p.as_slice();
                feed_u32(h, raw.len() as u32);
                feed_bytes(h, raw);
            }
            Self::Subaccount(s) => {
                feed_bytes(h, &s.to_bytes());
            }
            Self::Text(s) => {
                // If you need case/Unicode insensitivity, normalize; else skip (much faster)
                // let norm = normalize_nfkc_casefold(s);
                // feed_u32( h, norm.len() as u32);
                // feed_bytes( h, norm.as_bytes());
                feed_u32(h, s.len() as u32);
                feed_bytes(h, s.as_bytes());
            }
            Self::Timestamp(t) => {
                feed_u64(h, t.get());
            }
            Self::Uint(u) => {
                feed_u64(h, *u);
            }
            Self::Uint128(u) => {
                feed_u128(h, u.get());
            }
            Self::UintBig(v) => {
                feed_bytes(h, &v.to_leb128());
            }
            Self::Ulid(u) => {
                feed_bytes(h, &u.to_bytes());
            }
            Self::None | Self::Unit | Self::Unsupported => {}
        }
    }

    #[must_use]
    /// Stable hash used for index/storage fingerprints.
    pub fn hash_value(&self) -> [u8; 16] {
        const VERSION: u8 = 1;

        let mut h = Xxh3::with_seed(0);
        feed_u8(&mut h, VERSION); // version

        self.write_to_hasher(&mut h);
        h.digest128().to_be_bytes()
    }
}
