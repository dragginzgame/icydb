use crate::{
    traits::FieldValue,
    types::{Account, Principal, Subaccount, Timestamp, Ulid, Unit},
    value::Value,
};
use candid::{CandidType, Principal as WrappedPrincipal};
use derive_more::Display;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

///
/// Key
///
/// Treating IndexKey as the atomic, normalized unit of the keyspace
/// Backing primary keys and secondary indexes with the same value representation
/// Planning to enforce Copy semantics (i.e., fast, clean, safe)
///

#[derive(CandidType, Clone, Copy, Debug, Deserialize, Display, Eq, Hash, PartialEq, Serialize)]
pub enum Key {
    Account(Account),
    Int(i64),
    Principal(Principal),
    Subaccount(Subaccount),
    Timestamp(Timestamp),
    Uint(u64),
    Ulid(Ulid),
    Unit,
}

impl Key {
    // ── Variant tags (do not reorder) ─────────────────
    const TAG_ACCOUNT: u8 = 0;
    const TAG_INT: u8 = 1;
    const TAG_PRINCIPAL: u8 = 2;
    const TAG_SUBACCOUNT: u8 = 3;
    const TAG_TIMESTAMP: u8 = 4;
    const TAG_UINT: u8 = 5;
    const TAG_ULID: u8 = 6;
    const TAG_UNIT: u8 = 7;

    /// Fixed serialized size (do not change without migration)
    pub const STORED_SIZE: usize = 64;

    // ── Layout ─────────────────────────────────────
    const TAG_SIZE: usize = 1;
    const TAG_OFFSET: usize = 0;

    const PAYLOAD_OFFSET: usize = Self::TAG_SIZE;
    const PAYLOAD_SIZE: usize = Self::STORED_SIZE - Self::TAG_SIZE;

    // ── Payload sizes ──────────────────────────────
    const INT_SIZE: usize = 8;
    const UINT_SIZE: usize = 8;
    const TIMESTAMP_SIZE: usize = 8;
    const ULID_SIZE: usize = 16;
    const SUBACCOUNT_SIZE: usize = 32;
    const ACCOUNT_MAX_SIZE: usize = 62;

    const fn tag(&self) -> u8 {
        match self {
            Self::Account(_) => Self::TAG_ACCOUNT,
            Self::Int(_) => Self::TAG_INT,
            Self::Principal(_) => Self::TAG_PRINCIPAL,
            Self::Subaccount(_) => Self::TAG_SUBACCOUNT,
            Self::Timestamp(_) => Self::TAG_TIMESTAMP,
            Self::Uint(_) => Self::TAG_UINT,
            Self::Ulid(_) => Self::TAG_ULID,
            Self::Unit => Self::TAG_UNIT,
        }
    }

    #[must_use]
    /// Sentinel key representing the maximum storable account value.
    pub fn max_storable() -> Self {
        Self::Account(Account::max_storable())
    }

    #[must_use]
    pub const fn lower_bound() -> Self {
        Self::Int(i64::MIN)
    }

    #[must_use]
    pub const fn upper_bound() -> Self {
        Self::Unit
    }

    const fn variant_rank(&self) -> u8 {
        self.tag()
    }

    #[must_use]
    pub fn to_bytes(&self) -> [u8; Self::STORED_SIZE] {
        let mut buf = [0u8; Self::STORED_SIZE];

        // ── Tag ─────────────────────────────────────
        buf[Self::TAG_OFFSET] = self.tag();
        let payload = &mut buf[Self::PAYLOAD_OFFSET..];

        debug_assert_eq!(payload.len(), Self::PAYLOAD_SIZE);

        // ── Payload ─────────────────────────────────
        #[allow(clippy::cast_possible_truncation)]
        match self {
            Self::Account(v) => {
                let bytes = v.to_bytes();
                debug_assert_eq!(bytes.len(), Self::ACCOUNT_MAX_SIZE);
                payload[..bytes.len()].copy_from_slice(&bytes);
            }

            Self::Int(v) => {
                // Flip sign bit to preserve ordering in lexicographic bytes.
                let biased = (*v).cast_unsigned() ^ (1u64 << 63);
                payload[..Self::INT_SIZE].copy_from_slice(&biased.to_be_bytes());
            }

            Self::Uint(v) => {
                payload[..Self::UINT_SIZE].copy_from_slice(&v.to_be_bytes());
            }

            Self::Timestamp(v) => {
                payload[..Self::TIMESTAMP_SIZE].copy_from_slice(&v.get().to_be_bytes());
            }

            Self::Principal(v) => {
                let bytes = v.to_bytes();
                let len = bytes.len();
                assert!(
                    (1..=Principal::MAX_LENGTH_IN_BYTES as usize).contains(&len),
                    "invalid Key principal length"
                );
                payload[0] = len as u8;
                if len > 0 {
                    payload[1..=len].copy_from_slice(&bytes[..len]);
                }
            }

            Self::Subaccount(v) => {
                let bytes = v.to_array();
                debug_assert_eq!(bytes.len(), Self::SUBACCOUNT_SIZE);
                payload[..Self::SUBACCOUNT_SIZE].copy_from_slice(&bytes);
            }

            Self::Ulid(v) => {
                payload[..Self::ULID_SIZE].copy_from_slice(&v.to_bytes());
            }

            Self::Unit => {}
        }

        buf
    }

    pub fn try_from_bytes(bytes: &[u8]) -> Result<Self, &'static str> {
        if bytes.len() != Self::STORED_SIZE {
            return Err("corrupted Key: invalid size");
        }

        let tag = bytes[Self::TAG_OFFSET];
        let payload = &bytes[Self::PAYLOAD_OFFSET..];

        let ensure_zero_padding = |used: usize, context: &str| {
            if payload[used..].iter().all(|&b| b == 0) {
                Ok(())
            } else {
                Err(match context {
                    "account" => "corrupted Key: non-zero account padding",
                    "int" => "corrupted Key: non-zero int padding",
                    "principal" => "corrupted Key: non-zero principal padding",
                    "subaccount" => "corrupted Key: non-zero subaccount padding",
                    "timestamp" => "corrupted Key: non-zero timestamp padding",
                    "uint" => "corrupted Key: non-zero uint padding",
                    "ulid" => "corrupted Key: non-zero ulid padding",
                    "unit" => "corrupted Key: non-zero unit padding",
                    _ => "corrupted Key: non-zero padding",
                })
            }
        };

        match tag {
            Self::TAG_ACCOUNT => {
                let end = Account::STORED_SIZE as usize;
                ensure_zero_padding(end, "account")?;
                let account = Account::try_from_bytes(&payload[..end])?;
                Ok(Self::Account(account))
            }

            Self::TAG_INT => {
                let mut buf = [0u8; Self::INT_SIZE];
                buf.copy_from_slice(&payload[..Self::INT_SIZE]);
                let biased = u64::from_be_bytes(buf);
                ensure_zero_padding(Self::INT_SIZE, "int")?;
                Ok(Self::Int((biased ^ (1u64 << 63)).cast_signed()))
            }

            Self::TAG_PRINCIPAL => {
                let len = payload[0] as usize;
                if !(1..=Principal::MAX_LENGTH_IN_BYTES as usize).contains(&len) {
                    return Err("corrupted Key: invalid principal length");
                }
                let end = 1 + len;
                ensure_zero_padding(end, "principal")?;
                Ok(Self::Principal(Principal::from_slice(&payload[1..end])))
            }

            Self::TAG_SUBACCOUNT => {
                let mut buf = [0u8; Self::SUBACCOUNT_SIZE];
                buf.copy_from_slice(&payload[..Self::SUBACCOUNT_SIZE]);
                ensure_zero_padding(Self::SUBACCOUNT_SIZE, "subaccount")?;
                Ok(Self::Subaccount(Subaccount::from_array(buf)))
            }

            Self::TAG_TIMESTAMP => {
                let mut buf = [0u8; Self::TIMESTAMP_SIZE];
                buf.copy_from_slice(&payload[..Self::TIMESTAMP_SIZE]);
                ensure_zero_padding(Self::TIMESTAMP_SIZE, "timestamp")?;
                Ok(Self::Timestamp(Timestamp::from(u64::from_be_bytes(buf))))
            }

            Self::TAG_UINT => {
                let mut buf = [0u8; Self::UINT_SIZE];
                buf.copy_from_slice(&payload[..Self::UINT_SIZE]);
                ensure_zero_padding(Self::UINT_SIZE, "uint")?;
                Ok(Self::Uint(u64::from_be_bytes(buf)))
            }

            Self::TAG_ULID => {
                let mut buf = [0u8; Self::ULID_SIZE];
                buf.copy_from_slice(&payload[..Self::ULID_SIZE]);
                ensure_zero_padding(Self::ULID_SIZE, "ulid")?;
                Ok(Self::Ulid(Ulid::from_bytes(buf)))
            }

            Self::TAG_UNIT => {
                ensure_zero_padding(0, "unit")?;
                Ok(Self::Unit)
            }

            _ => Err("corrupted Key: invalid tag"),
        }
    }
}

impl FieldValue for Key {
    fn to_value(&self) -> Value {
        match self {
            Self::Account(v) => Value::Account(*v),
            Self::Int(v) => Value::Int(*v),
            Self::Principal(v) => Value::Principal(*v),
            Self::Subaccount(v) => Value::Subaccount(*v),
            Self::Timestamp(v) => Value::Timestamp(*v),
            Self::Uint(v) => Value::Uint(*v),
            Self::Ulid(v) => Value::Ulid(*v),
            Self::Unit => Value::Unit,
        }
    }
}

impl From<()> for Key {
    fn from((): ()) -> Self {
        Self::Unit
    }
}

impl From<Unit> for Key {
    fn from(_: Unit) -> Self {
        Self::Unit
    }
}

impl PartialEq<()> for Key {
    fn eq(&self, (): &()) -> bool {
        matches!(self, Self::Unit)
    }
}

impl PartialEq<Key> for () {
    fn eq(&self, other: &Key) -> bool {
        other == self
    }
}

/// Implements `From<T> for Key` for simple conversions
macro_rules! impl_from_key {
    ( $( $ty:ty => $variant:ident ),* $(,)? ) => {
        $(
            impl From<$ty> for Key {
                fn from(v: $ty) -> Self {
                    Self::$variant(v.into())
                }
            }
        )*
    }
}

/// Implements symmetric PartialEq between Key and another type
macro_rules! impl_eq_key {
    ( $( $ty:ty => $variant:ident ),* $(,)? ) => {
        $(
            impl PartialEq<$ty> for Key {
                fn eq(&self, other: &$ty) -> bool {
                    matches!(self, Self::$variant(val) if val == other)
                }
            }

            impl PartialEq<Key> for $ty {
                fn eq(&self, other: &Key) -> bool {
                    other == self
                }
            }
        )*
    }
}

impl_from_key! {
    Account => Account,
    i8  => Int,
    i16 => Int,
    i32 => Int,
    i64 => Int,
    Principal => Principal,
    WrappedPrincipal => Principal,
    Subaccount => Subaccount,
    Timestamp => Timestamp,
    u8  => Uint,
    u16 => Uint,
    u32 => Uint,
    u64 => Uint,
    Ulid => Ulid,
}

impl_eq_key! {
    Account => Account,
    i64 => Int,
    Principal => Principal,
    Subaccount => Subaccount,
    Timestamp => Timestamp,
    u64  => Uint,
    Ulid => Ulid,
}

impl Ord for Key {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Account(a), Self::Account(b)) => Ord::cmp(a, b),
            (Self::Int(a), Self::Int(b)) => Ord::cmp(a, b),
            (Self::Principal(a), Self::Principal(b)) => Ord::cmp(a, b),
            (Self::Uint(a), Self::Uint(b)) => Ord::cmp(a, b),
            (Self::Ulid(a), Self::Ulid(b)) => Ord::cmp(a, b),
            (Self::Subaccount(a), Self::Subaccount(b)) => Ord::cmp(a, b),
            (Self::Timestamp(a), Self::Timestamp(b)) => Ord::cmp(a, b),

            _ => Ord::cmp(&self.variant_rank(), &other.variant_rank()), // fallback for cross-type comparison
        }
    }
}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(Ord::cmp(self, other))
    }
}
///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_max_size_is_bounded() {
        let key = Key::max_storable();
        let size = key.to_bytes().len();

        assert!(
            size <= Key::STORED_SIZE,
            "serialized Key too large: got {size} bytes (limit {})",
            Key::STORED_SIZE
        );
    }

    #[test]
    fn key_storable_round_trip() {
        let keys = [
            Key::Account(Account::dummy(1)),
            Key::Int(-42),
            Key::Principal(Principal::from_slice(&[1, 2, 3])),
            Key::Subaccount(Subaccount::from_array([7; 32])),
            Key::Timestamp(Timestamp::from_seconds(42)),
            Key::Uint(42),
            Key::Ulid(Ulid::from_bytes([9; 16])),
            Key::Unit,
        ];

        for key in keys {
            let bytes = key.to_bytes();
            let decoded = Key::try_from_bytes(&bytes).unwrap();

            assert_eq!(decoded, key, "Key round trip failed for {key:?}");
        }
    }

    #[test]
    fn key_is_exactly_fixed_size() {
        let keys = [
            Key::Account(Account::dummy(1)),
            Key::Int(0),
            Key::Principal(Principal::anonymous()),
            Key::Subaccount(Subaccount::from_array([0; 32])),
            Key::Timestamp(Timestamp::from_seconds(0)),
            Key::Uint(0),
            Key::Ulid(Ulid::from_bytes([0; 16])),
            Key::Unit,
        ];

        for key in keys {
            let len = key.to_bytes().len();
            assert_eq!(
                len,
                Key::STORED_SIZE,
                "Key serialized length must be exactly {}",
                Key::STORED_SIZE
            );
        }
    }

    #[test]
    fn key_ordering_is_total_and_stable() {
        let keys = vec![
            Key::Account(Account::new(
                Principal::from_slice(&[1]),
                None::<Subaccount>,
            )),
            Key::Account(Account::new(Principal::from_slice(&[1]), Some([0u8; 32]))),
            Key::Int(-1),
            Key::Int(0),
            Key::Principal(Principal::from_slice(&[1])),
            Key::Subaccount(Subaccount::from_array([1; 32])),
            Key::Uint(0),
            Key::Uint(1),
            Key::Timestamp(Timestamp::from_seconds(1)),
            Key::Ulid(Ulid::from_bytes([9; 16])),
            Key::Unit,
        ];

        let mut sorted_by_ord = keys.clone();
        sorted_by_ord.sort();

        let mut sorted_by_bytes = keys;
        sorted_by_bytes.sort_by_key(Key::to_bytes);

        assert_eq!(
            sorted_by_ord, sorted_by_bytes,
            "Key Ord and byte ordering diverged"
        );
    }

    #[test]
    fn key_from_bytes_rejects_undersized() {
        let bytes = vec![0u8; Key::STORED_SIZE - 1];
        assert!(Key::try_from_bytes(&bytes).is_err());
    }

    #[test]
    fn key_from_bytes_rejects_oversized() {
        let bytes = vec![0u8; Key::STORED_SIZE + 1];
        assert!(Key::try_from_bytes(&bytes).is_err());
    }

    #[test]
    fn key_from_bytes_rejects_zero_principal_len() {
        let mut bytes = Key::Principal(Principal::from_slice(&[1])).to_bytes();
        bytes[Key::TAG_OFFSET] = Key::TAG_PRINCIPAL;
        bytes[Key::PAYLOAD_OFFSET] = 0;
        assert!(Key::try_from_bytes(&bytes).is_err());
    }

    #[test]
    #[allow(clippy::cast_possible_truncation)]
    fn key_from_bytes_rejects_oversized_principal_len() {
        let mut bytes = Key::Principal(Principal::from_slice(&[1])).to_bytes();
        bytes[Key::TAG_OFFSET] = Key::TAG_PRINCIPAL;
        bytes[Key::PAYLOAD_OFFSET] = (Principal::MAX_LENGTH_IN_BYTES as u8) + 1;
        assert!(Key::try_from_bytes(&bytes).is_err());
    }

    #[test]
    fn key_from_bytes_rejects_principal_padding() {
        let mut bytes = Key::Principal(Principal::from_slice(&[1])).to_bytes();
        bytes[Key::TAG_OFFSET] = Key::TAG_PRINCIPAL;
        bytes[Key::PAYLOAD_OFFSET] = 1;
        bytes[Key::PAYLOAD_OFFSET + 2] = 1;
        assert!(Key::try_from_bytes(&bytes).is_err());
    }

    #[test]
    fn key_from_bytes_rejects_account_padding() {
        let mut bytes = Key::Account(Account::new(
            Principal::from_slice(&[1]),
            None::<Subaccount>,
        ))
        .to_bytes();
        bytes[Key::TAG_OFFSET] = Key::TAG_ACCOUNT;
        bytes[Key::PAYLOAD_OFFSET + Account::STORED_SIZE as usize] = 1;
        assert!(Key::try_from_bytes(&bytes).is_err());
    }

    #[test]
    fn key_from_bytes_rejects_int_padding() {
        let mut bytes = Key::Int(0).to_bytes();
        bytes[Key::TAG_OFFSET] = Key::TAG_INT;
        bytes[Key::PAYLOAD_OFFSET + Key::INT_SIZE] = 1;
        assert!(Key::try_from_bytes(&bytes).is_err());
    }

    #[test]
    fn key_from_bytes_rejects_uint_padding() {
        let mut bytes = Key::Uint(0).to_bytes();
        bytes[Key::TAG_OFFSET] = Key::TAG_UINT;
        bytes[Key::PAYLOAD_OFFSET + Key::UINT_SIZE] = 1;
        assert!(Key::try_from_bytes(&bytes).is_err());
    }

    #[test]
    fn key_from_bytes_rejects_timestamp_padding() {
        let mut bytes = Key::Timestamp(Timestamp::from_seconds(0)).to_bytes();
        bytes[Key::TAG_OFFSET] = Key::TAG_TIMESTAMP;
        bytes[Key::PAYLOAD_OFFSET + Key::TIMESTAMP_SIZE] = 1;
        assert!(Key::try_from_bytes(&bytes).is_err());
    }

    #[test]
    fn key_from_bytes_rejects_subaccount_padding() {
        let mut bytes = Key::Subaccount(Subaccount::from_array([0; 32])).to_bytes();
        bytes[Key::TAG_OFFSET] = Key::TAG_SUBACCOUNT;
        bytes[Key::PAYLOAD_OFFSET + Key::SUBACCOUNT_SIZE] = 1;
        assert!(Key::try_from_bytes(&bytes).is_err());
    }

    #[test]
    fn key_from_bytes_rejects_ulid_padding() {
        let mut bytes = Key::Ulid(Ulid::from_bytes([0; 16])).to_bytes();
        bytes[Key::TAG_OFFSET] = Key::TAG_ULID;
        bytes[Key::PAYLOAD_OFFSET + Key::ULID_SIZE] = 1;
        assert!(Key::try_from_bytes(&bytes).is_err());
    }

    #[test]
    fn key_from_bytes_rejects_unit_padding() {
        let mut bytes = Key::Unit.to_bytes();
        bytes[Key::TAG_OFFSET] = Key::TAG_UNIT;
        bytes[Key::PAYLOAD_OFFSET] = 1;
        assert!(Key::try_from_bytes(&bytes).is_err());
    }

    #[test]
    fn principal_encoding_respects_max_size() {
        let max = Principal::from_slice(&[0xFF; 29]);
        let key = Key::Principal(max);

        let bytes = key.to_bytes();
        assert_eq!(bytes.len(), Key::STORED_SIZE);
    }

    #[test]
    #[allow(clippy::cast_possible_truncation)]
    fn key_decode_fuzz_roundtrip_is_canonical() {
        const RUNS: u64 = 1_000;

        let mut seed = 0x1234_5678_u64;
        for _ in 0..RUNS {
            let mut bytes = [0u8; Key::STORED_SIZE];
            for b in &mut bytes {
                seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                *b = (seed >> 24) as u8;
            }

            if let Ok(decoded) = Key::try_from_bytes(&bytes) {
                let re = decoded.to_bytes();
                assert_eq!(bytes, re, "decoded key must be canonical");
            }
        }
    }
}
