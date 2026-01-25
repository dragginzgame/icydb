mod convert;
#[cfg(test)]
mod tests;

use crate::types::{Account, Principal, Subaccount, Timestamp, Ulid};
use candid::CandidType;
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
    pub(crate) const TAG_ACCOUNT: u8 = 0;
    pub(crate) const TAG_INT: u8 = 1;
    pub(crate) const TAG_PRINCIPAL: u8 = 2;
    pub(crate) const TAG_SUBACCOUNT: u8 = 3;
    pub(crate) const TAG_TIMESTAMP: u8 = 4;
    pub(crate) const TAG_UINT: u8 = 5;
    pub(crate) const TAG_ULID: u8 = 6;
    pub(crate) const TAG_UNIT: u8 = 7;

    /// Fixed serialized size (do not change without migration)
    pub const STORED_SIZE: usize = 64;

    // ── Layout ─────────────────────────────────────
    const TAG_SIZE: usize = 1;
    pub(crate) const TAG_OFFSET: usize = 0;

    pub(crate) const PAYLOAD_OFFSET: usize = Self::TAG_SIZE;
    const PAYLOAD_SIZE: usize = Self::STORED_SIZE - Self::TAG_SIZE;

    // ── Payload sizes ──────────────────────────────
    pub(crate) const INT_SIZE: usize = 8;
    pub(crate) const UINT_SIZE: usize = 8;
    pub(crate) const TIMESTAMP_SIZE: usize = 8;
    pub(crate) const ULID_SIZE: usize = 16;
    pub(crate) const SUBACCOUNT_SIZE: usize = 32;
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
