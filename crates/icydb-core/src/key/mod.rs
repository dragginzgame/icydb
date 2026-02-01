mod convert;
#[cfg(test)]
mod tests;

use crate::{
    error::{ErrorClass, ErrorOrigin, InternalError},
    types::{
        Account, AccountEncodeError, Principal, PrincipalEncodeError, Subaccount, Timestamp, Ulid,
    },
};
use candid::CandidType;
use derive_more::Display;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use thiserror::Error as ThisError;

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

///
/// KeyEncodeError
///
/// Errors returned when encoding a key for persistence.
///

#[derive(Debug, ThisError)]
pub enum KeyEncodeError {
    #[error("account encoding failed: {0}")]
    Account(#[from] AccountEncodeError),

    #[error("account payload length mismatch: {len} bytes (expected {expected})")]
    AccountLengthMismatch { len: usize, expected: usize },

    #[error("principal encoding failed: {0}")]
    Principal(#[from] PrincipalEncodeError),
}

impl From<KeyEncodeError> for InternalError {
    fn from(err: KeyEncodeError) -> Self {
        Self::new(
            ErrorClass::Unsupported,
            ErrorOrigin::Serialize,
            err.to_string(),
        )
    }
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

    /// Fixed serialized size in bytes (stable, protocol-level)
    /// DO NOT CHANGE without migration.
    pub const STORED_SIZE_BYTES: u64 = 64;

    /// Fixed in-memory size (for buffers and indexing only)
    #[expect(clippy::cast_possible_truncation)]
    pub const STORED_SIZE_USIZE: usize = Self::STORED_SIZE_BYTES as usize;

    // ── Layout ─────────────────────────────────────
    const TAG_SIZE: usize = 1;
    pub(crate) const TAG_OFFSET: usize = 0;

    pub(crate) const PAYLOAD_OFFSET: usize = Self::TAG_SIZE;
    const PAYLOAD_SIZE: usize = Self::STORED_SIZE_USIZE - Self::TAG_SIZE;

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

    /// Global minimum key for scan bounds and key range construction.
    pub const MIN: Self = Self::Account(Account {
        owner: Principal::from_slice(&[]),
        subaccount: None,
    });

    #[must_use]
    pub const fn lower_bound() -> Self {
        Self::MIN
    }

    #[must_use]
    pub const fn upper_bound() -> Self {
        Self::Unit
    }

    const fn variant_rank(&self) -> u8 {
        self.tag()
    }

    /// Encode this key into its fixed-size storage representation.
    pub fn to_bytes(&self) -> Result<[u8; Self::STORED_SIZE_USIZE], KeyEncodeError> {
        let mut buf = [0u8; Self::STORED_SIZE_USIZE];

        // ── Tag ─────────────────────────────────────
        buf[Self::TAG_OFFSET] = self.tag();
        let payload = &mut buf[Self::PAYLOAD_OFFSET..=Self::PAYLOAD_SIZE];

        // ── Payload ─────────────────────────────────
        #[allow(clippy::cast_possible_truncation)]
        match self {
            Self::Account(v) => {
                let bytes = v.to_bytes()?;
                if bytes.len() != Self::ACCOUNT_MAX_SIZE {
                    return Err(KeyEncodeError::AccountLengthMismatch {
                        len: bytes.len(),
                        expected: Self::ACCOUNT_MAX_SIZE,
                    });
                }
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
                let bytes = v.to_bytes()?;
                let len = bytes.len();
                payload[0] = u8::try_from(len).map_err(|_| {
                    KeyEncodeError::Principal(PrincipalEncodeError::TooLarge {
                        len,
                        max: Principal::MAX_LENGTH_IN_BYTES as usize,
                    })
                })?;
                if len > 0 {
                    payload[1..=len].copy_from_slice(&bytes[..len]);
                }
            }

            Self::Subaccount(v) => {
                let bytes = v.to_array();
                payload[..Self::SUBACCOUNT_SIZE].copy_from_slice(&bytes);
            }

            Self::Ulid(v) => {
                payload[..Self::ULID_SIZE].copy_from_slice(&v.to_bytes());
            }

            Self::Unit => {}
        }

        Ok(buf)
    }

    pub fn try_from_bytes(bytes: &[u8]) -> Result<Self, &'static str> {
        if bytes.len() != Self::STORED_SIZE_USIZE {
            return Err("corrupted Key: invalid size");
        }

        let tag = bytes[Self::TAG_OFFSET];
        let payload = &bytes[Self::PAYLOAD_OFFSET..=Self::PAYLOAD_SIZE];

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
                if len > Principal::MAX_LENGTH_IN_BYTES as usize {
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

impl TryFrom<&[u8]> for Key {
    type Error = &'static str;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        Self::try_from_bytes(bytes)
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
