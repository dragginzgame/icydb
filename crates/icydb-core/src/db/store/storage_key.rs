//! StorageKey is a fixed-width, ordered, storage-normalized scalar used
//! exclusively by the storage and indexing layers.
//!
//! It MUST NOT be used as an identity or primary key abstraction.
//! Typed identity is represented by Ref<E>.

#![expect(clippy::cast_possible_truncation)]

use crate::{
    error::{ErrorClass, ErrorOrigin, InternalError},
    types::{
        Account, AccountEncodeError, Principal, PrincipalEncodeError, Subaccount, Timestamp, Ulid,
    },
    value::Value,
};
use candid::CandidType;
use derive_more::Display;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use thiserror::Error as ThisError;

///
/// StorageKeyEncodeError
/// Errors returned when encoding a storage key for persistence.
///

#[derive(Debug, ThisError)]
pub enum StorageKeyEncodeError {
    #[error("account encoding failed: {0}")]
    Account(#[from] AccountEncodeError),

    #[error("account payload length mismatch: {len} bytes (expected {expected})")]
    AccountLengthMismatch { len: usize, expected: usize },

    #[error("principal encoding failed: {0}")]
    Principal(#[from] PrincipalEncodeError),
}

impl From<StorageKeyEncodeError> for InternalError {
    fn from(err: StorageKeyEncodeError) -> Self {
        Self::new(
            ErrorClass::Unsupported,
            ErrorOrigin::Serialize,
            err.to_string(),
        )
    }
}

///
/// StorageKey
///
/// Storage-normalized scalar key used by persistence and indexing.
///
/// This type defines the *only* on-disk representation for scalar keys.
/// It is deliberately separated from typed identity (`Ref<E>`).
///

#[derive(CandidType, Clone, Copy, Debug, Deserialize, Display, Eq, Hash, PartialEq, Serialize)]
pub enum StorageKey {
    Account(Account),
    Int(i64),
    Principal(Principal),
    Subaccount(Subaccount),
    Timestamp(Timestamp),
    Uint(u64),
    Ulid(Ulid),
    Unit,
}

impl StorageKey {
    // ── Variant tags (DO NOT reorder) ────────────────────────────────
    pub(crate) const TAG_ACCOUNT: u8 = 0;
    pub(crate) const TAG_INT: u8 = 1;
    pub(crate) const TAG_PRINCIPAL: u8 = 2;
    pub(crate) const TAG_SUBACCOUNT: u8 = 3;
    pub(crate) const TAG_TIMESTAMP: u8 = 4;
    pub(crate) const TAG_UINT: u8 = 5;
    pub(crate) const TAG_ULID: u8 = 6;
    pub(crate) const TAG_UNIT: u8 = 7;

    /// Fixed serialized size in bytes (protocol invariant).
    /// DO NOT CHANGE without migration.
    pub const STORED_SIZE_BYTES: u64 = 64;
    pub const STORED_SIZE_USIZE: usize = Self::STORED_SIZE_BYTES as usize;

    const TAG_SIZE: usize = 1;
    pub(crate) const TAG_OFFSET: usize = 0;

    pub(crate) const PAYLOAD_OFFSET: usize = Self::TAG_SIZE;
    const PAYLOAD_SIZE: usize = Self::STORED_SIZE_USIZE - Self::TAG_SIZE;

    pub(crate) const INT_SIZE: usize = 8;
    pub(crate) const UINT_SIZE: usize = 8;
    pub(crate) const TIMESTAMP_SIZE: usize = 8;
    pub(crate) const ULID_SIZE: usize = 16;
    pub(crate) const SUBACCOUNT_SIZE: usize = 32;
    const ACCOUNT_MAX_SIZE: usize = 62;

    pub const fn try_from_value(value: &Value) -> Result<Self, StorageKeyEncodeError> {
        match value {
            Value::Account(v) => Ok(Self::Account(*v)),
            Value::Int(v) => Ok(Self::Int(*v)),
            Value::Principal(v) => Ok(Self::Principal(*v)),
            Value::Subaccount(v) => Ok(Self::Subaccount(*v)),
            Value::Timestamp(v) => Ok(Self::Timestamp(*v)),
            Value::Uint(v) => Ok(Self::Uint(*v)),
            Value::Ulid(v) => Ok(Self::Ulid(*v)),
            Value::Unit => Ok(Self::Unit),

            // Everything else is *not* storage-key compatible
            _ => Err(StorageKeyEncodeError::AccountLengthMismatch {
                // pick a better error variant if you want;
                // or introduce a new UnsupportedValue variant
                len: 0,
                expected: 0,
            }),
        }
    }

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

    /// Sentinel key representing the maximum storable value.
    #[must_use]
    pub fn max_storable() -> Self {
        Self::Account(Account::max_storable())
    }

    /// Global minimum key for scan bounds.
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

    /// Encode this key into its fixed-size on-disk representation.
    pub fn to_bytes(&self) -> Result<[u8; Self::STORED_SIZE_USIZE], StorageKeyEncodeError> {
        let mut buf = [0u8; Self::STORED_SIZE_USIZE];
        buf[Self::TAG_OFFSET] = self.tag();
        let payload = &mut buf[Self::PAYLOAD_OFFSET..=Self::PAYLOAD_SIZE];

        match self {
            Self::Account(v) => {
                let bytes = v.to_bytes()?;
                if bytes.len() != Self::ACCOUNT_MAX_SIZE {
                    return Err(StorageKeyEncodeError::AccountLengthMismatch {
                        len: bytes.len(),
                        expected: Self::ACCOUNT_MAX_SIZE,
                    });
                }
                payload[..bytes.len()].copy_from_slice(&bytes);
            }
            Self::Int(v) => {
                let biased = (*v).cast_unsigned() ^ (1u64 << 63);
                payload[..Self::INT_SIZE].copy_from_slice(&biased.to_be_bytes());
            }
            Self::Uint(v) => payload[..Self::UINT_SIZE].copy_from_slice(&v.to_be_bytes()),
            Self::Timestamp(v) => {
                payload[..Self::TIMESTAMP_SIZE].copy_from_slice(&v.get().to_be_bytes());
            }
            Self::Principal(v) => {
                let bytes = v.to_bytes()?;
                let len = bytes.len();
                payload[0] = u8::try_from(len).map_err(|_| {
                    StorageKeyEncodeError::Principal(PrincipalEncodeError::TooLarge {
                        len,
                        max: Principal::MAX_LENGTH_IN_BYTES as usize,
                    })
                })?;
                payload[1..=len].copy_from_slice(&bytes[..len]);
            }
            Self::Subaccount(v) => payload[..Self::SUBACCOUNT_SIZE].copy_from_slice(&v.to_array()),
            Self::Ulid(v) => payload[..Self::ULID_SIZE].copy_from_slice(&v.to_bytes()),
            Self::Unit => {}
        }

        Ok(buf)
    }

    pub fn try_from_bytes(bytes: &[u8]) -> Result<Self, &'static str> {
        if bytes.len() != Self::STORED_SIZE_USIZE {
            return Err("corrupted StorageKey: invalid size");
        }

        let tag = bytes[Self::TAG_OFFSET];
        let payload = &bytes[Self::PAYLOAD_OFFSET..=Self::PAYLOAD_SIZE];

        let ensure_zero_padding = |used: usize, ctx: &str| {
            if payload[used..].iter().all(|&b| b == 0) {
                Ok(())
            } else {
                Err(match ctx {
                    "account" => "corrupted StorageKey: non-zero account padding",
                    "int" => "corrupted StorageKey: non-zero int padding",
                    "principal" => "corrupted StorageKey: non-zero principal padding",
                    "subaccount" => "corrupted StorageKey: non-zero subaccount padding",
                    "timestamp" => "corrupted StorageKey: non-zero timestamp padding",
                    "uint" => "corrupted StorageKey: non-zero uint padding",
                    "ulid" => "corrupted StorageKey: non-zero ulid padding",
                    "unit" => "corrupted StorageKey: non-zero unit padding",
                    _ => "corrupted StorageKey: non-zero padding",
                })
            }
        };

        match tag {
            Self::TAG_ACCOUNT => {
                let end = Account::STORED_SIZE as usize;
                ensure_zero_padding(end, "account")?;
                Ok(Self::Account(Account::try_from_bytes(&payload[..end])?))
            }
            Self::TAG_INT => {
                let mut buf = [0u8; Self::INT_SIZE];
                buf.copy_from_slice(&payload[..Self::INT_SIZE]);
                ensure_zero_padding(Self::INT_SIZE, "int")?;
                Ok(Self::Int(
                    (u64::from_be_bytes(buf) ^ (1u64 << 63)).cast_signed(),
                ))
            }
            Self::TAG_PRINCIPAL => {
                let len = payload[0] as usize;
                if len > Principal::MAX_LENGTH_IN_BYTES as usize {
                    return Err("corrupted StorageKey: invalid principal length");
                }
                ensure_zero_padding(1 + len, "principal")?;
                Ok(Self::Principal(Principal::from_slice(&payload[1..=len])))
            }
            Self::TAG_SUBACCOUNT => {
                ensure_zero_padding(Self::SUBACCOUNT_SIZE, "subaccount")?;
                let mut buf = [0u8; Self::SUBACCOUNT_SIZE];
                buf.copy_from_slice(&payload[..Self::SUBACCOUNT_SIZE]);
                Ok(Self::Subaccount(Subaccount::from_array(buf)))
            }
            Self::TAG_TIMESTAMP => {
                ensure_zero_padding(Self::TIMESTAMP_SIZE, "timestamp")?;
                let mut buf = [0u8; Self::TIMESTAMP_SIZE];
                buf.copy_from_slice(&payload[..Self::TIMESTAMP_SIZE]);
                Ok(Self::Timestamp(Timestamp::from(u64::from_be_bytes(buf))))
            }
            Self::TAG_UINT => {
                ensure_zero_padding(Self::UINT_SIZE, "uint")?;
                let mut buf = [0u8; Self::UINT_SIZE];
                buf.copy_from_slice(&payload[..Self::UINT_SIZE]);
                Ok(Self::Uint(u64::from_be_bytes(buf)))
            }
            Self::TAG_ULID => {
                ensure_zero_padding(Self::ULID_SIZE, "ulid")?;
                let mut buf = [0u8; Self::ULID_SIZE];
                buf.copy_from_slice(&payload[..Self::ULID_SIZE]);
                Ok(Self::Ulid(Ulid::from_bytes(buf)))
            }
            Self::TAG_UNIT => {
                ensure_zero_padding(0, "unit")?;
                Ok(Self::Unit)
            }
            _ => Err("corrupted StorageKey: invalid tag"),
        }
    }

    /// Convert this storage-normalized key into a semantic Value.
    ///
    /// Intended ONLY for diagnostics, explain output, planner invariants,
    /// and fingerprinting. Must not be used for query semantics.
    #[must_use]
    pub const fn as_value(&self) -> Value {
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

impl Ord for StorageKey {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Account(a), Self::Account(b)) => a.cmp(b),
            (Self::Int(a), Self::Int(b)) => a.cmp(b),
            (Self::Principal(a), Self::Principal(b)) => a.cmp(b),
            (Self::Uint(a), Self::Uint(b)) => a.cmp(b),
            (Self::Ulid(a), Self::Ulid(b)) => a.cmp(b),
            (Self::Subaccount(a), Self::Subaccount(b)) => a.cmp(b),
            (Self::Timestamp(a), Self::Timestamp(b)) => a.cmp(b),
            _ => self.variant_rank().cmp(&other.variant_rank()),
        }
    }
}

impl PartialOrd for StorageKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl TryFrom<&[u8]> for StorageKey {
    type Error = &'static str;
    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        Self::try_from_bytes(bytes)
    }
}
