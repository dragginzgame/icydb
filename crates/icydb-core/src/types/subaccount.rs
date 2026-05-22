//! Module: types::subaccount
//! Defines the fixed-width subaccount value used by account identifiers, typed
//! values, and persistence key encoding.

use crate::{
    traits::{
        EntityKeyBytes, RuntimeValueDecode, RuntimeValueEncode, RuntimeValueKind, RuntimeValueMeta,
        SanitizeAuto, SanitizeCustom, ValidateAuto, ValidateCustom, Visitable,
    },
    types::{Principal, Ulid},
    value::Value,
};
use candid::CandidType;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

//
// Subaccount
//

type SubaccountBytes = [u8; 32];

#[derive(
    CandidType,
    Clone,
    Copy,
    Debug,
    Default,
    Eq,
    PartialEq,
    Hash,
    Ord,
    PartialOrd,
    Serialize,
    Deserialize,
)]
pub struct Subaccount(SubaccountBytes);

impl Subaccount {
    pub const MIN: Self = Self::from_array([0x00; 32]);
    pub const MAX: Self = Self::from_array([0xFF; 32]);

    #[must_use]
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub const fn to_array(&self) -> [u8; 32] {
        self.0
    }

    #[must_use]
    pub const fn from_array(array: [u8; 32]) -> Self {
        Self(array)
    }

    /// Recover a ULID from the lower 16 bytes of the subaccount.
    #[must_use]
    pub fn to_ulid(&self) -> Ulid {
        let bytes = self.to_array();
        let ulid_bytes: [u8; 16] = bytes[16..].try_into().expect("slice has exactly 16 bytes");

        Ulid::from_bytes(ulid_bytes)
    }

    #[must_use]
    pub const fn as_slice(&self) -> &[u8] {
        &self.0
    }

    #[must_use]
    pub const fn to_bytes(self) -> [u8; 32] {
        self.0
    }
}

impl Display for Subaccount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in &self.0 {
            write!(f, "{byte:02x}")?;
        }

        Ok(())
    }
}

impl EntityKeyBytes for Subaccount {
    const BYTE_LEN: usize = 32;

    fn write_bytes(&self, out: &mut [u8]) {
        assert_eq!(out.len(), Self::BYTE_LEN);
        out.copy_from_slice(&self.to_bytes());
    }
}

impl RuntimeValueMeta for Subaccount {
    fn kind() -> RuntimeValueKind {
        RuntimeValueKind::Atomic
    }
}

impl RuntimeValueEncode for Subaccount {
    fn to_value(&self) -> Value {
        Value::Subaccount(*self)
    }
}

impl RuntimeValueDecode for Subaccount {
    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Subaccount(v) => Some(*v),
            _ => None,
        }
    }
}

// code taken from
// <https://docs.rs/ic-ledger-types/latest/src/ic_ledger_types/lib.rs.html#140-148>
#[expect(clippy::cast_possible_truncation)]
impl From<Principal> for Subaccount {
    fn from(principal: Principal) -> Self {
        let mut bytes = [0u8; 32];
        let p = principal.as_slice();

        // Defensive check: Principals are currently <= 29 bytes
        let len = p.len().min(31); // reserve 1 byte for the length prefix
        bytes[0] = len as u8;

        // Copy safely without panic risk
        bytes[1..=len].copy_from_slice(&p[..len]);

        Self(bytes)
    }
}

impl SanitizeAuto for Subaccount {}

impl SanitizeCustom for Subaccount {}

impl ValidateAuto for Subaccount {}

impl ValidateCustom for Subaccount {}

impl Visitable for Subaccount {}

//
// TESTS
//

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subaccount_max_size_is_bounded() {
        let subaccount = Subaccount::MAX;
        let size = subaccount.to_bytes().len();

        assert_eq!(
            size,
            <Subaccount as EntityKeyBytes>::BYTE_LEN,
            "serialized Subaccount must be exactly {} bytes; got {size}",
            <Subaccount as EntityKeyBytes>::BYTE_LEN
        );
    }
}
