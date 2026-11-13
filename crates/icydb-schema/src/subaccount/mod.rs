//! Canonical fixed-width subaccount atom.

use crate::{Principal, SchemaContractError, atom::deserialize_fixed_bytes};

use serde::{Deserialize, Deserializer, Serialize};
use std::fmt::{self, Display};

//
// Subaccount
//

type SubaccountBytes = [u8; 32];

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
/// A canonical 32-byte ICRC account subaccount.
pub struct Subaccount(SubaccountBytes);

impl Subaccount {
    /// The lexicographically smallest subaccount.
    pub const MIN: Self = Self::from_array([0x00; 32]);
    /// The lexicographically largest subaccount.
    pub const MAX: Self = Self::from_array([0xFF; 32]);

    /// Return the fixed-width byte array.
    #[must_use]
    pub const fn to_array(&self) -> [u8; 32] {
        self.0
    }

    /// Construct from the exact fixed-width byte array.
    #[must_use]
    pub const fn from_array(array: [u8; 32]) -> Self {
        Self(array)
    }

    /// Parse 64 ASCII hex digits, optionally prefixed by lowercase `0x`.
    ///
    /// # Errors
    /// Returns [`SchemaContractError::InvalidLiteral`] for invalid width,
    /// non-ASCII input or non-hex characters. No trimming is performed.
    pub fn from_hex(text: &str) -> Result<Self, SchemaContractError> {
        let text = text.trim_prefix("0x");
        if text.len() != 64 || !text.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(SchemaContractError::InvalidLiteral);
        }
        let mut bytes = [0; 32];
        for (index, pair) in text.as_bytes().as_chunks::<2>().0.iter().enumerate() {
            let pair =
                std::str::from_utf8(pair).map_err(|_| SchemaContractError::InvalidLiteral)?;
            bytes[index] =
                u8::from_str_radix(pair, 16).map_err(|_| SchemaContractError::InvalidLiteral)?;
        }
        Ok(Self::from_array(bytes))
    }

    /// Borrow the fixed-width bytes.
    #[must_use]
    pub const fn as_slice(&self) -> &[u8] {
        &self.0
    }

    /// Consume the value and return its fixed-width bytes.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; 32] {
        self.0
    }
}

impl<'de> Deserialize<'de> for Subaccount {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            return SubaccountBytes::deserialize(deserializer).map(Self);
        }

        deserialize_fixed_bytes(deserializer).map(Self::from_array)
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

impl Serialize for Subaccount {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if serializer.is_human_readable() {
            self.0.serialize(serializer)
        } else {
            serializer.serialize_bytes(&self.0)
        }
    }
}

#[cfg(test)]
mod tests;
