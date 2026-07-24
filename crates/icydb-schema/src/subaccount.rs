//! Canonical fixed-width subaccount atom.

use crate::Principal;
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
