use crate::{
    db::primitives::NoFilterKind,
    traits::{
        FieldValue, Filterable, Inner, SanitizeAuto, SanitizeCustom, UpdateView, ValidateAuto,
        ValidateCustom, View, Visitable,
    },
    types::{Principal, Ulid},
    value::Value,
};
use candid::CandidType;
use canic_core::{
    impl_storable_bounded, types::Subaccount as WrappedSubaccount, utils::rand::next_u128,
};
use derive_more::{Deref, DerefMut};
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

///
/// Subaccount
///

#[derive(
    CandidType,
    Clone,
    Copy,
    Default,
    Debug,
    Deref,
    DerefMut,
    Eq,
    PartialEq,
    Hash,
    Ord,
    PartialOrd,
    Serialize,
    Deserialize,
)]
pub struct Subaccount(WrappedSubaccount);

impl Subaccount {
    pub const STORABLE_MAX_SIZE: u32 = 72;

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

    #[must_use]
    /// Encode a ULID into the lower 16 bytes of a subaccount.
    pub fn from_ulid(ulid: Ulid) -> Self {
        let mut bytes = [0u8; 32];
        bytes[16..].copy_from_slice(&ulid.to_bytes()); // right-align ULID

        Self::from_array(bytes)
    }

    #[must_use]
    /// Recover a ULID from the lower 16 bytes of the subaccount.
    pub fn to_ulid(&self) -> Ulid {
        let bytes = self.to_array();
        let ulid_bytes: [u8; 16] = bytes[16..].try_into().expect("slice has exactly 16 bytes");

        Ulid::from_bytes(ulid_bytes)
    }

    #[must_use]
    pub const fn dummy(v: u8) -> Self {
        Self([v; 32])
    }

    /// Generate a random subaccount using two 128-bit draws.
    #[must_use]
    pub fn random() -> Self {
        let hi = next_u128().to_le_bytes();
        let lo = next_u128().to_le_bytes();

        let mut bytes = [0u8; 32];
        bytes[..16].copy_from_slice(&hi);
        bytes[16..].copy_from_slice(&lo);

        Self::from_array(bytes)
    }

    #[must_use]
    pub const fn to_bytes(self) -> [u8; 32] {
        self.0
    }

    #[must_use]
    pub const fn max_storable() -> Self {
        Self([0xFF; 32])
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

impl FieldValue for Subaccount {
    fn to_value(&self) -> Value {
        Value::Subaccount(*self)
    }
}

impl Filterable for Subaccount {
    type Filter = NoFilterKind;
    type ListFilter = NoFilterKind;
}

/// code taken from
/// <https://docs.rs/ic-ledger-types/latest/src/ic_ledger_types/lib.rs.html#140-148>
#[allow(clippy::cast_possible_truncation)]
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

impl From<Subaccount> for WrappedSubaccount {
    fn from(sub: Subaccount) -> Self {
        sub.0
    }
}

impl From<WrappedSubaccount> for Subaccount {
    fn from(wrap: WrappedSubaccount) -> Self {
        Self(wrap)
    }
}

impl Inner<Self> for Subaccount {
    fn inner(&self) -> &Self {
        self
    }

    fn into_inner(self) -> Self {
        self
    }
}

impl PartialEq<Subaccount> for WrappedSubaccount {
    fn eq(&self, other: &Subaccount) -> bool {
        self == &other.0
    }
}

impl PartialEq<WrappedSubaccount> for Subaccount {
    fn eq(&self, other: &WrappedSubaccount) -> bool {
        &self.0 == other
    }
}

impl SanitizeAuto for Subaccount {}

impl SanitizeCustom for Subaccount {}

impl_storable_bounded!(Subaccount, Subaccount::STORABLE_MAX_SIZE, true);

impl UpdateView for Subaccount {
    type UpdateViewType = Self;

    fn merge(&mut self, v: Self::UpdateViewType) {
        *self = v;
    }
}

impl ValidateAuto for Subaccount {}

impl ValidateCustom for Subaccount {}

impl View for Subaccount {
    type ViewType = WrappedSubaccount;

    fn to_view(&self) -> Self::ViewType {
        self.0
    }

    fn from_view(view: Self::ViewType) -> Self {
        Self(view)
    }
}

impl Visitable for Subaccount {}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::Storable;

    #[test]
    fn subaccount_max_size_is_bounded() {
        let subaccount = Subaccount::max_storable();
        let size = Storable::to_bytes(&subaccount).len();

        assert!(
            size <= Subaccount::STORABLE_MAX_SIZE as usize,
            "serialized Subaccount too large: got {size} bytes (limit {})",
            Subaccount::STORABLE_MAX_SIZE
        );
    }

    #[test]
    fn generate_produces_valid_subaccount() {
        let sub = Subaccount::random();

        // Must always be exactly 32 bytes
        assert_eq!(sub.to_bytes().len(), 32);

        // Should not equal MIN or MAX every time
        assert_ne!(sub, Subaccount::MIN);
        assert_ne!(sub, Subaccount::MAX);
    }

    #[test]
    fn generate_produces_different_values() {
        let sub1 = Subaccount::random();
        let sub2 = Subaccount::random();

        // Extremely unlikely they’re equal in two calls
        assert_ne!(sub1, sub2);
    }

    #[test]
    fn generate_multiple_are_unique() {
        use std::collections::HashSet;

        let mut set = HashSet::new();
        for _ in 0..100 {
            let sub = Subaccount::random();
            assert!(set.insert(sub), "duplicate subaccount generated");
        }
    }

    #[test]
    fn display_hex_format_is_64_chars() {
        let sub = Subaccount::random();
        let hex = sub.to_string();

        // 32 bytes → 64 hex chars
        assert_eq!(hex.len(), 64);

        // Must be valid hex
        assert!(hex.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn round_trip_ulid_to_subaccount_and_back() {
        let ulid = Ulid::default();
        let sub = Subaccount::from_ulid(ulid);
        let ulid2 = sub.to_ulid();

        assert_eq!(ulid, ulid2);
    }

    #[test]
    fn different_ulids_produce_different_subaccounts() {
        let ulid1 = Ulid::generate();
        let ulid2 = Ulid::generate();
        assert_ne!(
            Subaccount::from_ulid(ulid1).to_array(),
            Subaccount::from_ulid(ulid2).to_array()
        );
    }
}
