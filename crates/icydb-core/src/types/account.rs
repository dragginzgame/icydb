use crate::{
    traits::{
        AsView, EntityKeyBytes, FieldValue, FieldValueKind, Inner, SanitizeAuto, SanitizeCustom,
        UpdateView, ValidateAuto, ValidateCustom, Visitable,
    },
    types::{Principal, PrincipalEncodeError, Subaccount},
    value::Value,
};
use candid::CandidType;
use canic_cdk::types::Account as IcrcAccount;
use serde::{Deserialize, Serialize};
use std::{
    fmt::{self, Display},
    str::FromStr,
};
use thiserror::Error as ThisError;

///
/// Account
///

#[derive(CandidType, Clone, Copy, Debug, Default, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct Account {
    pub owner: Principal,
    pub subaccount: Option<Subaccount>,
}

///
/// AccountEncodeError
///
/// Errors returned when encoding an account for persistence.
///

#[derive(Debug, ThisError)]
pub enum AccountEncodeError {
    #[error("account owner principal encoding failed: {0}")]
    OwnerEncode(#[from] PrincipalEncodeError),

    #[error("account owner principal exceeds max length: {len} bytes (limit {max})")]
    OwnerTooLarge { len: usize, max: usize },
}

impl Account {
    pub const STORED_SIZE: u32 = 62;

    const PRINCIPAL_MAX_LEN: usize = Principal::MAX_LENGTH_IN_BYTES as usize;
    const SUBACCOUNT_LEN: usize = 32;
    const TAG_SUBACCOUNT: u8 = 0x80;
    const LEN_MASK: u8 = 0x7F;

    /// Build an account from owner and optional subaccount.
    pub fn new<P: Into<Principal>, S: Into<Subaccount>>(owner: P, subaccount: Option<S>) -> Self {
        Self {
            owner: owner.into(),
            subaccount: subaccount.map(Into::into),
        }
    }

    /// Convert to the ICRC account representation.
    pub fn to_icrc_type(&self) -> IcrcAccount {
        IcrcAccount {
            owner: self.owner.into(),
            subaccount: self.subaccount.map(Into::into),
        }
    }

    #[must_use]
    /// Test helper that builds a deterministic account from a byte seed.
    pub fn dummy(v: u8) -> Self {
        let p = Principal::from_slice(&[v]);
        let s = [v; 32];

        Self::new(p, Some(s))
    }

    /// Convert the account into a deterministic, fixed-size byte representation.
    pub fn to_bytes(&self) -> Result<Vec<u8>, AccountEncodeError> {
        let principal_bytes = self.owner.to_bytes()?;
        let len = principal_bytes.len();
        if len > Self::PRINCIPAL_MAX_LEN {
            return Err(AccountEncodeError::OwnerTooLarge {
                len,
                max: Self::PRINCIPAL_MAX_LEN,
            });
        }

        let mut out = vec![0u8; Self::STORED_SIZE as usize];

        // Encode principal length and subaccount presence in the tag byte.
        #[allow(clippy::cast_possible_truncation)]
        let mut tag = u8::try_from(len).map_err(|_| AccountEncodeError::OwnerTooLarge {
            len,
            max: Self::PRINCIPAL_MAX_LEN,
        })?;
        if self.subaccount.is_some() {
            tag |= Self::TAG_SUBACCOUNT;
        }
        out[0] = tag;

        // Principal bytes (padded to fixed length).
        if len > 0 {
            out[1..=len].copy_from_slice(&principal_bytes);
        }

        // Subaccount bytes (fixed length).
        let subaccount_bytes = self.subaccount.unwrap_or_default().to_array();
        let sub_offset = 1 + Self::PRINCIPAL_MAX_LEN;
        out[sub_offset..sub_offset + Self::SUBACCOUNT_LEN].copy_from_slice(&subaccount_bytes);

        Ok(out)
    }

    /// Construct the maximum possible account for storage sizing tests.
    #[must_use]
    pub fn max_storable() -> Self {
        Self::new(Principal::MAX, Some(Subaccount::MAX))
    }

    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    fn ordering_tag(&self) -> u8 {
        let len = self.owner.as_slice().len();
        let len = len.min(u8::MAX as usize);
        let mut tag = len as u8;
        if self.subaccount.is_some() {
            tag |= Self::TAG_SUBACCOUNT;
        }
        tag
    }

    pub fn try_from_bytes(bytes: &[u8]) -> Result<Self, &'static str> {
        if bytes.len() != Self::STORED_SIZE as usize {
            return Err("corrupted Account: invalid size");
        }

        let tag = bytes[0];
        let has_subaccount = (tag & Self::TAG_SUBACCOUNT) != 0;
        let len = (tag & Self::LEN_MASK) as usize;
        if len > Self::PRINCIPAL_MAX_LEN {
            return Err("corrupted Account: invalid principal length");
        }

        let principal_end = 1 + len;
        let principal_region_end = 1 + Self::PRINCIPAL_MAX_LEN;
        let owner = Principal::from_slice(&bytes[1..principal_end]);

        let padding = &bytes[principal_end..principal_region_end];
        if padding.iter().any(|&b| b != 0) {
            return Err("corrupted Account: non-zero principal padding");
        }

        let sub_offset = principal_region_end;
        let mut sub = [0u8; Self::SUBACCOUNT_LEN];
        sub.copy_from_slice(&bytes[sub_offset..sub_offset + Self::SUBACCOUNT_LEN]);

        let subaccount = if has_subaccount {
            Some(Subaccount::from_array(sub))
        } else {
            if sub.iter().any(|&b| b != 0) {
                return Err("corrupted Account: non-zero subaccount bytes without flag");
            }
            None
        };

        Ok(Self { owner, subaccount })
    }
}

impl AsView for Account {
    type ViewType = Self;

    fn as_view(&self) -> Self::ViewType {
        *self
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

// Display logic is a bit convoluted and the code's in the icrc_ledger_types
// repo that I don't really want to wrap
impl Display for Account {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_icrc_type())
    }
}

impl EntityKeyBytes for Account {
    const BYTE_LEN: usize = Self::STORED_SIZE as usize;

    fn write_bytes(&self, out: &mut [u8]) {
        assert_eq!(out.len(), Self::BYTE_LEN);
        let encoded = self
            .to_bytes()
            .expect("account primary key encoding must remain valid");

        out.copy_from_slice(&encoded);
    }
}

impl FieldValue for Account {
    fn kind() -> FieldValueKind {
        FieldValueKind::Atomic
    }

    fn to_value(&self) -> Value {
        Value::Account(*self)
    }

    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Account(v) => Some(*v),
            _ => None,
        }
    }
}

impl From<Account> for IcrcAccount {
    fn from(acc: Account) -> Self {
        acc.to_icrc_type()
    }
}

impl From<IcrcAccount> for Account {
    fn from(acc: IcrcAccount) -> Self {
        Self {
            owner: acc.owner.into(),
            subaccount: acc.subaccount.map(Into::into),
        }
    }
}

impl FromStr for Account {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let icrc = IcrcAccount::from_str(s)?;

        Ok(Self {
            owner: icrc.owner.into(),
            subaccount: icrc.subaccount.map(Into::into),
        })
    }
}

impl<P: Into<Principal>> From<P> for Account {
    fn from(owner: P) -> Self {
        Self {
            owner: owner.into(),
            subaccount: None,
        }
    }
}

impl Inner<Self> for Account {
    fn inner(&self) -> &Self {
        self
    }

    fn into_inner(self) -> Self {
        self
    }
}

impl Ord for Account {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let tag_cmp = self.ordering_tag().cmp(&other.ordering_tag());
        if tag_cmp != std::cmp::Ordering::Equal {
            return tag_cmp;
        }

        let mut self_owner = [0u8; Self::PRINCIPAL_MAX_LEN];
        let self_bytes = self.owner.as_slice();
        self_owner[..self_bytes.len()].copy_from_slice(self_bytes);

        let mut other_owner = [0u8; Self::PRINCIPAL_MAX_LEN];
        let other_bytes = other.owner.as_slice();
        other_owner[..other_bytes.len()].copy_from_slice(other_bytes);

        let owner_cmp = self_owner.cmp(&other_owner);
        if owner_cmp != std::cmp::Ordering::Equal {
            return owner_cmp;
        }

        let self_sub = self.subaccount.unwrap_or_default().to_array();
        let other_sub = other.subaccount.unwrap_or_default().to_array();

        self_sub.cmp(&other_sub)
    }
}

impl PartialOrd for Account {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl SanitizeAuto for Account {}

impl SanitizeCustom for Account {}

impl TryFrom<&[u8]> for Account {
    type Error = &'static str;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        Self::try_from_bytes(bytes)
    }
}

impl UpdateView for Account {
    type UpdateViewType = Self;

    fn merge(&mut self, v: Self::UpdateViewType) -> Result<(), crate::traits::Error> {
        *self = v;

        Ok(())
    }
}

impl ValidateAuto for Account {}

impl ValidateCustom for Account {}

impl Visitable for Account {}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;

    fn principal() -> Principal {
        Principal::from_text("rrkah-fqaaa-aaaaa-aaaaq-cai").unwrap()
    }

    #[test]
    fn storable_bytes_are_exact_size() {
        let account = Account::max_storable();
        let bytes = account.to_bytes().expect("account encode");
        let size = bytes.len();

        assert!(
            size == Account::STORED_SIZE as usize,
            "serialized Account size mismatch (got {size}, expected {})",
            Account::STORED_SIZE
        );
    }

    #[test]
    fn to_bytes_is_deterministic() {
        let acc1 = Account::new(principal(), None::<Subaccount>);
        let acc2 = Account::new(principal(), None::<Subaccount>);
        assert_eq!(
            acc1.to_bytes().expect("account encode"),
            acc2.to_bytes().expect("account encode"),
            "encoding not deterministic"
        );
    }

    #[test]
    fn to_bytes_length_is_consistent() {
        let acc = Account::new(principal(), Some([1u8; 32]));
        let bytes = acc.to_bytes().expect("account encode");
        assert_eq!(
            bytes.len(),
            Account::STORED_SIZE as usize,
            "layout length mismatch"
        );
    }

    #[test]
    fn from_principal_creates_account_with_empty_subaccount() {
        let p = principal();
        let acc = Account::from(p);
        assert_eq!(acc.owner, p);
        assert!(acc.subaccount.is_none());
    }

    #[test]
    fn default_account_is_empty_principal_and_none_subaccount() {
        let acc = Account::default();
        assert!(acc.owner.as_slice().is_empty());
        assert!(acc.subaccount.is_none());
    }

    #[test]
    fn new_with_subaccount_sets_fields_correctly() {
        let sub: Subaccount = Subaccount::from_array([42u8; 32]);
        let acc = Account::new(principal(), Some(sub));
        assert_eq!(acc.owner, principal());
        assert_eq!(acc.subaccount, Some(sub));
    }

    #[test]
    fn to_bytes_produces_expected_layout() {
        let p = principal();
        let acc = Account::new(p, Some([0xAAu8; 32]));
        let bytes = acc.to_bytes().expect("account encode");

        let tag = bytes[0];
        let len = (tag & Account::LEN_MASK) as usize;
        let principal_part = if len > 0 { &bytes[1..=len] } else { &[] };
        let padding = if len < Account::PRINCIPAL_MAX_LEN {
            &bytes[1 + len..=Account::PRINCIPAL_MAX_LEN]
        } else {
            &[]
        };
        let sub_offset = 1 + Account::PRINCIPAL_MAX_LEN;
        let subaccount_part = &bytes[sub_offset..sub_offset + Account::SUBACCOUNT_LEN];

        assert_eq!(principal_part, p.as_slice(), "principal segment mismatch");
        assert!(
            tag & Account::TAG_SUBACCOUNT != 0,
            "subaccount flag missing"
        );
        assert!(
            padding.iter().all(|&b| b == 0),
            "principal padding not zero-filled"
        );
        assert_eq!(
            subaccount_part, &[0xAAu8; 32],
            "subaccount segment mismatch"
        );
    }

    #[test]
    fn to_bytes_with_none_subaccount_encodes_zero_bytes() {
        let p = principal();
        let acc = Account::new(p, None::<Subaccount>);
        let bytes = acc.to_bytes().expect("account encode");
        let tag = bytes[0];
        let sub_offset = 1 + Account::PRINCIPAL_MAX_LEN;
        let subaccount_part = &bytes[sub_offset..sub_offset + Account::SUBACCOUNT_LEN];
        assert_eq!(tag & Account::TAG_SUBACCOUNT, 0, "flag must be unset");
        assert!(
            subaccount_part.iter().all(|&b| b == 0),
            "None subaccount not zero-filled"
        );
    }

    #[test]
    fn to_bytes_distinguishes_none_and_zero_subaccount() {
        let p = principal();
        let none = Account::new(p, None::<Subaccount>);
        let zero = Account::new(p, Some([0u8; 32]));

        assert_ne!(
            none.to_bytes().expect("account encode"),
            zero.to_bytes().expect("account encode"),
            "None and zero subaccount must serialize differently"
        );
    }

    #[test]
    fn account_ordering_matches_bytes() {
        let accounts = vec![
            Account::new(Principal::from_slice(&[1]), None::<Subaccount>),
            Account::new(Principal::from_slice(&[1]), Some([0u8; 32])),
            Account::new(Principal::from_slice(&[1]), Some([1u8; 32])),
            Account::new(Principal::from_slice(&[1, 2]), None::<Subaccount>),
            Account::new(Principal::from_slice(&[2]), None::<Subaccount>),
        ];

        let mut sorted_by_ord = accounts.clone();
        sorted_by_ord.sort();

        let mut sorted_by_bytes = accounts;
        sorted_by_bytes.sort_by_key(|account| account.to_bytes().expect("account encode"));

        assert_eq!(
            sorted_by_ord, sorted_by_bytes,
            "Account Ord and byte ordering diverged"
        );
    }

    #[test]
    fn round_trip_via_storable_preserves_data() {
        let original = Account::new(principal(), Some([0xABu8; 32]));

        let bytes = original.to_bytes().expect("account encode");
        let decoded = Account::try_from_bytes(&bytes).expect("decode should succeed");

        assert_eq!(original, decoded, "Account did not round-trip correctly");
    }

    #[test]
    fn round_trip_custom_bytes_preserves_data() {
        let original = Account::new(principal(), Some([0xCDu8; 32]));
        let bytes = original.to_bytes().expect("account encode");

        let tag = bytes[0];
        let len = (tag & Account::LEN_MASK) as usize;
        let principal_bytes = if len > 0 { &bytes[1..=len] } else { &[] };
        let sub_offset = 1 + Account::PRINCIPAL_MAX_LEN;
        let sub_bytes = &bytes[sub_offset..sub_offset + Account::SUBACCOUNT_LEN];

        let owner = Principal::from_slice(principal_bytes);
        let mut sub = [0u8; 32];
        sub.copy_from_slice(sub_bytes);
        let sub_opt = if tag & Account::TAG_SUBACCOUNT != 0 {
            Some(Subaccount::from_array(sub))
        } else {
            None
        };

        let decoded = Account {
            owner,
            subaccount: sub_opt,
        };

        assert_eq!(original, decoded, "manual round-trip mismatch");
    }

    #[test]
    fn from_bytes_rejects_empty_input() {
        assert!(Account::try_from_bytes(&[]).is_err());
    }

    #[test]
    fn from_bytes_rejects_oversized_input() {
        let buf = vec![0u8; Account::STORED_SIZE as usize + 1];
        assert!(Account::try_from_bytes(&buf).is_err());
    }

    #[test]
    #[allow(clippy::cast_possible_truncation)]
    fn from_bytes_rejects_invalid_principal_len() {
        let mut buf = vec![0u8; Account::STORED_SIZE as usize];
        buf[0] = (Principal::MAX_LENGTH_IN_BYTES as u8) + 1;
        assert!(Account::try_from_bytes(&buf).is_err());
    }

    #[test]
    fn from_bytes_rejects_principal_padding() {
        let acc = Account::new(Principal::from_slice(&[1]), None::<Subaccount>);
        let mut bytes = acc.to_bytes().expect("account encode");
        bytes[1 + acc.owner.as_slice().len()] = 1;
        assert!(Account::try_from_bytes(&bytes).is_err());
    }

    #[test]
    fn from_bytes_rejects_subaccount_without_flag() {
        let acc = Account::new(Principal::from_slice(&[1]), None::<Subaccount>);
        let mut bytes = acc.to_bytes().expect("account encode");
        let sub_offset = 1 + Account::PRINCIPAL_MAX_LEN;
        bytes[sub_offset] = 1;
        assert!(Account::try_from_bytes(&bytes).is_err());
    }

    #[test]
    fn from_bytes_handles_anonymous_principal_with_subaccount() {
        let owner = Principal::anonymous();
        let owner_bytes = owner.as_slice();
        let mut bytes = vec![0u8; Account::STORED_SIZE as usize];
        let mut tag = u8::try_from(owner_bytes.len()).expect("principal length fits in u8");
        tag |= Account::TAG_SUBACCOUNT;
        bytes[0] = tag;
        if !owner_bytes.is_empty() {
            bytes[1..=owner_bytes.len()].copy_from_slice(owner_bytes);
        }

        let sub_offset = 1 + Account::PRINCIPAL_MAX_LEN;
        bytes[sub_offset] = 1;

        let decoded = Account::try_from_bytes(&bytes).expect("decode should succeed");
        assert_eq!(decoded.owner, owner);
        assert!(decoded.subaccount.is_some());
    }
}
