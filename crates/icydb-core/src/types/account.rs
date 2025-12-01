use crate::{
    db::primitives::{TextFilterKind, TextListFilterKind},
    traits::{
        FieldValue, Filterable, Inner, SanitizeAuto, SanitizeCustom, Storable, UpdateView,
        ValidateAuto, ValidateCustom, View, Visitable,
    },
    types::{Principal, Subaccount},
    value::Value,
};
use candid::CandidType;
use canic::{cdk::structures::storable::Bound, types::Account as IcrcAccount};
use serde::{Deserialize, Serialize};
use std::{
    borrow::Cow,
    fmt::{self, Display},
    str::FromStr,
};

///
/// Account
///

#[derive(
    CandidType,
    Debug,
    Clone,
    Copy,
    Default,
    Eq,
    PartialEq,
    Hash,
    Ord,
    Serialize,
    Deserialize,
    PartialOrd,
)]
pub struct Account {
    pub owner: Principal,
    pub subaccount: Option<Subaccount>,
}

impl Account {
    pub const STORABLE_MAX_SIZE: u32 = 62;

    pub fn new<P: Into<Principal>, S: Into<Subaccount>>(owner: P, subaccount: Option<S>) -> Self {
        Self {
            owner: owner.into(),
            subaccount: subaccount.map(Into::into),
        }
    }

    pub fn to_icrc_type(&self) -> IcrcAccount {
        IcrcAccount {
            owner: self.owner.into(),
            subaccount: self.subaccount.map(Into::into),
        }
    }

    #[must_use]
    pub fn dummy(v: u8) -> Self {
        let p = Principal::from_slice(&[v]);
        let s = [v; 32];

        Self::new(p, Some(s))
    }

    /// Convert the account into a deterministic IC-style byte representation.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let principal_bytes = self.owner.as_slice();
        let subaccount_bytes = self.subaccount.unwrap_or_default().to_bytes();
        let mut out = Vec::with_capacity(1 + principal_bytes.len() + 32);

        // Encode principal length (so reversible)
        #[allow(clippy::cast_possible_truncation)]
        out.push(principal_bytes.len() as u8);
        out.extend_from_slice(principal_bytes);
        out.extend_from_slice(&subaccount_bytes);

        out
    }

    /// Construct the maximum possible account for storage sizing tests.
    #[must_use]
    pub fn max_storable() -> Self {
        Self::new(Principal::MAX, Some(Subaccount::MAX))
    }
}

// Display logic is a bit convoluted and the code's in the icrc_ledger_types
// repo that I don't really want to wrap
impl Display for Account {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_icrc_type())
    }
}

impl FieldValue for Account {
    fn to_value(&self) -> Value {
        Value::Account(*self)
    }
}

impl Filterable for Account {
    type Filter = TextFilterKind;
    type ListFilter = TextListFilterKind;
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

impl SanitizeAuto for Account {}

impl SanitizeCustom for Account {}

impl Storable for Account {
    fn to_bytes(&self) -> Cow<'_, [u8]> {
        Cow::Owned(self.to_bytes()) // use your compact 1+len+32 format
    }

    fn into_bytes(self) -> Vec<u8> {
        self.to_bytes()
    }

    fn from_bytes(bytes: Cow<[u8]>) -> Self {
        let bytes = bytes.as_ref();
        // reconstruct from your custom binary layout
        let len = bytes[0] as usize;
        let principal_bytes = &bytes[1..=len];
        let subaccount_bytes = &bytes[1 + len..];

        let owner = Principal::from_slice(principal_bytes);
        let subaccount = {
            let mut sub = [0u8; 32];
            sub.copy_from_slice(subaccount_bytes);
            if sub == [0; 32] {
                None
            } else {
                Some(Subaccount::from_array(sub))
            }
        };

        Self { owner, subaccount }
    }

    const BOUND: Bound = Bound::Bounded {
        max_size: Self::STORABLE_MAX_SIZE,
        is_fixed_size: true,
    };
}

impl UpdateView for Account {
    type UpdateViewType = Self;

    fn merge(&mut self, v: Self::UpdateViewType) {
        *self = v;
    }
}

impl ValidateAuto for Account {}

impl ValidateCustom for Account {}

impl View for Account {
    type ViewType = Self;

    fn to_view(&self) -> Self::ViewType {
        *self
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

impl Visitable for Account {}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::Storable;
    use candid::Principal;

    fn principal() -> Principal {
        Principal::from_text("rrkah-fqaaa-aaaaa-aaaaq-cai").unwrap()
    }

    #[test]
    fn storable_bytes_are_exact_size() {
        let account = Account::max_storable();
        let bytes = Storable::to_bytes(&account);
        let size = bytes.len();

        assert!(
            size == Account::STORABLE_MAX_SIZE as usize,
            "serialized Account size mismatch (got {size}, expected {})",
            Account::STORABLE_MAX_SIZE
        );
    }

    #[test]
    fn to_bytes_is_deterministic() {
        let acc1 = Account::new(principal(), None::<Subaccount>);
        let acc2 = Account::new(principal(), None::<Subaccount>);
        assert_eq!(
            acc1.to_bytes(),
            acc2.to_bytes(),
            "encoding not deterministic"
        );
    }

    #[test]
    fn to_bytes_length_is_consistent() {
        let acc = Account::new(principal(), Some([1u8; 32]));
        let bytes = acc.to_bytes();
        assert_eq!(
            bytes.len(),
            bytes[0] as usize + 1 + 32,
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
        let bytes = acc.to_bytes();

        let len = bytes[0] as usize;
        let principal_part = &bytes[1..=len];
        let subaccount_part = &bytes[1 + len..];

        assert_eq!(principal_part, p.as_slice(), "principal segment mismatch");
        assert_eq!(
            subaccount_part, &[0xAAu8; 32],
            "subaccount segment mismatch"
        );
    }

    #[test]
    fn to_bytes_with_none_subaccount_encodes_zero_bytes() {
        let p = principal();
        let acc = Account::new(p, None::<Subaccount>);
        let bytes = acc.to_bytes();
        let len = bytes[0] as usize;
        let subaccount_part = &bytes[1 + len..];
        assert!(
            subaccount_part.iter().all(|&b| b == 0),
            "None subaccount not zero-filled"
        );
    }

    #[test]
    fn round_trip_via_storable_preserves_data() {
        let original = Account::new(principal(), Some([0xABu8; 32]));

        let bytes = Storable::to_bytes(&original);
        let decoded = Account::from_bytes(Cow::Borrowed(&bytes));

        assert_eq!(original, decoded, "Account did not round-trip correctly");
    }

    #[test]
    fn round_trip_custom_bytes_preserves_data() {
        let original = Account::new(principal(), Some([0xCDu8; 32]));
        let bytes = original.to_bytes();

        let len = bytes[0] as usize;
        let principal_bytes = &bytes[1..=len];
        let sub_bytes = &bytes[1 + len..];

        let owner = Principal::from_slice(principal_bytes);
        let mut sub = [0u8; 32];
        sub.copy_from_slice(sub_bytes);
        let sub_opt = if sub == [0; 32] {
            None
        } else {
            Some(Subaccount::from_array(sub))
        };

        let decoded = Account {
            owner: owner.into(),
            subaccount: sub_opt,
        };

        assert_eq!(original, decoded, "manual round-trip mismatch");
    }
}
