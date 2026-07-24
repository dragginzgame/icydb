//! Canonical account atom without storage authority.

use std::{
    fmt::{self, Display, Formatter},
    str::FromStr,
};

use candid::CandidType;
use icrc_ledger_types::icrc1::account::Account as LedgerAccount;
use serde::{Deserialize, Serialize};

use crate::{Principal, Subaccount};

/// Canonical ICRC account atom.
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct Account {
    owner: Principal,
    subaccount: Option<Subaccount>,
}

impl Account {
    const PRINCIPAL_MAX_LEN: usize = Principal::MAX_LENGTH_IN_BYTES as usize;
    const TAG_SUBACCOUNT: u8 = 0x80;

    /// Construct from convertible owner and optional subaccount values.
    pub fn new<P: Into<Principal>, S: Into<Subaccount>>(owner: P, subaccount: Option<S>) -> Self {
        Self {
            owner: owner.into(),
            subaccount: subaccount.map(Into::into),
        }
    }

    /// Construct from canonical components.
    #[must_use]
    pub const fn from_owner_and_subaccount(
        owner: Principal,
        subaccount: Option<Subaccount>,
    ) -> Self {
        Self { owner, subaccount }
    }

    /// Return the owner.
    #[must_use]
    pub const fn owner(&self) -> Principal {
        self.owner
    }

    /// Return the optional subaccount.
    #[must_use]
    pub const fn subaccount(&self) -> Option<Subaccount> {
        self.subaccount
    }

    /// Convert to the upstream ICRC account type.
    #[must_use]
    pub fn to_icrc_type(self) -> LedgerAccount {
        LedgerAccount {
            owner: self.owner.into(),
            subaccount: self.subaccount.map(|value| value.to_array()),
        }
    }

    #[expect(clippy::cast_possible_truncation)]
    fn ordering_tag(&self) -> u8 {
        let mut tag = self.owner.as_slice().len().min(u8::MAX as usize) as u8;
        if self.subaccount.is_some() {
            tag |= Self::TAG_SUBACCOUNT;
        }
        tag
    }
}

impl Display for Account {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.to_icrc_type(), formatter)
    }
}

impl From<Account> for LedgerAccount {
    fn from(value: Account) -> Self {
        value.to_icrc_type()
    }
}

impl From<LedgerAccount> for Account {
    fn from(value: LedgerAccount) -> Self {
        Self {
            owner: value.owner.into(),
            subaccount: value.subaccount.map(Subaccount::from_array),
        }
    }
}

impl From<Principal> for Account {
    fn from(owner: Principal) -> Self {
        Self {
            owner,
            subaccount: None,
        }
    }
}

impl FromStr for Account {
    type Err = String;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        LedgerAccount::from_str(input)
            .map(Self::from)
            .map_err(|error| error.to_string())
    }
}

impl Ord for Account {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.ordering_tag()
            .cmp(&other.ordering_tag())
            .then_with(|| {
                let mut left = [0; Self::PRINCIPAL_MAX_LEN];
                let left_bytes = self.owner.as_slice();
                left[..left_bytes.len()].copy_from_slice(left_bytes);
                let mut right = [0; Self::PRINCIPAL_MAX_LEN];
                let right_bytes = other.owner.as_slice();
                right[..right_bytes.len()].copy_from_slice(right_bytes);
                left.cmp(&right)
            })
            .then_with(|| {
                self.subaccount
                    .unwrap_or(Subaccount::MIN)
                    .cmp(&other.subaccount.unwrap_or(Subaccount::MIN))
            })
    }
}

impl PartialOrd for Account {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
