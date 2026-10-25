//! Canonical account atom without storage authority.

#[cfg(test)]
mod tests;

use std::{
    fmt::{self, Display, Formatter},
    str::FromStr,
};

use serde::{Deserialize, Serialize};

use crate::{Principal, PrincipalError, SchemaContractError, Subaccount};

/// IcyDB account atom with representation-preserving identity.
///
/// An absent subaccount and an explicit zero subaccount remain distinct database
/// identities. Explicit conversions to and from [`ic_platform::Account`] preserve
/// those fields; Platform compares both representations as the same ICRC account.
/// Each type retains its own binary serialization contract. Platform owns the
/// shared ICRC account text format; database equality remains local.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[cfg_attr(not(target_family = "wasm"), derive(ic_wire::Wire))]
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
        Display::fmt(&ic_platform::Account::from(*self), formatter)
    }
}

// Preserve the optional field itself, not Platform's effective subaccount.
impl From<ic_platform::Account> for Account {
    fn from(value: ic_platform::Account) -> Self {
        Self::from_owner_and_subaccount(
            value.owner.into(),
            value.subaccount.map(Subaccount::from_array),
        )
    }
}

impl From<Account> for ic_platform::Account {
    fn from(value: Account) -> Self {
        Self {
            owner: value.owner.into(),
            subaccount: value.subaccount.map(Subaccount::to_bytes),
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
        if input.len() > ic_platform::Account::MAX_LENGTH_IN_TEXT || !input.is_ascii() {
            return Err("invalid account text width or alphabet".into());
        }
        // Keep IcyDB's refusal precedence: after a checksum-shaped prefix,
        // empty/oversized subaccounts reject before attempting the owner parse.
        // Platform otherwise owns the grammar, checksum and hex decoding.
        if let Some((prefix, hex)) = input.split_once('.')
            && prefix
                .rsplit_once('-')
                .is_some_and(|(_, checksum)| checksum.len() == 7)
            && (hex.is_empty() || hex.len() > 64)
        {
            return Err("invalid account subaccount width or leading zero".into());
        }
        input
            .parse::<ic_platform::Account>()
            .map(Self::from)
            .map_err(|error| match error {
                ic_platform::AccountError::TextTooLong => {
                    "invalid account text width or alphabet".into()
                }
                ic_platform::AccountError::InvalidPrincipal(_) => {
                    PrincipalError::InvalidText.to_string()
                }
                ic_platform::AccountError::MissingChecksum => "missing account checksum".into(),
                ic_platform::AccountError::InvalidChecksum => "invalid account checksum".into(),
                ic_platform::AccountError::InvalidSubaccount => {
                    SchemaContractError::InvalidLiteral.to_string()
                }
                ic_platform::AccountError::LeadingZeroesInSubaccount
                | ic_platform::AccountError::DefaultSubaccountShouldBeOmitted => {
                    "invalid account subaccount width or leading zero".into()
                }
            })
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
