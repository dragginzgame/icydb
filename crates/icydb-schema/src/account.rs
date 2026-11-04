//! Canonical account atom without storage authority.

#[cfg(test)]
mod tests;

use std::{
    fmt::{self, Display, Formatter},
    str::FromStr,
};

use serde::{Deserialize, Serialize};

use crate::{Principal, PrincipalError, SchemaContractError, Subaccount};

/// Canonical ICRC account atom for database identity.
///
/// An absent and an explicit all-zero subaccount denote the same account and
/// are stored as absence. Constructors, deserialization and Platform conversion
/// share that normalization; nonzero subaccounts retain all 32 bytes. Platform
/// owns the shared account text grammar.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
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
        Self::from_owner_and_subaccount(owner.into(), subaccount.map(Into::into))
    }

    /// Construct from canonical components.
    #[must_use]
    pub const fn from_owner_and_subaccount(
        owner: Principal,
        subaccount: Option<Subaccount>,
    ) -> Self {
        Self {
            owner,
            subaccount: Self::canonical_subaccount(subaccount),
        }
    }

    // Keep one representation for the ICRC default account without allocating
    // or making const construction depend on non-const trait comparison.
    const fn canonical_subaccount(subaccount: Option<Subaccount>) -> Option<Subaccount> {
        let Some(value) = subaccount else {
            return None;
        };
        let bytes = value.to_bytes();
        let mut index = 0;
        while index < bytes.len() {
            if bytes[index] != 0 {
                return Some(value);
            }
            index += 1;
        }
        None
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

// Decode the same two fields as the derived serializer. Normalization belongs
// to construction, so a decoder cannot create a second default identity.
impl<'de> Deserialize<'de> for Account {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename = "Account")]
        struct Fields {
            owner: Principal,
            subaccount: Option<Subaccount>,
        }
        let fields = Fields::deserialize(deserializer)?;
        Ok(Self::from_owner_and_subaccount(
            fields.owner,
            fields.subaccount,
        ))
    }
}

impl Display for Account {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&ic_platform_types::Account::from(*self), formatter)
    }
}

// Normalize Platform's explicit default representation at the database boundary.
impl From<ic_platform_types::Account> for Account {
    fn from(value: ic_platform_types::Account) -> Self {
        Self::from_owner_and_subaccount(
            value.owner.into(),
            value.subaccount.map(Subaccount::from_array),
        )
    }
}

impl From<Account> for ic_platform_types::Account {
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
        if input.len() > ic_platform_types::Account::MAX_LENGTH_IN_TEXT || !input.is_ascii() {
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
            .parse::<ic_platform_types::Account>()
            .map(Self::from)
            .map_err(|error| match error {
                ic_platform_types::AccountError::TextTooLong => {
                    "invalid account text width or alphabet".into()
                }
                ic_platform_types::AccountError::InvalidPrincipal(_) => {
                    PrincipalError::InvalidText.to_string()
                }
                ic_platform_types::AccountError::MissingChecksum => {
                    "missing account checksum".into()
                }
                ic_platform_types::AccountError::InvalidChecksum => {
                    "invalid account checksum".into()
                }
                ic_platform_types::AccountError::InvalidSubaccount => {
                    SchemaContractError::InvalidLiteral.to_string()
                }
                ic_platform_types::AccountError::LeadingZeroesInSubaccount
                | ic_platform_types::AccountError::DefaultSubaccountShouldBeOmitted => {
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
