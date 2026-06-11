//! Module: types::account
//! Defines the normalized account value used for typed APIs, persistence
//! encoding, and ICRC account conversion.

use crate::{
    traits::{
        EntityKeyBytes, RuntimeValueDecode, RuntimeValueEncode, RuntimeValueKind, RuntimeValueMeta,
        SanitizeAuto, SanitizeCustom, ValidateAuto, ValidateCustom, Visitable,
    },
    types::{Principal, PrincipalEncodeError, Subaccount},
    value::Value,
};
use candid::CandidType;
use icrc_ledger_types::icrc1::account::Account as LedgerAccount;
use serde::Deserialize;
use std::{
    fmt::{self, Display},
    str::FromStr,
};

//
// Account
//

#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq)]
pub struct Account {
    owner: Principal,
    subaccount: Option<Subaccount>,
}

//
// AccountEncodeError
//
// Errors returned when encoding an account for persistence.
//

#[derive(Debug)]
pub enum AccountEncodeError {
    OwnerEncode(PrincipalEncodeError),

    OwnerTooLarge { len: usize, max: usize },
}

//
// AccountDecodeError
//
// Compact failure identity for stored-account decoding.
//

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AccountDecodeError {
    InvalidSize,
    InvalidPrincipalLength,
    NonZeroPrincipalPadding,
    NonZeroSubaccountWithoutFlag,
}

impl From<PrincipalEncodeError> for AccountEncodeError {
    fn from(err: PrincipalEncodeError) -> Self {
        Self::OwnerEncode(err)
    }
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

    /// Build an account from normalized owner/subaccount values.
    #[must_use]
    pub const fn from_owner_and_subaccount(
        owner: Principal,
        subaccount: Option<Subaccount>,
    ) -> Self {
        Self { owner, subaccount }
    }

    /// Return the account owner principal.
    #[must_use]
    pub const fn owner(&self) -> Principal {
        self.owner
    }

    /// Return the optional account subaccount.
    #[must_use]
    pub const fn subaccount(&self) -> Option<Subaccount> {
        self.subaccount
    }

    /// Convert to the upstream ledger account representation.
    #[must_use]
    pub fn to_icrc_type(self) -> LedgerAccount {
        LedgerAccount {
            owner: self.owner.into(),
            subaccount: self.subaccount.map(|subaccount| subaccount.to_array()),
        }
    }

    /// Encode the account into its fixed-size stored form without heap allocation.
    pub fn to_stored_bytes(self) -> Result<[u8; Self::STORED_SIZE as usize], AccountEncodeError> {
        let mut out = [0u8; Self::STORED_SIZE as usize];
        self.write_stored_bytes(&mut out)?;

        Ok(out)
    }

    /// Convert the account into a deterministic, fixed-size byte representation.
    pub fn to_bytes(self) -> Result<Vec<u8>, AccountEncodeError> {
        Ok(self.to_stored_bytes()?.to_vec())
    }

    // Encode the fixed-size stored account form directly into a caller-owned buffer
    // so row/index code can avoid the intermediate `Vec<u8>` allocation.
    fn write_stored_bytes(
        self,
        out: &mut [u8; Self::STORED_SIZE as usize],
    ) -> Result<(), AccountEncodeError> {
        let principal_bytes = self.owner.stored_bytes()?;
        let len = principal_bytes.len();
        if len > Self::PRINCIPAL_MAX_LEN {
            return Err(AccountEncodeError::OwnerTooLarge {
                len,
                max: Self::PRINCIPAL_MAX_LEN,
            });
        }

        out.fill(0);

        // Encode principal length and subaccount presence in the tag byte.
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
            out[1..=len].copy_from_slice(principal_bytes);
        }

        // Subaccount bytes (fixed length).
        let subaccount_bytes = self.subaccount.unwrap_or(Subaccount::MIN).to_array();
        let sub_offset = 1 + Self::PRINCIPAL_MAX_LEN;
        out[sub_offset..sub_offset + Self::SUBACCOUNT_LEN].copy_from_slice(&subaccount_bytes);

        Ok(())
    }

    #[must_use]
    #[expect(clippy::cast_possible_truncation)]
    fn ordering_tag(&self) -> u8 {
        let len = self.owner.as_slice().len();
        let len = len.min(u8::MAX as usize);
        let mut tag = len as u8;
        if self.subaccount.is_some() {
            tag |= Self::TAG_SUBACCOUNT;
        }
        tag
    }

    pub fn try_from_bytes(bytes: &[u8]) -> Result<Self, AccountDecodeError> {
        if bytes.len() != Self::STORED_SIZE as usize {
            return Err(AccountDecodeError::InvalidSize);
        }

        let tag = bytes[0];
        let has_subaccount = (tag & Self::TAG_SUBACCOUNT) != 0;
        let len = (tag & Self::LEN_MASK) as usize;
        if len > Self::PRINCIPAL_MAX_LEN {
            return Err(AccountDecodeError::InvalidPrincipalLength);
        }

        let principal_end = 1 + len;
        let principal_region_end = 1 + Self::PRINCIPAL_MAX_LEN;
        let owner = Principal::from_slice(&bytes[1..principal_end]);

        let padding = &bytes[principal_end..principal_region_end];
        if padding.iter().any(|&b| b != 0) {
            return Err(AccountDecodeError::NonZeroPrincipalPadding);
        }

        let sub_offset = principal_region_end;
        let mut sub = [0u8; Self::SUBACCOUNT_LEN];
        sub.copy_from_slice(&bytes[sub_offset..sub_offset + Self::SUBACCOUNT_LEN]);

        let subaccount = if has_subaccount {
            Some(Subaccount::from_array(sub))
        } else {
            if sub.iter().any(|&b| b != 0) {
                return Err(AccountDecodeError::NonZeroSubaccountWithoutFlag);
            }
            None
        };

        Ok(Self { owner, subaccount })
    }
}

// Delegate string formatting to the upstream ledger account implementation so
// this local persistence wrapper stays aligned with the canonical account text
// form instead of carrying a second formatter.
impl Display for Account {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_icrc_type())
    }
}

impl EntityKeyBytes for Account {
    const BYTE_LEN: usize = Self::STORED_SIZE as usize;

    fn write_bytes(&self, out: &mut [u8]) {
        assert_eq!(out.len(), Self::BYTE_LEN);
        let out: &mut [u8; Self::BYTE_LEN] = out.try_into().expect("account key invariant");
        self.write_stored_bytes(out).expect("account key invariant");
    }
}

impl RuntimeValueMeta for Account {
    fn kind() -> RuntimeValueKind {
        RuntimeValueKind::Atomic
    }
}

impl RuntimeValueEncode for Account {
    fn to_value(&self) -> Value {
        Value::Account(*self)
    }
}

impl RuntimeValueDecode for Account {
    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Account(v) => Some(*v),
            _ => None,
        }
    }
}

impl From<Account> for LedgerAccount {
    fn from(acc: Account) -> Self {
        acc.to_icrc_type()
    }
}

impl From<LedgerAccount> for Account {
    fn from(acc: LedgerAccount) -> Self {
        Self {
            owner: acc.owner.into(),
            subaccount: acc.subaccount.map(Subaccount::from_array),
        }
    }
}

impl FromStr for Account {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let icrc = LedgerAccount::from_str(s).map_err(|err| err.to_string())?;

        Ok(Self {
            owner: icrc.owner.into(),
            subaccount: icrc.subaccount.map(Subaccount::from_array),
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

        let self_sub = self.subaccount.unwrap_or(Subaccount::MIN).to_array();
        let other_sub = other.subaccount.unwrap_or(Subaccount::MIN).to_array();

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
    type Error = AccountDecodeError;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        Self::try_from_bytes(bytes)
    }
}

impl ValidateAuto for Account {}

impl ValidateCustom for Account {}

impl Visitable for Account {}

//
// TESTS
//

#[cfg(test)]
mod tests;
