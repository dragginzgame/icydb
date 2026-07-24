//! Engine storage and runtime operations for the schema-owned `Account` atom.

#[cfg(test)]
mod tests;

pub use icydb_schema::Account;

use crate::{
    db::{EntityKeyBytes, EntityKeyBytesError, validate_entity_key_bytes_buffer},
    types::{Principal, PrincipalEncodeError, Subaccount},
    value::{RuntimeValueDecode, RuntimeValueEncode, RuntimeValueKind, RuntimeValueMeta, Value},
    visitor::{SanitizeAuto, SanitizeCustom, ValidateAuto, ValidateCustom, Visitable},
};

/// Failure while encoding the fixed account storage representation.
#[derive(Debug)]
pub enum AccountEncodeError {
    /// Principal encoding failed.
    OwnerEncode(PrincipalEncodeError),
    /// Principal bytes exceed the fixed account representation.
    OwnerTooLarge {
        /// Actual byte length.
        len: usize,
        /// Maximum admitted byte length.
        max: usize,
    },
}

/// Failure while decoding the fixed account storage representation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AccountDecodeError {
    /// The fixed-width payload has the wrong size.
    InvalidSize,
    /// The principal-length tag exceeds the principal region.
    InvalidPrincipalLength,
    /// Principal padding is not canonical.
    NonZeroPrincipalPadding,
    /// A missing subaccount carries nonzero bytes.
    NonZeroSubaccountWithoutFlag,
}

impl From<PrincipalEncodeError> for AccountEncodeError {
    fn from(error: PrincipalEncodeError) -> Self {
        Self::OwnerEncode(error)
    }
}

/// Core-owned fixed account storage codec.
pub trait AccountStorageCodec: Sized {
    /// Canonical fixed storage size.
    const STORED_SIZE: u32 = 62;

    /// Encode without heap allocation.
    ///
    /// # Errors
    ///
    /// Returns an error when the owner principal cannot fit the canonical
    /// fixed-width representation.
    fn to_stored_bytes(self) -> Result<[u8; 62], AccountEncodeError>;

    /// Encode into owned fixed bytes.
    ///
    /// # Errors
    ///
    /// Returns an error when the owner principal cannot fit the canonical
    /// fixed-width representation.
    fn to_bytes(self) -> Result<Vec<u8>, AccountEncodeError>;

    /// Decode the current fixed storage representation.
    ///
    /// # Errors
    ///
    /// Returns an error for a wrong-sized or non-canonical payload.
    fn try_from_bytes(bytes: &[u8]) -> Result<Self, AccountDecodeError>;
}

impl AccountStorageCodec for Account {
    fn to_stored_bytes(self) -> Result<[u8; 62], AccountEncodeError> {
        const PRINCIPAL_MAX_LEN: usize = Principal::MAX_LENGTH_IN_BYTES as usize;
        const SUBACCOUNT_LEN: usize = 32;
        const TAG_SUBACCOUNT: u8 = 0x80;

        let owner = self.owner();
        let principal = owner.stored_bytes()?;
        if principal.len() > PRINCIPAL_MAX_LEN {
            return Err(AccountEncodeError::OwnerTooLarge {
                len: principal.len(),
                max: PRINCIPAL_MAX_LEN,
            });
        }
        let mut bytes = [0; Self::STORED_SIZE as usize];
        bytes[0] =
            u8::try_from(principal.len()).map_err(|_| AccountEncodeError::OwnerTooLarge {
                len: principal.len(),
                max: PRINCIPAL_MAX_LEN,
            })?;
        if self.subaccount().is_some() {
            bytes[0] |= TAG_SUBACCOUNT;
        }
        bytes[1..=principal.len()].copy_from_slice(principal);
        let subaccount_offset = 1 + PRINCIPAL_MAX_LEN;
        bytes[subaccount_offset..subaccount_offset + SUBACCOUNT_LEN]
            .copy_from_slice(&self.subaccount().unwrap_or(Subaccount::MIN).to_array());
        Ok(bytes)
    }

    fn to_bytes(self) -> Result<Vec<u8>, AccountEncodeError> {
        Ok(self.to_stored_bytes()?.to_vec())
    }

    fn try_from_bytes(bytes: &[u8]) -> Result<Self, AccountDecodeError> {
        const PRINCIPAL_MAX_LEN: usize = Principal::MAX_LENGTH_IN_BYTES as usize;
        const SUBACCOUNT_LEN: usize = 32;
        const TAG_SUBACCOUNT: u8 = 0x80;
        const LEN_MASK: u8 = 0x7F;

        if bytes.len() != Self::STORED_SIZE as usize {
            return Err(AccountDecodeError::InvalidSize);
        }
        let has_subaccount = (bytes[0] & TAG_SUBACCOUNT) != 0;
        let principal_len = (bytes[0] & LEN_MASK) as usize;
        if principal_len > PRINCIPAL_MAX_LEN {
            return Err(AccountDecodeError::InvalidPrincipalLength);
        }
        let principal_end = 1 + principal_len;
        let principal_region_end = 1 + PRINCIPAL_MAX_LEN;
        if bytes[principal_end..principal_region_end]
            .iter()
            .any(|byte| *byte != 0)
        {
            return Err(AccountDecodeError::NonZeroPrincipalPadding);
        }
        let owner = Principal::from_slice(&bytes[1..principal_end]);
        let mut subaccount = [0; SUBACCOUNT_LEN];
        subaccount
            .copy_from_slice(&bytes[principal_region_end..principal_region_end + SUBACCOUNT_LEN]);
        let subaccount = if has_subaccount {
            Some(Subaccount::from_array(subaccount))
        } else if subaccount.iter().any(|byte| *byte != 0) {
            return Err(AccountDecodeError::NonZeroSubaccountWithoutFlag);
        } else {
            None
        };
        Ok(Self::from_owner_and_subaccount(owner, subaccount))
    }
}

impl EntityKeyBytes for Account {
    const BYTE_LEN: usize = Self::STORED_SIZE as usize;

    fn write_bytes(&self, out: &mut [u8]) -> Result<(), EntityKeyBytesError> {
        validate_entity_key_bytes_buffer(out, Self::BYTE_LEN)?;
        let bytes = self.to_stored_bytes().map_err(account_key_bytes_error)?;
        out.copy_from_slice(&bytes);
        Ok(())
    }
}

const fn account_key_bytes_error(error: AccountEncodeError) -> EntityKeyBytesError {
    match error {
        AccountEncodeError::OwnerEncode(PrincipalEncodeError::TooLarge { len, max })
        | AccountEncodeError::OwnerTooLarge { len, max } => {
            EntityKeyBytesError::ValueTooLong { len, max }
        }
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
            Value::Account(value) => Some(*value),
            _ => None,
        }
    }
}

impl SanitizeAuto for Account {}

impl SanitizeCustom for Account {}

impl ValidateAuto for Account {}

impl ValidateCustom for Account {}

impl Visitable for Account {
    fn requires_application_write_callbacks() -> bool {
        false
    }
}
