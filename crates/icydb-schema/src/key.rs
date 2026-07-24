//! Immutable source identities and opaque proposal routing tokens.

use std::fmt::{self, Display, Formatter};

use candid::CandidType;
use serde::{Deserialize, Deserializer, Serialize, de::Error as DeError};

use crate::{
    MAX_SCHEMA_NAME_BYTES, MAX_SCHEMA_SUBMISSION_KEY_BYTES, MAX_SOURCE_KEY_BYTES,
    SchemaContractError,
};

fn validate_source_key(value: &str) -> Result<(), SchemaContractError> {
    validate_bounded_identity(value, MAX_SOURCE_KEY_BYTES)?;
    if !value.bytes().all(|byte| {
        byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b':' | b'/')
    }) {
        return Err(SchemaContractError::InvalidSourceKey);
    }
    Ok(())
}

const fn validate_bounded_identity(value: &str, max: usize) -> Result<(), SchemaContractError> {
    if value.is_empty() {
        return Err(SchemaContractError::EmptyIdentity);
    }
    if value.len() > max {
        return Err(SchemaContractError::IdentityTooLong {
            len: value.len(),
            max,
        });
    }
    Ok(())
}

macro_rules! source_key {
    ($name:ident) => {
        #[doc = concat!("Immutable author identity for one ", stringify!($name), ".")]
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
        pub struct $name(String);

        impl $name {
            /// Construct a bounded canonical source key.
            ///
            /// # Errors
            ///
            /// Returns a typed contract error for empty, oversized, or
            /// non-canonical input.
            pub fn try_new(value: impl Into<String>) -> Result<Self, SchemaContractError> {
                let value = value.into();
                validate_source_key(&value)?;
                Ok(Self(value))
            }

            /// Borrow the canonical key text.
            #[must_use]
            pub const fn as_str(&self) -> &str {
                self.0.as_str()
            }
        }

        impl Display for $name {
            fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
                formatter.write_str(self.as_str())
            }
        }

        impl CandidType for $name {
            fn _ty() -> candid::types::Type {
                <String as CandidType>::_ty()
            }

            fn idl_serialize<S>(&self, serializer: S) -> Result<(), S::Error>
            where
                S: candid::types::Serializer,
            {
                serializer.serialize_text(self.as_str())
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                Self::try_new(value).map_err(D::Error::custom)
            }
        }
    };
}

source_key!(EntitySourceKey);
source_key!(FieldSourceKey);
source_key!(TypeSourceKey);
source_key!(ConstraintSourceKey);
source_key!(IndexSourceKey);
source_key!(RelationSourceKey);

/// Bounded editable SQL/display name.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct SchemaName(String);

impl SchemaName {
    /// Construct a bounded nonempty name.
    ///
    /// # Errors
    ///
    /// Returns a typed contract error for empty, oversized, or control-bearing
    /// input.
    pub fn try_new(value: impl Into<String>) -> Result<Self, SchemaContractError> {
        let value = value.into();
        validate_bounded_identity(&value, MAX_SCHEMA_NAME_BYTES)?;
        if value.chars().any(char::is_control) {
            return Err(SchemaContractError::InvalidSourceKey);
        }
        Ok(Self(value))
    }

    /// Borrow the name.
    #[must_use]
    pub const fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl<'de> Deserialize<'de> for SchemaName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::try_new(String::deserialize(deserializer)?).map_err(D::Error::custom)
    }
}

impl CandidType for SchemaName {
    fn _ty() -> candid::types::Type {
        <String as CandidType>::_ty()
    }

    fn idl_serialize<S>(&self, serializer: S) -> Result<(), S::Error>
    where
        S: candid::types::Serializer,
    {
        serializer.serialize_text(self.as_str())
    }
}

/// Caller-generated immutable schema-submission key.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct SchemaSubmissionKey(String);

impl SchemaSubmissionKey {
    /// Construct a bounded submission key.
    ///
    /// # Errors
    ///
    /// Returns a typed contract error for empty or oversized input.
    pub fn try_new(value: impl Into<String>) -> Result<Self, SchemaContractError> {
        let value = value.into();
        validate_bounded_identity(&value, MAX_SCHEMA_SUBMISSION_KEY_BYTES)?;
        Ok(Self(value))
    }

    /// Borrow the key.
    #[must_use]
    pub const fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl<'de> Deserialize<'de> for SchemaSubmissionKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::try_new(String::deserialize(deserializer)?).map_err(D::Error::custom)
    }
}

impl CandidType for SchemaSubmissionKey {
    fn _ty() -> candid::types::Type {
        <String as CandidType>::_ty()
    }

    fn idl_serialize<S>(&self, serializer: S) -> Result<(), S::Error>
    where
        S: candid::types::Serializer,
    {
        serializer.serialize_text(self.as_str())
    }
}

macro_rules! opaque_token {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(
            CandidType,
            Clone,
            Copy,
            Debug,
            Deserialize,
            Eq,
            Hash,
            Ord,
            PartialEq,
            PartialOrd,
            Serialize,
        )]
        #[serde(transparent)]
        pub struct $name([u8; 32]);

        impl $name {
            /// Construct from opaque bytes issued by IcyDB.
            #[must_use]
            pub const fn from_bytes(bytes: [u8; 32]) -> Self {
                Self(bytes)
            }

            /// Return the opaque bytes.
            #[must_use]
            pub const fn to_bytes(self) -> [u8; 32] {
                self.0
            }
        }
    };
}

opaque_token!(
    TargetDatabaseIdentity,
    "Opaque identity binding a proposal to one target database."
);
opaque_token!(
    TargetStoreIdentity,
    "Opaque identity routing one entity to a store in the target database."
);
opaque_token!(
    ExpectedSchemaFingerprint,
    "Opaque expected accepted-schema fingerprint."
);
opaque_token!(
    SchemaProposalDigest,
    "Canonical digest of one current-form schema proposal."
);
