//! Current source names and opaque proposal routing tokens.

use std::fmt::{self, Display, Formatter};

use candid::CandidType;
use serde::{Deserialize, Deserializer, Serialize, de::Error as DeError};
use sha2::{Digest, Sha256};

use crate::{MAX_SCHEMA_SUBMISSION_KEY_BYTES, MAX_SOURCE_KEY_BYTES, SchemaContractError};

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
        #[doc = concat!("Typed proposal key derived from one current ", stringify!($name), " name.")]
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

            pub(crate) fn from_name(name: &SchemaName) -> Self {
                Self(name.0.clone())
            }
        }

        impl Display for $name {
            fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
                formatter.write_str(self.as_str())
            }
        }

        impl CandidType for $name {
            fn ty() -> candid::types::Type {
                <String as CandidType>::ty()
            }

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
source_key!(RuleSourceKey);

impl ConstraintSourceKey {
    /// Derive one targeted-rule proposal key under a persisted root.
    ///
    /// The domain-separated digest keeps the result within the frozen source
    /// key bound even when every authored identity uses its maximum length.
    #[must_use]
    pub fn for_targeted_field_rule(
        field: &FieldSourceKey,
        target_type: &TypeSourceKey,
        rule: &RuleSourceKey,
    ) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(b"icydb:constraint-source:targeted-field-rule:v1");
        hash_bounded_part(&mut hasher, field.as_str());
        hash_bounded_part(&mut hasher, target_type.as_str());
        hash_bounded_part(&mut hasher, rule.as_str());
        let digest = hasher.finalize();
        let mut value = String::with_capacity(5 + (digest.len() * 2));
        value.push_str("rule:");
        for byte in digest {
            value.push(HEX_DIGITS[usize::from(byte >> 4)] as char);
            value.push(HEX_DIGITS[usize::from(byte & 0x0f)] as char);
        }
        Self(value)
    }
}

const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";

fn hash_bounded_part(hasher: &mut Sha256, value: &str) {
    hasher.update(u32::try_from(value.len()).unwrap_or(u32::MAX).to_be_bytes());
    hasher.update(value.as_bytes());
}

/// Bounded current schema name.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct SchemaName(String);

impl SchemaName {
    /// Construct a bounded nonempty name.
    ///
    /// # Errors
    ///
    /// Returns a typed contract error for empty, oversized, or non-canonical
    /// input.
    pub fn try_new(value: impl Into<String>) -> Result<Self, SchemaContractError> {
        let value = value.into();
        validate_source_key(&value)?;
        Ok(Self(value))
    }

    /// Borrow the name.
    #[must_use]
    pub const fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub(crate) fn for_targeted_rule(source_key: &ConstraintSourceKey) -> Self {
        Self(format!("__icydb_{}", source_key.as_str().replace(':', "_")))
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
    fn ty() -> candid::types::Type {
        <String as CandidType>::ty()
    }

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
    fn ty() -> candid::types::Type {
        <String as CandidType>::ty()
    }

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
opaque_token!(
    SchemaMigrationPlanDigest,
    "Canonical digest of one current-form coordinated schema migration plan."
);
opaque_token!(
    EntitySourceDigest,
    "Canonical generated-source meaning digest for one current entity."
);

#[cfg(test)]
mod tests {
    use candid::CandidType;

    use super::{
        ConstraintSourceKey, EntitySourceKey, FieldSourceKey, IndexSourceKey, RelationSourceKey,
        RuleSourceKey, SchemaName, SchemaSubmissionKey, TypeSourceKey,
    };

    #[test]
    fn text_backed_keys_delegate_their_candid_type() {
        let text = String::ty();

        assert_eq!(EntitySourceKey::ty(), text);
        assert_eq!(FieldSourceKey::ty(), text);
        assert_eq!(TypeSourceKey::ty(), text);
        assert_eq!(ConstraintSourceKey::ty(), text);
        assert_eq!(IndexSourceKey::ty(), text);
        assert_eq!(RelationSourceKey::ty(), text);
        assert_eq!(RuleSourceKey::ty(), text);
        assert_eq!(SchemaName::ty(), text);
        assert_eq!(SchemaSubmissionKey::ty(), text);
    }
}
