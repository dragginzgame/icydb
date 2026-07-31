//! Typed failures at the public proposal-contract boundary.

use std::fmt::{self, Display, Formatter};

use thiserror::Error;

/// Compact scalar parsing failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TypeParseError {
    /// Date text is invalid.
    InvalidDate,
    /// Decimal text is invalid.
    InvalidDecimal,
    /// Duration text is invalid.
    InvalidDuration,
    /// Signed big-integer text is invalid.
    InvalidIntBig,
    /// Timestamp text is invalid.
    InvalidTimestamp,
}

impl Display for TypeParseError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidDate => "invalid date",
            Self::InvalidDecimal => "invalid decimal",
            Self::InvalidDuration => "invalid duration",
            Self::InvalidIntBig => "invalid signed big integer",
            Self::InvalidTimestamp => "invalid timestamp",
        })
    }
}

/// Failure while constructing, validating, encoding, or decoding proposal data.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum SchemaContractError {
    /// A required bounded text identity is empty.
    #[error("schema contract identity is empty")]
    EmptyIdentity,

    /// A bounded text identity exceeds its byte limit.
    #[error("schema contract identity exceeds its byte limit")]
    IdentityTooLong {
        /// Actual byte length.
        len: usize,
        /// Maximum admitted byte length.
        max: usize,
    },

    /// A source key contains a non-canonical byte.
    #[error("schema source key contains a non-canonical byte")]
    InvalidSourceKey,

    /// One bounded collection exceeds its item limit.
    #[error("schema contract collection exceeds its item limit")]
    TooManyItems {
        /// Collection vocabulary used for bounded diagnostics.
        kind: &'static str,
        /// Actual item count.
        len: usize,
        /// Maximum admitted item count.
        max: usize,
    },

    /// One definition, assignment, or removal key occurs more than once.
    #[error("schema contract contains a duplicate source key")]
    DuplicateSourceKey,

    /// One definition collides with an explicit removal.
    #[error("schema contract defines and removes the same source key")]
    DefinitionRemovalConflict,

    /// Two definitions in one namespace use the same current name.
    #[error("schema contract contains a duplicate current name")]
    DuplicateName,

    /// A definition refers to an absent key in its local closure.
    #[error("schema contract contains an unresolved local reference")]
    InvalidLocalReference,

    /// One inline field-type contract exceeds the maintained depth bound.
    #[error("schema field type exceeds its inline depth bound")]
    FieldTypeDepthExceeded,

    /// An enum literal names a non-enum type or an absent local variant.
    #[error("schema contract contains an invalid enum literal reference")]
    InvalidEnumLiteral,

    /// A relation's source and target field contracts differ.
    #[error("schema relation source and target field types differ")]
    RelationTypeMismatch,

    /// One explicit removal deletes a definition still referenced by the proposal.
    #[error("schema contract removes a referenced definition")]
    RemovedReference,

    /// One entity does not have exactly one target-store assignment.
    #[error("schema proposal entity routing is incomplete")]
    MissingEntityStoreAssignment,

    /// A field combines incompatible nullability, insert, type, or management policy.
    #[error("schema field policy is invalid")]
    InvalidFieldPolicy,

    /// A field type carries an impossible width, scale, or bound.
    #[error("schema field type is invalid")]
    InvalidFieldType,

    /// A field default literal does not fit the exact declared field contract.
    #[error("schema field default does not match its exact field type")]
    LiteralTypeMismatch,

    /// One ordered field/reference list is empty or contains duplicates.
    #[error("schema contract contains an invalid ordered reference list")]
    InvalidReferenceList,

    /// A literal is malformed or non-canonical.
    #[error("schema proposal literal is malformed")]
    InvalidLiteral,

    /// A source check expression is malformed.
    #[error("schema source check expression is malformed")]
    InvalidExpression,

    /// A targeted durable-rule operation or its operand ordering is invalid.
    #[error("schema targeted durable-rule operation is invalid")]
    InvalidRuleOperation,

    /// A targeted durable rule cannot select the declared nominal value.
    #[error("schema targeted durable-rule target is invalid")]
    InvalidRuleTarget,

    /// The proposal contract version is not the maintained current version.
    #[error("schema proposal contract version is unsupported")]
    UnsupportedVersion {
        /// Version carried by the proposal.
        found: u16,
        /// Sole current version understood by this crate.
        supported: u16,
    },

    /// The proposal requires a capability not understood by this contract.
    #[error("schema proposal requires an unsupported capability")]
    UnsupportedCapability,

    /// A decoded proposal is structurally valid but not canonically ordered.
    #[error("schema proposal is not canonically ordered")]
    NonCanonical,

    /// Encoded bytes exceed the relevant transport limit.
    #[error("encoded schema contract exceeds its byte limit")]
    EncodedTooLarge {
        /// Actual byte length.
        len: usize,
        /// Maximum admitted byte length.
        max: usize,
    },

    /// Serialization failed.
    #[error("schema contract encoding failed")]
    Encode,

    /// Bounded current-form decoding failed.
    #[error("schema contract decoding failed")]
    Decode,
}
