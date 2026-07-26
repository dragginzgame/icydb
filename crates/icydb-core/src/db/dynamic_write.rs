//! Module: db::dynamic_write
//! Responsibility: entity-name-driven structural write requests and results.
//! Does not own: accepted policy resolution, row encoding, or commit execution.
//! Boundary: public dynamic intent is lowered once by the session write owner.

use crate::{
    error::InternalError,
    value::{InputValue, OutputValue},
};
use candid::CandidType;
use icydb_schema::ScalarType;
use serde::Deserialize;

///
/// DynamicWriteCell
///
/// One structural field-write intent crossing the facade-to-core boundary.
/// Omission remains distinct from an explicit default request, `NULL`, and an
/// authored value until accepted write policy resolves the final after-image.
///

#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DynamicWriteCell {
    /// Supply no authored value for this field.
    Omitted,
    /// Explicitly request the accepted database default.
    Default,
    /// Explicitly author a nullable value.
    Null,
    /// Author one concrete public input value.
    Value(InputValue),
}

///
/// DynamicStructuralPatch
///
/// Field-name-driven structural patch consumed by the accepted write lane.
/// Field names are resolved against the selected accepted snapshot; this type
/// carries no physical slots or generated-model ordering.
///

#[doc(hidden)]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DynamicStructuralPatch {
    fields: Vec<(String, DynamicWriteCell)>,
}

impl DynamicStructuralPatch {
    /// Build one field-name-driven structural patch.
    #[must_use]
    pub const fn new(fields: Vec<(String, DynamicWriteCell)>) -> Self {
        Self { fields }
    }

    /// Borrow the authored field intents in caller order.
    #[must_use]
    pub const fn fields(&self) -> &[(String, DynamicWriteCell)] {
        self.fields.as_slice()
    }
}

///
/// DynamicMutation
///
/// One entity-name-driven structural mutation request.
/// Variant shape owns row-existence and key requirements so callers cannot
/// combine an insert-only identity mode with update/delete semantics.
///

#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DynamicMutation {
    /// Insert one row, resolving its identity from the accepted after-image.
    Insert {
        /// Accepted entity display name.
        entity: String,
        /// Authored insert intent.
        patch: DynamicStructuralPatch,
    },
    /// Patch one existing row selected by its public primary-key value.
    Update {
        /// Accepted entity display name.
        entity: String,
        /// Scalar or composite primary-key value.
        key: InputValue,
        /// Authored patch intent.
        patch: DynamicStructuralPatch,
    },
    /// Replace one row, inserting when the selected key does not yet exist.
    Replace {
        /// Accepted entity display name.
        entity: String,
        /// Scalar or composite primary-key value.
        key: InputValue,
        /// Authored replacement intent.
        patch: DynamicStructuralPatch,
    },
    /// Delete one existing row selected by its public primary-key value.
    Delete {
        /// Accepted entity display name.
        entity: String,
        /// Scalar or composite primary-key value.
        key: InputValue,
    },
}

impl DynamicMutation {
    /// Borrow the accepted entity display name selected by this request.
    #[must_use]
    pub const fn entity(&self) -> &str {
        match self {
            Self::Insert { entity, .. }
            | Self::Update { entity, .. }
            | Self::Replace { entity, .. }
            | Self::Delete { entity, .. } => entity.as_str(),
        }
    }
}

///
/// DynamicMutationResult
///
/// Row-oriented result from one accepted-schema-driven structural mutation.
///

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct DynamicMutationResult {
    /// Accepted entity name used for the mutation.
    pub entity: String,
    /// Complete accepted output-column names in row order.
    pub columns: Vec<String>,
    /// Canonical row values produced or removed by the mutation.
    pub rows: Vec<Vec<OutputValue>>,
    /// Number of rows whose logical or physical state changed.
    pub affected_rows: u32,
}

///
/// DynamicTypedFieldBindingRequest
///
/// Generated logical field contract supplied only while issuing an opaque
/// accepted adapter binding.
///

#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DynamicTypedFieldBindingRequest {
    pub(crate) field_type: DynamicTypedFieldType,
    pub(crate) nullable: bool,
    pub(crate) source_key: String,
}

impl DynamicTypedFieldBindingRequest {
    /// Construct one generated field binding request.
    #[must_use]
    pub const fn new(
        source_key: String,
        field_type: DynamicTypedFieldType,
        nullable: bool,
    ) -> Self {
        Self {
            field_type,
            nullable,
            source_key,
        }
    }
}

/// Logical generated field shape used only for accepted compatibility checks.
#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DynamicTypedFieldType {
    /// Exact schema-owned scalar contract.
    Scalar(ScalarType),
    /// Ordered repeated values with one exact item contract.
    List(Box<Self>),
    /// Named contract selected by immutable source key.
    Named(String),
}

/// Typed binding issuance failure before an opaque binding exists.
#[doc(hidden)]
#[derive(Debug)]
pub enum DynamicTypedBindingError {
    /// A requested immutable source identity is unavailable.
    FieldUnavailable,
    /// The requested logical field contract disagrees with accepted authority.
    IncompatibleField,
    /// Accepted database inspection failed.
    Internal(InternalError),
}

impl From<InternalError> for DynamicTypedBindingError {
    fn from(error: InternalError) -> Self {
        Self::Internal(error)
    }
}

/// Opaque accepted-schema identity issued for one generated typed adapter.
///
/// Public facade code may retain and return this value, but its accepted field
/// mapping remains private to IcyDB.
#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DynamicTypedEntityBinding {
    pub(crate) database_incarnation: [u8; 16],
    pub(crate) entity: String,
    pub(crate) entity_tag: u64,
    pub(crate) accepted_revision: u64,
    pub(crate) accepted_fingerprint: [u8; 16],
    pub(crate) entity_generation: u32,
    pub(crate) fields: Vec<(String, String)>,
    pub(crate) named_types: Vec<(String, String)>,
    pub(crate) enum_variants: Vec<(String, String, String)>,
    pub(crate) composite_fields: Vec<(String, String, String)>,
}

impl DynamicTypedEntityBinding {
    /// Borrow the accepted entity display name without exposing physical identity.
    #[must_use]
    pub const fn entity(&self) -> &str {
        self.entity.as_str()
    }

    /// Resolve one immutable source key to its current accepted display name.
    #[must_use]
    pub fn field_name(&self, source_key: &str) -> Option<&str> {
        self.fields
            .iter()
            .find_map(|(source, name)| (source == source_key).then_some(name.as_str()))
    }

    /// Resolve one immutable named-type source key to its accepted display path.
    #[must_use]
    pub fn named_type_name(&self, source_key: &str) -> Option<&str> {
        self.named_types
            .iter()
            .find_map(|(source, name)| (source == source_key).then_some(name.as_str()))
    }

    /// Resolve one immutable enum-variant source key to its accepted display name.
    #[must_use]
    pub fn enum_variant_name(&self, type_source_key: &str, source_key: &str) -> Option<&str> {
        self.enum_variants
            .iter()
            .find_map(|(bound_type, source, name)| {
                (bound_type == type_source_key && source == source_key).then_some(name.as_str())
            })
    }

    /// Resolve one immutable record-member source key to its accepted display name.
    #[must_use]
    pub fn composite_field_name(&self, type_source_key: &str, source_key: &str) -> Option<&str> {
        self.composite_fields
            .iter()
            .find_map(|(bound_type, source, name)| {
                (bound_type == type_source_key && source == source_key).then_some(name.as_str())
            })
    }
}
