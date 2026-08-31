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
use std::collections::BTreeSet;

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

impl DynamicMutationResult {
    /// Return the number of row payloads carried by this result.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.rows.len()
    }

    /// Return whether this result carries no row payloads.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

/// Static logical field shape used only for accepted compatibility checks.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TypedFieldType {
    /// Exact schema-owned scalar contract.
    Scalar(ScalarType),
    /// Ordered repeated values with one exact item contract.
    List(&'static Self),
    /// Named contract selected by immutable source key.
    Named(&'static str),
}

/// One generated static field descriptor supplied while issuing an opaque binding.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TypedFieldDescriptor {
    pub(crate) field_type: TypedFieldType,
    pub(crate) nullable: bool,
    pub(crate) source_key: &'static str,
}

impl TypedFieldDescriptor {
    /// Construct one generated static field descriptor.
    #[must_use]
    pub const fn new(source_key: &'static str, field_type: TypedFieldType, nullable: bool) -> Self {
        Self {
            field_type,
            nullable,
            source_key,
        }
    }
}

/// One generated static entity contract validated against accepted authority.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TypedEntityDescriptor {
    pub(crate) entity_source_key: &'static str,
    pub(crate) fields: &'static [TypedFieldDescriptor],
    pub(crate) primary_key_source_keys: &'static [&'static str],
}

impl TypedEntityDescriptor {
    /// Construct one generated static entity descriptor.
    #[must_use]
    pub const fn new(
        entity_source_key: &'static str,
        primary_key_source_keys: &'static [&'static str],
        fields: &'static [TypedFieldDescriptor],
    ) -> Self {
        Self {
            entity_source_key,
            fields,
            primary_key_source_keys,
        }
    }
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

#[derive(Clone, Debug, Eq, PartialEq)]
struct DynamicTypedFieldBinding {
    source_key: String,
    field_id: u32,
    slot: u16,
    label: String,
}

///
/// DynamicTypedStructuralPatch
///
/// Opaque accepted-ID/slot patch produced by a current typed binding.
///

#[doc(hidden)]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DynamicTypedStructuralPatch {
    entity_source: String,
    entity_tag: u64,
    accepted_fingerprint: [u8; 16],
    fields: Vec<(u32, u16, DynamicWriteCell)>,
}

impl DynamicTypedStructuralPatch {
    /// Borrow accepted field ID/slot intents for core mutation lowering.
    #[must_use]
    pub(crate) const fn fields(&self) -> &[(u32, u16, DynamicWriteCell)] {
        self.fields.as_slice()
    }

    pub(crate) fn is_bound_to(&self, binding: &DynamicTypedEntityBinding) -> bool {
        self.entity_source == binding.entity_source
            && self.entity_tag == binding.entity_tag
            && self.accepted_fingerprint == binding.accepted_fingerprint
    }
}

///
/// DynamicTypedMutation
///
/// One source-bound generated mutation whose fields already carry accepted
/// field IDs and slots. Entity and field names are not routing authority.
///

#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DynamicTypedMutation {
    /// Insert one accepted row.
    Insert {
        /// Bound authored field intents.
        patch: DynamicTypedStructuralPatch,
    },
    /// Patch one accepted row.
    Update {
        /// Scalar or composite public primary key.
        key: InputValue,
        /// Bound authored field intents.
        patch: DynamicTypedStructuralPatch,
    },
    /// Replace one accepted row, inserting it when absent.
    Replace {
        /// Scalar or composite public primary key.
        key: InputValue,
        /// Bound authored field intents.
        patch: DynamicTypedStructuralPatch,
    },
    /// Delete one accepted row.
    Delete {
        /// Scalar or composite public primary key.
        key: InputValue,
    },
}

/// Opaque accepted-schema identity issued for one generated typed adapter.
///
/// Public facade code may retain and return this value, but its accepted field
/// mapping remains private to IcyDB.
#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DynamicTypedEntityBinding {
    pub(crate) database_incarnation: [u8; 16],
    pub(crate) entity_source: String,
    pub(crate) entity_label: String,
    pub(crate) entity_tag: u64,
    pub(crate) accepted_revision: u64,
    pub(crate) accepted_fingerprint: [u8; 16],
    pub(crate) entity_generation: u32,
    fields: Vec<DynamicTypedFieldBinding>,
    pub(crate) named_types: Vec<(String, String)>,
    pub(crate) enum_variants: Vec<(String, String, String)>,
    pub(crate) composite_fields: Vec<(String, String, String)>,
}

impl DynamicTypedEntityBinding {
    #[expect(
        clippy::too_many_arguments,
        reason = "the opaque binding keeps every accepted authority component explicit"
    )]
    pub(crate) fn new(
        database_incarnation: [u8; 16],
        entity_source: String,
        entity_label: String,
        entity_tag: u64,
        accepted_revision: u64,
        accepted_fingerprint: [u8; 16],
        entity_generation: u32,
        fields: Vec<(String, u32, u16, String)>,
        named_types: Vec<(String, String)>,
        enum_variants: Vec<(String, String, String)>,
        composite_fields: Vec<(String, String, String)>,
    ) -> Result<Self, InternalError> {
        let mut sources = BTreeSet::new();
        let mut ids = BTreeSet::new();
        let mut slots = BTreeSet::new();
        let fields = fields
            .into_iter()
            .map(|(source_key, field_id, slot, label)| {
                if !sources.insert(source_key.clone())
                    || !ids.insert(field_id)
                    || !slots.insert(slot)
                {
                    return Err(InternalError::store_invariant());
                }
                Ok(DynamicTypedFieldBinding {
                    source_key,
                    field_id,
                    slot,
                    label,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            database_incarnation,
            entity_source,
            entity_label,
            entity_tag,
            accepted_revision,
            accepted_fingerprint,
            entity_generation,
            fields,
            named_types,
            enum_variants,
            composite_fields,
        })
    }

    /// Borrow the accepted entity display label.
    #[must_use]
    pub const fn entity(&self) -> &str {
        self.entity_label.as_str()
    }

    /// Borrow the immutable entity source identity.
    #[must_use]
    pub const fn entity_source(&self) -> &str {
        self.entity_source.as_str()
    }

    /// Resolve one immutable field source key directly to its accepted slot.
    #[must_use]
    pub fn field_slot(&self, source_key: &str) -> Option<u16> {
        self.fields
            .iter()
            .find_map(|field| (field.source_key == source_key).then_some(field.slot))
    }

    /// Resolve one accepted output label to its binding-owned accepted slot.
    #[must_use]
    pub fn output_field_slot(&self, label: &str) -> Option<u16> {
        self.fields
            .iter()
            .find_map(|field| (field.label == label).then_some(field.slot))
    }

    pub(crate) fn field_identity_bindings(&self) -> impl Iterator<Item = (&str, u32, u16)> {
        self.fields
            .iter()
            .map(|field| (field.source_key.as_str(), field.field_id, field.slot))
    }

    /// Bind generated source-key write intent to accepted field IDs and slots.
    #[must_use]
    pub fn bind_write_fields(
        &self,
        fields: Vec<(String, DynamicWriteCell)>,
    ) -> Option<DynamicTypedStructuralPatch> {
        let mut seen_ids = BTreeSet::new();
        let mut seen_slots = BTreeSet::new();
        let mut bound = Vec::with_capacity(fields.len());
        for (source_key, cell) in fields {
            let field = self
                .fields
                .iter()
                .find(|field| field.source_key == source_key)?;
            if !seen_ids.insert(field.field_id) || !seen_slots.insert(field.slot) {
                return None;
            }
            bound.push((field.field_id, field.slot, cell));
        }
        Some(DynamicTypedStructuralPatch {
            entity_source: self.entity_source.clone(),
            entity_tag: self.entity_tag,
            accepted_fingerprint: self.accepted_fingerprint,
            fields: bound,
        })
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

    /// Resolve one accepted enum-variant display name to its immutable source key.
    #[must_use]
    pub fn enum_variant_source_key(
        &self,
        type_source_key: &str,
        accepted_name: &str,
    ) -> Option<&str> {
        self.enum_variants
            .iter()
            .find_map(|(bound_type, source, name)| {
                (bound_type == type_source_key && name == accepted_name).then_some(source.as_str())
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

#[cfg(test)]
mod tests {
    use super::{DynamicMutationResult, DynamicTypedEntityBinding};

    fn binding() -> DynamicTypedEntityBinding {
        DynamicTypedEntityBinding::new(
            [0; 16],
            "EntitySource".to_string(),
            "Entity".to_string(),
            1,
            1,
            [1; 16],
            1,
            Vec::new(),
            vec![("ChoiceSource".to_string(), "RenamedChoice".to_string())],
            vec![
                (
                    "ChoiceSource".to_string(),
                    "FirstSource".to_string(),
                    "RenamedFirst".to_string(),
                ),
                (
                    "ChoiceSource".to_string(),
                    "SecondSource".to_string(),
                    "Second".to_string(),
                ),
            ],
            Vec::new(),
        )
        .expect("test binding should be internally consistent")
    }

    #[test]
    fn accepted_enum_variant_name_resolves_to_immutable_source_key() {
        let binding = binding();

        assert_eq!(
            binding.enum_variant_source_key("ChoiceSource", "RenamedFirst"),
            Some("FirstSource"),
        );
        assert_eq!(
            binding.enum_variant_source_key("ChoiceSource", "Second"),
            Some("SecondSource"),
        );
        assert_eq!(
            binding.enum_variant_source_key("OtherSource", "RenamedFirst"),
            None,
        );
        assert_eq!(
            binding.enum_variant_source_key("ChoiceSource", "FirstSource"),
            None,
        );
    }

    #[test]
    fn dynamic_mutation_result_derives_cardinality_from_rows() {
        let result = DynamicMutationResult {
            entity: "Entity".to_string(),
            columns: Vec::new(),
            rows: vec![Vec::new()],
            affected_rows: 1,
        };

        assert_eq!(result.len(), 1);
        assert!(!result.is_empty());
    }
}
