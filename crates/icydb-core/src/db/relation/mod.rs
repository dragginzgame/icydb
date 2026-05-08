//! Module: relation
//! Responsibility: relation-domain validation and reverse-index mutation helpers.
//! Does not own: query planning, executor routing, or storage codec policy.
//! Boundary: executor/commit paths delegate relation semantics to this module.

mod metadata;
mod reverse_index;
mod save_validate;
mod validate;

use crate::{
    db::{
        Db,
        data::{RawDataKey, StorageKeyEncodeError},
    },
    error::InternalError,
    value::Value,
};
use std::{collections::BTreeSet, fmt::Display};

pub(in crate::db) use metadata::{
    RelationDescriptor, RelationDescriptorCardinality, relation_descriptors_for_model_iter,
};
pub(crate) use reverse_index::{
    ReverseRelationSourceInfo, prepare_reverse_relation_index_mutations_for_source_slot_readers,
};
pub(in crate::db) use save_validate::validate_save_strong_relations_with_accepted_contract;
pub(in crate::db) use validate::validate_delete_strong_relations_for_source;

///
/// StrongRelationDeleteValidateFn
///
/// Function-pointer contract for delete-side strong relation validators.
///

pub(crate) type StrongRelationDeleteValidateFn<C> =
    fn(&Db<C>, &str, &BTreeSet<RawDataKey>) -> Result<(), InternalError>;

///
/// RelationTargetDecodeContext
/// Call-site context labels for relation target key decode diagnostics.
///

#[derive(Clone, Copy, Debug)]
enum RelationTargetDecodeContext {
    DeleteValidation,
    ReverseIndexPrepare,
}

///
/// RelationTargetMismatchPolicy
/// Defines whether relation target entity mismatches are skipped or rejected.
///

#[derive(Clone, Copy, Debug)]
enum RelationTargetMismatchPolicy {
    Skip,
    Reject,
}

///
/// RelationTargetRawKeyError
/// Error variants for building a relation target `RawDataKey` from user value.
///

#[derive(Debug)]
pub(in crate::db::relation) enum RelationTargetRawKeyError {
    StorageKeyEncode(StorageKeyEncodeError),
}

impl InternalError {
    /// Map a relation-target key normalization failure into a typed `InternalError`.
    pub(in crate::db::relation) fn relation_target_raw_key_error(
        err: RelationTargetRawKeyError,
        source_path: &'static str,
        field_name: &str,
        target_path: &str,
        value: &Value,
        storage_compat_message: &'static str,
    ) -> Self {
        match err {
            RelationTargetRawKeyError::StorageKeyEncode(err) => {
                Self::executor_unsupported(format!(
                    "{storage_compat_message}: source={source_path} field={field_name} target={target_path} value={value:?} ({err})",
                ))
            }
        }
    }

    /// Construct the canonical strong-relation invalid target-name error.
    pub(in crate::db) fn strong_relation_target_name_invalid(
        source_path: &str,
        field_name: &str,
        target_path: &str,
        target_entity_name: &str,
        err: impl Display,
    ) -> Self {
        Self::executor_internal(format!(
            "strong relation target name invalid: source={source_path} field={field_name} target={target_path} name={target_entity_name} ({err})",
        ))
    }

    /// Construct the canonical strong-relation target identity mismatch error.
    pub(in crate::db) fn strong_relation_target_identity_mismatch(
        source_path: &str,
        field_name: &str,
        target_path: &str,
        detail: impl Display,
    ) -> Self {
        Self::executor_internal(format!(
            "strong relation target identity mismatch: source={source_path} field={field_name} target={target_path} ({detail})",
        ))
    }

    /// Construct the canonical save-time strong-relation missing-target error.
    pub(crate) fn strong_relation_target_missing(
        source_path: &'static str,
        field_name: &str,
        target_path: &str,
        value: &Value,
    ) -> Self {
        Self::executor_unsupported(format!(
            "strong relation missing: source={source_path} field={field_name} target={target_path} key={value:?}",
        ))
    }

    /// Construct the canonical save-time strong-relation missing-store error.
    pub(crate) fn strong_relation_target_store_missing(
        source_path: &'static str,
        field_name: &str,
        target_path: &str,
        target_store_path: &str,
        value: &Value,
        err: impl Display,
    ) -> Self {
        Self::executor_internal(format!(
            "strong relation target store missing: source={source_path} field={field_name} target={target_path} store={target_store_path} key={value:?} ({err})",
        ))
    }
}

/// Visit concrete relation target values for one relation field payload.
///
/// Runtime relation List/Set shapes are represented as `Value::List`, and
/// optional relation slots may be explicit `Value::Null`.
pub(super) fn for_each_relation_target_value(
    value: &Value,
    mut visit: impl FnMut(&Value) -> Result<(), InternalError>,
) -> Result<(), InternalError> {
    match value {
        Value::List(items) => {
            for item in items {
                if matches!(item, Value::Null) {
                    continue;
                }
                visit(item)?;
            }
        }
        Value::Null => {}
        _ => visit(value)?,
    }

    Ok(())
}
