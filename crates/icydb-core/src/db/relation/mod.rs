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
        data::{DataKey, RawDataKey, StorageKey, StorageKeyEncodeError},
        identity::{EntityName, EntityNameError},
    },
    error::InternalError,
    types::EntityTag,
    value::Value,
};
use std::{collections::BTreeSet, fmt::Display};

use metadata::StrongRelationInfo;

pub(crate) use metadata::{StrongRelationTargetInfo, strong_relation_target_from_kind};
pub(crate) use reverse_index::{
    ReverseRelationSourceInfo, prepare_reverse_relation_index_mutations_for_source_rows,
};
pub(in crate::db) use save_validate::{
    model_has_strong_relation_targets, validate_save_strong_relations,
};
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
pub(super) enum RelationTargetRawKeyError {
    StorageKeyEncode(StorageKeyEncodeError),
    TargetEntityName(EntityNameError),
}

impl InternalError {
    /// Map a relation-target key normalization failure into a typed `InternalError`.
    #[expect(clippy::too_many_arguments)]
    pub(in crate::db) fn relation_target_raw_key_error(
        err: RelationTargetRawKeyError,
        source_path: &'static str,
        field_name: &str,
        target_path: &str,
        target_entity_name: &str,
        value: &Value,
        storage_compat_message: &'static str,
        invalid_target_message: &'static str,
    ) -> Self {
        match err {
            RelationTargetRawKeyError::StorageKeyEncode(err) => {
                crate::db::error::executor_unsupported(format!(
                    "{storage_compat_message}: source={source_path} field={field_name} target={target_path} value={value:?} ({err})",
                ))
            }
            RelationTargetRawKeyError::TargetEntityName(err) => {
                crate::db::error::executor_internal(format!(
                    "{invalid_target_message}: source={source_path} field={field_name} target={target_path} name={target_entity_name} ({err})",
                ))
            }
        }
    }
}

/// Build a consistent strong-relation target-key mismatch error for save validation.
pub(crate) fn target_key_mismatch_error(
    source_path: &'static str,
    field_name: &str,
    target_path: &str,
    value: &Value,
) -> InternalError {
    crate::db::error::executor_unsupported(format!(
        "strong relation missing: source={source_path} field={field_name} target={target_path} key={value:?}",
    ))
}

/// Build a consistent strong-relation target-store incompatibility error.
pub(crate) fn incompatible_store_error(
    source_path: &'static str,
    field_name: &str,
    target_path: &str,
    target_store_path: &str,
    value: &Value,
    err: impl Display,
) -> InternalError {
    crate::db::error::executor_internal(format!(
        "strong relation target store missing: source={source_path} field={field_name} target={target_path} store={target_store_path} key={value:?} ({err})",
    ))
}

/// Convert a relation target `Value` into its canonical `RawDataKey` representation.
pub(super) fn build_relation_target_raw_key(
    target_entity_tag: EntityTag,
    target_entity_name: &str,
    value: &Value,
) -> Result<RawDataKey, RelationTargetRawKeyError> {
    let storage_key =
        StorageKey::try_from_value(value).map_err(RelationTargetRawKeyError::StorageKeyEncode)?;
    let _ = EntityName::try_from_str(target_entity_name)
        .map_err(RelationTargetRawKeyError::TargetEntityName)?;

    DataKey::raw_from_parts(target_entity_tag, storage_key)
        .map_err(RelationTargetRawKeyError::StorageKeyEncode)
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

/// Convert a relation value to its target raw data key representation.
pub(in crate::db::relation) fn raw_relation_target_key(
    source_path: &'static str,
    field_name: &str,
    relation: StrongRelationInfo,
    value: &Value,
) -> Result<RawDataKey, InternalError> {
    build_relation_target_raw_key(
        relation.target_entity_tag,
        relation.target_entity_name,
        value,
    )
    .map_err(|err| {
        InternalError::relation_target_raw_key_error(
            err,
            source_path,
            field_name,
            relation.target_path,
            relation.target_entity_name,
            value,
            "strong relation key not storage-compatible during relation processing",
            "strong relation target entity invalid during relation processing",
        )
    })
}
