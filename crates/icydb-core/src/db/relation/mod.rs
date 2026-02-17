use crate::{
    db::{
        Db,
        identity::{EntityName, EntityNameError},
        store::{DataKey, RawDataKey, StorageKey, StorageKeyEncodeError},
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    traits::{EntityKind, EntityValue},
    value::Value,
};
use std::collections::BTreeSet;

mod metadata;
mod reverse_index;
mod validate;

use metadata::StrongRelationInfo;

pub(crate) use metadata::{StrongRelationTargetInfo, strong_relation_target_from_kind};
pub(crate) use reverse_index::prepare_reverse_relation_index_mutations_for_source;
#[cfg(test)]
pub(crate) use validate::validate_delete_strong_relations_for_source;

///
/// StrongRelationDeleteValidateFn
///
/// Function-pointer contract for delete-side strong relation validators.
///

pub type StrongRelationDeleteValidateFn<C> =
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

// Build one relation target raw key from validated entity+storage key components.
fn raw_relation_target_key_from_parts(
    entity_name: EntityName,
    storage_key: StorageKey,
) -> Result<RawDataKey, StorageKeyEncodeError> {
    DataKey::raw_from_parts(entity_name, storage_key)
}

/// Convert a relation target `Value` into its canonical `RawDataKey` representation.
pub(super) fn build_relation_target_raw_key(
    target_entity_name: &str,
    value: &Value,
) -> Result<RawDataKey, RelationTargetRawKeyError> {
    let storage_key =
        StorageKey::try_from_value(value).map_err(RelationTargetRawKeyError::StorageKeyEncode)?;
    let entity_name = EntityName::try_from_str(target_entity_name)
        .map_err(RelationTargetRawKeyError::TargetEntityName)?;

    raw_relation_target_key_from_parts(entity_name, storage_key)
        .map_err(RelationTargetRawKeyError::StorageKeyEncode)
}

// Visit concrete relation target values for one relation field payload.
// Runtime relation List/Set shapes are represented as `Value::List`, and
// optional relation slots may be explicit `Value::Null`.
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

// Convert a relation value to its target raw data key representation.
fn raw_relation_target_key<S>(
    field_name: &str,
    relation: StrongRelationInfo,
    value: &Value,
) -> Result<RawDataKey, InternalError>
where
    S: EntityKind + EntityValue,
{
    build_relation_target_raw_key(relation.target_entity_name, value).map_err(|err| match err {
        RelationTargetRawKeyError::StorageKeyEncode(err) => InternalError::new(
            ErrorClass::Unsupported,
            ErrorOrigin::Executor,
            format!(
                "strong relation key not storage-compatible during relation processing: source={} field={} target={} value={value:?} ({err})",
                S::PATH,
                field_name,
                relation.target_path,
            ),
        ),
        RelationTargetRawKeyError::TargetEntityName(err) => InternalError::new(
            ErrorClass::Internal,
            ErrorOrigin::Executor,
            format!(
                "strong relation target entity invalid during relation processing: source={} field={} target={} name={} ({err})",
                S::PATH,
                field_name,
                relation.target_path,
                relation.target_entity_name,
            ),
        ),
    })
}

// Decode a relation target key and validate it against the relation target entity.
fn decode_relation_target_data_key_for_relation<S>(
    relation: StrongRelationInfo,
    target_raw_key: &RawDataKey,
    context: RelationTargetDecodeContext,
    mismatch_policy: RelationTargetMismatchPolicy,
) -> Result<Option<DataKey>, InternalError>
where
    S: EntityKind,
{
    let target_data_key = DataKey::try_from_raw(target_raw_key).map_err(|err| {
        InternalError::new(
            ErrorClass::Corruption,
            ErrorOrigin::Store,
            format!(
                "{}: source={} field={} target={} ({err})",
                relation_target_decode_message(context),
                S::PATH,
                relation.field_name,
                relation.target_path,
            ),
        )
    })?;

    let target_entity = EntityName::try_from_str(relation.target_entity_name).map_err(|err| {
        InternalError::new(
            ErrorClass::Internal,
            ErrorOrigin::Executor,
            format!(
                "{}: source={} field={} target={} name={} ({err})",
                relation_target_entity_name_message(context),
                S::PATH,
                relation.field_name,
                relation.target_path,
                relation.target_entity_name,
            ),
        )
    })?;

    if target_data_key.entity_name() != &target_entity {
        if matches!(mismatch_policy, RelationTargetMismatchPolicy::Skip) {
            return Ok(None);
        }

        return Err(InternalError::new(
            ErrorClass::Corruption,
            ErrorOrigin::Store,
            format!(
                "{}: source={} field={} target={} expected={} actual={}",
                relation_target_entity_mismatch_message(context),
                S::PATH,
                relation.field_name,
                relation.target_path,
                relation.target_entity_name,
                target_data_key.entity_name(),
            ),
        ));
    }

    Ok(Some(target_data_key))
}

const fn relation_target_decode_message(context: RelationTargetDecodeContext) -> &'static str {
    match context {
        RelationTargetDecodeContext::DeleteValidation => "delete relation target key decode failed",
        RelationTargetDecodeContext::ReverseIndexPrepare => {
            "relation target key decode failed while preparing reverse index"
        }
    }
}

const fn relation_target_entity_name_message(context: RelationTargetDecodeContext) -> &'static str {
    match context {
        RelationTargetDecodeContext::DeleteValidation => {
            "strong relation target entity invalid during delete validation"
        }
        RelationTargetDecodeContext::ReverseIndexPrepare => {
            "relation target entity invalid while preparing reverse index"
        }
    }
}

const fn relation_target_entity_mismatch_message(
    context: RelationTargetDecodeContext,
) -> &'static str {
    match context {
        RelationTargetDecodeContext::DeleteValidation => {
            "relation target entity mismatch during delete validation"
        }
        RelationTargetDecodeContext::ReverseIndexPrepare => {
            "relation target entity mismatch while preparing reverse index"
        }
    }
}
