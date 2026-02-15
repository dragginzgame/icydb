use crate::{
    db::{
        Db,
        identity::{EntityName, EntityNameError},
        store::{DataKey, RawDataKey, StorageKey, StorageKeyEncodeError},
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    traits::{EntityKind, EntityValue, Storable},
    value::Value,
};
use std::collections::BTreeSet;

mod metadata;
mod reverse_index;
mod validate;

use metadata::StrongRelationInfo;
pub use metadata::{StrongRelationTargetInfo, strong_relation_target_from_kind};
pub use reverse_index::prepare_reverse_relation_index_mutations_for_source;
pub use validate::validate_delete_strong_relations_for_source;

///
/// StrongRelationDeleteValidateFn
///
/// Function-pointer contract for delete-side strong relation validators.
///

pub type StrongRelationDeleteValidateFn<C> =
    fn(&Db<C>, &str, &BTreeSet<RawDataKey>) -> Result<(), InternalError>;

///
/// RelationTargetRawKeyError
/// Error variants for building a relation target `RawDataKey` from user value.
///

#[derive(Debug)]
pub enum RelationTargetRawKeyError {
    StorageKeyEncode(StorageKeyEncodeError),
    TargetEntityName(EntityNameError),
}

// Build one relation target raw key from validated entity+storage key components.
fn raw_relation_target_key_from_parts(
    entity_name: EntityName,
    storage_key: StorageKey,
) -> Result<RawDataKey, StorageKeyEncodeError> {
    let entity_bytes = entity_name.to_bytes();
    let key_bytes = storage_key.to_bytes()?;
    let mut raw_bytes = [0u8; DataKey::STORED_SIZE_USIZE];
    raw_bytes[..EntityName::STORED_SIZE_USIZE].copy_from_slice(&entity_bytes);
    raw_bytes[EntityName::STORED_SIZE_USIZE..].copy_from_slice(&key_bytes);

    Ok(<RawDataKey as Storable>::from_bytes(
        std::borrow::Cow::Borrowed(raw_bytes.as_slice()),
    ))
}

/// Convert a relation target `Value` into its canonical `RawDataKey` representation.
pub fn build_relation_target_raw_key(
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
