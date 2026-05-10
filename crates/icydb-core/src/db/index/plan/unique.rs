//! Module: index::plan::unique
//! Responsibility: preflight unique-constraint validation against the active
//! planner reader view (committed state or preflight overlay).
//! Does not own: commit-op encoding or apply-time writes.
//! Boundary: internal helper for index planning.

use crate::{
    db::{
        data::{DataKey, StorageKey, StructuralRowContract, StructuralSlotReader},
        index::{
            IndexId, IndexKey, IndexPlanReadView, IndexReadContract, plan::error::IndexPlanError,
        },
        schema::{SchemaIndexFieldPathInfo, SchemaIndexInfo},
    },
    error::InternalError,
    model::index::IndexModel,
    types::EntityTag,
};
use std::ops::Bound;

enum UniqueKeyAuthority<'a> {
    AcceptedFieldPath(&'a SchemaIndexInfo),
    GeneratedExpression(&'a IndexModel),
}

impl UniqueKeyAuthority<'_> {
    const fn index_id(&self, entity_tag: EntityTag) -> IndexId {
        match self {
            Self::AcceptedFieldPath(index) => IndexId::new(entity_tag, index.ordinal()),
            Self::GeneratedExpression(index) => IndexId::new(entity_tag, index.ordinal()),
        }
    }

    fn build_index_key_from_row_slots(
        &self,
        entity_tag: EntityTag,
        storage_key: StorageKey,
        row_contract: &StructuralRowContract,
        row_fields: &StructuralSlotReader<'_>,
    ) -> Result<Option<IndexKey>, InternalError> {
        match self {
            Self::AcceptedFieldPath(index) => {
                IndexKey::new_from_slots_with_accepted_field_path_index(
                    entity_tag,
                    storage_key,
                    index,
                    row_fields,
                )
            }
            Self::GeneratedExpression(index) => IndexKey::new_from_slots_with_contract(
                entity_tag,
                storage_key,
                row_contract,
                row_fields,
                index,
            ),
        }
    }

    fn unique_violation(&self, entity_path: &'static str) -> IndexPlanError {
        match self {
            Self::AcceptedFieldPath(index) => {
                let fields = index
                    .fields()
                    .iter()
                    .map(SchemaIndexFieldPathInfo::field_name)
                    .collect::<Vec<_>>();
                IndexPlanError::unique_violation(entity_path, &fields)
            }
            Self::GeneratedExpression(index) => {
                IndexPlanError::unique_violation(entity_path, index.fields())
            }
        }
    }
}

/// Validate one accepted field-path unique index constraint.
#[expect(clippy::too_many_arguments)]
pub(super) fn validate_unique_constraint_accepted_field_path_structural(
    entity_path: &'static str,
    entity_tag: EntityTag,
    read_view: &dyn IndexPlanReadView,
    row_contract: &StructuralRowContract,
    accepted_index: &SchemaIndexInfo,
    read_contract: IndexReadContract<'_>,
    index_fields: &str,
    new_storage_key: Option<StorageKey>,
    new_index_key: Option<&IndexKey>,
) -> Result<(), IndexPlanError> {
    validate_unique_constraint_structural_impl(
        entity_path,
        entity_tag,
        read_view,
        row_contract,
        UniqueKeyAuthority::AcceptedFieldPath(accepted_index),
        read_contract,
        index_fields,
        new_storage_key,
        new_index_key,
    )
}

/// Validate one generated expression unique index constraint.
#[expect(clippy::too_many_arguments)]
pub(super) fn validate_unique_constraint_structural(
    entity_path: &'static str,
    entity_tag: EntityTag,
    read_view: &dyn IndexPlanReadView,
    row_contract: &StructuralRowContract,
    index: &IndexModel,
    read_contract: IndexReadContract<'_>,
    index_fields: &str,
    new_storage_key: Option<StorageKey>,
    new_index_key: Option<&IndexKey>,
) -> Result<(), IndexPlanError> {
    validate_unique_constraint_structural_impl(
        entity_path,
        entity_tag,
        read_view,
        row_contract,
        UniqueKeyAuthority::GeneratedExpression(index),
        read_contract,
        index_fields,
        new_storage_key,
        new_index_key,
    )
}

#[expect(clippy::too_many_arguments)]
fn validate_unique_constraint_structural_impl(
    entity_path: &'static str,
    entity_tag: EntityTag,
    read_view: &dyn IndexPlanReadView,
    row_contract: &StructuralRowContract,
    key_authority: UniqueKeyAuthority<'_>,
    read_contract: IndexReadContract<'_>,
    index_fields: &str,
    new_storage_key: Option<StorageKey>,
    new_index_key: Option<&IndexKey>,
) -> Result<(), IndexPlanError> {
    // Phase 1: fast exits for non-unique or non-insert/update paths.
    if !read_contract.unique() {
        return Ok(());
    }

    let Some(new_index_key) = new_index_key else {
        // Delete/no-op paths do not need unique validation.
        return Ok(());
    };

    let Some(new_storage_key) = new_storage_key else {
        return Err(InternalError::index_unique_validation_entity_key_required().into());
    };

    let index_id = key_authority.index_id(entity_tag);
    if new_index_key.index_id() != &index_id {
        return Err(InternalError::index_unique_validation_corruption(
            entity_path,
            index_fields,
            "mismatched unique key index id",
        )
        .into());
    }
    let (lower, upper) = new_index_key.raw_bounds_for_all_components();
    let lower = Bound::Included(lower);
    let upper = Bound::Included(upper);

    // Unique validation only needs to distinguish 0, 1, or "more than 1".
    // Capping this probe avoids scanning large corrupted buckets.
    let unique_probe_limit = 2usize;
    let matching_storage_keys = read_view.read_index_keys_in_raw_range(
        entity_path,
        entity_tag,
        read_contract,
        (&lower, &upper),
        unique_probe_limit,
    )?;

    if matching_storage_keys.is_empty() {
        return Ok(());
    }

    if matching_storage_keys.len() > 1 {
        return Err(InternalError::index_unique_validation_corruption(
            entity_path,
            index_fields,
            format_args!("{} keys", matching_storage_keys.len()),
        )
        .into());
    }

    let existing_key = matching_storage_keys[0];
    if existing_key == new_storage_key {
        return Ok(());
    }

    // Phase 3: prove that the stored row still belongs to this key and value
    // through the structural persisted-row decode path only.
    let data_key = DataKey::new(entity_tag, existing_key);
    let row = read_view
        .read_primary_row(&data_key)?
        .ok_or_else(|| InternalError::index_unique_validation_row_required(&data_key))?;
    let row_fields = decode_unique_row_slots(&data_key, &row, row_contract)?;

    let Some(stored_index_key) = build_unique_index_key_from_row_slots(
        entity_tag,
        entity_path,
        &data_key,
        existing_key,
        row_contract,
        &row_fields,
        &key_authority,
    )?
    else {
        return Err(InternalError::index_unique_validation_corruption(
            entity_path,
            index_fields,
            "stored entity is not indexable for unique key",
        )
        .into());
    };
    if !stored_index_key.has_same_components(new_index_key) {
        return Err(InternalError::index_unique_validation_corruption(
            entity_path,
            index_fields,
            "index canonical collision",
        )
        .into());
    }

    Err(key_authority.unique_violation(entity_path))
}

// Decode one stored row through the canonical structural persisted-row scanner
// and validate its authoritative primary-key slot for unique validation.
fn decode_unique_row_slots<'a>(
    data_key: &DataKey,
    row: &'a crate::db::data::RawRow,
    row_contract: &StructuralRowContract,
) -> Result<StructuralSlotReader<'a>, InternalError> {
    let row_fields =
        StructuralSlotReader::from_raw_row_with_validated_contract(row, row_contract.clone())
            .map_err(|source| {
                InternalError::index_unique_validation_row_deserialize_failed(data_key, source)
            })?;
    row_fields
        .validate_storage_key(data_key)
        .map_err(|source| {
            InternalError::index_unique_validation_primary_key_decode_failed(data_key, source)
        })?;

    Ok(row_fields)
}

// Build the canonical stored unique index key from one structural row slot
// reader without reconstructing the full typed entity.
fn build_unique_index_key_from_row_slots(
    entity_tag: EntityTag,
    entity_path: &'static str,
    data_key: &DataKey,
    storage_key: StorageKey,
    row_contract: &StructuralRowContract,
    row_fields: &StructuralSlotReader<'_>,
    key_authority: &UniqueKeyAuthority<'_>,
) -> Result<Option<IndexKey>, InternalError> {
    let key = key_authority.build_index_key_from_row_slots(
        entity_tag,
        storage_key,
        row_contract,
        row_fields,
    );

    key.map_err(|err| {
        InternalError::index_unique_validation_key_rebuild_failed(data_key, entity_path, err)
    })
}
