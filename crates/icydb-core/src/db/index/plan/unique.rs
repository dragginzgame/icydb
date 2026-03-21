//! Module: index::plan::unique
//! Responsibility: preflight unique-constraint validation against the active
//! planner reader view (committed state or preflight overlay).
//! Does not own: commit-op encoding or apply-time writes.
//! Boundary: internal helper for index planning.

use crate::{
    db::{
        data::{DataKey, StorageKey, StructuralSlotReader},
        index::{
            IndexEntryCorruption, IndexEntryReader, IndexId, IndexKey, IndexStore, PrimaryRowReader,
        },
    },
    error::InternalError,
    metrics::sink::{MetricsEvent, record},
    model::{entity::resolve_primary_key_slot, index::IndexModel},
    traits::{EntityKind, EntityValue},
};
use std::{cell::RefCell, collections::BTreeSet, ops::Bound, thread::LocalKey};

/// Validate unique index constraints against existing rows that share the same
/// index-component prefix.
///
/// This detects:
/// - Index corruption (multiple existing keys for a unique value)
/// - Uniqueness violations (conflicting key ownership)
///
/// Validation is performed against the active preflight reader view before
/// commit-op synthesis. In commit-window preflight this reader view can include
/// staged prior row ops (overlay), so same-window conflicts are detected
/// deterministically. The check allows self-ownership (entry contains `new_key`)
/// and rejects only conflicting ownership.
#[expect(clippy::too_many_lines)]
pub(super) fn validate_unique_constraint<E: EntityKind + EntityValue>(
    row_reader: &dyn PrimaryRowReader<E>,
    index_reader: &dyn IndexEntryReader<E>,
    index: &IndexModel,
    store: &'static LocalKey<RefCell<IndexStore>>,
    new_storage_key: Option<StorageKey>,
    new_index_key: Option<&IndexKey>,
) -> Result<(), InternalError> {
    // Phase 1: fast exits for non-unique or non-insert/update paths.
    if !index.is_unique() {
        return Ok(());
    }

    let Some(new_index_key) = new_index_key else {
        // Delete/no-op paths do not need unique validation.
        return Ok(());
    };

    let Some(new_storage_key) = new_storage_key else {
        return Err(InternalError::index_invariant(
            "missing entity key during unique validation".to_string(),
        ));
    };

    let mut encoded_prefix = Vec::with_capacity(new_index_key.component_count());
    for component_index in 0..new_index_key.component_count() {
        let Some(component) = new_index_key.component(component_index) else {
            return Err(InternalError::index_invariant(format!(
                "index key missing component {} during unique validation: {} ({})",
                component_index,
                E::PATH,
                index.fields().join(", ")
            )));
        };
        encoded_prefix.push(component.to_vec());
    }

    let index_id = IndexId::new(E::ENTITY_TAG, index.ordinal());
    let (lower, upper) = IndexKey::bounds_for_prefix(
        &index_id,
        new_index_key.component_count(),
        encoded_prefix.as_slice(),
    );
    let lower = Bound::Included(lower.to_raw());
    let upper = Bound::Included(upper.to_raw());

    // Unique validation only needs to distinguish 0, 1, or "more than 1".
    // Capping this probe avoids scanning large corrupted buckets.
    let unique_probe_limit = 2usize;
    let matching_storage_keys = index_reader.read_index_keys_in_raw_range(
        store,
        index,
        (&lower, &upper),
        unique_probe_limit,
    )?;
    let mut matching_keys = BTreeSet::new();
    for key in matching_storage_keys {
        matching_keys.insert(key);
    }

    if matching_keys.is_empty() {
        return Ok(());
    }

    if matching_keys.len() > 1 {
        return Err(InternalError::index_plan_index_corruption(format!(
            "index corrupted: {} ({}) -> {} keys",
            E::PATH,
            index.fields().join(", "),
            matching_keys.len()
        )));
    }

    if matching_keys.contains(&new_storage_key) {
        return Ok(());
    }

    let existing_key = matching_keys.iter().next().copied().ok_or_else(|| {
        InternalError::index_plan_index_corruption(format!(
            "index corrupted: {} ({}) -> failed to resolve existing key",
            E::PATH,
            index.fields().join(", "),
        ))
    })?;

    // Phase 3: prove that the stored row still belongs to this key and value
    // through the structural persisted-row decode path only.
    let data_key = DataKey::new(E::ENTITY_TAG, existing_key);
    let row = row_reader.read_primary_row(&data_key)?.ok_or_else(|| {
        InternalError::index_plan_store_corruption(format!("missing row: {data_key}"))
    })?;
    let row_fields = decode_unique_row_fields::<E>(&data_key, &row)?;
    let stored_key = decode_unique_row_storage_key::<E>(&data_key, &row_fields)?;
    if stored_key != existing_key {
        // Stored row decoded successfully but key disagreement is a cross-component invariant
        // failure, not a structural decode/persistence corruption.
        return Err(InternalError::index_plan_store_invariant(format!(
            "index invariant violated: {} ({}) -> {}",
            E::PATH,
            index.fields().join(", "),
            IndexEntryCorruption::RowKeyMismatch {
                indexed_key: Box::new(existing_key.as_value()),
                row_key: Box::new(stored_key.as_value()),
            }
        )));
    }

    let Some(stored_index_key) =
        build_unique_index_key_from_row_slots::<E>(&data_key, existing_key, &row_fields, index)?
    else {
        return Err(InternalError::index_plan_index_corruption(format!(
            "index corrupted: {} ({}) -> stored entity is not indexable for unique key",
            E::PATH,
            index.fields().join(", "),
        )));
    };
    if stored_index_key.component_count() != new_index_key.component_count() {
        return Err(InternalError::index_plan_index_corruption(format!(
            "index corrupted: {} ({}) -> mismatched unique key component count",
            E::PATH,
            index.fields().join(", "),
        )));
    }

    for component_index in 0..new_index_key.component_count() {
        let Some(expected) = new_index_key.component(component_index) else {
            return Err(InternalError::index_invariant(format!(
                "index key missing expected component {} during unique validation: {} ({})",
                component_index,
                E::PATH,
                index.fields().join(", "),
            )));
        };
        let Some(actual) = stored_index_key.component(component_index) else {
            return Err(InternalError::index_plan_index_corruption(format!(
                "index corrupted: {} ({}) -> stored entity missing component {}",
                E::PATH,
                index.fields().join(", "),
                component_index,
            )));
        };

        if expected != actual {
            return Err(InternalError::index_plan_index_corruption(format!(
                "index canonical collision: {} ({})",
                E::PATH,
                index.fields().join(", ")
            )));
        }
    }

    record(MetricsEvent::UniqueViolation {
        entity_path: E::PATH,
    });

    Err(InternalError::index_violation(E::PATH, index.fields()))
}

// Decode one stored row through the canonical structural persisted-row scanner
// for unique validation.
fn decode_unique_row_fields<'a, E: EntityKind>(
    data_key: &DataKey,
    row: &'a crate::db::data::RawRow,
) -> Result<StructuralSlotReader<'a>, InternalError> {
    StructuralSlotReader::from_raw_row(row, E::MODEL).map_err(|source| {
        InternalError::index_plan_serialize_corruption(format!(
            "failed to structurally deserialize row: {data_key} ({source})"
        ))
    })
}

// Decode the authoritative primary-key slot structurally and verify that it
// still matches the row storage key carried by the unique index entry.
fn decode_unique_row_storage_key<E: EntityKind>(
    data_key: &DataKey,
    row_fields: &StructuralSlotReader<'_>,
) -> Result<StorageKey, InternalError> {
    let _ = resolve_primary_key_slot(E::MODEL).ok_or_else(|| {
        InternalError::index_invariant(format!(
            "entity primary key field missing during unique validation: {} field={}",
            E::PATH,
            E::PRIMARY_KEY
        ))
    })?;
    row_fields
        .validate_storage_key_for_entity::<E>(data_key)
        .map_err(|source| {
            InternalError::index_plan_serialize_corruption(format!(
                "failed to decode structural primary-key slot: {data_key} ({source})"
            ))
        })?;

    Ok(data_key.storage_key())
}

// Build the canonical stored unique index key from one structural row slot
// reader without reconstructing the full typed entity.
fn build_unique_index_key_from_row_slots<E: EntityKind>(
    data_key: &DataKey,
    storage_key: StorageKey,
    row_fields: &StructuralSlotReader<'_>,
    index: &IndexModel,
) -> Result<Option<IndexKey>, InternalError> {
    IndexKey::new_from_slots(E::ENTITY_TAG, storage_key, row_fields, index).map_err(|err| {
        InternalError::index_plan_serialize_corruption(format!(
            "failed to structurally decode unique key row {data_key}: {err}",
        ))
    })
}
