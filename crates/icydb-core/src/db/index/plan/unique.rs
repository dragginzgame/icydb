//! Module: index::plan::unique
//! Responsibility: preflight unique-constraint validation against the active
//! planner reader view (committed state or preflight overlay).
//! Does not own: commit-op encoding or apply-time writes.
//! Boundary: internal helper for index planning.

use crate::{
    db::{
        data::{
            DataKey, StorageKey, StructuralRowDecodeError, StructuralRowObject, StructuralRowSlots,
            decode_structural_field_value,
        },
        index::{
            IndexEntryCorruption, IndexEntryReader, IndexId, IndexKey, IndexStore, PrimaryRowReader,
        },
    },
    error::InternalError,
    metrics::sink::{MetricsEvent, record},
    model::{entity::resolve_primary_key_slot, index::IndexModel},
    traits::{EntityKind, EntityValue, FieldValue},
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
    new_key: Option<&E::Key>,
    new_entity: Option<&E>,
) -> Result<(), InternalError> {
    // Phase 1: fast exits for non-unique or non-insert/update paths.
    if !index.is_unique() {
        return Ok(());
    }

    let Some(new_entity) = new_entity else {
        // Delete/no-op paths do not need unique validation.
        return Ok(());
    };

    let Some(new_key) = new_key else {
        return Err(InternalError::index_invariant(
            "missing entity key during unique validation".to_string(),
        ));
    };
    let new_storage_key = StorageKey::try_from_value(&new_key.to_value())?;

    // Phase 2: build canonical unique-prefix components for the incoming row.
    let Some(new_index_key) = IndexKey::new(new_entity, index)? else {
        return Ok(());
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
    let row_object = decode_unique_row_object::<E>(&data_key, &row)?;
    let row_slots = row_object.slots_for_model(E::MODEL);
    let stored_key = decode_unique_row_storage_key::<E>(&data_key, &row_slots)?;
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
        build_unique_index_key_from_row_slots::<E>(&data_key, existing_key, &row_slots, index)?
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

// Decode one stored row through the canonical structural persisted-row path for
// unique validation.
fn decode_unique_row_object<E: EntityKind>(
    data_key: &DataKey,
    row: &crate::db::data::RawRow,
) -> Result<StructuralRowObject, InternalError> {
    StructuralRowObject::from_raw_row(row).map_err(|err| match err {
        StructuralRowDecodeError::Deserialize(source) => {
            InternalError::index_plan_serialize_corruption(format!(
                "failed to structurally deserialize row: {data_key} ({source})"
            ))
        }
        StructuralRowDecodeError::ExpectedTopLevelMap => {
            InternalError::index_plan_serialize_corruption(format!(
                "failed to structurally deserialize row: {data_key} (expected top-level CBOR map for {})",
                E::PATH
            ))
        }
    })
}

// Decode the authoritative primary-key slot structurally and verify that it
// still matches the row storage key carried by the unique index entry.
fn decode_unique_row_storage_key<E: EntityKind>(
    data_key: &DataKey,
    row_slots: &StructuralRowSlots<'_>,
) -> Result<StorageKey, InternalError> {
    let Some(primary_key_slot) = resolve_primary_key_slot(E::MODEL) else {
        return Err(InternalError::index_invariant(format!(
            "entity primary key field missing during unique validation: {} field={}",
            E::PATH,
            E::PRIMARY_KEY
        )));
    };
    let primary_key_value =
        decode_unique_row_slot_value::<E>(data_key, row_slots, primary_key_slot)?.ok_or_else(
            || {
                InternalError::index_plan_serialize_corruption(format!(
                    "missing primary-key slot while validating unique index row: {data_key}"
                ))
            },
        )?;

    StorageKey::try_from_value(&primary_key_value).map_err(|err| {
        InternalError::index_plan_serialize_corruption(format!(
            "failed to decode structural primary-key slot: {data_key} ({err})"
        ))
    })
}

// Build the canonical stored unique index key from one structural row slot
// reader without reconstructing the full typed entity.
fn build_unique_index_key_from_row_slots<E: EntityKind>(
    data_key: &DataKey,
    storage_key: StorageKey,
    row_slots: &StructuralRowSlots<'_>,
    index: &IndexModel,
) -> Result<Option<IndexKey>, InternalError> {
    let mut slot_decode_error = None;
    let mut read_slot = |slot| match decode_unique_row_slot_value::<E>(data_key, row_slots, slot) {
        Ok(value) => value,
        Err(err) => {
            slot_decode_error = Some(err);
            None
        }
    };
    let index_key = IndexKey::new_from_slot_reader(
        E::ENTITY_TAG,
        storage_key,
        E::MODEL,
        index,
        &mut read_slot,
    )?;

    if let Some(err) = slot_decode_error {
        return Err(err);
    }

    Ok(index_key)
}

// Decode one structural slot value using the canonical persisted-field decode
// contract for unique validation.
fn decode_unique_row_slot_value<E: EntityKind>(
    data_key: &DataKey,
    row_slots: &StructuralRowSlots<'_>,
    slot: usize,
) -> Result<Option<crate::value::Value>, InternalError> {
    let field = E::MODEL.fields().get(slot).ok_or_else(|| {
        InternalError::index_invariant(format!(
            "slot lookup outside model bounds during unique validation: {} slot={slot}",
            E::PATH
        ))
    })?;
    let Some(raw_value) = row_slots.field(slot) else {
        return Ok(None);
    };

    decode_structural_field_value(raw_value, field.kind(), field.storage_decode())
        .map(Some)
        .map_err(|err| {
            InternalError::index_plan_serialize_corruption(format!(
                "failed to structurally decode field '{}' while validating unique row {data_key}: {err}",
                field.name()
            ))
        })
}
