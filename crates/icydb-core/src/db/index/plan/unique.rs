use crate::{
    db::{
        Db,
        data::DataKey,
        index::{Direction, EncodedValue, IndexEntryCorruption, IndexId, IndexKey},
    },
    error::InternalError,
    model::{entity::resolve_field_slot, index::IndexModel},
    obs::sink::{MetricsEvent, record},
    traits::{EntityKind, EntityValue, FieldValue},
};
use std::{collections::BTreeSet, ops::Bound};

/// Validate unique index constraints against existing rows that share the same
/// index-component prefix.
///
/// This detects:
/// - Index corruption (multiple existing keys for a unique value)
/// - Uniqueness violations (conflicting key ownership)
///
/// Validation is performed against the current committed store state before
/// commit-op synthesis. It allows self-ownership (`matching_keys` contains
/// `new_key`) and rejects only conflicting ownership.
#[expect(clippy::too_many_lines)]
pub(super) fn validate_unique_constraint<E: EntityKind + EntityValue>(
    db: &Db<E::Canister>,
    index: &IndexModel,
    new_key: Option<&E::Key>,
    new_entity: Option<&E>,
) -> Result<(), InternalError> {
    if !index.unique {
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

    let mut indexed_field_slots = Vec::with_capacity(index.fields.len());
    for field in index.fields {
        let Some(field_index) = resolve_field_slot(E::MODEL, field) else {
            return Err(InternalError::index_invariant(format!(
                "index field missing on entity model: {} ({})",
                E::PATH,
                field
            )));
        };

        indexed_field_slots.push((*field, field_index));
    }

    // Phase 1: build the semantic prefix and short-circuit when the value is
    // not canonically indexable (for example Null/unsupported kinds).
    let mut encoded_prefix = Vec::with_capacity(index.fields.len());
    for (field, field_index) in indexed_field_slots.iter().copied() {
        let expected = new_entity.get_value_by_index(field_index).ok_or_else(|| {
            InternalError::index_invariant(format!(
                "index field missing on lookup entity: {} ({})",
                E::PATH,
                field
            ))
        })?;

        let Ok(encoded_value) = EncodedValue::try_from_ref(&expected) else {
            return Ok(());
        };
        encoded_prefix.push(encoded_value);
    }

    // Phase 2: resolve all rows currently indexed at this unique prefix.
    let index_store = db
        .with_store_registry(|registry| registry.try_get_store(index.store))?
        .index_store();
    let index_id = IndexId::new::<E>(index);
    let (lower, upper) =
        IndexKey::bounds_for_prefix(&index_id, index.fields.len(), encoded_prefix.as_slice());
    let (lower, upper) = (
        Bound::Included(lower.to_raw()),
        Bound::Included(upper.to_raw()),
    );

    // Unique validation only needs to distinguish 0, 1, or "more than 1".
    // Capping this probe avoids scanning large corrupted buckets.
    let unique_probe_limit = 2usize;
    let matching_data_keys = index_store.with_borrow(|store| {
        store.resolve_data_values_in_raw_range_limited::<E>(
            index,
            (&lower, &upper),
            None,
            Direction::Asc,
            unique_probe_limit,
            None,
        )
    })?;

    let mut matching_keys = BTreeSet::new();
    for data_key in matching_data_keys {
        matching_keys.insert(data_key.try_key::<E>()?);
    }

    if matching_keys.is_empty() {
        return Ok(());
    }

    if matching_keys.len() > 1 {
        return Err(InternalError::index_plan_index_corruption(format!(
            "index corrupted: {} ({}) -> {} keys",
            E::PATH,
            index.fields.join(", "),
            matching_keys.len()
        )));
    }

    if matching_keys.contains(new_key) {
        return Ok(());
    }

    let existing_key = matching_keys.iter().next().copied().ok_or_else(|| {
        InternalError::index_plan_index_corruption(format!(
            "index corrupted: {} ({}) -> failed to resolve existing key",
            E::PATH,
            index.fields.join(", "),
        ))
    })?;

    // Phase 3: verify the stored row still belongs to this key and value.
    let stored = {
        let data_key = DataKey::try_new::<E>(existing_key)?;
        let row = db.context::<E>().read_strict(&data_key)?;
        row.try_decode::<E>().map_err(|err| {
            InternalError::index_plan_serialize_corruption(format!(
                "failed to deserialize row: {data_key} ({err})"
            ))
        })?
    };

    let stored_key = stored.id().key();
    if stored_key != existing_key {
        // Stored row decoded successfully but key mismatch indicates index/data divergence; treat as corruption.
        return Err(InternalError::index_plan_store_corruption(format!(
            "index corrupted: {} ({}) -> {}",
            E::PATH,
            index.fields.join(", "),
            IndexEntryCorruption::RowKeyMismatch {
                indexed_key: Box::new(existing_key.to_value()),
                row_key: Box::new(stored_key.to_value()),
            }
        )));
    }

    for (field, field_index) in indexed_field_slots.iter().copied() {
        let expected = new_entity.get_value_by_index(field_index).ok_or_else(|| {
            InternalError::index_invariant(format!(
                "index field missing on lookup entity: {} ({})",
                E::PATH,
                field
            ))
        })?;
        let actual = stored.get_value_by_index(field_index).ok_or_else(|| {
            InternalError::index_plan_index_corruption(format!(
                "index corrupted: {} ({}) -> stored entity missing field",
                E::PATH,
                field
            ))
        })?;

        if expected != actual {
            return Err(InternalError::index_plan_index_corruption(format!(
                "index canonical collision: {} ({})",
                E::PATH,
                field
            )));
        }
    }

    record(MetricsEvent::UniqueViolation {
        entity_path: E::PATH,
    });

    Err(InternalError::index_violation(E::PATH, index.fields))
}
