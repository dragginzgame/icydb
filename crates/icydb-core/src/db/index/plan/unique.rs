//! Module: index::plan::unique
//! Responsibility: preflight unique-constraint validation against the active
//! planner reader view (committed state or preflight overlay).
//! Does not own: commit-op encoding or apply-time writes.
//! Boundary: internal helper for index planning.

use crate::{
    db::{
        data::DataKey,
        index::{
            IndexEntryCorruption, IndexEntryReader, IndexId, IndexKey, IndexStore, PrimaryRowReader,
        },
    },
    error::InternalError,
    metrics::sink::{MetricsEvent, record},
    model::index::IndexModel,
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
    row_reader: &impl PrimaryRowReader<E>,
    index_reader: &impl IndexEntryReader<E>,
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

    let index_id = IndexId::new::<E>(index);
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
    let matching_data_keys = index_reader.read_index_keys_in_raw_range(
        store,
        index,
        (&lower, &upper),
        unique_probe_limit,
    )?;
    let mut matching_keys = BTreeSet::new();
    for key in matching_data_keys {
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

    if matching_keys.contains(new_key) {
        return Ok(());
    }

    let existing_key = matching_keys.iter().next().copied().ok_or_else(|| {
        InternalError::index_plan_index_corruption(format!(
            "index corrupted: {} ({}) -> failed to resolve existing key",
            E::PATH,
            index.fields().join(", "),
        ))
    })?;

    // Phase 3: verify the stored row still belongs to this key and value.
    let stored = {
        let data_key = DataKey::try_new::<E>(existing_key)?;
        let row = row_reader.read_primary_row(&data_key)?.ok_or_else(|| {
            InternalError::index_plan_store_corruption(format!("missing row: {data_key}"))
        })?;
        row.try_decode::<E>().map_err(|err| {
            InternalError::index_plan_serialize_corruption(format!(
                "failed to deserialize row: {data_key} ({err})"
            ))
        })?
    };

    let stored_key = stored.id().key();
    if stored_key != existing_key {
        // Stored row decoded successfully but key disagreement is a cross-component invariant
        // failure, not a structural decode/persistence corruption.
        return Err(InternalError::index_plan_store_invariant(format!(
            "index invariant violated: {} ({}) -> {}",
            E::PATH,
            index.fields().join(", "),
            IndexEntryCorruption::RowKeyMismatch {
                indexed_key: Box::new(existing_key.to_value()),
                row_key: Box::new(stored_key.to_value()),
            }
        )));
    }

    let Some(stored_index_key) = IndexKey::new(&stored, index)? else {
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
