use crate::{
    db::{
        index::{
            IndexEntry, IndexEntryCorruption,
            key::encode_canonical_index_component,
            plan::{corruption_error, index_violation_error},
        },
        store::DataKey,
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    model::index::IndexModel,
    obs::sink::{MetricsEvent, record},
    traits::{EntityKind, EntityValue, FieldValue},
};
use std::collections::BTreeSet;

/// Validate unique index constraints against existing rows that share the same
/// index-component prefix.
///
/// This detects:
/// - Index corruption (multiple existing keys for a unique value)
/// - Uniqueness violations (conflicting key ownership)
#[expect(clippy::too_many_lines)]
pub(super) fn validate_unique_constraint<E: EntityKind + EntityValue>(
    db: &crate::db::Db<E::Canister>,
    index: &IndexModel,
    _entry: Option<&IndexEntry<E>>,
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
        return Err(InternalError::new(
            ErrorClass::InvariantViolation,
            ErrorOrigin::Index,
            "missing entity key during unique validation".to_string(),
        ));
    };

    // Phase 1: build the semantic prefix and short-circuit when the value is
    // not canonically indexable (for example Null/unsupported kinds).
    let mut prefix_values = Vec::with_capacity(index.fields.len());
    for field in index.fields {
        let expected = new_entity.get_value(field).ok_or_else(|| {
            InternalError::new(
                ErrorClass::InvariantViolation,
                ErrorOrigin::Index,
                format!(
                    "index field missing on lookup entity: {} ({})",
                    E::PATH,
                    field
                ),
            )
        })?;

        if encode_canonical_index_component(&expected).is_err() {
            return Ok(());
        }

        prefix_values.push(expected);
    }

    // Phase 2: resolve all rows currently indexed at this unique prefix.
    let index_store = db
        .with_store_registry(|registry| registry.try_get_store(index.store))?
        .index_store();

    let matching_data_keys =
        index_store.with_borrow(|store| store.resolve_data_values::<E>(index, &prefix_values))?;

    let mut matching_keys = BTreeSet::new();
    for data_key in matching_data_keys {
        matching_keys.insert(data_key.try_key::<E>()?);
    }

    if matching_keys.is_empty() {
        return Ok(());
    }

    if matching_keys.len() > 1 {
        return Err(corruption_error(
            ErrorOrigin::Index,
            format!(
                "index corrupted: {} ({}) -> {} keys",
                E::PATH,
                index.fields.join(", "),
                matching_keys.len()
            ),
        ));
    }

    if matching_keys.contains(new_key) {
        return Ok(());
    }

    let existing_key = matching_keys.iter().next().copied().ok_or_else(|| {
        corruption_error(
            ErrorOrigin::Index,
            format!(
                "index corrupted: {} ({}) -> failed to resolve existing key",
                E::PATH,
                index.fields.join(", "),
            ),
        )
    })?;

    // Phase 3: verify the stored row still belongs to this key and value.
    let stored = {
        let data_key = DataKey::try_new::<E>(existing_key)?;
        let row = db.context::<E>().read_strict(&data_key)?;
        row.try_decode::<E>().map_err(|err| {
            corruption_error(
                ErrorOrigin::Serialize,
                format!("failed to deserialize row: {data_key} ({err})"),
            )
        })?
    };

    let stored_key = stored.id().key();
    if stored_key != existing_key {
        // Stored row decoded successfully but key mismatch indicates index/data divergence; treat as corruption.
        return Err(corruption_error(
            ErrorOrigin::Store,
            format!(
                "index corrupted: {} ({}) -> {}",
                E::PATH,
                index.fields.join(", "),
                IndexEntryCorruption::RowKeyMismatch {
                    indexed_key: Box::new(existing_key.to_value()),
                    row_key: Box::new(stored_key.to_value()),
                }
            ),
        ));
    }

    for field in index.fields {
        let expected = new_entity.get_value(field).ok_or_else(|| {
            InternalError::new(
                ErrorClass::InvariantViolation,
                ErrorOrigin::Index,
                format!(
                    "index field missing on lookup entity: {} ({})",
                    E::PATH,
                    field
                ),
            )
        })?;
        let actual = stored.get_value(field).ok_or_else(|| {
            corruption_error(
                ErrorOrigin::Index,
                format!(
                    "index corrupted: {} ({}) -> stored entity missing field",
                    E::PATH,
                    field
                ),
            )
        })?;

        if expected != actual {
            return Err(corruption_error(
                ErrorOrigin::Index,
                format!("index canonical collision: {} ({})", E::PATH, field),
            ));
        }
    }

    record(MetricsEvent::UniqueViolation {
        entity_path: E::PATH,
    });

    Err(index_violation_error(E::PATH, index.fields))
}
