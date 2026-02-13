use crate::{
    db::{
        index::{
            IndexEntry, IndexEntryCorruption,
            plan::{corruption_error, index_violation_error},
        },
        store::DataKey,
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    model::index::IndexModel,
    obs::sink::{self, MetricsEvent},
    traits::{EntityKind, EntityValue, FieldValue},
};

/// Validate unique index constraints against the existing index entry.
///
/// This detects:
/// - Index corruption (multiple keys in a unique entry)
/// - Uniqueness violations (conflicting key ownership)
pub(super) fn validate_unique_constraint<E: EntityKind + EntityValue>(
    db: &crate::db::Db<E::Canister>,
    index: &IndexModel,
    entry: Option<&IndexEntry<E>>,
    new_key: Option<&E::Key>,
    new_entity: Option<&E>,
) -> Result<(), InternalError> {
    if !index.unique {
        return Ok(());
    }

    let Some(entry) = entry else {
        return Ok(());
    };

    if entry.len() > 1 {
        return Err(corruption_error(
            ErrorOrigin::Index,
            format!(
                "index corrupted: {} ({}) -> {} keys",
                E::PATH,
                index.fields.join(", "),
                entry.len()
            ),
        ));
    }

    let Some(new_key) = new_key else {
        return Ok(());
    };
    if entry.contains(*new_key) {
        return Ok(());
    }

    let Some(new_entity) = new_entity else {
        return Err(InternalError::new(
            ErrorClass::InvariantViolation,
            ErrorOrigin::Index,
            "missing entity payload during unique validation".to_string(),
        ));
    };

    let existing_key = entry.single_id().ok_or_else(|| {
        corruption_error(
            ErrorOrigin::Index,
            format!(
                "index corrupted: {} ({}) -> {} keys",
                E::PATH,
                index.fields.join(", "),
                entry.len()
            ),
        )
    })?;

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
                format!("index hash collision: {} ({})", E::PATH, field),
            ));
        }
    }

    sink::record(MetricsEvent::UniqueViolation {
        entity_path: E::PATH,
    });

    Err(index_violation_error(E::PATH, index.fields))
}
