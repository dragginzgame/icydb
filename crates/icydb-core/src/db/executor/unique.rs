use crate::{
    db::{
        Db,
        executor::ExecutorError,
        store::{DataKey, IndexKey},
    },
    error::{ErrorOrigin, InternalError},
    model::index::IndexModel,
    serialize::deserialize,
    traits::{EntityKind, FromKey},
};

/// Resolve the primary key for a unique index, if an entry exists.
///
/// Invariants enforced here:
/// - Unique index contains exactly one key when present (len == 1)
/// - Index entry must reference an existing entity in the primary store
/// - Stored `Key` must be convertible into `E::PrimaryKey`
pub fn resolve_unique_pk<E>(
    db: &Db<E::Canister>,
    index: &'static IndexModel,
    entity: &E,
) -> Result<Option<E::PrimaryKey>, InternalError>
where
    E: EntityKind,
    E::PrimaryKey: FromKey,
{
    let fields = || index.fields.join(", ");
    let Some(index_key) = IndexKey::new(entity, index) else {
        return Err(ExecutorError::IndexKeyMissing(E::PATH.to_string(), fields()).into());
    };

    let store = db.with_index(|reg| reg.try_get_store(index.store))?;
    let Some(entry) = store.with_borrow(|s| s.get(&index_key)) else {
        return Ok(None);
    };

    // corruption error
    let len = entry.len();
    let corrupted = || {
        ExecutorError::corruption(
            ErrorOrigin::Index,
            format!(
                "index corrupted: {} ({}) -> {} keys",
                E::PATH,
                fields(),
                len
            ),
        )
    };

    // index checks
    if len != 1 {
        return Err(corrupted().into());
    }
    let key = entry.single_key().ok_or_else(corrupted)?;

    // Ensure the index doesn't point to a missing primary record.
    let data_key = DataKey::new::<E>(key);
    let bytes = db.context::<E>().with_store(|store| store.get(&data_key))?;

    let Some(bytes) = bytes else {
        return Err(ExecutorError::corruption(
            ErrorOrigin::Store,
            format!("index points to missing row: {data_key}"),
        )
        .into());
    };

    let stored = deserialize::<E>(&bytes).map_err(|_| {
        ExecutorError::corruption(
            ErrorOrigin::Serialize,
            format!("failed to deserialize row: {data_key}"),
        )
    })?;

    // Re-validate indexed field values to guard against hash collisions.
    for field in index.fields {
        let expected = entity.get_value(field).ok_or_else(|| {
            ExecutorError::corruption(
                ErrorOrigin::Index,
                format!(
                    "index field missing on lookup entity: {} ({})",
                    E::PATH,
                    field
                ),
            )
        })?;
        let actual = stored.get_value(field).ok_or_else(|| {
            ExecutorError::corruption(
                ErrorOrigin::Index,
                format!(
                    "index field missing on stored entity: {} ({})",
                    E::PATH,
                    field
                ),
            )
        })?;

        if expected != actual {
            return Err(ExecutorError::corruption(
                ErrorOrigin::Index,
                format!("index hash collision: {} ({})", E::PATH, field),
            )
            .into());
        }
    }

    let pk = E::PrimaryKey::try_from_key(key).ok_or_else(|| {
        ExecutorError::KeyTypeMismatch(
            std::any::type_name::<E::PrimaryKey>().to_string(),
            key.to_string(),
        )
    })?;

    Ok(Some(pk))
}
