use crate::{
    IndexSpec,
    db::{
        Db,
        executor::ExecutorError,
        store::{DataKey, IndexKey},
    },
    runtime_error::RuntimeError,
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
    index: &'static IndexSpec,
    entity: &E,
) -> Result<Option<E::PrimaryKey>, RuntimeError>
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

    let len = entry.len();
    if len != 1 {
        return Err(ExecutorError::IndexCorrupted(E::PATH.to_string(), fields(), len).into());
    }

    let key = entry
        .single_key()
        .ok_or_else(|| ExecutorError::IndexCorrupted(E::PATH.to_string(), fields(), len))?;

    // Ensure the index doesn't point to a missing primary record.
    let data_key = DataKey::new::<E>(key);
    let exists = db
        .context::<E>()
        .with_store(|store| store.get(&data_key).is_some())?;

    if !exists {
        return Err(ExecutorError::IndexCorrupted(E::PATH.to_string(), fields(), len).into());
    }

    let pk = E::PrimaryKey::try_from_key(key).ok_or_else(|| {
        ExecutorError::KeyTypeMismatch(
            std::any::type_name::<E::PrimaryKey>().to_string(),
            key.to_string(),
        )
    })?;

    Ok(Some(pk))
}
