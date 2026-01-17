use crate::{
    db::{
        Db,
        executor::ExecutorError,
        store::{DataKey, IndexKey},
    },
    error::{ErrorOrigin, InternalError},
    model::index::IndexModel,
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
    let exists = db
        .context::<E>()
        .with_store(|store| store.get(&data_key).is_some())?;

    if !exists {
        return Err(ExecutorError::corruption(
            ErrorOrigin::Store,
            format!("index points to missing row: {data_key}"),
        )
        .into());
    }

    let pk = E::PrimaryKey::try_from_key(key).ok_or_else(|| {
        ExecutorError::KeyTypeMismatch(
            std::any::type_name::<E::PrimaryKey>().to_string(),
            key.to_string(),
        )
    })?;

    Ok(Some(pk))
}
