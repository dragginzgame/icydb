use crate::{
    db::index::{IndexEntry, IndexKey, IndexStore, plan::corruption_error},
    error::{ErrorOrigin, InternalError},
    model::index::IndexModel,
    traits::{EntityKind, EntityValue},
};
use std::{cell::RefCell, thread::LocalKey};

pub(super) fn load_existing_entry<E: EntityKind + EntityValue>(
    store: &'static LocalKey<RefCell<IndexStore>>,
    index: &'static IndexModel,
    entity: Option<&E>,
) -> Result<Option<IndexEntry<E>>, InternalError> {
    let Some(entity) = entity else {
        return Ok(None);
    };

    let Some(key) = IndexKey::new(entity, index)? else {
        return Ok(None);
    };

    store
        .with_borrow(|index_store| index_store.get(&key.to_raw()))
        .map(|raw_entry| {
            raw_entry.try_decode().map_err(|err| {
                corruption_error(
                    ErrorOrigin::Index,
                    format!(
                        "index corrupted: {} ({}) -> {}",
                        E::PATH,
                        index.fields.join(", "),
                        err
                    ),
                )
            })
        })
        .transpose()
}
