//! Test-only helpers for the parent module.

use super::*;

pub(in crate::db) fn accepted_runtime_entity_for_name<C: CanisterKind>(
    db: &Db<C>,
    entity_name: &str,
) -> Result<Option<AcceptedRuntimeEntity>, InternalError> {
    let mut matched = None;
    for (store_path, store) in sorted_registered_stores(db) {
        let entity = store.with_schema(|schema| {
            schema.current_accepted_runtime_entity_for_name(store_path, entity_name)
        })?;
        merge_unique_entity_match(&mut matched, entity)?;
    }
    Ok(matched)
}
