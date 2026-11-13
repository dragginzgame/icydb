use crate::{
    node::{Entity, Schema, Store},
    prelude::*,
};
use std::collections::BTreeMap;

pub fn validate_entity_naming(schema: &Schema, errs: &mut ErrorTree) {
    let mut by_canister: BTreeMap<String, BTreeMap<String, (String, String)>> = BTreeMap::new();

    for (entity_path, entity) in schema.get_nodes::<Entity>() {
        let store = match schema.cast_node::<Store>(entity.store()) {
            Ok(store) => store,
            Err(e) => {
                errs.addf(format_args!("{e}"));
                continue;
            }
        };

        let canister = store.canister().to_string();
        let name = entity.name().to_string();
        let canonical_name = name.to_ascii_lowercase();
        let entity_path = entity_path.to_string();

        let entry = by_canister.entry(canister.clone()).or_default();

        if let Some((prev_name, prev_path)) =
            entry.insert(canonical_name, (name.clone(), entity_path.clone()))
        {
            err!(
                errs,
                "duplicate entity name '{name}' in canister '{canister}' for '{prev_path}' and '{entity_path}' (case-insensitive conflict with '{prev_name}')"
            );
        }
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests;
