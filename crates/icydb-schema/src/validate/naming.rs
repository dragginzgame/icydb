use crate::{
    node::{Entity, Schema, Store},
    prelude::*,
};
use std::collections::BTreeMap;

pub fn validate_entity_naming(schema: &Schema, errs: &mut ErrorTree) {
    let mut by_canister: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();

    for (entity_path, entity) in schema.get_nodes::<Entity>() {
        let store = match schema.cast_node::<Store>(entity.store) {
            Ok(store) => store,
            Err(e) => {
                errs.add(e);
                continue;
            }
        };

        let canister = store.canister.to_string();
        let name = entity.resolved_name().to_string();
        let entity_path = entity_path.to_string();

        let entry = by_canister.entry(canister.clone()).or_default();

        if let Some(prev) = entry.insert(name.clone(), entity_path.clone()) {
            err!(
                errs,
                "duplicate entity name '{name}' in canister '{canister}' for '{prev}' and '{entity_path}'"
            );
        }
    }
}
