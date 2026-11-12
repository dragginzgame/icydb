use crate::{
    node::{Enum, List, Map, Newtype, Record, Schema, Set, Tuple},
    prelude::*,
};
use std::collections::BTreeMap;

/// Enforce graph-wide uniqueness for current named-type declarations.
pub(super) fn validate_type_names(schema: &Schema, errors: &mut ErrorTree) {
    let mut types = BTreeMap::new();
    for (path, node) in schema.get_nodes::<Record>() {
        record_unique_name(&mut types, node.name(), path, "type", errors);
    }
    for (path, node) in schema.get_nodes::<Enum>() {
        record_unique_name(&mut types, node.name(), path, "type", errors);
    }
    for (path, node) in schema.get_nodes::<Newtype>() {
        record_unique_name(&mut types, node.name(), path, "type", errors);
    }
    for (path, node) in schema.get_nodes::<List>() {
        record_unique_name(&mut types, node.name(), path, "type", errors);
    }
    for (path, node) in schema.get_nodes::<Set>() {
        record_unique_name(&mut types, node.name(), path, "type", errors);
    }
    for (path, node) in schema.get_nodes::<Map>() {
        record_unique_name(&mut types, node.name(), path, "type", errors);
    }
    for (path, node) in schema.get_nodes::<Tuple>() {
        record_unique_name(&mut types, node.name(), path, "type", errors);
    }
}

fn record_unique_name<'a>(
    seen: &mut BTreeMap<&'a str, &'a str>,
    name: &'a str,
    path: &'a str,
    kind: &str,
    errors: &mut ErrorTree,
) {
    if let Some(previous_path) = seen.insert(name, path) {
        err!(
            errors,
            "duplicate {kind} name '{name}' for '{previous_path}' and '{path}'",
        );
    }
}

#[cfg(test)]
mod tests;
