pub mod reserved;
pub mod validate;

use crate::{
    Error, ThisError,
    node::{Entity, Schema, Store, VisitableNode},
    prelude::*,
    visit::ValidateVisitor,
};
use std::{
    collections::BTreeMap,
    sync::{LazyLock, OnceLock, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

///
/// BuildError
///

#[derive(Debug, ThisError)]
pub enum BuildError {
    #[error("validation failed: {0}")]
    Validation(ErrorTree),
}

///
/// SCHEMA
/// the static data structure
///

static SCHEMA: LazyLock<RwLock<Schema>> = LazyLock::new(|| RwLock::new(Schema::new()));

static SCHEMA_VALIDATED: OnceLock<bool> = OnceLock::new();

/// Acquire a write guard to the global schema during build-time codegen.
pub fn schema_write() -> RwLockWriteGuard<'static, Schema> {
    SCHEMA
        .write()
        .expect("schema RwLock poisoned while acquiring write lock")
}

// schema_read
// just reads the schema directly without validation
pub(crate) fn schema_read() -> RwLockReadGuard<'static, Schema> {
    SCHEMA
        .read()
        .expect("schema RwLock poisoned while acquiring read lock")
}

/// Read the global schema, validating it exactly once per process.
pub fn get_schema() -> Result<RwLockReadGuard<'static, Schema>, Error> {
    let schema = schema_read();
    validate(&schema).map_err(BuildError::Validation)?;

    Ok(schema)
}

// validate
fn validate(schema: &Schema) -> Result<(), ErrorTree> {
    if *SCHEMA_VALIDATED.get_or_init(|| false) {
        return Ok(());
    }

    // validate
    let mut visitor = ValidateVisitor::new();
    schema.accept(&mut visitor);
    validate_entity_names(schema, &mut visitor.errors);
    visitor.errors.result()?;

    SCHEMA_VALIDATED.set(true).ok();

    Ok(())
}

fn validate_entity_names(schema: &Schema, errs: &mut ErrorTree) {
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
