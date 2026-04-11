use crate::{Error, ThisError, node::Schema, prelude::*, validate::validate_schema};
use std::sync::{LazyLock, OnceLock, RwLock, RwLockReadGuard, RwLockWriteGuard};

///
/// BuildError
///
/// Error returned when the process-global schema graph fails validation before
/// build-time code generation.
///

#[derive(Debug, ThisError)]
pub enum BuildError {
    #[error("validation failed: {0}")]
    Validation(ErrorTree),
}

/// Process-global schema graph used during build-time code generation.
static SCHEMA: LazyLock<RwLock<Schema>> = LazyLock::new(|| RwLock::new(Schema::new()));

static SCHEMA_VALIDATED: OnceLock<bool> = OnceLock::new();

/// Acquire a write guard to the global schema during build-time codegen.
pub fn schema_write() -> RwLockWriteGuard<'static, Schema> {
    SCHEMA
        .write()
        .expect("schema RwLock poisoned while acquiring write lock")
}

/// Read the schema graph without triggering validation.
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

/// Validate the schema once per process before exposing it to codegen.
fn validate(schema: &Schema) -> Result<(), ErrorTree> {
    if *SCHEMA_VALIDATED.get_or_init(|| false) {
        return Ok(());
    }

    validate_schema(schema)?;

    SCHEMA_VALIDATED.set(true).ok();

    Ok(())
}
