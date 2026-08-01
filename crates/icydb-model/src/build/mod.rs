#[cfg(not(target_arch = "wasm32"))]
mod actor;

use crate::{
    ThisError,
    node::{Schema, SchemaGraphError},
    prelude::*,
};
use std::sync::{LazyLock, RwLock, RwLockReadGuard};

#[cfg(not(target_arch = "wasm32"))]
pub use actor::generate;

#[cfg(not(target_arch = "wasm32"))]
use crate::{Error, node::SchemaNode, schema_validate::validate_schema};
#[cfg(not(target_arch = "wasm32"))]
use std::sync::{Mutex, RwLockWriteGuard};

///
/// BuildError
///
/// Error returned when the process-global schema graph fails validation before
/// build-time code generation.
///

#[derive(Debug, ThisError)]
pub enum BuildError {
    /// Constructor registration did not produce one unique collecting graph.
    #[error(transparent)]
    Graph(#[from] SchemaGraphError),

    /// Whole-graph validation rejected the collected declarations.
    #[error("validation failed: {0}")]
    Validation(ErrorTree),
}

/// Process-global schema graph used during build-time code generation.
static SCHEMA: LazyLock<RwLock<Schema>> = LazyLock::new(|| RwLock::new(Schema::new()));
/// Serializes constructor registration with validation and sealing.
#[cfg(not(target_arch = "wasm32"))]
static REGISTRATION_GATE: Mutex<()> = Mutex::new(());

/// Acquire a write guard to the global schema during build-time codegen.
///
/// # Panics
///
/// Panics if the process-global schema lock has been poisoned.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn schema_write() -> RwLockWriteGuard<'static, Schema> {
    SCHEMA
        .write()
        .expect("schema RwLock poisoned while acquiring write lock")
}

/// Register one constructor-produced declaration in the collecting graph.
///
/// This is the only non-test mutation entrypoint generated declarations use.
/// Duplicate and late registrations remain recorded until graph sealing.
///
/// # Panics
///
/// Panics if the process-global registration gate or schema lock is poisoned.
#[cfg(not(target_arch = "wasm32"))]
pub fn register_node(node: SchemaNode) {
    let _registration = REGISTRATION_GATE
        .lock()
        .expect("schema registration gate poisoned while registering node");
    schema_write().insert_node(node);
}

/// Read the schema graph without triggering validation.
pub(crate) fn schema_read() -> RwLockReadGuard<'static, Schema> {
    SCHEMA
        .read()
        .expect("schema RwLock poisoned while acquiring read lock")
}

/// Read the immutable global schema after validating and sealing it.
///
/// # Errors
///
/// Returns a typed graph or whole-graph validation failure.
///
/// # Panics
///
/// Panics if the process-global registration gate or schema lock is poisoned.
#[cfg(not(target_arch = "wasm32"))]
pub fn get_schema() -> Result<RwLockReadGuard<'static, Schema>, Error> {
    let _registration = REGISTRATION_GATE
        .lock()
        .expect("schema registration gate poisoned while sealing graph");
    {
        let schema = schema_read();
        if !schema.is_sealed() {
            validate_schema(&schema).map_err(BuildError::Validation)?;
        }
    }
    {
        let mut schema = schema_write();
        schema.seal().map_err(BuildError::Graph)?;
    }

    Ok(schema_read())
}
