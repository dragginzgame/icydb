//! ## Crate layout
//! - `base`: builtin design-time helpers, sanitizers, and validators.
//! - `build`: internal codegen helpers used by macros and tests.
//! - `core`: runtime data model, filters, queries, values, and observability.
//! - `error`: shared error types for generated and runtime code.
//! - `macros`: derive macros for entities, schemas, and views.
//! - `schema`: schema AST, builder, and validation utilities.
//!
//! The `prelude` module mirrors the runtime surface used inside actor code;
//! `design::prelude` exposes schema and macro-facing helpers.

pub use icydb_base as base;
pub use icydb_build as build;
pub use icydb_core as core;
pub use icydb_error as error;
pub use icydb_macros as macros;
pub use icydb_schema as schema;

//
// Consts
//

/// Workspace version re-export for downstream tooling/tests.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

//
// Macros
//

pub use core::{Error, db, start};
pub use icydb_build::build;

//
// Actor Prelude
//

/// Bring the runtime-facing types, traits, and helpers into scope for actor code.
pub mod prelude {
    pub use icydb_core::prelude::*;
}

//
// Design Prelude
// For schema/design code (macros, traits, base helpers).
//

/// Schema/design-facing helpers (separate from the actor/runtime prelude).
pub mod design {
    pub mod prelude {
        pub use icydb_core::design::prelude::*;
    }
}
