//! ## Crate layout
//! - `core`: runtime data model, filters, queries, values, and observability.
//! - `macros`: derive macros for entities, schemas, and views.
//! - `schema`: schema AST, builder, and validation utilities.
//! - `base`: builtin design-time helpers, sanitizers, and validators.
//! - `error`: shared error types for generated and runtime code.
//! - `build`: internal codegen helpers used by macros and tests.
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
// Macros
//

pub use core::{Error, build, db, start};

//
// Actor Prelude
//

pub mod prelude {
    pub use icydb_core::prelude::*;
}

//
// Design Prelude
// For schema/design code (macros, traits, base helpers).
//

pub mod design {
    pub mod prelude {
        pub use icydb_core::design::prelude::*;
    }
}
