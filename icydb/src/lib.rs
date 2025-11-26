//! IcyDB — Data Models, Queries, and IC Integration for Canisters
//!
//! This is the public meta-crate. Downstream users depend on **icydb** only.
//!
//! It re-exports the stable public API from:
//!   - `icydb-base`     (design-time helpers)
//!   - `icydb-core`     (runtime data model, filters, queries, values…)
//!   - `icydb-error`    (error types)
//!   - `icydb-macros`   (derive macros)
//!   - `icydb-schema`   (schema definitions)
//!
//! Everything else (`icydb-build`) is internal.

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
