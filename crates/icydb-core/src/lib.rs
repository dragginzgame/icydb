//! Module: lib
//! Responsibility: crate root for the IcyDB core runtime surface.
//! Does not own: canister-facing facade APIs from the public `icydb` crate.
//! Boundary: exposes the engine subsystems used by schema, query, executor, and storage layers.

//! Core runtime for IcyDB: accepted-schema execution, values, storage, and
//! the low-level vocabulary exported through the facade.
#![warn(unreachable_pub)]
// The no-default test target intentionally type-checks shared test/helper
// surfaces whose consuming tests live behind SQL, SQL-explain, or diagnostics
// features. Keep production and all-features dead-code linting strict.
#![cfg_attr(
    all(test, not(feature = "sql")),
    expect(
        dead_code,
        reason = "shared test helpers have SQL, SQL-explain, and diagnostics consumers"
    )
)]
// The query-only build owns the complete engine-neutral planner/executor
// substrate. SQL is an optional frontend over that substrate and is currently
// the only consumer of some grouped, aggregate, cursor, and diagnostic
// capabilities, so those internal extension points are intentionally dormant
// when `query` is enabled without `sql`.
#![cfg_attr(
    all(not(test), feature = "query", not(feature = "sql")),
    expect(
        dead_code,
        reason = "SQL is an optional frontend and currently consumes engine-neutral grouped, aggregate, cursor, and diagnostic capabilities not exposed by the query-only facade"
    )
)]

extern crate self as icydb;

#[macro_use]
pub(crate) mod scalar_registry;

// public exports are one module level down
pub mod db;
pub mod error;
pub mod metrics;
pub(crate) mod runtime;
pub mod traits;
pub mod types;
pub mod value;

#[cfg(test)]
pub(crate) mod testing;

///
/// CONSTANTS
///

/// Maximum number of indexed fields allowed on an entity.
///
/// This limit keeps hashed index keys within bounded, storable sizes and
/// simplifies sizing tests in the stores.
pub const MAX_INDEX_FIELDS: usize = 4;

///
/// Prelude
///
/// Prelude contains only domain vocabulary.
/// No errors, executors, stores, serializers, or helpers are re-exported here.
///

pub mod prelude {
    pub use crate::{
        traits::Path,
        value::{InputValue, OutputValue},
    };
}
