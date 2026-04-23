//! Module: lib
//! Responsibility: crate root for the IcyDB core runtime surface.
//! Does not own: canister-facing facade APIs from the public `icydb` crate.
//! Boundary: exposes the engine subsystems used by schema, query, executor, and storage layers.

//! Core runtime for IcyDB: entity traits, values, executors, visitors, and
//! the ergonomics exported via the `prelude`.
#![warn(unreachable_pub)] // too complex to adhere to right now

extern crate self as icydb;

#[macro_use]
pub(crate) mod scalar_registry;

// public exports are one module level down
pub mod db;
pub mod error;
pub mod metrics;
pub mod model;
pub mod sanitize;
pub mod traits;
pub mod types;
pub mod validate;
pub mod value;
pub mod visitor;

// testing
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
        model::{entity::EntityModel, index::IndexModel},
        traits::{EntityKind, Path},
        value::{InputValue, OutputValue},
    };
}

// Macro/runtime wiring surface used by generated code in local core tests.
// This mirrors the facade crate's hidden generated-code boundary so derive
// output can target one stable path regardless of which workspace crate owns
// the test harness.
#[doc(hidden)]
pub mod __macro {
    pub use crate::error::InternalError;
    pub use crate::traits::{
        EnumValue, FieldProjection, PersistedFieldMetaCodec, PersistedStructuredFieldCodec,
        ValueSurfaceDecode, ValueSurfaceEncode, ValueSurfaceKind, ValueSurfaceMeta,
        value_surface_btree_map_from_value, value_surface_btree_set_from_value,
        value_surface_collection_to_value, value_surface_from_value, value_surface_from_vec_into,
        value_surface_from_vec_into_btree_map, value_surface_from_vec_into_btree_set,
        value_surface_into, value_surface_map_collection_to_value, value_surface_to_value,
        value_surface_vec_from_value,
    };
    pub use crate::value::{Value, ValueEnum};
}
