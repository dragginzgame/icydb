//! Core runtime for IcyDB: entity traits, values, executors, visitors, and
//! the ergonomics exported via the `prelude`.
#![warn(unreachable_pub)] // too complex to adhere to right now

extern crate self as icydb;

#[macro_use]
pub(crate) mod scalar_registry;

// public exports are one module level down
pub mod db;
pub mod error;
pub mod model;
pub mod obs;
pub mod patch;
pub mod sanitize;
pub mod serialize;
pub mod traits;
pub mod types;
pub mod validate;
pub mod value;
pub mod visitor;

// test
#[cfg(test)]
pub(crate) mod test_fixtures;
#[cfg(test)]
pub(crate) mod test_support;

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
        traits::{EntityIdentity, EntityKind, Path},
        value::Value,
    };
}
