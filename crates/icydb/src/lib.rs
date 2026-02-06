//! # icydb
//!
//! `icydb` is the **public facade crate** for the IcyDB runtime.
//! It is the recommended dependency for downstream canister projects.
//!
//! This crate exposes:
//! - the stable runtime surface used inside canister actor code,
//! - schema and design-time helpers for macros and validation,
//! - and a small set of macros and entry points that wire generated code.
//!
//! Low-level execution, storage, and engine internals live in
//! `icydb-core` and are exposed only through `__internal`.
//!
//! ## Crate layout
//!
//! - `base`
//!   Design-time helpers, sanitizers, and validators used by schemas and macros.
//!
//! - `build`
//!   Internal code generation helpers used by macros and tests
//!   (not intended for direct use).
//!
//! - `model` / `traits` / `types` / `value` / `view` / `visitor`
//!   Stable runtime and schema-facing building blocks used by generated code.
//!
//! - `__internal::core` *(internal)*
//!   Full engine internals for macros/tests. Not covered by semver guarantees.
//!
//! - `error`
//!   Shared error types for generated code and runtime boundaries.
//!
//! - `macros`
//!   Derive macros for entities, schemas, and views.
//!
//! - `schema`
//!   Schema AST, builders, and validation utilities.
//!
//! - `db`
//!   The public database façade: session handles, query builders,
//!   and typed responses.
//!
//! ## Preludes
//!
//! - `prelude`
//!   Opinionated runtime prelude for canister actor code.
//!   Intended to be glob-imported in `lib.rs` to keep endpoints concise.
//!
//! - `design::prelude`
//!   Prelude for schema and design-time code (macros, validators,
//!   and base helpers).
//!
//! ## Internal boundaries
//!
//! The `__internal` module exposes selected engine internals strictly for
//! generated code and macro expansion. It is not part of the supported API
//! surface and may change without notice.

// export so things just work in base/
extern crate self as icydb;

// crates
pub use icydb_build as build;
pub use icydb_build::build;
pub use icydb_schema as schema;
pub use icydb_schema_derive as macros;

// core modules
pub use icydb_core::{model, obs, traits, types, value, view, visitor};

// canic modules
pub mod base;
pub mod db;
pub mod error;
pub use error::Error;

/// Internal
#[doc(hidden)]
pub mod __internal {
    pub use icydb_core as core;
}

/// re-exports
///
/// macros can use these, stops the user having to specify all the dependencies
/// in the Cargo.toml file manually
///
/// these have to be in icydb_core because of the base library not being able to import icydb
pub mod __reexports {
    pub use canic_cdk;
    pub use canic_memory;
    pub use ctor;
    pub use derive_more;
    pub use icydb_derive;
    pub use num_traits;
    pub use remain;
}

///
/// Actor Prelude
/// using _ brings traits into scope and avoids name conflicts
///

pub mod prelude {
    pub use crate::{
        db,
        db::{
            query,
            query::{FilterExpr, SortExpr, builder::FieldRef, predicate::Predicate},
        },
        traits::{
            Collection as _, CreateView as _, EntityKind as _, Inner as _, MapCollection as _,
            Path as _, UpdateView as _, View as _,
        },
        types::*,
        value::Value,
        view::{Create, Update, View},
    };
    pub use candid::CandidType;
    pub use serde::{Deserialize, Serialize};
}

///
/// Design Prelude
/// For schema/design code (macros, traits, base helpers).
///

pub mod design {
    pub mod prelude {
        pub use ::candid::CandidType;
        pub use ::derive_more;

        pub use crate::{
            base, db,
            db::query::builder::FieldRef,
            macros::*,
            traits::{
                Collection as _, EntityKind, FieldValue as _, Inner as _, MapCollection as _,
                Path as _, Sanitize as _, Sanitizer, Serialize as _, Validate as _, ValidateCustom,
                Validator, View as _, Visitable as _,
            },
            types::*,
            value::Value,
            view::View,
            visitor::VisitorContext,
        };
    }
}

/// -------------------------- CODE -----------------------------------
use icydb_core::{error::InternalError, traits::Visitable};
use serde::{Serialize, de::DeserializeOwned};

///
/// Consts
///

/// Workspace version re-export for downstream tooling/tests.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

///
/// Macros
///

/// Include the generated actor module emitted by `build!` (placed in `OUT_DIR/actor.rs`).
#[macro_export]
macro_rules! start {
    () => {
        // actor.rs
        include!(concat!(env!("OUT_DIR"), "/actor.rs"));
    };
}

/// Access the current canister's database session; use `db!().debug()` for verbose tracing.
#[macro_export]
#[allow(clippy::crate_in_macro_def)]
macro_rules! db {
    () => {
        crate::db()
    };
}

///
/// Helpers
///

/// Run sanitization over a mutable visitable tree.
pub fn sanitize(node: &mut dyn Visitable) -> Result<(), Error> {
    icydb_core::sanitize::sanitize(node)
        .map_err(InternalError::from)
        .map_err(Error::from)
}

/// Validate a visitable tree, collecting issues by path.
pub fn validate(node: &dyn Visitable) -> Result<(), Error> {
    icydb_core::validate::validate(node)
        .map_err(InternalError::from)
        .map_err(Error::from)
}

/// Serialize a visitable value into bytes.
///
/// The encoding format is an internal detail of icydb and is only
/// guaranteed to round-trip via `deserialize`.
pub fn serialize<T>(ty: &T) -> Result<Vec<u8>, Error>
where
    T: Serialize,
{
    icydb_core::serialize::serialize(ty)
        .map_err(InternalError::from)
        .map_err(Error::from)
}

/// Deserialize bytes into a concrete visitable value.
///
/// This is intended for testing, tooling, and round-trip verification.
/// It should not be used in hot runtime paths.
pub fn deserialize<T>(bytes: &[u8]) -> Result<T, Error>
where
    T: DeserializeOwned,
{
    icydb_core::serialize::deserialize(bytes)
        .map_err(InternalError::from)
        .map_err(Error::from)
}
