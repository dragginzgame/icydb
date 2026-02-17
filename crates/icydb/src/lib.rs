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
//! - `traits` / `types` / `value` / `view` / `visitor`
//!   Stable runtime and schema-facing building blocks used by generated code.
//!
//! - `model` / `obs` *(internal)*
//!   Runtime model and metrics internals. Exposed for advanced tooling only;
//!   not part of the supported semver surface.
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
#[doc(hidden)]
pub use icydb_core::{types, value};

#[doc(hidden)]
pub mod model {
    pub use icydb_core::model::{EntityModel, FieldModel, IndexModel};
}

#[doc(hidden)]
pub mod obs {
    pub use icydb_core::obs::{
        EventReport, MetricsSink, StorageReport, metrics_report, metrics_reset_all, storage_report,
    };
}

pub mod visitor {
    pub use icydb_core::{
        Issue, PathSegment, ScopedContext, VisitorContext, VisitorCore, VisitorError,
        VisitorIssues, VisitorMutCore, perform_visit, perform_visit_mut,
    };
}

// facade modules
pub mod base;
pub mod db;
pub mod error;
pub mod patch;
pub mod traits;
pub use error::Error;

/// Internal
#[doc(hidden)]
pub mod __internal {
    pub use icydb_core as core;
}

/// Macro/runtime wiring surface used by generated code.
/// This is intentionally narrow and not semver-stable.
#[doc(hidden)]
pub mod __macro {
    pub use icydb_core::db::{
        DataStore, Db, EntityRuntimeHooks, IndexStore, StoreRegistry,
        prepare_row_commit_for_entity, validate_delete_strong_relations_for_source,
    };
}

/// re-exports
///
/// macros can use these, stops the user having to specify all the dependencies
/// in the Cargo.toml file manually
///
/// these have to be in icydb_core because of the base library not being able to import icydb
#[doc(hidden)]
pub mod __reexports {
    pub use candid;
    pub use canic_cdk;
    pub use canic_memory;
    pub use ctor;
    pub use derive_more;
    pub use icydb_derive;
    pub use num_traits;
    pub use remain;
    pub use serde;
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
            AsView, Collection as _, Create, CreateView as _, EntityKind as _, EntityValue,
            Inner as _, MapCollection as _, Path as _, Update, UpdateView as _, View,
        },
        types::*,
        value::Value,
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
                AsView, Collection as _, Create, CreateView, EntityKind, EntityValue as _,
                FieldValue as _, Inner as _, MapCollection as _, Path as _, Sanitize as _,
                Sanitizer, Serialize as _, Update, UpdateView, Validate as _, ValidateCustom,
                Validator, View, Visitable as _,
            },
            types::*,
            value::Value,
            visitor::VisitorContext,
        };
    }
}

///
/// -------------------------- CODE -----------------------------------
///
use icydb_core::{InternalError, traits::Visitable};
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
#[expect(clippy::crate_in_macro_def)]
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
    icydb_core::sanitize(node)
        .map_err(InternalError::from)
        .map_err(Error::from)
}

/// Validate a visitable tree, collecting issues by path.
pub fn validate(node: &dyn Visitable) -> Result<(), Error> {
    icydb_core::validate(node)
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
    icydb_core::serialize(ty)
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
    icydb_core::deserialize(bytes)
        .map_err(InternalError::from)
        .map_err(Error::from)
}
