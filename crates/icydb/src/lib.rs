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

pub use icydb_build as build;
pub use icydb_core as core;
pub use icydb_macros as macros;
pub use icydb_schema as schema;

pub mod base;

// export so things just work in base/
extern crate self as icydb;

/// re-exports
///
/// macros can use these, stops the user having to specify all the dependencies
/// in the Cargo.toml file manually
///
/// these have to be in icydb_core because of the base library not being able to import icydb
pub mod __reexports {
    pub use canic_cdk;
    pub use canic_memory;
    pub use canic_utils;
    pub use ctor;
    pub use derive_more;
    pub use num_traits;
    pub use remain;
}

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

///
/// Actor Prelude
/// using _ brings traits into scope and avoids name conflicts
///

pub mod prelude {
    pub use crate::core::{
        db,
        db::{
            executor::SaveExecutor,
            primitives::{
                self, Cmp, FilterClause, FilterDsl, FilterExpr, FilterExt as _, LimitExpr,
                LimitExt as _, SortExpr, SortExt as _,
            },
            query,
            response::{Response, ResponseExt},
        },
        key::Key,
        traits::{
            CreateView as _, EntityKind as _, FilterView as _, Inner as _, Path as _,
            UpdateView as _, View as _,
        },
        types::*,
        value::Value,
        view::{Create, Filter, Update, View},
    };
    pub use candid::CandidType;
    pub use serde::{Deserialize, Serialize};
}

//
// Design Prelude
// For schema/design code (macros, traits, base helpers).
//

/// Schema/design-facing helpers (separate from the actor/runtime prelude).
pub mod design {
    pub mod prelude {
        pub use ::candid::CandidType;
        pub use ::derive_more;

        pub use crate::{
            base,
            core::{
                Key, Value, db,
                db::Db,
                traits::{
                    EntityKind, FieldValue as _, Inner as _, Path as _, Sanitize as _,
                    Sanitizer as _, Serialize as _, Validate as _, ValidateCustom, Validator as _,
                    View as _, Visitable as _,
                },
                types::*,
                view::View,
                visitor::VisitorContext,
            },
            macros::*,
        };
    }
}
