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
//! `icydb-core` and are re-exposed selectively through stable facade modules.
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
//! - `traits` / `types` / `value` / `visitor`
//!   Stable runtime and schema-facing building blocks used by generated code.
//!
//! - `model` / `metrics` *(internal)*
//!   Runtime model and metrics internals. Exposed for advanced tooling only;
//!   not part of the supported semver surface.
//!
//! - `error`
//!   Shared error types for generated code and runtime boundaries.
//!
//! - `macros`
//!   Derive macros for entities, canisters, and schema helpers.
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
//! Generated code targets explicit facade surfaces (`traits`, `patch`,
//! and `__macro`) instead of a broad internal-export module.

// export so things just work in base/
extern crate self as icydb;

use icydb_core::{error::InternalError, traits::Visitable};
// crates
pub use icydb_build as build;
pub use icydb_build::build;
pub use icydb_schema as schema;
pub use icydb_schema_derive as macros;

// core modules
#[doc(hidden)]
pub use icydb_core::types;

pub mod value {
    pub use icydb_core::value::{
        InputValue, InputValueEnum, OutputValue, OutputValueEnum, StorageKey,
        StorageKeyDecodeError, StorageKeyEncodeError, ValueTag,
    };
}

#[doc(hidden)]
pub mod model {
    pub mod entity {
        pub use icydb_core::model::EntityModel;
    }

    pub mod field {
        pub use icydb_core::model::{
            EnumVariantModel, FieldInsertGeneration, FieldKind, FieldModel, FieldStorageDecode,
            FieldWriteManagement, RelationStrength,
        };
    }

    pub mod index {
        pub use icydb_core::model::{
            IndexExpression, IndexKeyItem, IndexKeyItemsRef, IndexModel, IndexPredicateMetadata,
        };
    }

    pub use entity::EntityModel;
    pub use field::FieldModel;
    pub use index::{IndexExpression, IndexModel};
}

#[doc(hidden)]
pub mod metrics {
    pub use icydb_core::metrics::{
        EventCounters, EventReport, MetricsSink, metrics_report, metrics_reset_all,
    };
}

pub mod visitor {
    pub use icydb_core::visitor::{
        Issue, PathSegment, SanitizeFieldDescriptor, ScopedContext, ValidateFieldDescriptor,
        VisitableFieldDescriptor, VisitorContext, VisitorCore, VisitorError, VisitorIssues,
        VisitorMutCore, drive_sanitize_fields, drive_validate_fields, drive_visitable_fields,
        drive_visitable_fields_mut, perform_visit, perform_visit_mut,
    };
    pub use icydb_core::{
        sanitize::{SanitizeWriteContext, SanitizeWriteMode, sanitize, sanitize_with_context},
        validate::validate,
    };
}

// facade modules
pub mod base;
pub mod db;
pub mod error;
pub mod traits;
pub use error::Error;

/// Generic create-input alias for one entity type.
pub type Create<E> = <E as icydb_core::traits::EntityCreateType>::Create;

// Macro/runtime wiring surface used by generated code.
// This is intentionally narrow and not semver-stable.
#[doc(hidden)]
pub mod __macro {
    pub use crate::db::execute_generated_storage_report;
    #[cfg(feature = "sql")]
    pub use icydb_core::db::LoweredSqlCommand;
    pub use icydb_core::db::{
        DataStore, DbSession as CoreDbSession, EntityRuntimeHooks, IndexStore, StoreRegistry,
    };
    pub use icydb_core::traits::{
        EnumValue, FieldProjection, FieldValue, FieldValueKind, ValueCodec,
        value_codec_btree_map_from_value, value_codec_btree_set_from_value,
        value_codec_collection_to_value, value_codec_from_vec_into,
        value_codec_from_vec_into_btree_map, value_codec_from_vec_into_btree_set, value_codec_into,
        value_codec_map_collection_to_value, value_codec_vec_from_value,
    };
    pub use icydb_core::value::{Value, ValueEnum};
}

// re-exports
//
// macros can use these, stops the user having to specify all the dependencies
// in the Cargo.toml file manually
//
// these have to be in icydb_core because of the base library not being able to import icydb
#[doc(hidden)]
pub mod __reexports {
    pub use candid;
    pub use canic_cdk;
    pub use canic_memory;
    pub use ctor;
    pub use derive_more;
    pub use icydb_derive;
    pub use remain;
    pub use serde;
}

//
// Actor Prelude
// using _ brings traits into scope and avoids name conflicts
//

pub mod prelude {
    pub use crate::{
        db,
        db::{
            query,
            query::{
                FilterExpr, FilterValue, OrderExpr, OrderTerm, asc,
                builder::{
                    FieldRef, count, count_by, exists, first, last, max, max_by, min, min_by, sum,
                },
                desc, field,
            },
        },
        traits::{
            Collection as _, EntityKind as _, EntityValue, Inner as _, MapCollection as _,
            Path as _,
        },
        types::*,
        value::{InputValue, OutputValue},
    };
    pub use candid::CandidType;
    pub use serde::{Deserialize, Serialize};
}

//
// Design Prelude
// For schema/design code (macros, traits, base helpers).
//

pub mod design {
    pub mod prelude {
        pub use ::candid::CandidType;
        pub use ::derive_more;

        pub use crate::{
            base, db,
            db::query::builder::{
                FieldRef, count, count_by, exists, first, last, max, max_by, min, min_by, sum,
            },
            macros::*,
            traits::{
                Collection as _, EntityKind, EntityValue as _, Inner as _, MapCollection as _,
                Path as _, Sanitize as _, Sanitizer, Serialize as _, Validate as _, ValidateCustom,
                Validator, Visitable as _,
            },
            types::*,
            value::{InputValue, OutputValue},
            visitor::VisitorContext,
            visitor::{SanitizeWriteContext, SanitizeWriteMode},
        };
    }
}

//
// -------------------------- CODE -----------------------------------
//
//
// Consts
//

// Workspace version re-export for downstream tooling/tests.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

//
// Macros
//

// Include the generated actor module emitted by `build!` (placed in `OUT_DIR/actor.rs`).
#[macro_export]
macro_rules! start {
    () => {
        // actor.rs
        include!(concat!(env!("OUT_DIR"), "/actor.rs"));
    };
}

// Access the current canister's database session; use `db!().debug()` for verbose tracing.
#[macro_export]
#[expect(clippy::crate_in_macro_def)]
macro_rules! db {
    () => {
        crate::db()
    };
}

//
// Helpers
//

// Run sanitization over a mutable visitable tree.
pub fn sanitize(node: &mut dyn Visitable) -> Result<(), Error> {
    icydb_core::sanitize::sanitize(node)
        .map_err(InternalError::from)
        .map_err(Error::from)
}

// Validate a visitable tree, collecting issues by path.
pub fn validate(node: &dyn Visitable) -> Result<(), Error> {
    icydb_core::validate::validate(node)
        .map_err(InternalError::from)
        .map_err(Error::from)
}
