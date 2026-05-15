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
//! - `Error` / `ErrorKind` / `ErrorOrigin`
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
            EnumVariantModel, FieldDatabaseDefault, FieldInsertGeneration, FieldKind, FieldModel,
            FieldStorageDecode, FieldWriteManagement, RelationStrength,
        };
    }

    pub mod index {
        pub use icydb_core::model::{
            IndexExpression, IndexKeyItem, IndexKeyItemsRef, IndexModel, IndexPredicateMetadata,
        };
    }

    pub use entity::EntityModel;
    pub use field::{FieldDatabaseDefault, FieldModel};
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
mod error;
pub mod traits;
pub use error::{Error, ErrorKind, ErrorOrigin, QueryErrorKind, RuntimeErrorKind};

/// Generic create-input alias for one entity type.
pub type Create<E> = <E as icydb_core::traits::EntityCreateType>::Create;

// Macro/runtime wiring surface used by generated code.
// This is intentionally narrow and not semver-stable.
#[doc(hidden)]
pub mod __macro {
    pub use crate::db::execute_generated_storage_report;
    pub use icydb_core::__macro::{
        GeneratedStructuralEnumPayload, GeneratedStructuralMapPayloadSlices,
        decode_generated_structural_enum_payload_bytes,
        decode_generated_structural_list_payload_bytes,
        decode_generated_structural_map_payload_bytes,
        decode_generated_structural_text_payload_bytes, decode_persisted_many_slot_payload_by_meta,
        decode_persisted_option_scalar_slot_payload, decode_persisted_option_slot_payload_by_kind,
        decode_persisted_option_slot_payload_by_meta, decode_persisted_scalar_slot_payload,
        decode_persisted_slot_payload_by_kind, decode_persisted_slot_payload_by_meta,
        decode_persisted_structured_many_slot_payload, decode_persisted_structured_slot_payload,
        decode_schema_runtime_field_slot, encode_generated_structural_enum_payload_bytes,
        encode_generated_structural_list_payload_bytes,
        encode_generated_structural_map_payload_bytes,
        encode_generated_structural_text_payload_bytes, encode_persisted_many_slot_payload_by_meta,
        encode_persisted_option_scalar_slot_payload, encode_persisted_option_slot_payload_by_meta,
        encode_persisted_scalar_slot_payload, encode_persisted_slot_payload_by_kind,
        encode_persisted_slot_payload_by_meta, encode_persisted_structured_many_slot_payload,
        encode_persisted_structured_slot_payload, encode_schema_runtime_field_slot,
        generated_persisted_structured_payload_decode_failed,
    };
    pub use icydb_core::__macro::{PersistedScalar, ScalarSlotValueRef, ScalarValueRef};
    pub use icydb_core::db::{
        DataStore, DbSession as CoreDbSession, EntityRuntimeHooks, IndexStore, SchemaStore,
        StoreRegistry,
    };
    #[cfg(feature = "sql")]
    pub use icydb_core::db::{
        LoweredSqlCommand, identifiers_tail_match, sql_statement_entity_name,
    };
    pub use icydb_core::error::InternalError;
    pub use icydb_core::traits::{
        EnumValue, FieldProjection, PersistedByKindCodec, PersistedFieldMetaCodec,
        PersistedFieldSlotCodec, PersistedStructuredFieldCodec, RuntimeValueDecode,
        RuntimeValueEncode, RuntimeValueKind, RuntimeValueMeta, runtime_value_btree_map_from_value,
        runtime_value_btree_set_from_value, runtime_value_collection_to_value,
        runtime_value_from_value, runtime_value_from_vec_into,
        runtime_value_from_vec_into_btree_map, runtime_value_from_vec_into_btree_set,
        runtime_value_into, runtime_value_map_collection_to_value, runtime_value_to_value,
        runtime_value_vec_from_value,
    };
    pub use icydb_core::value::{InputValue, Value, ValueEnum};
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
                FieldRef, FilterExpr, FilterValue, OrderExpr, OrderTerm, asc, count, count_by,
                desc, exists, field, first, last, max, max_by, min, min_by, sum,
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
            db::query::{
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

// Export controller-gated admin SQL endpoints. Invoke from a canister crate
// after `icydb::start!()`.
#[macro_export]
macro_rules! admin_sql_query {
    () => {
        #[cfg(feature = "sql")]
        fn icydb_admin_sql_require_controller(action: &str) -> Result<(), ::icydb::Error> {
            let caller = ::icydb::__reexports::canic_cdk::api::msg_caller();
            if !::icydb::__reexports::canic_cdk::api::is_controller(&caller) {
                return Err(::icydb::Error::new(
                    ::icydb::ErrorKind::Runtime(::icydb::RuntimeErrorKind::Unsupported),
                    ::icydb::ErrorOrigin::Interface,
                    format!("admin SQL {action} requires a controller caller"),
                ));
            }

            Ok(())
        }

        #[cfg(all(feature = "sql", feature = "diagnostics"))]
        #[derive(::icydb::__reexports::candid::CandidType, Clone, Debug, Eq, PartialEq)]
        struct IcydbAdminSqlQueryPerfResult {
            result: ::icydb::db::sql::SqlQueryResult,
            instructions: u64,
            planner_instructions: u64,
            store_instructions: u64,
            executor_instructions: u64,
            pure_covering_decode_instructions: u64,
            pure_covering_row_assembly_instructions: u64,
            decode_instructions: u64,
            compiler_instructions: u64,
        }

        #[cfg(all(feature = "sql", feature = "diagnostics"))]
        impl IcydbAdminSqlQueryPerfResult {
            fn from_attribution(
                result: ::icydb::db::sql::SqlQueryResult,
                attribution: ::icydb::db::SqlQueryExecutionAttribution,
            ) -> Self {
                Self {
                    result,
                    instructions: attribution.total_local_instructions,
                    planner_instructions: attribution.execution.planner_local_instructions,
                    store_instructions: attribution.execution.store_local_instructions,
                    executor_instructions: attribution.execution.executor_local_instructions,
                    pure_covering_decode_instructions: attribution
                        .pure_covering
                        .map_or(0, |pure_covering| pure_covering.decode_local_instructions),
                    pure_covering_row_assembly_instructions: attribution
                        .pure_covering
                        .map_or(0, |pure_covering| {
                            pure_covering.row_assembly_local_instructions
                        }),
                    decode_instructions: attribution.response_decode_local_instructions,
                    compiler_instructions: attribution.compile_local_instructions,
                }
            }
        }

        #[cfg(all(feature = "sql", feature = "diagnostics"))]
        #[::icydb::__reexports::canic_cdk::query]
        fn icydb_admin_sql_query(
            sql: String,
        ) -> Result<IcydbAdminSqlQueryPerfResult, ::icydb::Error> {
            icydb_admin_sql_require_controller("query")?;

            let (result, attribution) = icydb_admin_sql_query_dispatch(sql.as_str())?;

            Ok(IcydbAdminSqlQueryPerfResult::from_attribution(
                result,
                attribution,
            ))
        }

        #[cfg(feature = "sql")]
        #[::icydb::__reexports::canic_cdk::update]
        fn ddl(sql: String) -> Result<::icydb::db::sql::SqlQueryResult, ::icydb::Error> {
            icydb_admin_sql_require_controller("DDL")?;

            icydb_admin_sql_ddl_dispatch(sql.as_str())
        }

        #[cfg(feature = "sql")]
        #[::icydb::__reexports::canic_cdk::update]
        fn fixtures_reset() -> Result<(), ::icydb::Error> {
            icydb_admin_sql_require_controller("lifecycle reset")?;

            icydb_admin_sql_reset_all_tables()
        }

        #[cfg(feature = "sql")]
        #[::icydb::__reexports::canic_cdk::update]
        fn fixtures_load_default() -> Result<(), ::icydb::Error> {
            icydb_admin_sql_require_controller("lifecycle load_default")?;
            let hook: fn() -> Result<(), ::icydb::Error> = crate::icydb_admin_sql_load_default;

            icydb_admin_sql_reset_all_tables()?;
            hook()
        }
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
