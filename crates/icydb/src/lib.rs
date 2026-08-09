//! Module: lib
//!
//! Responsibility: public facade crate surface and generated-code wiring.
//! Does not own: core execution, storage internals, or schema mutation semantics.
//! Boundary: re-exports stable runtime and generated actor-wiring surfaces.

//! # icydb
//!
//! `icydb` is the **public facade crate** for the IcyDB runtime.
//! It is the recommended dependency for downstream canister projects.
//!
//! This crate exposes:
//! - the stable runtime surface used inside canister actor code,
//! - accepted-schema-bound database request and response types,
//! - and a small set of entry points that wire generated actor code.
//!
//! Low-level execution, storage, and engine internals live in
//! `icydb-core` and are re-exposed selectively through stable facade modules.
//!
//! ## Crate layout
//!
//! - `build` *(host builds)*
//!   Host-side build-script facade for generated actor glue. Downstream
//!   canister `build.rs` files should use this module rather than depending on
//!   lower-level implementation crates directly.
//!
//! - `traits` / `types` / `value`
//!   Stable runtime building blocks used by generated code.
//!
//! - `metrics` *(internal)*
//!   Runtime metrics internals exposed for generated administration endpoints.
//!
//! - `Error` / `ErrorKind` / `ErrorOrigin`
//!   Shared error types for generated code and runtime boundaries.
//!
//! - `db`
//!   The public database façade: sessions, SQL/dynamic reads, structural
//!   mutations, and accepted-schema-bound typed adapters.
//!
//! Generated SQL endpoints are controller-gated admin surfaces. They are not
//! generated public read endpoint templates.
//!
//! The operational lane contract lives in
//! `docs/contracts/READ_ADMISSION.md`.
//! Endpoint migration recipes live in `docs/guides/read-intent.md`.
//!
//! ## Preludes
//!
//! - `prelude`
//!   Opinionated runtime prelude for canister actor code.
//!   Intended to be glob-imported in `lib.rs` to keep endpoints concise.
//!
//! ## Internal boundaries
//!
//! Generated code targets explicit facade surfaces (`traits`, `db`, and
//! `__macro`) instead of a broad internal-export module.

// Generated actor glue resolves this package through its canonical crate name.
extern crate self as icydb;

pub use icydb_model_macros::{request_execution, test};

// core modules
#[doc(hidden)]
pub use icydb_core::types;

pub mod value {
    pub use icydb_core::value::{
        InputValue, InputValueEnum, OutputValue, OutputValueEnum, ValueTag,
    };
}

#[doc(hidden)]
pub mod metrics {
    pub use icydb_core::metrics::{
        CompactEntityMetrics, CompactEventCounters, CompactMetric, CompactMetricsReport,
        EntitySummary, EventCounters, EventOps, EventReport, MetricRatio, MetricsSink,
        compact_metric_code, compact_metrics_report, metrics_report, metrics_reset_all,
    };
}

// facade modules
#[cfg(not(target_arch = "wasm32"))]
pub mod build {
    //! Host-side build-script facade for generated actor glue.
    //!
    //! This module is the advertised downstream build-script API. Add `icydb`
    //! to `[build-dependencies]`, then call `icydb::build::build_canister!()`
    //! from `build.rs`. Model-graph code generation is owned by `icydb-model`.
    //! This module is host-only and is not part of Wasm runtime builds.

    pub use crate::build_canister;

    /// Emit one generated private actor module for a build script.
    ///
    /// This function is expansion support for [`build_canister!`].
    ///
    /// # Errors
    ///
    /// Returns an environment or filesystem error when Cargo does not provide
    /// `OUT_DIR` or the generated actor cannot be written.
    #[doc(hidden)]
    pub fn __emit_canister_for_build_script(
        canister_path: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        println!("cargo:rerun-if-changed=build.rs");
        let out_dir = std::env::var("OUT_DIR")?;
        let actor_file = std::path::PathBuf::from(out_dir).join("actor.rs");
        let actor = icydb_model::build::generate(canister_path);
        std::fs::write(actor_file, actor)?;

        Ok(())
    }
}
pub mod db;
pub mod diagnostic {
    //! Compact diagnostic identity for CLI and canister callers.

    pub use icydb_diagnostic_code::{
        Diagnostic, DiagnosticAggregateKind, DiagnosticCode, DiagnosticComponentKind,
        DiagnosticConstraintContext, DiagnosticConstraintKind, DiagnosticDecodeReason,
        DiagnosticDetail, DiagnosticExecutionBudgetResource, DiagnosticExecutionBudgetScope,
        DiagnosticExecutionLane, DiagnosticFactSchemaMismatch, DiagnosticFactTag,
        DiagnosticFunctionKind, DiagnosticMutationOperation, DiagnosticOperatorKind,
        DiagnosticTypeFamily, ErrorClass, ErrorCode, ErrorOrigin, MAX_PUBLIC_DIAGNOSTIC_FACTS,
        QueryErrorKind, QueryProjectionCode, QueryReadAdmissionCode, QueryResultShapeCode,
        RuntimeBoundaryCode, RuntimeErrorKind, SchemaDdlAdmissionCode, SchemaMigrationCode,
        SqlFeatureCode, SqlLoweringCode, SqlSurfaceMismatchCode, SqlWriteBoundaryCode,
        pack_u32_pair, unpack_u32_pair, validate_known_diagnostic_fact_schema,
        validate_raw_diagnostic_fact_schema,
    };
}
mod error;
pub mod traits;
pub use error::{
    ConstraintValidationFindingOutput, ConstraintValuePath, ConstraintValuePathComponent,
    DiagnosticFact, Error, ErrorKind, ErrorOrigin, QueryErrorKind, RuntimeErrorKind,
};
pub use icydb_diagnostic_code::ErrorCode;

// Macro/runtime wiring surface used by generated code.
// This is intentionally narrow and not semver-stable.
#[doc(hidden)]
pub mod __macro {
    pub use crate::db::{
        TypedFieldBindingRequest, TypedFieldType, ensure_default_memory_manager,
        execute_generated_storage_report,
    };
    pub use ic_memory::{ic_memory_declaration, ic_memory_key, ic_memory_range};
    pub use icydb_core::db::{
        CompositePrimaryKeyValue, DataStore, DbSession as CoreDbSession, EntityKey, EntityKeyBytes,
        EntityKeyBytesError, IndexStore, JournalTailStore, KeyValueCodec, PrimaryKeyDecode,
        PrimaryKeyEncode, PrimaryKeyEncodeError, PrimaryKeyValue, SchemaStore,
        StoreAllocationIdentities, StoreAllocationIdentity, StoreRegistry,
        StoreRuntimeStorageCapabilities, validate_entity_key_bytes_buffer,
    };
    #[cfg(feature = "sql")]
    pub use icydb_core::db::{sql_statement_dispatch, sql_statement_entity_name};
    pub use icydb_core::error::{ErrorClass, ErrorOrigin, InternalError};
    pub use icydb_core::metrics::with_query_metrics_context;
    pub use icydb_core::traits::{CanisterKind, Path};
    pub use icydb_core::value::Value;
    pub use icydb_schema::{DEFAULT_BIG_INT_MAX_BYTES, ScalarType};
}

// Dependencies used by generated actor glue. Application-model macro
// dependencies are owned separately by `icydb-model`.
#[doc(hidden)]
pub mod __reexports {
    pub use candid;
    pub use ic_cdk;
    pub use ic_cdk_timers;
}

//
// Actor Prelude
// using _ brings traits into scope and avoids name conflicts
//

pub mod prelude {
    pub use crate::db::{
        query,
        query::{
            FieldRef, FilterExpr, FilterValue, OrderExpr, OrderTerm, asc, count, count_by, desc,
            exists, field, first, last, max, max_by, min, min_by, sum,
        },
    };
    pub use crate::{
        db,
        traits::{Inner as _, Path as _},
        types::*,
        value::{InputValue, OutputValue},
    };
    pub use candid::CandidType;
    pub use serde::{Deserialize, Serialize};
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

/// Generate one canister's private actor module from its authored schema type.
#[cfg(not(target_arch = "wasm32"))]
#[macro_export]
macro_rules! build_canister {
    ($canister_ty:ty) => {{
        let _ = ::std::any::TypeId::of::<$canister_ty>();
        $crate::build::__emit_canister_for_build_script(stringify!($canister_ty))
    }};
}

/// Include the generated private actor module emitted by [`build_canister!`].
#[macro_export]
#[expect(
    clippy::crate_in_macro_def,
    reason = "start! must bind generated items in the consuming canister crate"
)]
macro_rules! start {
    () => {
        #[doc(hidden)]
        struct __IcydbStartRootMarker;

        #[doc(hidden)]
        const fn __icydb_start_root_binding(_: __IcydbStartRootMarker) {}

        const _: fn(__IcydbStartRootMarker) = crate::__icydb_start_root_binding;

        #[allow(dead_code)]
        mod __icydb_generated {
            #[doc(hidden)]
            pub(crate) const __ICYDB_START_BINDING: () = ();

            include!(concat!(env!("OUT_DIR"), "/actor.rs"));
        }

        #[allow(unused_imports)]
        use __icydb_generated::{db, db_with_request_root};
    };
}

#[doc(hidden)]
#[cfg(feature = "sql")]
#[macro_export]
macro_rules! __icydb_with_sql_items {
    ($($item:item)*) => { $($item)* };
}

#[doc(hidden)]
#[cfg(not(feature = "sql"))]
#[macro_export]
macro_rules! __icydb_with_sql_items {
    ($($item:item)*) => {};
}

#[doc(hidden)]
#[cfg(feature = "migration")]
#[macro_export]
macro_rules! __icydb_with_migration_items {
    ($($item:item)*) => { $($item)* };
}

#[doc(hidden)]
#[cfg(not(feature = "migration"))]
#[macro_export]
macro_rules! __icydb_with_migration_items {
    ($($item:item)*) => {};
}

#[doc(hidden)]
#[cfg(feature = "sql")]
#[macro_export]
macro_rules! __icydb_with_sql_endpoint {
    ($endpoint:literal; $($item:item)*) => { $($item)* };
}

#[doc(hidden)]
#[cfg(feature = "migration")]
#[macro_export]
macro_rules! __icydb_with_migration_endpoint {
    ($endpoint:literal; $($item:item)*) => { $($item)* };
}

#[doc(hidden)]
#[cfg(not(feature = "migration"))]
#[macro_export]
macro_rules! __icydb_with_migration_endpoint {
    ($endpoint:literal; $($item:item)*) => {
        compile_error!(concat!(
            "endpoint declaration `",
            $endpoint,
            "` requires the `icydb/migration` Cargo feature"
        ));
    };
}

#[doc(hidden)]
#[cfg(not(feature = "sql"))]
#[macro_export]
macro_rules! __icydb_with_sql_endpoint {
    ($endpoint:literal; $($item:item)*) => {
        compile_error!(concat!(
            "endpoint declaration `",
            $endpoint,
            "` requires the `icydb/sql` Cargo feature"
        ));
    };
}

#[doc(hidden)]
#[cfg(feature = "migration")]
#[macro_export]
macro_rules! __icydb_require_migration_capability {
    () => {};
}

#[doc(hidden)]
#[cfg(not(feature = "migration"))]
#[macro_export]
macro_rules! __icydb_require_migration_capability {
    () => {
        compile_error!("source migration declarations require the `icydb/migration` Cargo feature");
    };
}

/// Declare the complete fixed IcyDB endpoint surface exported by this canister.
#[macro_export]
#[expect(
    clippy::crate_in_macro_def,
    reason = "endpoints! must prove crate-root placement in the consuming canister"
)]
macro_rules! endpoints {
    ($($declaration:tt)*) => {
        #[doc(hidden)]
        struct __IcydbEndpointsRootMarker;

        #[doc(hidden)]
        const fn __icydb_endpoints_root_binding(_: __IcydbEndpointsRootMarker) {}

        const _: fn(__IcydbEndpointsRootMarker) = crate::__icydb_endpoints_root_binding;

        #[doc(hidden)]
        #[allow(unused_imports)]
        use $crate as __icydb_facade;

        #[used]
        static __ICYDB_ENDPOINT_DECLARATIONS: () =
            crate::__icydb_generated::__ICYDB_START_BINDING;

        $crate::__icydb_endpoints_internal!($($declaration)*);
    };
}

#[doc(hidden)]
#[macro_export]
#[expect(
    clippy::crate_in_macro_def,
    reason = "endpoint wrappers call the consuming canister's private generated module"
)]
macro_rules! __icydb_endpoints_internal {
    () => {};

    ($(#[cfg($($cfg:tt)*)])* icydb_sql_query(introspection = false); $($rest:tt)*) => {
        $(#[cfg($($cfg)*)])*
        #[used]
        static __ICYDB_ENDPOINT_DECLARATION_QUERY: () = ();
        $(#[cfg($($cfg)*)])*
        $crate::__icydb_with_sql_endpoint! {
            "icydb_sql_query";
            #[$crate::__reexports::ic_cdk::query(name = "icydb_query")]
            fn __icydb_export_icydb_query(
                sql: String,
            ) -> Result<__icydb_facade::db::sql::SqlQueryPerfResult, __icydb_facade::Error> {
                crate::__icydb_generated::endpoint_authorization::require_sql_controller()?;
                $crate::__macro::with_query_metrics_context(|| {
                    $crate::db::with_request_execution(|| {
                        crate::__icydb_generated::endpoint_handlers::sql_query::<false>(sql)
                    })
                })
            }
        }
        $crate::__icydb_endpoints_internal!($($rest)*);
    };

    ($(#[cfg($($cfg:tt)*)])* icydb_sql_query(introspection = true); $($rest:tt)*) => {
        $(#[cfg($($cfg)*)])*
        #[used]
        static __ICYDB_ENDPOINT_DECLARATION_QUERY: () = ();
        $(#[cfg($($cfg)*)])*
        $crate::__icydb_with_sql_endpoint! {
            "icydb_sql_query";
            #[$crate::__reexports::ic_cdk::query(name = "icydb_query")]
            fn __icydb_export_icydb_query(
                sql: String,
            ) -> Result<__icydb_facade::db::sql::SqlQueryPerfResult, __icydb_facade::Error> {
                crate::__icydb_generated::endpoint_authorization::require_sql_controller()?;
                $crate::__macro::with_query_metrics_context(|| {
                    $crate::db::with_request_execution(|| {
                        crate::__icydb_generated::endpoint_handlers::sql_query::<true>(sql)
                    })
                })
            }
        }
        $crate::__icydb_endpoints_internal!($($rest)*);
    };

    ($(#[cfg($($cfg:tt)*)])* icydb_ddl; $($rest:tt)*) => {
        $(#[cfg($($cfg)*)])*
        #[used]
        static __ICYDB_ENDPOINT_DECLARATION_DDL: () = ();
        $(#[cfg($($cfg)*)])*
        $crate::__icydb_with_sql_endpoint! {
            "icydb_ddl";
            #[$crate::__reexports::ic_cdk::update(name = "icydb_ddl")]
            fn __icydb_export_icydb_ddl(
                sql: String,
            ) -> Result<__icydb_facade::db::sql::SqlQueryResult, __icydb_facade::Error> {
                crate::__icydb_generated::endpoint_authorization::require_sql_controller()?;
                $crate::db::with_request_execution(|| {
                    crate::__icydb_generated::endpoint_handlers::sql_ddl(sql)
                })
            }
        }
        $crate::__icydb_endpoints_internal!($($rest)*);
    };

    ($(#[cfg($($cfg:tt)*)])* icydb_update(admission = primary_key_only); $($rest:tt)*) => {
        $(#[cfg($($cfg)*)])*
        #[used]
        static __ICYDB_ENDPOINT_DECLARATION_UPDATE: () = ();
        $(#[cfg($($cfg)*)])*
        $crate::__icydb_with_sql_endpoint! {
            "icydb_update";
            #[$crate::__reexports::ic_cdk::update(name = "icydb_update")]
            fn __icydb_export_icydb_update(
                sql: String,
            ) -> Result<__icydb_facade::db::sql::SqlQueryResult, __icydb_facade::Error> {
                crate::__icydb_generated::endpoint_authorization::require_sql_controller()?;
                $crate::db::with_request_execution(|| {
                    crate::__icydb_generated::endpoint_handlers::sql_update_primary_key(sql)
                })
            }
        }
        $crate::__icydb_endpoints_internal!($($rest)*);
    };

    ($(#[cfg($($cfg:tt)*)])* icydb_update(admission = bounded_deterministic); $($rest:tt)*) => {
        $(#[cfg($($cfg)*)])*
        #[used]
        static __ICYDB_ENDPOINT_DECLARATION_UPDATE: () = ();
        $(#[cfg($($cfg)*)])*
        $crate::__icydb_with_sql_endpoint! {
            "icydb_update";
            #[$crate::__reexports::ic_cdk::update(name = "icydb_update")]
            fn __icydb_export_icydb_update(
                sql: String,
            ) -> Result<__icydb_facade::db::sql::SqlQueryResult, __icydb_facade::Error> {
                crate::__icydb_generated::endpoint_authorization::require_sql_controller()?;
                $crate::db::with_request_execution(|| {
                    crate::__icydb_generated::endpoint_handlers::sql_update_bounded(sql)
                })
            }
        }
        $crate::__icydb_endpoints_internal!($($rest)*);
    };

    ($(#[cfg($($cfg:tt)*)])* icydb_integrity; $($rest:tt)*) => {
        $(#[cfg($($cfg)*)])*
        #[used]
        static __ICYDB_ENDPOINT_DECLARATION_INTEGRITY: () = ();
        $(#[cfg($($cfg)*)])*
        $crate::__icydb_with_sql_endpoint! {
            "icydb_integrity";
            #[allow(clippy::result_large_err)]
            #[$crate::__reexports::ic_cdk::update(name = "icydb_integrity")]
            fn __icydb_export_icydb_integrity(
                sql: String,
            ) -> Result<__icydb_facade::db::IntegrityCheckResult, __icydb_facade::db::SqlIntegrityError> {
                crate::__icydb_generated::endpoint_authorization::require_sql_controller()
                    .map_err(__icydb_facade::db::SqlIntegrityError::Sql)?;
                $crate::db::with_request_execution(|| {
                    crate::__icydb_generated::endpoint_handlers::sql_integrity(sql)
                })
            }
        }
        $crate::__icydb_endpoints_internal!($($rest)*);
    };

    ($(#[cfg($($cfg:tt)*)])* icydb_fixtures_reset; $($rest:tt)*) => {
        $(#[cfg($($cfg)*)])*
        #[used]
        static __ICYDB_ENDPOINT_DECLARATION_FIXTURES_RESET: () = ();
        $(#[cfg($($cfg)*)])*
        #[cfg(not(feature = "test-admin-api"))]
        compile_error!("endpoint declaration `icydb_fixtures_reset` requires the canister `test-admin-api` Cargo feature");
        $(#[cfg($($cfg)*)])*
        #[cfg(feature = "test-admin-api")]
        $crate::__icydb_with_sql_endpoint! {
            "icydb_fixtures_reset";
            #[$crate::__reexports::ic_cdk::update(name = "icydb_fixtures_reset")]
            fn __icydb_export_icydb_fixtures_reset() -> Result<(), __icydb_facade::Error> {
                crate::__icydb_generated::endpoint_authorization::require_sql_controller()?;
                $crate::db::with_request_execution(|| {
                    crate::__icydb_generated::endpoint_handlers::fixtures_reset()
                })
            }
        }
        $crate::__icydb_endpoints_internal!($($rest)*);
    };

    ($(#[cfg($($cfg:tt)*)])* icydb_fixtures_load(handler = $handler:path); $($rest:tt)*) => {
        $(#[cfg($($cfg)*)])*
        #[used]
        static __ICYDB_ENDPOINT_DECLARATION_FIXTURES_LOAD: () = ();
        $(#[cfg($($cfg)*)])*
        #[cfg(not(feature = "test-admin-api"))]
        compile_error!("endpoint declaration `icydb_fixtures_load` requires the canister `test-admin-api` Cargo feature");
        $(#[cfg($($cfg)*)])*
        #[cfg(feature = "test-admin-api")]
        $crate::__icydb_with_sql_endpoint! {
            "icydb_fixtures_load";
            #[$crate::__reexports::ic_cdk::update(name = "icydb_fixtures_load")]
            fn __icydb_export_icydb_fixtures_load() -> Result<(), __icydb_facade::Error> {
                crate::__icydb_generated::endpoint_authorization::require_sql_controller()?;
                let handler: fn() -> Result<(), $crate::Error> = $handler;
                $crate::db::with_request_execution(|| {
                    crate::__icydb_generated::endpoint_handlers::fixtures_load(handler)
                })
            }
        }
        $crate::__icydb_endpoints_internal!($($rest)*);
    };

    ($(#[cfg($($cfg:tt)*)])* icydb_metrics(authorization = public); $($rest:tt)*) => {
        $(#[cfg($($cfg)*)])*
        #[used]
        static __ICYDB_ENDPOINT_DECLARATION_METRICS: () = ();
        $(#[cfg($($cfg)*)])*
        #[$crate::__reexports::ic_cdk::query(name = "icydb_metrics")]
        fn __icydb_export_icydb_metrics(
            window_start_ms: Option<u64>,
        ) -> Result<__icydb_facade::metrics::CompactMetricsReport, __icydb_facade::Error> {
            $crate::__macro::with_query_metrics_context(|| {
                crate::__icydb_generated::endpoint_handlers::metrics(window_start_ms)
            })
        }
        $crate::__icydb_endpoints_internal!($($rest)*);
    };

    ($(#[cfg($($cfg:tt)*)])* icydb_metrics(authorization = controller); $($rest:tt)*) => {
        $(#[cfg($($cfg)*)])*
        #[used]
        static __ICYDB_ENDPOINT_DECLARATION_METRICS: () = ();
        $(#[cfg($($cfg)*)])*
        #[$crate::__reexports::ic_cdk::query(name = "icydb_metrics")]
        fn __icydb_export_icydb_metrics(
            window_start_ms: Option<u64>,
        ) -> Result<__icydb_facade::metrics::CompactMetricsReport, __icydb_facade::Error> {
            crate::__icydb_generated::endpoint_authorization::require_operational_controller()?;
            $crate::__macro::with_query_metrics_context(|| {
                crate::__icydb_generated::endpoint_handlers::metrics(window_start_ms)
            })
        }
        $crate::__icydb_endpoints_internal!($($rest)*);
    };

    ($(#[cfg($($cfg:tt)*)])* icydb_metrics_extended(authorization = public); $($rest:tt)*) => {
        $(#[cfg($($cfg)*)])*
        #[used]
        static __ICYDB_ENDPOINT_DECLARATION_METRICS_EXTENDED: () = ();
        $(#[cfg($($cfg)*)])*
        #[$crate::__reexports::ic_cdk::query(name = "icydb_metrics_extended")]
        fn __icydb_export_icydb_metrics_extended(
            window_start_ms: Option<u64>,
        ) -> Result<__icydb_facade::metrics::EventReport, __icydb_facade::Error> {
            $crate::__macro::with_query_metrics_context(|| {
                crate::__icydb_generated::endpoint_handlers::metrics_extended(window_start_ms)
            })
        }
        $crate::__icydb_endpoints_internal!($($rest)*);
    };

    ($(#[cfg($($cfg:tt)*)])* icydb_metrics_extended(authorization = controller); $($rest:tt)*) => {
        $(#[cfg($($cfg)*)])*
        #[used]
        static __ICYDB_ENDPOINT_DECLARATION_METRICS_EXTENDED: () = ();
        $(#[cfg($($cfg)*)])*
        #[$crate::__reexports::ic_cdk::query(name = "icydb_metrics_extended")]
        fn __icydb_export_icydb_metrics_extended(
            window_start_ms: Option<u64>,
        ) -> Result<__icydb_facade::metrics::EventReport, __icydb_facade::Error> {
            crate::__icydb_generated::endpoint_authorization::require_operational_controller()?;
            $crate::__macro::with_query_metrics_context(|| {
                crate::__icydb_generated::endpoint_handlers::metrics_extended(window_start_ms)
            })
        }
        $crate::__icydb_endpoints_internal!($($rest)*);
    };

    ($(#[cfg($($cfg:tt)*)])* icydb_metrics_reset; $($rest:tt)*) => {
        $(#[cfg($($cfg)*)])*
        #[used]
        static __ICYDB_ENDPOINT_DECLARATION_METRICS_RESET: () = ();
        $(#[cfg($($cfg)*)])*
        #[$crate::__reexports::ic_cdk::update(name = "icydb_metrics_reset")]
        fn __icydb_export_icydb_metrics_reset() -> Result<(), __icydb_facade::Error> {
            crate::__icydb_generated::endpoint_authorization::require_operational_controller()?;
            crate::__icydb_generated::endpoint_handlers::metrics_reset()
        }
        $crate::__icydb_endpoints_internal!($($rest)*);
    };

    ($(#[cfg($($cfg:tt)*)])* icydb_snapshot; $($rest:tt)*) => {
        $(#[cfg($($cfg)*)])*
        #[used]
        static __ICYDB_ENDPOINT_DECLARATION_SNAPSHOT: () = ();
        $(#[cfg($($cfg)*)])*
        #[$crate::__reexports::ic_cdk::query(name = "icydb_snapshot")]
        fn __icydb_export_icydb_snapshot() -> Result<__icydb_facade::db::StorageReport, __icydb_facade::Error> {
            crate::__icydb_generated::endpoint_authorization::require_operational_controller()?;
            $crate::__macro::with_query_metrics_context(|| {
                $crate::db::with_request_execution(|| {
                    crate::__icydb_generated::endpoint_handlers::snapshot()
                })
            })
        }
        $crate::__icydb_endpoints_internal!($($rest)*);
    };

    ($(#[cfg($($cfg:tt)*)])* icydb_schema(authorization = public); $($rest:tt)*) => {
        $(#[cfg($($cfg)*)])*
        #[used]
        static __ICYDB_ENDPOINT_DECLARATION_SCHEMA: () = ();
        $(#[cfg($($cfg)*)])*
        #[$crate::__reexports::ic_cdk::query(name = "icydb_schema")]
        fn __icydb_export_icydb_schema(
        ) -> Result<Vec<__icydb_facade::db::EntitySchemaDescription>, __icydb_facade::Error> {
            $crate::__macro::with_query_metrics_context(|| {
                $crate::db::with_request_execution(|| {
                    crate::__icydb_generated::endpoint_handlers::schema()
                })
            })
        }
        $crate::__icydb_endpoints_internal!($($rest)*);
    };

    ($(#[cfg($($cfg:tt)*)])* icydb_schema(authorization = controller); $($rest:tt)*) => {
        $(#[cfg($($cfg)*)])*
        #[used]
        static __ICYDB_ENDPOINT_DECLARATION_SCHEMA: () = ();
        $(#[cfg($($cfg)*)])*
        #[$crate::__reexports::ic_cdk::query(name = "icydb_schema")]
        fn __icydb_export_icydb_schema(
        ) -> Result<Vec<__icydb_facade::db::EntitySchemaDescription>, __icydb_facade::Error> {
            crate::__icydb_generated::endpoint_authorization::require_schema_controller()?;
            $crate::__macro::with_query_metrics_context(|| {
                $crate::db::with_request_execution(|| {
                    crate::__icydb_generated::endpoint_handlers::schema()
                })
            })
        }
        $crate::__icydb_endpoints_internal!($($rest)*);
    };

    ($(#[cfg($($cfg:tt)*)])* icydb_schema_migrate; $($rest:tt)*) => {
        $(#[cfg($($cfg)*)])*
        #[used]
        static __ICYDB_ENDPOINT_DECLARATION_SCHEMA_MIGRATE: () = ();
        $(#[cfg($($cfg)*)])*
        $crate::__icydb_with_migration_endpoint! {
            "icydb_schema_migrate";
            #[$crate::__reexports::ic_cdk::update(name = "icydb_schema_migrate")]
            fn __icydb_export_icydb_schema_migrate(
                command: __icydb_facade::db::SchemaMigrationCommand,
            ) -> Result<__icydb_facade::db::SchemaMigrationStatusPage, __icydb_facade::Error> {
                crate::__icydb_generated::endpoint_authorization::require_operational_controller()?;
                $crate::db::with_request_execution(|| {
                    crate::__icydb_generated::endpoint_handlers::schema_migrate(command)
                })
            }
        }
        $crate::__icydb_endpoints_internal!($($rest)*);
    };

    ($(#[cfg($($cfg:tt)*)])* icydb_schema_migration; $($rest:tt)*) => {
        $(#[cfg($($cfg)*)])*
        #[used]
        static __ICYDB_ENDPOINT_DECLARATION_SCHEMA_MIGRATION: () = ();
        $(#[cfg($($cfg)*)])*
        $crate::__icydb_with_migration_endpoint! {
            "icydb_schema_migration";
            #[$crate::__reexports::ic_cdk::query(name = "icydb_schema_migration")]
            fn __icydb_export_icydb_schema_migration(
                request: __icydb_facade::db::SchemaMigrationStatusRequest,
            ) -> Result<__icydb_facade::db::SchemaMigrationStatusPage, __icydb_facade::Error> {
                crate::__icydb_generated::endpoint_authorization::require_operational_controller()?;
                $crate::__macro::with_query_metrics_context(|| {
                    $crate::db::with_request_execution(|| {
                        crate::__icydb_generated::endpoint_handlers::schema_migration(&request)
                    })
                })
            }
        }
        $crate::__icydb_endpoints_internal!($($rest)*);
    };

    ($(#[cfg($($cfg:tt)*)])* $endpoint:ident $($rest:tt)*) => {
        compile_error!(concat!("unknown or invalid IcyDB endpoint declaration `", stringify!($endpoint), "`"));
    };

    (#[$attribute:meta] $($rest:tt)*) => {
        compile_error!("IcyDB endpoint declarations accept only `#[cfg(...)]` attributes");
    };

    ($($invalid:tt)+) => {
        compile_error!("invalid IcyDB endpoint declaration syntax");
    };
}

/// Access the active request's database session.
///
/// Use `db!()` in ordinary generated endpoints and nested helpers. Every call
/// shares the execution counters installed at request entry. Manual IC-CDK,
/// framework lifecycle, and timer entries establish that boundary with
/// [`request_execution`].
///
/// `db!(&request_root)` is the explicit low-level integration form for a
/// framework that already owns a request root. Obtain that root from
/// [`db::with_request_execution_root`](crate::db::with_request_execution_root);
/// passing it never creates fresh counters and fails if a different root is
/// already active.
#[macro_export]
#[expect(clippy::crate_in_macro_def)]
macro_rules! db {
    () => {
        crate::db()
    };
    ($request_root:expr) => {
        crate::db_with_request_root($request_root)
    };
}

//
// Helpers
//

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use crate::build;

    struct ModelWrapper(u64);

    impl icydb_model::Inner<u64> for ModelWrapper {
        fn inner(&self) -> &u64 {
            &self.0
        }

        fn into_inner(self) -> u64 {
            self.0
        }
    }

    #[test]
    fn build_facade_exports_typed_entrypoint() {
        fn assert_model_inner<T: crate::traits::Inner<u64>>() {}

        assert_model_inner::<ModelWrapper>();
        let wrapper = ModelWrapper(7);
        assert_eq!(*crate::traits::Inner::inner(&wrapper), 7);
        assert_eq!(crate::traits::Inner::into_inner(wrapper), 7);

        std::hint::black_box(
            build_facade_macros_resolve as fn() -> Result<(), Box<dyn std::error::Error>>,
        );
    }

    fn build_facade_macros_resolve() -> Result<(), Box<dyn std::error::Error>> {
        build::build_canister!(())?;

        Ok(())
    }
}
