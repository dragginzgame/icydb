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
//! Generated SQL endpoints are controller-gated by default. A declaration may
//! instead install one synchronous application read guard; neither form is an
//! anonymous public read endpoint template.
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

pub use icydb_core::types::{ParseU256Error, U256};
pub use icydb_model_macros::{request_execution, test};

// core modules
#[doc(hidden)]
pub use icydb_core::types;

pub mod value {
    pub use icydb_core::value::{InputValue, OutputValue, PublicEnumValue, PublicValue, ValueTag};
}

#[doc(hidden)]
pub mod metrics {
    pub use icydb_core::metrics::{
        CompactEntityMetrics, CompactEventCounters, CompactMetric, CompactMetricsReport,
        EntitySummary, EventCounters, EventOps, EventReport, MetricRatio, MetricsSink,
        MutationJobMetrics, compact_metric_code, compact_metrics_report, metrics_report,
        metrics_reset_all,
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
        println!("cargo:rustc-check-cfg=cfg(feature, values(\"test-admin-api\"))");
        let out_dir = std::env::var("OUT_DIR")?;
        let actor_file = std::path::PathBuf::from(out_dir).join("actor.rs");
        let actor = icydb_model::build::generate(canister_path);
        std::fs::write(actor_file, actor)?;

        Ok(())
    }
}
pub mod db;
pub mod guards;
pub mod diagnostic {
    //! Compact diagnostic identity for CLI and canister callers.

    pub use icydb_diagnostic_code::{
        Diagnostic, DiagnosticAggregateKind, DiagnosticBacklogResource, DiagnosticCode,
        DiagnosticComponentKind, DiagnosticConstraintContext, DiagnosticConstraintKind,
        DiagnosticDecodeReason, DiagnosticDetail, DiagnosticExecutionBudgetResource,
        DiagnosticExecutionBudgetScope, DiagnosticExecutionLane, DiagnosticFactSchemaMismatch,
        DiagnosticFactTag, DiagnosticFunctionKind, DiagnosticMutationOperation,
        DiagnosticOperatorKind, DiagnosticTypeFamily, ErrorClass, ErrorCode, ErrorOrigin,
        MAX_PUBLIC_DIAGNOSTIC_FACTS, MAX_PUBLIC_QUERY_FIELD_BYTES, QueryErrorKind, QueryFieldRole,
        QueryFieldSchemaMismatch, QueryProjectionCode, QueryReadAdmissionCode,
        QueryResultShapeCode, RuntimeBoundaryCode, RuntimeErrorKind, SchemaDdlAdmissionCode,
        SchemaMigrationCode, SqlFeatureCode, SqlLoweringCode, SqlSurfaceMismatchCode,
        SqlWriteBoundaryCode, pack_u32_pair, unpack_u32_pair,
        validate_known_diagnostic_fact_schema, validate_query_field_schema,
        validate_raw_diagnostic_fact_schema,
    };
}
mod error;
pub mod traits;
pub use error::{
    ConstraintValidationFindingOutput, ConstraintValuePath, ConstraintValuePathComponent,
    DiagnosticFact, Error, ErrorKind, ErrorOrigin, QueryErrorKind, QueryFieldDiagnostic,
    RuntimeErrorKind,
};
pub use guards::{
    ReadAuthorizationContext, ReadAuthorizationDecision, ReadAuthorizationGuard,
    ReadAuthorizationSurface,
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
    pub use crate::guards::{authorize_schema_read, authorize_sql_read};
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
    pub use ic_timers;
}

//
// Actor Prelude
// using _ brings traits into scope and avoids name conflicts
//

pub mod prelude {
    pub use crate::db::{
        query,
        query::{
            CollectionOperator, CompareOperator, FieldCompareOperator, FieldRef, FilterExpr,
            FilterValue, JunctionOperator, OrderExpr, OrderTerm, SetOperator, StateOperator, asc,
            count, count_by, desc, exists, field, first, last, max, max_by, min, min_by, sum,
        },
    };
    pub use crate::{
        db,
        traits::{EntitySource as _, Inner as _, Path as _},
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
///
/// The zero-argument form owns hidden install and post-upgrade lifecycle
/// entries for canisters without application hooks. Applications that own
/// the complete lifecycle root use participant mode and invoke the matching
/// hidden IcyDB participant synchronously:
///
/// ```ignore
/// icydb::start!(participant);
///
/// #[ic_cdk::init]
/// fn init() {
///     crate::__icydb_lifecycle_participant::init();
/// }
///
/// #[ic_cdk::post_upgrade]
/// fn post_upgrade() {
///     crate::__icydb_lifecycle_participant::post_upgrade();
/// }
/// ```
///
/// Applications that want IcyDB to own the lifecycle exports while running
/// application callbacks afterward use the composed form:
///
/// ```ignore
/// icydb::start! {
///     init(args: InitArgs) => application::init;
///     post_upgrade() => application::post_upgrade;
/// }
/// ```
///
/// IcyDB registers its startup watchdog before invoking either callback. The
/// callback remains application-owned and must observe `startup_state()`
/// before restoring timers, caches, or other database-dependent state.
#[macro_export]
macro_rules! start {
    () => {
        $crate::__icydb_start_actor!();
        $crate::__icydb_start_lifecycle!();
    };

    (participant) => {
        $crate::__icydb_start_actor!();
        $crate::__icydb_start_participant_lifecycle!();
    };

    (
        init($($init_arg:ident : $init_ty:ty),* $(,)?) => $init:path;
        post_upgrade($($upgrade_arg:ident : $upgrade_ty:ty),* $(,)?) => $post_upgrade:path;
    ) => {
        $crate::__icydb_start_actor!();
        $crate::__icydb_start_lifecycle! {
            init($($init_arg: $init_ty),*) => $init;
            post_upgrade($($upgrade_arg: $upgrade_ty),*) => $post_upgrade;
        }
    };
}

#[doc(hidden)]
#[macro_export]
#[expect(
    clippy::crate_in_macro_def,
    reason = "participant functions must call the consuming canister's generated actor"
)]
macro_rules! __icydb_start_participant_lifecycle {
    () => {
        #[doc(hidden)]
        #[allow(
            dead_code,
            reason = "downstream canisters may select participant hooks through generated lifecycle wiring"
        )]
        pub(crate) mod __icydb_lifecycle_participant {
            use std::cell::Cell;

            #[derive(Clone, Copy)]
            enum State {
                Idle,
                Running,
                Completed,
            }

            std::thread_local! {
                static STATE: Cell<State> = const { Cell::new(State::Idle) };
            }

            // Native traps unwind instead of rolling back a replicated IC
            // message. Reset only that test/runtime model so retry exercises
            // the same latch transition that message rollback provides on Wasm.
            #[cfg(not(target_family = "wasm"))]
            struct NativeRollbackGuard {
                completed: bool,
            }

            #[cfg(not(target_family = "wasm"))]
            impl NativeRollbackGuard {
                const fn new() -> Self {
                    Self { completed: false }
                }

                const fn complete(&mut self) {
                    self.completed = true;
                }
            }

            #[cfg(not(target_family = "wasm"))]
            impl Drop for NativeRollbackGuard {
                fn drop(&mut self) {
                    if !self.completed {
                        STATE.with(|state| state.set(State::Idle));
                    }
                }
            }

            // Both lifecycle phases deliberately converge here. Completed is
            // shared across them so any duplicate returns before generated
            // timer or recovery state can be observed or changed.
            fn participate(work: fn() -> ()) {
                let should_run = STATE.with(|state| match state.get() {
                    State::Idle => {
                        state.set(State::Running);
                        true
                    }
                    State::Running => $crate::__reexports::ic_cdk::trap(
                        "IcyDB lifecycle participant re-entered while running",
                    ),
                    State::Completed => false,
                });

                if !should_run {
                    return;
                }

                #[cfg(not(target_family = "wasm"))]
                let mut rollback = NativeRollbackGuard::new();

                work();
                STATE.with(|state| state.set(State::Completed));

                #[cfg(not(target_family = "wasm"))]
                rollback.complete();
            }

            /// Participate synchronously in the canister's init lifecycle.
            #[doc(hidden)]
            pub(crate) fn init() -> () {
                let participate: fn() -> () = crate::__icydb_generated::__icydb_startup_init;
                self::participate(participate);
            }

            /// Participate synchronously in the canister's post-upgrade lifecycle.
            #[doc(hidden)]
            pub(crate) fn post_upgrade() -> () {
                let participate: fn() -> () =
                    crate::__icydb_generated::__icydb_startup_post_upgrade;
                self::participate(participate);
            }
        }
    };
}

#[doc(hidden)]
#[macro_export]
#[expect(
    clippy::crate_in_macro_def,
    reason = "lifecycle wrappers must call the consuming canister's generated actor"
)]
macro_rules! __icydb_start_lifecycle {
    () => {
        #[$crate::__reexports::ic_cdk::init(hidden = true)]
        fn __icydb_startup_init() {
            crate::__icydb_generated::__icydb_startup_init();
        }

        #[$crate::__reexports::ic_cdk::post_upgrade(hidden = true)]
        fn __icydb_startup_post_upgrade() {
            crate::__icydb_generated::__icydb_startup_post_upgrade();
        }
    };

    (
        init($($init_arg:ident : $init_ty:ty),* $(,)?) => $init:path;
        post_upgrade($($upgrade_arg:ident : $upgrade_ty:ty),* $(,)?) => $post_upgrade:path;
    ) => {
        #[$crate::__reexports::ic_cdk::init]
        fn __icydb_startup_init($($init_arg: $init_ty),*) {
            crate::__icydb_generated::__icydb_startup_init();
            let (): () = $crate::db::with_request_execution(|| ($init)($($init_arg),*));
        }

        #[$crate::__reexports::ic_cdk::post_upgrade]
        fn __icydb_startup_post_upgrade($($upgrade_arg: $upgrade_ty),*) {
            crate::__icydb_generated::__icydb_startup_post_upgrade();
            let (): () = $crate::db::with_request_execution(
                || ($post_upgrade)($($upgrade_arg),*)
            );
        }
    };
}

#[doc(hidden)]
#[macro_export]
#[expect(
    clippy::crate_in_macro_def,
    reason = "generated actor bindings must live in the consuming canister crate"
)]
macro_rules! __icydb_start_actor {
    () => {
        #[doc(hidden)]
        struct __IcydbStartRootMarker;

        #[doc(hidden)]
        const fn __icydb_start_root_binding(_: __IcydbStartRootMarker) {}

        const _: fn(__IcydbStartRootMarker) = crate::__icydb_start_root_binding;

        #[allow(
            dead_code,
            reason = "generated actor members are feature-dependent in downstream canisters"
        )]
        mod __icydb_generated {
            #[doc(hidden)]
            pub(crate) const __ICYDB_START_BINDING: () = ();

            include!(concat!(env!("OUT_DIR"), "/actor.rs"));
        }

        #[allow(
            unused_imports,
            reason = "minimal downstream canisters may use only a subset of generated actor conveniences"
        )]
        use __icydb_generated::{db, db_with_request_root, startup_state};
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
        #[allow(
            unused_imports,
            reason = "declared endpoint families determine whether the generated facade alias is referenced"
        )]
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

    ($(#[cfg($($cfg:tt)*)])* icydb_sql_query(
        introspection = false,
        authorization = guard($guard:path) $(,)?
    ); $($rest:tt)*) => {
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
                let guard: $crate::ReadAuthorizationGuard = $guard;
                $crate::__macro::authorize_sql_read(
                    $crate::__reexports::ic_cdk::api::msg_caller(),
                    guard,
                )?;
                $crate::__macro::with_query_metrics_context(|| {
                    $crate::db::with_request_execution(|| {
                        crate::__icydb_generated::endpoint_handlers::sql_query::<false>(sql)
                    })
                })
            }
        }
        $crate::__icydb_endpoints_internal!($($rest)*);
    };

    ($(#[cfg($($cfg:tt)*)])* icydb_sql_query(
        introspection = true,
        authorization = guard($guard:path) $(,)?
    ); $($rest:tt)*) => {
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
                let guard: $crate::ReadAuthorizationGuard = $guard;
                $crate::__macro::authorize_sql_read(
                    $crate::__reexports::ic_cdk::api::msg_caller(),
                    guard,
                )?;
                $crate::__macro::with_query_metrics_context(|| {
                    $crate::db::with_request_execution(|| {
                        crate::__icydb_generated::endpoint_handlers::sql_query::<true>(sql)
                    })
                })
            }
        }
        $crate::__icydb_endpoints_internal!($($rest)*);
    };

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
            #[allow(
                clippy::result_large_err,
                reason = "generated integrity endpoints preserve the public typed error contract"
            )]
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

    ($(#[cfg($($cfg:tt)*)])* icydb_schema(
        authorization = guard($guard:path) $(,)?
    ); $($rest:tt)*) => {
        $(#[cfg($($cfg)*)])*
        #[used]
        static __ICYDB_ENDPOINT_DECLARATION_SCHEMA: () = ();
        $(#[cfg($($cfg)*)])*
        #[$crate::__reexports::ic_cdk::query(name = "icydb_schema")]
        fn __icydb_export_icydb_schema(
        ) -> Result<Vec<__icydb_facade::db::EntitySchemaDescription>, __icydb_facade::Error> {
            let guard: $crate::ReadAuthorizationGuard = $guard;
            $crate::__macro::authorize_schema_read(
                $crate::__reexports::ic_cdk::api::msg_caller(),
                guard,
            )?;
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
/// framework, and timer entries establish that boundary with
/// [`request_execution`]; lifecycle callbacks declared by the composed
/// [`start!`] form receive it automatically.
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
