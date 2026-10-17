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
//! - `model`
//!   Application-model declarations, macros, reusable types, validation, and
//!   normalization. Runtime-enabled downstream crates should use this
//!   re-export instead of adding a second IcyDB package dependency.
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
/// Application-model authoring through the public IcyDB facade.
///
/// Runtime-enabled consumers should normally depend only on `icydb` and use
/// `icydb::model::prelude`. Schema-only tooling may still depend directly on
/// the independently published `icydb-model` package.
pub use icydb_model as model;
pub use icydb_model_macros::{request_execution, test};

/// Scalar values and entity-bound identities for application inputs and outputs.
#[doc(inline)]
pub use icydb_core::types;

pub mod value {
    pub use icydb_core::value::{InputValue, OutputValue, PublicEnumValue, PublicValue, ValueTag};
}

#[doc(hidden)]
#[cfg(feature = "metrics")]
pub mod metrics {
    pub use icydb_core::metrics::{
        ConvergenceMetrics, EntityMetrics, InstructionMetrics, MetricsReport,
        SchemaLifecycleMetrics, metrics_reset_all,
    };

    /// Read the current metrics window and exact bounded journal debt.
    ///
    /// Returns a typed error when the database's committed journal controls
    /// cannot be observed. Query reads do not advance recovery or record work.
    pub fn metrics_report() -> Result<MetricsReport, crate::Error> {
        Ok(icydb_core::metrics::metrics_report()?)
    }
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
    ///
    /// # Panics
    ///
    /// Panics if model validation fails, the canonical path is not a canister,
    /// or the consuming package's IcyDB dependency cannot be resolved.
    #[doc(hidden)]
    pub fn __emit_canister_for_build_script(
        canister_path: &str,
        build_script: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        println!("cargo:rerun-if-changed={build_script}");
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
        QueryFieldSchemaMismatch, QueryProjectionCode, QueryReadAdmissionCode, RuntimeBoundaryCode,
        RuntimeErrorKind, SchemaDdlAdmissionCode, SchemaMigrationCode, SqlFeatureCode,
        SqlLoweringCode, SqlSurfaceMismatchCode, SqlWriteBoundaryCode, pack_u32_pair,
        unpack_u32_pair, validate_known_diagnostic_fact_schema, validate_query_field_schema,
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
        TypedEntityDescriptor, TypedFieldDescriptor, TypedFieldType, ensure_default_memory_manager,
        execute_generated_storage_report,
    };
    pub use crate::guards::{authorize_schema_read, authorize_sql_read};
    #[cfg(feature = "sql")]
    pub use icydb_core::db::sql_statement_dispatch;
    pub use icydb_core::db::{
        CompositePrimaryKeyValue, DataStore, DbSession as CoreDbSession, EntityKey, EntityKeyBytes,
        EntityKeyBytesError, IndexStore, JournalTailStore, KeyValueCodec, PrimaryKeyDecode,
        PrimaryKeyEncode, PrimaryKeyEncodeError, PrimaryKeyValue, SchemaStore,
        StoreAllocationIdentities, StoreAllocationIdentity, StoreRegistry,
        StoreRuntimeStorageCapabilities, validate_entity_key_bytes_buffer,
    };
    pub use icydb_core::db::{open_memory, with_selected_commit_memory};
    pub use icydb_core::error::{ErrorClass, ErrorOrigin, InternalError};
    pub use icydb_core::traits::{CanisterKind, Path};
    pub use icydb_core::value::Value;
    pub use icydb_schema::ScalarType;
}

//
// Actor Prelude
// using _ brings traits into scope and avoids name conflicts
//

pub mod wire;

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
///
/// Resolves the type through its canonical model [`Path`](model::Path), so
/// relative and imported type paths use the same registered identity.
/// Track shared declaration files with Cargo `rerun-if-changed` directives.
///
/// # Panics
///
/// Panics if model validation fails, the type is not a registered canister,
/// or the consuming package's IcyDB dependency cannot be resolved.
#[cfg(not(target_arch = "wasm32"))]
#[macro_export]
macro_rules! build_canister {
    ($canister_ty:ty) => {{
        $crate::build::__emit_canister_for_build_script(
            <$canister_ty as $crate::model::Path>::PATH,
            file!(),
        )
    }};
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
        pub use __icydb_generated::icydb_memory_requests;
    };
}

#[doc(hidden)]
#[cfg(feature = "sql")]
#[macro_export]
macro_rules! __icydb_with_sql_items {
    ($($item:item)*) => { $($item)* };
}

#[doc(hidden)]
#[cfg(feature = "metrics")]
#[macro_export]
macro_rules! __icydb_with_metrics_items {
    ($($item:item)*) => { $($item)* };
}

#[doc(hidden)]
#[cfg(not(feature = "metrics"))]
#[macro_export]
macro_rules! __icydb_with_metrics_items {
    ($($item:item)*) => {};
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
#[cfg(feature = "metrics")]
#[macro_export]
macro_rules! __icydb_with_metrics_endpoint {
    ($endpoint:literal; $($item:item)*) => { $($item)* };
}

#[doc(hidden)]
#[cfg(not(feature = "metrics"))]
#[macro_export]
macro_rules! __icydb_with_metrics_endpoint {
    ($endpoint:literal; $($item:item)*) => {
        compile_error!(concat!(
            "endpoint declaration `",
            $endpoint,
            "` requires the `icydb/metrics` Cargo feature"
        ));
    };
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

/// Access the active request's database session.
///
/// Use `db!()` in ordinary generated endpoints and nested helpers. Every call
/// shares the execution counters installed at request entry. Manual IC-CDK,
/// framework, and timer entries establish that boundary with
/// [`request_execution`]; lifecycle callbacks declared by the composed
/// the host adapter's `start!` form receive it automatically.
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

    impl icydb_model::Path for ModelWrapper {
        const PATH: &'static str = "icydb::tests::ModelWrapper";
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
        build::build_canister!(ModelWrapper)?;

        Ok(())
    }
}
