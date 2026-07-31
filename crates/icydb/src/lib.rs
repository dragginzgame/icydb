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

// core modules
#[doc(hidden)]
pub use icydb_core::types;

pub mod value {
    pub use icydb_core::value::{
        InputValue, InputValueEnum, OutputValue, OutputValueEnum, ValueTag,
    };
    pub use icydb_model::{Collection, MapCollection};
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
    //! to `[build-dependencies]`, then call
    //! `icydb::build::build_configured_canister!()` from `build.rs`.
    //!
    //! `icydb-config` remains the configuration implementation behind this
    //! facade. Model-graph code generation is owned by `icydb-model`.
    //! This module is host-only and is not part of Wasm runtime builds.

    pub use icydb_config::{
        ConfigError, GeneratedBuildTarget, GeneratedCanisterConfig, GeneratedIcydbConfig,
        GeneratedMetricsMode, GeneratedMetricsPolicy, GeneratedSqlIntrospectionPolicy,
        GeneratedSqlUpdatePolicy, ResolvedIcydbConfig, build_configured_canister,
        emit_config_for_build_script, emit_configured_canister_for_build_script,
        load_resolved_icydb_toml, resolve_existing_icydb_toml,
    };
}
pub mod db;
pub mod diagnostic {
    //! Compact diagnostic identity for CLI and canister callers.

    pub use icydb_diagnostic_code::{
        Diagnostic, DiagnosticCode, DiagnosticDetail, ErrorClass, ErrorCode, ErrorOrigin,
        QueryErrorKind, QueryProjectionCode, QueryReadAdmissionCode, QueryResultShapeCode,
        RuntimeBoundaryCode, RuntimeErrorKind, SchemaDdlAdmissionCode, SqlFeatureCode,
        SqlLoweringCode, SqlSurfaceMismatchCode, SqlWriteBoundaryCode,
    };
}
mod error;
pub mod traits;
pub use error::{
    ConstraintDiagnostic, ConstraintDiagnosticContext, ConstraintDiagnosticKind,
    ConstraintValuePath, ConstraintValuePathComponent, Error, ErrorKind, ErrorOrigin,
    QueryErrorKind, RuntimeErrorKind,
};
pub use icydb_diagnostic_code::ErrorCode;

// Macro/runtime wiring surface used by generated code.
// This is intentionally narrow and not semver-stable.
#[doc(hidden)]
pub mod __macro {
    pub use crate::db::execute_generated_storage_report;
    pub use crate::db::{
        TypedEntityAdapter, TypedFieldBindingRequest, TypedFieldType, TypedInputValue,
        TypedNamedType, TypedOutputValue, TypedRowAdapter,
    };
    pub use ic_memory::{
        bootstrap_default_memory_manager, ic_memory_declaration, ic_memory_key, ic_memory_range,
    };
    pub use icydb_core::db::{
        CompositePrimaryKeyValue, CompositePrimaryKeyValueError, DataStore,
        DbSession as CoreDbSession, EntityKeyBytes, EntityKeyBytesError, IndexStore,
        JournalTailStore, KeyValueCodec, PrimaryKeyDecode, PrimaryKeyEncode, PrimaryKeyEncodeError,
        PrimaryKeyValue, SchemaStore, StoreAllocationIdentities, StoreAllocationIdentity,
        StoreRegistry, StoreRuntimeStorageCapabilities, validate_entity_key_bytes_buffer,
    };
    #[cfg(feature = "sql")]
    pub use icydb_core::db::{
        LoweredSqlCommand, sql_statement_dispatch, sql_statement_entity_name,
    };
    pub use icydb_core::error::{ErrorClass, ErrorOrigin, InternalError};
    pub use icydb_core::traits::{CanisterKind, Path};
    pub use icydb_core::value::Value;
    pub use icydb_schema::{
        DEFAULT_BIG_INT_MAX_BYTES, Decimal as SchemaDecimal, FieldSourceKey, ScalarLiteral,
        ScalarType, SchemaContractError, SourceCheckExpr, SourceCheckInstruction,
    };
}

// Dependencies used by generated actor glue. Application-model macro
// dependencies are owned separately by `icydb-model`.
#[doc(hidden)]
pub mod __reexports {
    pub use candid;
    pub use ctor;
    pub use derive_more;
    pub use ic_cdk;
    pub use ic_memory;
    pub use remain;
    pub use serde;
}

//
// Actor Prelude
// using _ brings traits into scope and avoids name conflicts
//

pub mod prelude {
    #[cfg(feature = "query")]
    pub use crate::db::{
        query,
        query::{
            FieldRef, FilterExpr, FilterValue, OrderExpr, OrderTerm, asc, count, count_by, desc,
            exists, field, first, last, max, max_by, min, min_by, sum,
        },
    };
    pub use crate::{
        db,
        traits::{Collection as _, Inner as _, MapCollection as _, Path as _},
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

// Include the generated actor module emitted by `build!` (placed in `OUT_DIR/actor.rs`).
#[macro_export]
macro_rules! start {
    () => {
        // actor.rs
        include!(concat!(env!("OUT_DIR"), "/actor.rs"));
    };
}

// Access the current canister's fallible database session; propagate with `db!()?`.
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
    fn build_facade_exports_configured_entrypoint_metadata() {
        fn assert_model_inner<T: crate::traits::Inner<u64>>() {}

        assert_eq!(
            build::GeneratedBuildTarget::default(),
            build::GeneratedBuildTarget::Unknown
        );
        assert_model_inner::<ModelWrapper>();
        let wrapper = ModelWrapper(7);
        assert_eq!(*crate::traits::Inner::inner(&wrapper), 7);
        assert_eq!(crate::traits::Inner::into_inner(wrapper), 7);

        std::hint::black_box(
            build_facade_macros_resolve as fn() -> Result<(), Box<dyn std::error::Error>>,
        );
    }

    fn build_facade_macros_resolve() -> Result<(), Box<dyn std::error::Error>> {
        build::build_configured_canister!((), "crate::Canister", "canister");

        Ok(())
    }
}
