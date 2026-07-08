//! Module: lib
//!
//! Responsibility: public facade crate surface and generated-code wiring.
//! Does not own: core execution, storage internals, or schema mutation semantics.
//! Boundary: re-exports stable runtime, design-time, and macro-facing surfaces.

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
//! - `build` *(host builds)*
//!   Host-side build-script facade for generated actor glue. Downstream
//!   canister `build.rs` files should use this module rather than depending on
//!   lower-level implementation crates directly.
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
//! ## Read execution defaults
//!
//! Ordinary typed/fluent reads through fluent `execute`, `execute_rows`,
//! cursor-paged execution, and terminal helpers use the default bounded
//! read-admission gate. Caller-facing endpoints still own caller authorization
//! before entering IcyDB. Trusted read helpers are for controller/admin or
//! maintenance code with a separate resource policy.
//!
//! Prefer semantic read intents for caller-facing APIs:
//! - exact rows use primary-key access plus `try_one()`;
//! - public lists use `page(limit)` / `next_page(limit, cursor)` cursor pagination;
//! - complete small sets use `collect_complete()`;
//! - exact aggregates use semantic helpers such as `count_exact()`,
//!   `sum_exact(field)`, `min_exact_by(field)`, or `avg_exact(field)`;
//! - trusted maintenance batches use `trusted_read_unchecked().admin_batch(...)`.
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
//! - `design::prelude`
//!   Prelude for schema and design-time code (macros, validators,
//!   and base helpers).
//!
//! ## Internal boundaries
//!
//! Generated code targets explicit facade surfaces (`traits`, `model`,
//! and `__macro`) instead of a broad internal-export module.

// export so things just work in base/
extern crate self as icydb;

use icydb_core::{error::InternalError, traits::Visitable};
pub use icydb_schema as schema;
pub use icydb_schema_derive as macros;

// core modules
#[doc(hidden)]
pub use icydb_core::types;

pub mod value {
    pub use icydb_core::value::{
        InputValue, InputValueEnum, OutputValue, OutputValueEnum, ValueTag,
    };
}

#[doc(hidden)]
pub mod model {
    pub mod entity {
        pub use icydb_core::model::{
            EntityModel, PrimaryKeyModel, PrimaryKeyModelFieldIter, PrimaryKeyModelFields,
            RelationEdgeModel,
        };
    }

    pub mod field {
        pub use icydb_core::model::{
            DEFAULT_BIG_INT_MAX_BYTES, EnumVariantModel, FieldDatabaseDefault,
            FieldInsertGeneration, FieldKind, FieldModel, FieldStorageDecode, FieldWriteManagement,
            RelationStrength,
        };
    }

    pub mod index {
        pub use icydb_core::model::{
            IndexExpression, IndexKeyItem, IndexKeyItemsRef, IndexModel, IndexPredicateMetadata,
        };
    }

    pub use entity::{EntityModel, PrimaryKeyModel};
    pub use field::{FieldDatabaseDefault, FieldModel};
    pub use index::{IndexExpression, IndexModel};
}

#[doc(hidden)]
pub mod metrics {
    pub use icydb_core::metrics::{
        CompactEntityMetrics, CompactEventCounters, CompactMetric, CompactMetricsReport,
        EntitySummary, EventCounters, EventOps, EventReport, MetricsSink, compact_metric_code,
        compact_metrics_report, metrics_report, metrics_reset_all,
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
#[cfg(not(target_arch = "wasm32"))]
pub mod build {
    //! Host-side build-script facade for generated actor glue.
    //!
    //! This module is the advertised downstream build-script API. Add `icydb`
    //! to `[build-dependencies]`, then call
    //! `icydb::build::build_configured_canister!()` from `build.rs`.
    //!
    //! `icydb-build` and `icydb-config` remain implementation crates behind
    //! this facade. The module is host-only and is not part of wasm runtime
    //! builds.

    pub use icydb_build::build_with_options;
    pub use icydb_build::{BuildOptions, BuildSqlUpdatePolicy, generate_with_options};
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
pub use error::{Error, ErrorKind, ErrorOrigin, QueryErrorKind, RuntimeErrorKind};
pub use icydb_diagnostic_code::ErrorCode;

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
    pub use icydb_core::__macro::{
        bootstrap_default_memory_manager, ic_memory_declaration, ic_memory_key, ic_memory_range,
    };
    pub use icydb_core::db::{
        CompositePrimaryKeyValue, CompositePrimaryKeyValueError, DataStore,
        DbSession as CoreDbSession, EntityRuntimeHooks, IndexStore, JournalTailStore, PersistedRow,
        PrimaryKeyComponent, PrimaryKeyValue, SchemaStore, SlotReader, SlotWriter,
        StoreAllocationIdentities, StoreAllocationIdentity, StoreRegistry,
        StoreRuntimeStorageCapabilities,
    };
    #[cfg(feature = "sql")]
    pub use icydb_core::db::{
        LoweredSqlCommand, identifiers_tail_match, sql_statement_dispatch,
        sql_statement_entity_name,
    };
    pub use icydb_core::error::{ErrorClass, ErrorOrigin, InternalError};
    pub use icydb_core::traits::{
        EntityKeyBytes, EntityValue, EnumValue, FieldProjection, KeyValueCodec,
        PersistedByKindCodec, PersistedFieldMetaCodec, PersistedFieldSlotCodec,
        PersistedStructuredFieldCodec, PrimaryKeyCodec, PrimaryKeyDecode, PrimaryKeyEncodeError,
        RuntimeValueDecode, RuntimeValueEncode, RuntimeValueKind, RuntimeValueMeta,
        ScalarRelationTargetKey, ScalarRelationTargetKeyMatchesDeclaredPrimitive,
        runtime_value_btree_map_from_value, runtime_value_btree_set_from_value,
        runtime_value_collection_to_value, runtime_value_from_value, runtime_value_from_vec_into,
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
    pub use ctor;
    pub use derive_more;
    pub use ic_cdk;
    pub use ic_memory;
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
            Collection as _, Entity as _, EntityKind as _, Inner as _, MapCollection as _,
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
                Collection as _, Entity as _, EntityKind, Inner as _, MapCollection as _,
                Path as _, Sanitize as _, Sanitizer, Serialize as _, Validate as _, ValidateCustom,
                Validator, Visitable as _,
            },
            types::*,
            value::{InputValue, OutputValue},
            visitor::{Issue, VisitorContext},
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

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use crate::build;

    #[test]
    fn build_facade_exports_configured_and_manual_entrypoints() {
        let options = build::BuildOptions::default()
            .with_metrics_enabled(true)
            .with_sql_update_policy(Some(build::BuildSqlUpdatePolicy::PublicPrimaryKeyOnly));
        assert!(options.metrics_enabled());
        assert_eq!(
            options.sql_update_policy(),
            Some(build::BuildSqlUpdatePolicy::PublicPrimaryKeyOnly)
        );
        assert_eq!(
            build::GeneratedBuildTarget::default(),
            build::GeneratedBuildTarget::Unknown
        );
    }

    #[allow(dead_code)]
    fn build_facade_macros_resolve() -> Result<(), Box<dyn std::error::Error>> {
        build::build_configured_canister!((), "crate::Canister", "canister");
        build::build_with_options!("crate::Canister", build::BuildOptions::default());

        Ok(())
    }
}
