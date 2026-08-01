//! Module: access
//! Responsibility: access-path contracts, canonicalization, validation, and lowering.
//! Does not own: predicate semantics or index storage internals.
//! Boundary: query planning produces access plans; executor consumes lowered forms.

pub(crate) mod canonical;
pub(in crate::db) mod execution_contract;
pub(crate) mod lowering;
pub(crate) mod path;
pub(crate) mod plan;
pub(in crate::db) mod shape_facts;
pub(crate) mod validate;

// Canonical planner access surface.
pub(in crate::db) use crate::db::index::SemanticIndexExpression;
pub(in crate::db) use canonical::normalize_access_plan_value;
pub(in crate::db) use path::{
    AccessPath, IndexBranchSetSpec, MAX_INDEX_BRANCH_SET_VALUES, SemanticIndexAccessContract,
    SemanticIndexKeyItem, SemanticIndexKeyItemRef, SemanticIndexRangeSpec,
};
pub(in crate::db) use plan::AccessPlan;
pub(crate) use validate::AccessPlanError;
pub(in crate::db) use validate::validate_access_runtime_invariants_with_schema;

// Boundary-local access-shape fact helpers.
pub(in crate::db) use path::AccessPathKind;
pub(in crate::db) use shape_facts::{
    AccessShapeFacts, IndexShapeDetails, SinglePathAccessShapeFacts,
};

// Executor-facing access contract and lowering surface.
pub(in crate::db) use execution_contract::{
    ExecutableAccessNode, ExecutableAccessPlan, ExecutionPathPayload,
};
#[cfg(feature = "sql")]
pub(in crate::db) use lowering::lower_exact_user_index_prefix_cardinality_keys_for_prefix_access;
pub(in crate::db) use lowering::{
    LoweredAccessError, LoweredIndexPrefixSpec, LoweredIndexRangeSpec, LoweredIndexScanContract,
    LoweredKey, lower_access_with_schema_info,
};
