//! Module: access
//! Responsibility: access-path contracts, canonicalization, validation, and lowering.
//! Does not own: predicate semantics or index storage internals.
//! Boundary: query planning produces access plans; executor consumes lowered forms.

pub(crate) mod canonical;
pub(in crate::db) mod capabilities;
pub(in crate::db) mod execution_contract;
pub(crate) mod lowering;
pub(crate) mod path;
pub(crate) mod plan;
#[cfg(test)]
mod tests;
pub(crate) mod validate;

// Canonical planner access surface.
pub(crate) use canonical::normalize_access_plan_value;
pub(crate) use path::{AccessPath, SemanticIndexRangeSpec};
pub(crate) use plan::AccessPlan;
pub(in crate::db) use validate::validate_access_runtime_invariants_with_schema;
pub(crate) use validate::{AccessPlanError, validate_access_structure_model};

// Boundary-local path and capability helpers.
pub(in crate::db) use capabilities::{
    AccessCapabilities, IndexShapeDetails, SinglePathAccessCapabilities,
};
pub(in crate::db) use path::AccessPathKind;

// Executor-facing access contract and lowering surface.
pub(in crate::db) use execution_contract::{
    ExecutableAccessNode, ExecutableAccessPlan, ExecutionPathPayload,
    summarize_executable_access_plan,
};
pub(in crate::db) use lowering::{
    LoweredAccess, LoweredAccessError, LoweredIndexPrefixSpec, LoweredIndexRangeSpec, LoweredKey,
    lower_access,
};
