//! Module: access
//! Responsibility: access-path contracts, canonicalization, validation, and lowering.
//! Does not own: predicate semantics or index storage internals.
//! Boundary: query planning produces access plans; executor consumes lowered forms.

pub(crate) mod canonical;
pub(in crate::db) mod capabilities;
pub(in crate::db) mod dispatch;
pub(in crate::db) mod execution_contract;
pub(crate) mod lowering;
pub(crate) mod path;
pub(crate) mod plan;
#[cfg(test)]
mod tests;
pub(crate) mod validate;

pub(in crate::db) use canonical::canonicalize_value_set;
pub(crate) use canonical::normalize_access_plan_value;
pub(in crate::db) use capabilities::single_path_capabilities;
pub(in crate::db) use dispatch::{
    AccessPathDispatch, AccessPathKind, AccessPlanDispatch, dispatch_access_plan,
};
pub(in crate::db) use dispatch::{ExecutableAccessPathDispatch, dispatch_executable_access_path};
pub(in crate::db) use execution_contract::{
    AccessExecutionMode, AccessRouteClass, AccessStrategy, ExecutableAccessNode,
    ExecutableAccessPath, ExecutableAccessPlan, ExecutionBounds, ExecutionDistinctMode,
    ExecutionOrdering, ExecutionPathPayload,
};
#[cfg(test)]
pub(in crate::db) use lowering::lower_executable_access_path;
pub(in crate::db) use lowering::{
    LOWERED_INDEX_PREFIX_SPEC_INVALID, LOWERED_INDEX_RANGE_SPEC_INVALID, LoweredIndexPrefixSpec,
    LoweredIndexRangeSpec, LoweredKey, lower_index_prefix_specs, lower_index_range_specs,
};
pub(crate) use path::{AccessPath, IndexRangePathRef, SemanticIndexRangeSpec};
pub(crate) use plan::{
    AccessPlan, PushdownApplicability, PushdownSurfaceEligibility,
    SecondaryOrderPushdownEligibility, SecondaryOrderPushdownRejection,
};
pub(crate) use validate::{
    AccessPlanError, validate_access_structure, validate_access_structure_model,
};
