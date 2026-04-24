//! Module: access
//! Responsibility: access-path contracts, canonicalization, validation, and lowering.
//! Does not own: predicate semantics or index storage internals.
//! Boundary: query planning produces access plans; executor consumes lowered forms.

pub(crate) mod canonical;
pub(in crate::db) mod capabilities;
pub(in crate::db) mod dispatch;
pub(in crate::db) mod execution_contract;
pub(crate) mod lowering;
mod order_pushdown;
pub(crate) mod path;
pub(crate) mod plan;
#[cfg(test)]
mod tests;
pub(crate) mod validate;

// Canonical planner access surface.
pub(crate) use canonical::normalize_access_plan_value;
pub(crate) use order_pushdown::{
    PushdownApplicability, PushdownSurfaceEligibility, SecondaryOrderPushdownEligibility,
    SecondaryOrderPushdownRejection,
};
pub(crate) use path::{AccessPath, SemanticIndexRangeSpec};
pub(crate) use plan::AccessPlan;
pub(crate) use validate::{AccessPlanError, validate_access_structure_model};

// Boundary-local dispatch and capability helpers.
pub(in crate::db) use capabilities::AccessCapabilities;
pub(in crate::db) use dispatch::{AccessPathDispatch, AccessPathKind, dispatch_access_path};

// Executor-facing access contract and lowering surface.
pub(in crate::db) use execution_contract::{
    AccessStrategy, ExecutableAccessNode, ExecutableAccessPath, ExecutableAccessPlan,
    ExecutionPathPayload,
};
pub(in crate::db) use lowering::{
    LoweredAccessError, LoweredIndexPrefixSpec, LoweredIndexRangeSpec, LoweredKey, lower_access,
};
