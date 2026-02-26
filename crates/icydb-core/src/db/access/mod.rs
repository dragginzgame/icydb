//! Shared access-layer contracts.
//!
//! This module owns passive types used across query planning, lowering, and
//! execution. It intentionally contains no routing, planning, or runtime logic.

pub(crate) mod path;
pub(crate) mod predicate;
pub(crate) mod pushdown;
pub(crate) mod validate;

#[allow(unused_imports)]
pub(crate) use path::{AccessPath, AccessPlan, IndexRangePathRef, SemanticIndexRangeSpec};
pub(crate) use predicate::{
    IndexCompareOp, IndexLiteral, IndexPredicateProgram, eval_index_compare,
};
#[cfg(test)]
pub(crate) use pushdown::assess_secondary_order_pushdown_if_applicable;
pub(crate) use pushdown::{
    PushdownApplicability, PushdownSurfaceEligibility, SecondaryOrderPushdownEligibility,
    SecondaryOrderPushdownRejection, assess_secondary_order_pushdown,
    assess_secondary_order_pushdown_if_applicable_validated,
};
pub(crate) use validate::{AccessPlanError, validate_access_plan, validate_access_plan_model};
