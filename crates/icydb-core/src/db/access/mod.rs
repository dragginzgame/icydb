//! Shared access-layer contracts.
//!
//! This module owns shared access contracts and access-shape lowering helpers
//! used by query planning and executor runtime.

pub(crate) mod index_predicate;
pub(crate) mod lowering;
pub(crate) mod path;
pub(crate) mod plan;
pub(crate) mod validate;

pub(crate) use index_predicate::{
    IndexPredicateCompileMode, compile_index_predicate_program_from_slots,
};
pub(crate) use index_predicate::{IndexPredicateProgram, eval_index_compare};
pub(in crate::db) use lowering::{
    LOWERED_INDEX_PREFIX_SPEC_INVALID, LOWERED_INDEX_RANGE_SPEC_INVALID, LoweredIndexPrefixSpec,
    LoweredIndexRangeSpec, LoweredKey, lower_cursor_anchor_index_range_bounds,
    lower_index_prefix_specs, lower_index_range_specs,
};
pub(crate) use path::{AccessPath, IndexRangePathRef, SemanticIndexRangeSpec};
#[cfg(test)]
pub(crate) use plan::assess_secondary_order_pushdown_if_applicable;
pub(crate) use plan::{
    AccessPlan, PushdownApplicability, PushdownSurfaceEligibility,
    SecondaryOrderPushdownEligibility, SecondaryOrderPushdownRejection,
    assess_secondary_order_pushdown, assess_secondary_order_pushdown_if_applicable_validated,
};
pub(crate) use validate::{AccessPlanError, validate_access_plan, validate_access_plan_model};
