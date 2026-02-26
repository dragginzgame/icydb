//! Shared access-layer contracts.
//!
//! This module owns passive types used across query planning, lowering, and
//! execution. It intentionally contains no routing, planning, or runtime logic.

pub(crate) mod path;
pub(crate) mod predicate;
pub(crate) mod pushdown;

#[allow(unused_imports)]
pub(crate) use path::{AccessPath, AccessPlan, IndexRangePathRef, SemanticIndexRangeSpec};
pub(crate) use predicate::{
    IndexCompareOp, IndexLiteral, IndexPredicateProgram, eval_index_compare,
};
#[cfg(test)]
pub(crate) use pushdown::assess_secondary_order_pushdown_if_applicable_from_parts;
pub(crate) use pushdown::{
    PushdownApplicability, PushdownSurfaceEligibility, SecondaryOrderPushdownEligibility,
    SecondaryOrderPushdownRejection, assess_secondary_order_pushdown_from_parts,
    assess_secondary_order_pushdown_if_applicable_validated_from_parts,
};
