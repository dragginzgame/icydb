//! Shared access-layer contracts.
//!
//! This module owns shared access contracts and access-shape lowering helpers
//! used by query planning and executor runtime.

pub(crate) mod lowering;
pub(crate) mod path;
pub(crate) mod plan;
pub(crate) mod validate;

pub(in crate::db) use lowering::{
    LOWERED_INDEX_PREFIX_SPEC_INVALID, LOWERED_INDEX_RANGE_SPEC_INVALID, LoweredIndexPrefixSpec,
    LoweredIndexRangeSpec, LoweredKey, lower_cursor_anchor_index_range_bounds,
    lower_index_prefix_specs, lower_index_range_specs,
};
pub(crate) use path::{AccessPath, IndexRangePathRef, SemanticIndexRangeSpec};
pub(crate) use plan::{
    AccessPlan, PushdownApplicability, PushdownSurfaceEligibility,
    SecondaryOrderPushdownEligibility, SecondaryOrderPushdownRejection,
};
pub(crate) use validate::{
    AccessPlanError, validate_access_structure, validate_access_structure_model,
};
