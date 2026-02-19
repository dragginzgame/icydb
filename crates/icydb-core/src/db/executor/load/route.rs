use crate::{
    db::{
        executor::load::{IndexRangeLimitSpec, LoadExecutor},
        index::RawIndexKey,
        query::plan::{CursorBoundary, LogicalPlan, validate::PushdownApplicability},
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

///
/// FastPathPlan
///
/// Planned fast-path routing decisions derived from one validated load plan.
/// Keeps route eligibility checks centralized and execution-agnostic.
///

pub(super) struct FastPathPlan {
    pub(super) secondary_pushdown_applicability: PushdownApplicability,
    pub(super) index_range_limit_spec: Option<IndexRangeLimitSpec>,
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Build canonical fast-path routing decisions once per load execution.
    pub(super) fn build_fast_path_plan(
        plan: &LogicalPlan<E::Key>,
        cursor_boundary: Option<&CursorBoundary>,
        index_range_anchor: Option<&RawIndexKey>,
    ) -> Result<FastPathPlan, InternalError> {
        Self::validate_pk_fast_path_boundary_if_applicable(plan, cursor_boundary)?;

        Ok(FastPathPlan {
            secondary_pushdown_applicability: Self::assess_secondary_order_pushdown_applicability(
                plan,
            ),
            index_range_limit_spec: Self::assess_index_range_limit_pushdown(
                plan,
                cursor_boundary,
                index_range_anchor,
            ),
        })
    }

    // Assess secondary-index ORDER BY pushdown once for this execution and
    // map matrix outcomes to executor decisions.
    fn assess_secondary_order_pushdown_applicability(
        plan: &LogicalPlan<E::Key>,
    ) -> PushdownApplicability {
        crate::db::query::plan::validate::assess_secondary_order_pushdown_if_applicable_validated(
            E::MODEL,
            plan,
        )
    }
}
