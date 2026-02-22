use crate::{
    db::{
        executor::load::{IndexRangeLimitSpec, LoadExecutor},
        index::RawIndexKey,
        query::plan::{
            CursorBoundary, LogicalPlan, compute_page_window, validate::PushdownApplicability,
        },
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
    pub(super) probe_fetch_hint: Option<usize>,
}

///
/// FastPathOrder
///
/// Shared fast-path precedence model used by load and aggregate routing.
/// Routing implementations remain separate, but they iterate one canonical order.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum FastPathOrder {
    PrimaryKey,
    SecondaryPrefix,
    PrimaryScan,
    IndexRange,
    Composite,
}

pub(super) const LOAD_FAST_PATH_ORDER: [FastPathOrder; 3] = [
    FastPathOrder::PrimaryKey,
    FastPathOrder::SecondaryPrefix,
    FastPathOrder::IndexRange,
];

pub(super) const AGGREGATE_FAST_PATH_ORDER: [FastPathOrder; 5] = [
    FastPathOrder::PrimaryKey,
    FastPathOrder::SecondaryPrefix,
    FastPathOrder::PrimaryScan,
    FastPathOrder::IndexRange,
    FastPathOrder::Composite,
];

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Build canonical fast-path routing decisions once per load execution.
    pub(super) fn build_fast_path_plan(
        plan: &LogicalPlan<E::Key>,
        cursor_boundary: Option<&CursorBoundary>,
        index_range_anchor: Option<&RawIndexKey>,
        probe_fetch_hint: Option<usize>,
    ) -> Result<FastPathPlan, InternalError> {
        Self::validate_pk_fast_path_boundary_if_applicable(plan, cursor_boundary)?;

        Ok(FastPathPlan {
            secondary_pushdown_applicability:
                crate::db::query::plan::validate::assess_secondary_order_pushdown_if_applicable_validated(
                    E::MODEL,
                    plan,
                ),
            index_range_limit_spec: Self::assess_index_range_limit_pushdown(
                plan,
                cursor_boundary,
                index_range_anchor,
                probe_fetch_hint,
            ),
            probe_fetch_hint,
        })
    }

    // Assess index-range limit pushdown once for this execution and produce
    // the bounded fetch spec when all eligibility gates pass.
    fn assess_index_range_limit_pushdown(
        plan: &LogicalPlan<E::Key>,
        cursor_boundary: Option<&CursorBoundary>,
        index_range_anchor: Option<&RawIndexKey>,
        probe_fetch_hint: Option<usize>,
    ) -> Option<IndexRangeLimitSpec> {
        if !Self::is_index_range_limit_pushdown_shape_eligible(plan) {
            return None;
        }
        if cursor_boundary.is_some() && index_range_anchor.is_none() {
            return None;
        }
        if let Some(fetch) = probe_fetch_hint {
            return Some(IndexRangeLimitSpec { fetch });
        }

        let page = plan.page.as_ref()?;
        let limit = page.limit?;
        if limit == 0 {
            return Some(IndexRangeLimitSpec { fetch: 0 });
        }

        let fetch = compute_page_window(plan.effective_page_offset(cursor_boundary), limit, true)
            .fetch_count;

        Some(IndexRangeLimitSpec { fetch })
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::db::executor::load::route::{
        AGGREGATE_FAST_PATH_ORDER, FastPathOrder, LOAD_FAST_PATH_ORDER,
    };

    #[test]
    fn load_fast_path_order_matches_expected_precedence() {
        assert_eq!(
            LOAD_FAST_PATH_ORDER,
            [
                FastPathOrder::PrimaryKey,
                FastPathOrder::SecondaryPrefix,
                FastPathOrder::IndexRange,
            ],
            "load fast-path precedence must stay stable"
        );
    }

    #[test]
    fn aggregate_fast_path_order_matches_expected_precedence() {
        assert_eq!(
            AGGREGATE_FAST_PATH_ORDER,
            [
                FastPathOrder::PrimaryKey,
                FastPathOrder::SecondaryPrefix,
                FastPathOrder::PrimaryScan,
                FastPathOrder::IndexRange,
                FastPathOrder::Composite,
            ],
            "aggregate fast-path precedence must stay stable"
        );
    }
}
