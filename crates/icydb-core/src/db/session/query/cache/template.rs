//! Module: db::session::query::cache::template
//! Responsibility: reusable parameterized query-template residents and binding.
//! Does not own: cache identity, cache storage, or public prepared-statement APIs.
//! Boundary: retains value-independent access authority and produces one concrete
//! execution plan from the current normalized parameter values.

use crate::db::{
    QueryError,
    access::SemanticIndexAccessContract,
    executor::SharedPreparedExecutionPlan,
    query::{
        intent::StructuralQuery,
        plan::{AccessPlannedQuery, PreparedScalarPlanningState},
    },
};

const PREPARED_QUERY_TEMPLATE_RETAINED_BYTES_ESTIMATE: usize = 24 * 1024;

///
/// PreparedQueryTemplate
///
/// Shared parameterized planning resident. Its selected index authority is
/// value-independent; a replaceable memo retains at most one bound execution.
///
#[derive(Clone, Debug)]
pub(super) struct PreparedQueryTemplate {
    selected_indexes: Vec<SemanticIndexAccessContract>,
    recent_bound: Option<BoundQueryExecutionMemo>,
}

#[derive(Clone, Debug)]
struct BoundQueryExecutionMemo {
    predicate_fingerprint: [u8; 32],
    prepared_plan: SharedPreparedExecutionPlan,
}

impl PreparedQueryTemplate {
    pub(super) fn from_plan(plan: &AccessPlannedQuery) -> Self {
        Self {
            selected_indexes: plan.access.selected_index_contracts(),
            recent_bound: None,
        }
    }

    pub(super) fn reused_bound_plan(
        &self,
        predicate_fingerprint: [u8; 32],
    ) -> Option<SharedPreparedExecutionPlan> {
        self.recent_bound.as_ref().and_then(|bound| {
            (bound.predicate_fingerprint == predicate_fingerprint)
                .then(|| bound.prepared_plan.clone())
        })
    }

    pub(super) fn bind(
        &self,
        query: &StructuralQuery,
        planning_state: PreparedScalarPlanningState<'_>,
    ) -> Result<AccessPlannedQuery, QueryError> {
        query.build_plan_from_parameterized_template(&self.selected_indexes, planning_state)
    }

    pub(super) fn remember_bound_plan(
        &mut self,
        predicate_fingerprint: [u8; 32],
        prepared_plan: SharedPreparedExecutionPlan,
    ) {
        self.recent_bound = Some(BoundQueryExecutionMemo {
            predicate_fingerprint,
            prepared_plan,
        });
    }

    pub(super) const fn estimated_retained_bytes() -> usize {
        PREPARED_QUERY_TEMPLATE_RETAINED_BYTES_ESTIMATE
    }
}
