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

const PREPARED_QUERY_TEMPLATE_BASE_RETAINED_BYTES_ESTIMATE: usize = 24 * 1024;
const PREPARED_QUERY_TEMPLATE_INDEX_RETAINED_BYTES_ESTIMATE: usize = 512;

///
/// PreparedQueryTemplate
///
/// Shared parameterized planning resident. Its candidate authority is
/// value-independent; a replaceable memo retains at most one bound execution.
///
#[derive(Clone, Debug)]
pub(super) struct PreparedQueryTemplate {
    candidate_indexes: Vec<SemanticIndexAccessContract>,
    recent_bound: Option<BoundQueryExecutionMemo>,
}

#[derive(Clone, Debug)]
struct BoundQueryExecutionMemo {
    predicate_fingerprint: [u8; 32],
    prepared_plan: SharedPreparedExecutionPlan,
}

impl PreparedQueryTemplate {
    pub(super) fn new(candidate_indexes: &[SemanticIndexAccessContract]) -> Self {
        Self {
            candidate_indexes: candidate_indexes.to_vec(),
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

    pub(super) const fn candidate_indexes(&self) -> &[SemanticIndexAccessContract] {
        self.candidate_indexes.as_slice()
    }

    pub(super) fn bind(
        &self,
        query: &StructuralQuery,
        planning_state: PreparedScalarPlanningState<'_>,
    ) -> Result<AccessPlannedQuery, QueryError> {
        query.build_plan_from_parameterized_template(&self.candidate_indexes, planning_state)
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

    pub(super) fn estimated_retained_bytes(&self) -> usize {
        self.candidate_indexes.iter().fold(
            PREPARED_QUERY_TEMPLATE_BASE_RETAINED_BYTES_ESTIMATE,
            |total, index| {
                let key_bytes = index.key_items().iter().fold(0usize, |bytes, item| {
                    bytes.saturating_add(item.as_ref().canonical_text().len())
                });
                total
                    .saturating_add(PREPARED_QUERY_TEMPLATE_INDEX_RETAINED_BYTES_ESTIMATE)
                    .saturating_add(index.name().len())
                    .saturating_add(index.store_path().len())
                    .saturating_add(key_bytes)
            },
        )
    }
}
