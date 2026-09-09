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
        preparation::PreparationWork,
    },
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;
use std::rc::Rc;

///
/// PreparedQueryTemplate
///
/// Shared parameterized planning resident. Its candidate authority is
/// value-independent; a replaceable memo retains at most one bound execution.
///
#[derive(Clone, Debug)]
pub(super) struct PreparedQueryTemplate {
    candidate_indexes: Rc<[SemanticIndexAccessContract]>,
    recent_bound: Option<BoundQueryExecutionMemo>,
}

#[derive(Clone, Debug)]
struct BoundQueryExecutionMemo {
    predicate_fingerprint: [u8; 32],
    prepared_plan: SharedPreparedExecutionPlan,
}

impl PreparedQueryTemplate {
    pub(super) fn new(
        candidate_indexes: &[SemanticIndexAccessContract],
        work: &PreparationWork<'_>,
    ) -> Result<Self, QueryError> {
        // The contracts already share immutable metadata. Allocate their handle
        // array once, including Rc's two reference counts; warm snapshots share
        // that array and keep only the replaceable bound memo independent.
        work.charge(
            Resource::PredicateExpressionSteps,
            candidate_indexes.len() as u64,
        )?;
        work.charge(
            Resource::TemporaryBytes,
            (size_of_val(candidate_indexes) as u64).saturating_add((2 * size_of::<usize>()) as u64),
        )?;
        Ok(Self {
            candidate_indexes: Rc::from(candidate_indexes),
            recent_bound: None,
        })
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

    pub(super) fn candidate_indexes(&self) -> &[SemanticIndexAccessContract] {
        &self.candidate_indexes
    }

    pub(super) fn bind(
        &self,
        query: &StructuralQuery,
        planning_state: PreparedScalarPlanningState<'_>,
        work: &PreparationWork<'_>,
    ) -> Result<AccessPlannedQuery, QueryError> {
        query.build_plan_from_parameterized_template(&self.candidate_indexes, planning_state, work)
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

    pub(super) fn retained_plan(&self) -> Option<&SharedPreparedExecutionPlan> {
        self.recent_bound.as_ref().map(|bound| &bound.prepared_plan)
    }
}

// Exhaustive cache-retention coverage; new owned fields require accounting.
crate::retained::retained_fields!(BoundQueryExecutionMemo {
Self{predicate_fingerprint,prepared_plan} => [predicate_fingerprint,prepared_plan],
});
crate::retained::retained_fields!(PreparedQueryTemplate {
Self{candidate_indexes,recent_bound} => [candidate_indexes,recent_bound],
});
