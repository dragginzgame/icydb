//! Module: db::session::query::attribution
//! Responsibility: bounded operation-local attribution for ordinary reads.
//! Does not own: planning, admission, execution, retained metrics, or query identity.
//! Boundary: projects one completed live-page execution into fixed enums and counters.

use crate::db::{
    access::{AccessPath, AccessPlan},
    executor::{SharedPreparedExecutionPlan, StructuralProjectionExecutionRoute},
    session::query::QueryPlanCacheAttribution,
};
use candid::CandidType;
use serde::Deserialize;

/// One normal read result paired with bounded operation-local attribution.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct AttributedRead<T> {
    /// The unchanged result produced by the ordinary read path.
    pub result: T,
    /// Fixed-size operational attribution for this call only.
    pub attribution: OperationReadAttribution,
}

/// Coarse accepted access route selected for one read.
///
/// The enum intentionally excludes entity, field, index, predicate and literal
/// identity so an attributed query cannot create an unbounded label surface.
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub enum ReadAccessRoute {
    /// One direct primary-key lookup.
    PrimaryKey,
    /// One bounded set of direct primary-key lookups.
    PrimaryKeySet,
    /// One inclusive primary-key range.
    PrimaryKeyRange,
    /// One accepted secondary-index prefix.
    SecondaryIndexPrefix,
    /// One bounded secondary-index multi-lookup.
    SecondaryIndexMultiLookup,
    /// One bounded secondary-index branch set.
    SecondaryIndexBranchSet,
    /// One accepted secondary-index range.
    SecondaryIndexRange,
    /// One authoritative primary-store scan.
    FullScan,
    /// One canonical union of bounded child access routes.
    Union,
    /// One canonical intersection of bounded child access routes.
    Intersection,
}

/// Physical execution route used to produce one read result.
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub enum ReadExecutionRoute {
    /// Projection was satisfied from accepted index components.
    Covering,
    /// Scalar execution streamed the selected access route.
    Streaming,
    /// Scalar execution materialized candidates before final output.
    Materialized,
}

/// Shared plan-cache outcome for the selected read plan.
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub enum ReadPlanCacheOutcome {
    /// The selected prepared plan was reused.
    Hit,
    /// The selected prepared plan was built for this call.
    Miss,
    /// The selected path did not report a cache lookup outcome.
    Bypassed,
}

/// Fixed-size operation-local cost and route attribution for one read.
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub struct OperationReadAttribution {
    /// Complete local instructions observed by the outward read terminal.
    pub total_local_instructions: u64,
    /// Local instructions observed inside the accepted dynamic execution path.
    pub engine_local_instructions: u64,
    /// Local instructions used to decode accepted rows into typed output.
    /// Dynamic reads report zero because their structural result is final.
    pub response_decode_local_instructions: u64,
    /// Coarse accepted access route selected by the planner.
    pub access_route: ReadAccessRoute,
    /// Physical route used by the executor.
    pub execution_route: ReadExecutionRoute,
    /// Cache outcome for the selected prepared plan.
    pub plan_cache: ReadPlanCacheOutcome,
    /// Physical keys or index entries visited while producing this page.
    pub rows_scanned: u64,
    /// Logical rows returned by this page.
    pub rows_emitted: u64,
}

pub(in crate::db::session) struct OperationReadAttributionBuilder {
    access_route: Option<ReadAccessRoute>,
    execution_route: Option<ReadExecutionRoute>,
    plan_cache: Option<ReadPlanCacheOutcome>,
    rows_scanned: u64,
    rows_emitted: u64,
}

impl OperationReadAttributionBuilder {
    #[must_use]
    pub(in crate::db::session) const fn new() -> Self {
        Self {
            access_route: None,
            execution_route: None,
            plan_cache: None,
            rows_scanned: 0,
            rows_emitted: 0,
        }
    }

    pub(in crate::db::session) fn record_plan(
        &mut self,
        prepared_plan: &SharedPreparedExecutionPlan,
        cache: QueryPlanCacheAttribution,
    ) {
        self.access_route = Some(ReadAccessRoute::from_plan(
            &prepared_plan.logical_plan().access,
        ));
        self.plan_cache = Some(ReadPlanCacheOutcome::from_cache(cache));
    }

    pub(in crate::db::session) fn record_execution(
        &mut self,
        route: StructuralProjectionExecutionRoute,
        rows_scanned: usize,
        rows_emitted: u32,
    ) {
        self.execution_route = Some(ReadExecutionRoute::from_projection(route));
        self.rows_scanned = u64::try_from(rows_scanned).unwrap_or(u64::MAX);
        self.rows_emitted = u64::from(rows_emitted);
    }

    pub(in crate::db::session) fn finish(
        self,
        engine_local_instructions: u64,
    ) -> Result<OperationReadAttribution, crate::db::QueryError> {
        Ok(OperationReadAttribution {
            total_local_instructions: engine_local_instructions,
            engine_local_instructions,
            response_decode_local_instructions: 0,
            access_route: self
                .access_route
                .ok_or_else(crate::db::QueryError::invariant)?,
            execution_route: self
                .execution_route
                .ok_or_else(crate::db::QueryError::invariant)?,
            plan_cache: self
                .plan_cache
                .ok_or_else(crate::db::QueryError::invariant)?,
            rows_scanned: self.rows_scanned,
            rows_emitted: self.rows_emitted,
        })
    }
}

impl ReadAccessRoute {
    fn from_plan(plan: &AccessPlan<crate::value::Value>) -> Self {
        match plan {
            AccessPlan::Path(path) => match path.as_ref() {
                AccessPath::ByKey(_) => Self::PrimaryKey,
                AccessPath::ByKeys(_) => Self::PrimaryKeySet,
                AccessPath::KeyRange { .. } => Self::PrimaryKeyRange,
                AccessPath::IndexPrefix { .. } => Self::SecondaryIndexPrefix,
                AccessPath::IndexMultiLookup { .. } => Self::SecondaryIndexMultiLookup,
                AccessPath::IndexBranchSet { .. } => Self::SecondaryIndexBranchSet,
                AccessPath::IndexRange { .. } => Self::SecondaryIndexRange,
                AccessPath::FullScan => Self::FullScan,
            },
            AccessPlan::Union(_) => Self::Union,
            AccessPlan::Intersection(_) => Self::Intersection,
        }
    }
}

impl ReadExecutionRoute {
    const fn from_projection(route: StructuralProjectionExecutionRoute) -> Self {
        match route {
            StructuralProjectionExecutionRoute::Covering => Self::Covering,
            StructuralProjectionExecutionRoute::Streaming => Self::Streaming,
            StructuralProjectionExecutionRoute::Materialized => Self::Materialized,
        }
    }
}

impl ReadPlanCacheOutcome {
    const fn from_cache(cache: QueryPlanCacheAttribution) -> Self {
        if cache.hits > 0 && cache.misses == 0 {
            Self::Hit
        } else if cache.misses > 0 && cache.hits == 0 {
            Self::Miss
        } else {
            Self::Bypassed
        }
    }
}

#[must_use]
#[cfg(target_arch = "wasm32")]
pub(in crate::db::session) fn read_operation_local_instruction_counter() -> u64 {
    crate::runtime::performance_counter(1)
}

#[must_use]
#[cfg(not(target_arch = "wasm32"))]
pub(in crate::db::session) const fn read_operation_local_instruction_counter() -> u64 {
    0
}

#[cfg(test)]
mod tests {
    use super::{
        OperationReadAttribution, ReadAccessRoute, ReadExecutionRoute, ReadPlanCacheOutcome,
    };

    #[test]
    fn operation_read_attribution_has_a_fixed_small_candid_envelope() {
        let attribution = OperationReadAttribution {
            total_local_instructions: u64::MAX,
            engine_local_instructions: u64::MAX,
            response_decode_local_instructions: u64::MAX,
            access_route: ReadAccessRoute::SecondaryIndexBranchSet,
            execution_route: ReadExecutionRoute::Materialized,
            plan_cache: ReadPlanCacheOutcome::Bypassed,
            rows_scanned: u64::MAX,
            rows_emitted: u64::MAX,
        };
        let encoded = candid::encode_one(attribution)
            .expect("fixed operation-local attribution should encode");

        assert_eq!(encoded.len(), 200);
        assert!(encoded.len() <= 256);
    }
}
