//! Module: executor::projection::covering
//! Responsibility: executor-owned covering projection execution.
//! Does not own: response envelopes or projected-row DISTINCT finalization.
//! Boundary: consumes prepared access plans and emits structural projection rows.

mod contracts;
mod hybrid;
mod pure;
mod shared;

use std::rc::Rc;

use self::contracts::{
    AccessPlannedQuery, CoveringHybridReadExecutionPlan, CoveringReadExecutionPlan,
};
use crate::{
    db::{
        Db,
        cursor::IndexScanContinuationInput,
        executor::{
            EntityAuthority, IndexScan, LoweredIndexPrefixSpec, LoweredIndexRangeSpec,
            decode_single_covering_projection_pairs, projection::MaterializedProjectionRows,
        },
        index::{
            IndexKey, envelope_is_empty, predicate::IndexPredicateExecution,
            raw_keys_for_component_prefix_with_kind,
        },
        predicate::MissingRowPolicy,
    },
    error::InternalError,
    traits::CanisterKind,
    value::Value,
};
use std::ops::Bound;

///
/// PreparedCoveringProjectionRuntime
///
/// Runtime-only covering projection inputs that travel together from the
/// shared prepared plan into pure or hybrid covering execution.
///

#[derive(Clone, Copy)]
pub(in crate::db::executor) struct PreparedCoveringProjectionRuntime<'a> {
    plan: &'a AccessPlannedQuery,
    index_prefix_specs: &'a [LoweredIndexPrefixSpec],
    index_range_specs: &'a [LoweredIndexRangeSpec],
    index_predicate_execution: Option<IndexPredicateExecution<'a>>,
}

impl<'a> PreparedCoveringProjectionRuntime<'a> {
    #[must_use]
    pub(in crate::db::executor) const fn new(
        plan: &'a AccessPlannedQuery,
        index_prefix_specs: &'a [LoweredIndexPrefixSpec],
        index_range_specs: &'a [LoweredIndexRangeSpec],
        index_predicate_execution: Option<IndexPredicateExecution<'a>>,
    ) -> Self {
        Self {
            plan,
            index_prefix_specs,
            index_range_specs,
            index_predicate_execution,
        }
    }
}

pub(in crate::db::executor) fn try_execute_prepared_covering_projection_rows_for_canister<C>(
    db: &Db<C>,
    authority: EntityAuthority,
    runtime: PreparedCoveringProjectionRuntime<'_>,
    covering: Option<Rc<CoveringReadExecutionPlan>>,
    hybrid: impl FnOnce() -> Option<Rc<CoveringHybridReadExecutionPlan>>,
) -> Result<Option<MaterializedProjectionRows>, InternalError>
where
    C: CanisterKind,
{
    if let Some(covering) = covering
        && let Some(projected) = pure::try_execute_covering_projection_rows_with_plan_for_canister(
            db,
            authority.clone(),
            runtime.plan,
            runtime.index_prefix_specs,
            runtime.index_range_specs,
            &covering,
            runtime.index_predicate_execution,
        )?
    {
        return Ok(Some(MaterializedProjectionRows::from_value_rows(projected)));
    }

    let Some(hybrid) = hybrid() else {
        return Ok(None);
    };

    hybrid::try_execute_hybrid_covering_projection_rows_with_plan_for_canister(
        db, authority, runtime, &hybrid,
    )
    .map(|projected| projected.map(MaterializedProjectionRows::from_value_rows))
}

/// Visit one representative per component-zero group under a planner proof.
pub(in crate::db::executor) fn try_execute_ordered_distinct_group_seek_for_canister<C>(
    db: &Db<C>,
    authority: EntityAuthority,
    index_range_specs: &[LoweredIndexRangeSpec],
    covering: &CoveringReadExecutionPlan,
) -> Result<Option<Vec<Value>>, InternalError>
where
    C: CanisterKind,
{
    let Some(contract) = covering.ordered_distinct_group_seek_contract() else {
        return Ok(None);
    };
    let direction = contract.direction();
    let [range] = index_range_specs else {
        return Err(InternalError::query_executor_invariant());
    };
    let scan_contract = range.scan_contract();
    let store = db.recovered_store(scan_contract.store_path())?;
    let mut lower = range.lower().clone();
    let mut upper = range.upper().clone();
    let mut representatives = Vec::with_capacity(contract.representative_budget());

    while representatives.len() < contract.representative_budget() {
        let chunk = IndexScan::components_chunk_structural(
            store,
            authority.entity_tag(),
            &scan_contract,
            &lower,
            &upper,
            IndexScanContinuationInput::new(None, direction),
            1,
            Some(1),
            &[0],
            None,
        )?;
        let (mut rows, raw_anchor) = chunk.into_component_rows_and_resume_anchor();
        let Some(row) = rows.pop() else {
            break;
        };
        let raw_anchor = raw_anchor.ok_or_else(InternalError::query_executor_invariant)?;
        let decoded_anchor = IndexKey::try_from_raw(&raw_anchor).map_err(|error| {
            InternalError::index_scan_key_corrupted_during("DISTINCT group seek", error)
        })?;
        let [projected_component] = row.2.as_ref() else {
            return Err(InternalError::query_executor_invariant());
        };
        if decoded_anchor.component(0) != Some(projected_component.as_slice()) {
            return Err(InternalError::index_entry_decode_failed());
        }
        let (group_low, group_high) = raw_keys_for_component_prefix_with_kind(
            decoded_anchor.index_id(),
            decoded_anchor.key_kind(),
            decoded_anchor.component_count(),
            row.2.as_ref(),
        )
        .map_err(|_| InternalError::query_executor_invariant())?;
        let value = decode_single_covering_projection_pairs(
            vec![row],
            store,
            MissingRowPolicy::Error,
            covering.existing_row_mode,
            Ok::<Value, InternalError>,
        )?
        .and_then(|mut decoded| decoded.pop())
        .map(|(_data_key, value)| value)
        .ok_or_else(InternalError::query_executor_invariant)?;
        representatives.push(value);
        let boundary = match direction {
            crate::db::direction::Direction::Asc => group_high,
            crate::db::direction::Direction::Desc => group_low,
        };
        match direction {
            crate::db::direction::Direction::Asc => lower = Bound::Excluded(boundary),
            crate::db::direction::Direction::Desc => upper = Bound::Excluded(boundary),
        }
        if envelope_is_empty(&lower, &upper) {
            break;
        }
    }

    Ok(Some(representatives))
}
