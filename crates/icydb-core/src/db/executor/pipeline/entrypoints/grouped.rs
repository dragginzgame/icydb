//! Module: db::executor::pipeline::entrypoints::grouped
//! Defines grouped pipeline entrypoints from prepared route shapes into grouped
//! runtime execution.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::executor::{SharedPreparedExecutionPlan, StructuralGroupedProjectionResult};
use crate::db::registry::StoreHandle;
use crate::{
    db::{
        commit::cursor_authentication_key,
        cursor::{ValidatedGroupedCursor, encoded_cursor_len},
        executor::{
            EntityAuthority, PreparedGroupedRuntimeResidents, PreparedLoadPlan,
            aggregate::runtime::{
                build_grouped_stream_with_runtime, execute_group_fold_stage,
                try_execute_grouped_count_metadata,
            },
            budget::{
                charge_current_execution_budget, charge_runtime_grouped_rows,
                prepared_read_execution_context, runtime_value_work, with_read_execution_budget,
            },
            pipeline::contracts::{ExecutionRuntimeAdapter, GroupedCursorPage, GroupedRouteStage},
            pipeline::grouped_runtime::resolve_grouped_route_for_plan,
            pipeline::runtime::{GroupedStreamStage, StructuralGroupedRowRuntime},
            stream::access::TraversalRuntime,
        },
        schema::cardinality_generation::CardinalityAcceptedRootIdentity,
    },
    error::InternalError,
    metrics::EntityMetricsSpan,
    traits::CanisterKind,
};
use icydb_diagnostic_code::{DiagnosticExecutionBudgetResource, DiagnosticExecutionLane};
use std::rc::Rc;

/// Execute one generic-free shared grouped plan through the canonical runtime.
pub(in crate::db) fn execute_shared_grouped_plan_for_canister<C>(
    db: &crate::db::Db<C>,
    plan: SharedPreparedExecutionPlan,
    cursor: ValidatedGroupedCursor,
    execution_lane: DiagnosticExecutionLane,
) -> Result<StructuralGroupedProjectionResult, InternalError>
where
    C: CanisterKind,
{
    let context = prepared_read_execution_context(&plan, execution_lane);
    with_read_execution_budget(db.request_execution_scope(), context, || {
        execute_shared_grouped_plan_for_canister_inner(db, plan, cursor)
    })
}

fn execute_shared_grouped_plan_for_canister_inner<C>(
    db: &crate::db::Db<C>,
    plan: SharedPreparedExecutionPlan,
    cursor: ValidatedGroupedCursor,
) -> Result<StructuralGroupedProjectionResult, InternalError>
where
    C: CanisterKind,
{
    let entity_path = plan.authority_ref().entity_path_handle();
    let _metrics_span = EntityMetricsSpan::new(entity_path.as_ref());
    charge_grouped_cursor_input(&cursor)?;
    let value_catalog = plan
        .authority_ref()
        .accepted_schema_info()
        .value_catalog_handle()
        .clone();
    let prepared =
        prepare_grouped_route_runtime_for_load_plan(db, plan.into_prepared_load_plan(), cursor)?;
    let page = execute_prepared_grouped_route_runtime(prepared)?;
    let next_cursor = charge_grouped_page_result(&page)?;

    Ok(StructuralGroupedProjectionResult::new(
        page.rows,
        next_cursor,
        value_catalog,
    ))
}
// Seal once while the execution budget is active, and retain those exact bytes
// for the response boundary rather than re-reading the key and encoding again.
fn charge_grouped_page_result(page: &GroupedCursorPage) -> Result<Option<Vec<u8>>, InternalError> {
    charge_runtime_grouped_rows(&page.rows)?;
    if let Some(cursor) = page.next_cursor.as_ref() {
        let encoded = cursor
            .encode(&cursor_authentication_key()?)
            .map_err(|_| InternalError::query_executor_invariant())?;
        charge_grouped_cursor_bytes(&encoded)?;
        return Ok(Some(encoded));
    }

    Ok(None)
}

// Binary scratch and outward Base64 bytes are distinct resources. Charge the
// existing resources without allocating a temporary display string.
fn charge_grouped_cursor_bytes(encoded: &[u8]) -> Result<(), InternalError> {
    charge_current_execution_budget(DiagnosticExecutionBudgetResource::CursorSteps, 1)?;
    charge_current_execution_budget(
        DiagnosticExecutionBudgetResource::TemporaryBytes,
        u64::try_from(encoded.len()).unwrap_or(u64::MAX),
    )?;
    charge_current_execution_budget(
        DiagnosticExecutionBudgetResource::ResultBytes,
        u64::try_from(encoded_cursor_len(encoded.len())).unwrap_or(u64::MAX),
    )
}

fn charge_grouped_cursor_input(cursor: &ValidatedGroupedCursor) -> Result<(), InternalError> {
    let Some(group_key) = cursor.last_group_key() else {
        return Ok(());
    };
    let (bytes, nested_steps) = group_key.iter().fold((0_u64, 0_u64), |total, value| {
        let value_work = runtime_value_work(value);
        (
            total.0.saturating_add(value_work.0),
            total.1.saturating_add(value_work.1),
        )
    });
    charge_current_execution_budget(
        DiagnosticExecutionBudgetResource::CursorSteps,
        u64::try_from(group_key.len()).unwrap_or(u64::MAX),
    )?;
    charge_current_execution_budget(
        DiagnosticExecutionBudgetResource::NestedValueSteps,
        nested_steps,
    )?;
    charge_current_execution_budget(DiagnosticExecutionBudgetResource::TemporaryBytes, bytes)
}

///
/// GroupedPathRuntimeContext
///
/// GroupedPathRuntimeContext is the owner-local runtime context needed by the
/// grouped execution spine after the frontend resolves store authority.
/// Shared grouped entrypoint orchestration stays monomorphic by driving this
/// structural context directly.
///

struct GroupedPathRuntimeContext {
    traversal_runtime: TraversalRuntime,
    row_store: StoreHandle,
    authority: EntityAuthority,
}

///
/// PreparedGroupedRouteRuntime
///
/// PreparedGroupedRouteRuntime is the generic-free grouped execution bundle
/// emitted once the frontend has resolved route metadata and structural
/// runtime authority.
/// Grouped runtime execution consumes this bundle directly.
///

pub(in crate::db::executor) struct PreparedGroupedRouteRuntime {
    route: GroupedRouteStage,
    runtime: GroupedPathRuntimeContext,
    prepared_residents: Rc<PreparedGroupedRuntimeResidents>,
}

impl GroupedPathRuntimeContext {
    // Build the grouped runtime spine once from one recovered store handle and
    // its resolved structural entity authority.
    fn from_store(store: StoreHandle, authority: EntityAuthority) -> Result<Self, InternalError> {
        let entity_tag = authority.entity_tag();
        let accepted_schema = authority.accepted_schema_authority();
        let accepted_root = CardinalityAcceptedRootIdentity::new(
            accepted_schema.revision(),
            accepted_schema.fingerprint(),
        )?;

        Ok(Self {
            traversal_runtime: TraversalRuntime::new(
                store,
                entity_tag,
                authority
                    .accepted_runtime_root_identity()
                    .database_incarnation(),
                accepted_root,
            ),
            row_store: store,
            authority,
        })
    }

    /// Build one grouped execution stream for an already resolved route.
    fn build_grouped_stream(
        &self,
        route: &GroupedRouteStage,
        prepared_residents: Rc<PreparedGroupedRuntimeResidents>,
    ) -> Result<GroupedStreamStage, InternalError> {
        let runtime = ExecutionRuntimeAdapter::from_stream_runtime(self.traversal_runtime);
        let single_grouped_path = if prepared_residents
            .execution_preparation()
            .effective_runtime_filter_program()
            .is_none()
            && matches!(route.grouped_aggregate_execution_specs(), [aggregate] if aggregate.admits_count_rows_dedicated_fold())
        {
            route
                .group_fields()
                .as_path_aware()
                .and_then(|fields| match fields {
                    [field] => field.as_scalar_path(),
                    _ => None,
                })
        } else {
            None
        };
        let row_runtime = StructuralGroupedRowRuntime::new(
            self.row_store,
            self.authority.row_layout(),
            prepared_residents.grouped_slot_layout().clone(),
            single_grouped_path,
        );
        build_grouped_stream_with_runtime(route, &runtime, prepared_residents, row_runtime)
    }
}

impl PreparedGroupedRouteRuntime {
    // Build one prepared grouped runtime bundle from one resolved route and
    // one structural grouped runtime core without duplicating plan prep logic.
    const fn new(
        route: GroupedRouteStage,
        runtime: GroupedPathRuntimeContext,
        prepared_residents: Rc<PreparedGroupedRuntimeResidents>,
    ) -> Self {
        Self {
            route,
            runtime,
            prepared_residents,
        }
    }
}

// Prepare one grouped runtime bundle from one prepared load plan plus the
// caller-resolved grouped cursor so entrypoints and orchestrator strategy
// share one route/runtime assembly seam.
pub(in crate::db::executor) fn prepare_grouped_route_runtime_for_load_plan<C>(
    db: &crate::db::Db<C>,
    plan: PreparedLoadPlan,
    cursor: ValidatedGroupedCursor,
) -> Result<PreparedGroupedRouteRuntime, InternalError>
where
    C: CanisterKind,
{
    let authority = plan.authority();
    let prepared_residents = plan.grouped_runtime_residents()?;
    let route = resolve_grouped_route_for_plan(plan, cursor)?;
    let store = db.recovered_store(authority.store_path())?;

    Ok(PreparedGroupedRouteRuntime::new(
        route,
        GroupedPathRuntimeContext::from_store(store, authority)?,
        prepared_residents,
    ))
}

// Execute one fully resolved grouped route through the canonical grouped
// runtime spine. The grouped route/stream/page contracts are already structural,
// so this orchestration can stay monomorphic.
fn execute_grouped_route_path(
    runtime: &GroupedPathRuntimeContext,
    route: GroupedRouteStage,
    prepared_residents: Rc<PreparedGroupedRuntimeResidents>,
) -> Result<GroupedCursorPage, InternalError> {
    if let Some(page) = try_execute_grouped_count_metadata(
        runtime.row_store,
        runtime.authority.entity_tag(),
        &route,
    )? {
        return Ok(page);
    }
    let stream = runtime.build_grouped_stream(&route, prepared_residents)?;
    execute_group_fold_stage(&route, stream)
}

// Execute one fully prepared grouped runtime bundle through the canonical
// grouped runtime spine without re-entering typed executor state.
pub(in crate::db::executor) fn execute_prepared_grouped_route_runtime(
    prepared: PreparedGroupedRouteRuntime,
) -> Result<GroupedCursorPage, InternalError> {
    let PreparedGroupedRouteRuntime {
        route,
        runtime,
        prepared_residents,
    } = prepared;

    execute_grouped_route_path(&runtime, route, prepared_residents)
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::{
            QueryError,
            cursor::{ContinuationSignature, GroupedContinuationToken, encode_cursor},
            direction::Direction,
            executor::budget::{
                HardExecutionBudget, HardExecutionContext, HardExecutionFailureHeadroom,
                with_query_execution_budget_for_tests,
            },
        },
        value::Value,
    };
    use icydb_diagnostic_code::{DiagnosticExecutionBudgetScope, DiagnosticFactTag};

    fn budget() -> HardExecutionBudget {
        HardExecutionBudget::uniform_for_tests(1_000_000, HardExecutionFailureHeadroom::new(1, 1))
    }

    fn context() -> HardExecutionContext {
        HardExecutionContext::new(
            DiagnosticExecutionBudgetScope::Execution,
            DiagnosticExecutionLane::TrustedRead,
            0,
        )
    }

    #[test]
    fn grouped_cursor_charges_exact_binary_and_base64_bounds() {
        // Exercise all three unpadded Base64 remainder lengths with real tokens.
        for length in 1..=3 {
            let token = GroupedContinuationToken::new_with_direction(
                ContinuationSignature::from_bytes([1; 32]),
                vec![Value::Text("x".repeat(length))],
                Direction::Asc,
                0,
            );
            let encoded = token.encode(&[2; 32]).unwrap();
            for (resource, exact) in [
                (DiagnosticExecutionBudgetResource::CursorSteps, 1),
                (
                    DiagnosticExecutionBudgetResource::TemporaryBytes,
                    encoded.len() as u64,
                ),
                (
                    DiagnosticExecutionBudgetResource::ResultBytes,
                    encode_cursor(&encoded).len() as u64,
                ),
            ] {
                for (limit, accepted) in [(exact, true), (exact - 1, false)] {
                    let result = with_query_execution_budget_for_tests(
                        budget().with_limit_for_tests(resource, limit),
                        context(),
                        || charge_grouped_cursor_bytes(&encoded).map_err(QueryError::execute),
                    );
                    if accepted {
                        result.expect("exact resource bound must admit");
                    } else {
                        let error = result.expect_err("one byte/step below must reject");
                        assert!(
                            error
                                .diagnostic_facts()
                                .contains(&(DiagnosticFactTag::BudgetResource, resource.raw(),))
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn grouped_page_without_cursor_consumes_no_cursor_bytes_or_steps() {
        let budget = budget()
            .with_limit_for_tests(DiagnosticExecutionBudgetResource::CursorSteps, 0)
            .with_limit_for_tests(DiagnosticExecutionBudgetResource::TemporaryBytes, 0)
            .with_limit_for_tests(DiagnosticExecutionBudgetResource::ResultBytes, 0);
        let page = GroupedCursorPage {
            rows: vec![],
            next_cursor: None,
        };
        let result = with_query_execution_budget_for_tests(budget, context(), || {
            charge_grouped_page_result(&page).map_err(QueryError::execute)
        })
        .unwrap();
        assert!(result.is_none());
    }
}
