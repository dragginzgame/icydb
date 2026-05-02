//! Module: executor::delete::runtime
//! Responsibility: delete runtime setup, candidate row resolution, and shared
//! post-access filtering.
//! Does not own: typed output packaging, SQL structural projection, or commit
//! payload assembly.
//! Boundary: prepares delete execution state and resolves candidate data rows.

use crate::{
    db::{
        Db, PersistedRow,
        data::DataRow,
        executor::{
            AccessScanContinuationInput, AccessStreamBindings, ExecutionKernel,
            ExecutionPreparation, OrderReadableRow, PreparedExecutionPlan, TraversalRuntime,
            mutation::{mutation_write_context, preflight_mutation_plan_for_authority},
            pipeline::contracts::{
                ExecutionInputs, ExecutionRuntimeAdapter, PreparedExecutionInputParts,
                PreparedExecutionProjection, ProjectionMaterializationMode,
            },
            pipeline::runtime::ExecutionAttemptKernel,
            planning::preparation::slot_map_for_model_plan,
            read_data_row_with_consistency_from_store,
            route::{RoutePlanRequest, build_execution_route_plan},
        },
        index::IndexCompilePolicy,
        predicate::MissingRowPolicy,
        query::plan::AccessPlannedQuery,
        registry::StoreHandle,
    },
    error::InternalError,
    traits::EntityValue,
};
use std::sync::Arc;

use crate::db::executor::delete::types::{
    DeleteExecutionAuthority, PreparedDeleteExecutionState, validate_delete_plan_shape,
};

// Prepare one generic-free delete execution state after the typed plan shell is consumed.
fn prepare_delete_execution_state(
    authority: DeleteExecutionAuthority,
    logical_plan: Arc<AccessPlannedQuery>,
    index_prefix_specs: Arc<[crate::db::access::LoweredIndexPrefixSpec]>,
    index_range_specs: Arc<[crate::db::access::LoweredIndexRangeSpec]>,
) -> Result<PreparedDeleteExecutionState, InternalError> {
    // Phase 1: validate the structural mutation plan before touching store access.
    preflight_mutation_plan_for_authority(authority.entity, &logical_plan)?;

    // Phase 2: build reusable delete predicate/index preparation once.
    let route_plan = build_execution_route_plan(&logical_plan, RoutePlanRequest::MutationDelete)?;
    let slot_map = slot_map_for_model_plan(&logical_plan);
    let execution_preparation = ExecutionPreparation::from_runtime_plan(&logical_plan, slot_map);

    Ok(PreparedDeleteExecutionState {
        authority,
        logical_plan,
        route_plan,
        execution_preparation,
        index_prefix_specs,
        index_range_specs,
    })
}

// Prepare one typed delete runtime state from the consumed prepared plan. This
// is the shared outer setup for typed row, typed count, and structural
// returning delete entrypoints.
pub(in crate::db::executor::delete) fn prepare_delete_runtime<E>(
    db: &Db<E::Canister>,
    plan: PreparedExecutionPlan<E>,
) -> Result<(PreparedDeleteExecutionState, StoreHandle), InternalError>
where
    E: PersistedRow + EntityValue,
{
    validate_delete_plan_shape(&plan)?;

    let prepared = plan.into_access_plan_parts()?;
    let authority = DeleteExecutionAuthority::for_type::<E>();
    let prepared = prepare_delete_execution_state(
        authority,
        prepared.plan,
        prepared.index_prefix_specs,
        prepared.index_range_specs,
    )?;
    let ctx = mutation_write_context::<E>(db)?;
    let store = ctx.structural_store()?;

    Ok((prepared, store))
}

// Resolve structural access rows for one delete execution through the shared
// scalar key-stream resolver, then keep delete-owned row collection local.
pub(in crate::db::executor::delete) fn resolve_delete_candidate_rows_as<T>(
    store: StoreHandle,
    prepared: &PreparedDeleteExecutionState,
    mut map_row: impl FnMut(DataRow) -> Result<T, InternalError>,
) -> Result<(Vec<T>, usize), InternalError> {
    // Phase 1: assemble the same execution-input snapshot consumed by scalar
    // runtime key-stream resolution, but suppress row materialization concerns.
    let runtime = ExecutionRuntimeAdapter::from_stream_runtime_parts(TraversalRuntime::new(
        store,
        prepared.authority.entity.entity_tag(),
    ));
    let execution_inputs = ExecutionInputs::new_prepared(PreparedExecutionInputParts {
        runtime: &runtime,
        plan: &prepared.logical_plan,
        executable_access: prepared.logical_plan.access.executable_contract(),
        stream_bindings: AccessStreamBindings::new(
            prepared.index_prefix_specs.as_ref(),
            prepared.index_range_specs.as_ref(),
            AccessScanContinuationInput::initial_asc(),
        ),
        execution_preparation: &prepared.execution_preparation,
        projection_materialization: ProjectionMaterializationMode::None,
        prepared_projection: PreparedExecutionProjection::empty(),
        emit_cursor: false,
    });

    // Phase 2: resolve keys through the canonical runtime resolver. Delete
    // owns the later row collection and commit/rollback preparation only.
    let mut resolved = ExecutionAttemptKernel::new(&execution_inputs)
        .resolve_execution_key_stream(
            &prepared.route_plan,
            IndexCompilePolicy::ConservativeSubset,
        )?;

    // Phase 3: materialize rows through the structural consistency boundary.
    collect_delete_rows_from_key_stream::<_, T>(
        store,
        resolved.key_stream_mut(),
        prepared.consistency(),
        &mut map_row,
    )
}

// Materialize ordered delete rows from one structural key stream directly into
// the caller's post-access row representation.
fn collect_delete_rows_from_key_stream<S, T>(
    store: StoreHandle,
    key_stream: &mut S,
    consistency: MissingRowPolicy,
    map_row: &mut impl FnMut(DataRow) -> Result<T, InternalError>,
) -> Result<(Vec<T>, usize), InternalError>
where
    S: crate::db::executor::OrderedKeyStream + ?Sized,
{
    let mut rows = Vec::with_capacity(key_stream.exact_key_count_hint().unwrap_or(0));
    let mut rows_loaded = 0usize;

    while let Some(key) = key_stream.next_key()? {
        if let Some(row) = read_data_row_with_consistency_from_store(store, &key, consistency)? {
            rows.push(map_row(row)?);
            rows_loaded = rows_loaded.saturating_add(1);
        }
    }

    Ok((rows, rows_loaded))
}

// Apply the shared delete-only post-access contract once after the caller has
// chosen its row representation.
pub(in crate::db::executor::delete) fn apply_delete_post_access_rows<R>(
    prepared: &PreparedDeleteExecutionState,
    rows: &mut Vec<R>,
) -> Result<(), InternalError>
where
    R: OrderReadableRow,
{
    let stats = ExecutionKernel::apply_delete_post_access_with_filter_program(
        &prepared.logical_plan,
        rows,
        prepared.logical_plan.effective_runtime_filter_program(),
    )?;
    let _ = stats.delete_was_limited;
    let _ = stats.rows_after_cursor;

    Ok(())
}
