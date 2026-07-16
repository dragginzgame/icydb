//! Module: executor::delete::runtime
//! Responsibility: delete runtime setup and candidate row resolution.
//! Does not own: typed output packaging, SQL structural projection, or commit
//! payload assembly.
//! Boundary: prepares delete execution state and resolves candidate data rows.

use crate::{
    db::{
        Db, PersistedRow,
        data::DataRow,
        executor::{
            AccessScanContinuationInput, AccessStreamBindings, ExecutionPreparation,
            OrderReadableRow, PreparedExecutionPlan, TraversalRuntime,
            mutation::{mutation_write_context, preflight_mutation_plan_for_authority},
            pipeline::contracts::{
                ExecutionInputs, ExecutionRuntimeAdapter, PreparedExecutionInputContext,
                PreparedExecutionProjection, ProjectionMaterializationMode,
            },
            pipeline::runtime::ExecutionAttemptKernel,
            plan_metrics::record_rows_scanned_for_path,
            planning::preparation::slot_map_for_model_plan,
            read_owned_data_row_with_consistency_from_store,
            route::{RoutePlanRequest, build_execution_route_plan},
        },
        index::IndexCompilePolicy,
        predicate::MissingRowPolicy,
        query::plan::AccessPlannedQuery,
        registry::StoreHandle,
        schema::{accepted_commit_schema_fingerprint, ensure_accepted_schema_snapshot},
    },
    error::InternalError,
    traits::Path,
};
use std::sync::Arc;

use crate::db::executor::delete::{
    apply_delete_post_access_rows, prepare_delete_commit,
    types::{
        DeleteExecutionAuthority, DeleteLeaf, PreparedDeleteExecutionState, PreparedDeleteOutput,
        validate_delete_plan_shape,
    },
};

// Prepare one generic-free delete execution state after the typed plan shell is consumed.
fn prepare_delete_execution_state(
    authority: DeleteExecutionAuthority,
    logical_plan: Arc<AccessPlannedQuery>,
    index_prefix_specs: Arc<[crate::db::access::LoweredIndexPrefixSpec]>,
    index_range_specs: Arc<[crate::db::access::LoweredIndexRangeSpec]>,
) -> Result<PreparedDeleteExecutionState, InternalError> {
    // Phase 1: validate the structural mutation plan before touching store access.
    preflight_mutation_plan_for_authority(authority.entity.clone(), &logical_plan)?;

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
    E: PersistedRow,
{
    validate_delete_plan_shape(&plan)?;

    let prepared = plan.into_access_plan_handoff()?;
    let accepted_schema = {
        let store = db.recovered_store(E::Store::PATH)?;
        store.with_schema_mut(|schema_store| {
            ensure_accepted_schema_snapshot(
                schema_store,
                E::ENTITY_TAG,
                E::PATH,
                E::Store::PATH,
                E::MODEL,
            )
        })?
    };
    let schema_fingerprint = accepted_commit_schema_fingerprint(&accepted_schema)?;
    let authority =
        DeleteExecutionAuthority::from_entity_authority(prepared.authority, schema_fingerprint);
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

// Resolve delete access rows for one delete execution through the shared
// scalar key-stream resolver, then keep delete-owned row collection local.
fn resolve_delete_candidate_rows_as<T>(
    store: StoreHandle,
    prepared: &PreparedDeleteExecutionState,
    mut map_row: impl FnMut(DataRow) -> Result<T, InternalError>,
) -> Result<(Vec<T>, usize), InternalError> {
    // Phase 1: assemble the same execution-input snapshot consumed by scalar
    // runtime key-stream resolution, but suppress row materialization concerns.
    let runtime = ExecutionRuntimeAdapter::from_stream_runtime(TraversalRuntime::new(
        store,
        prepared.authority.entity.entity_tag(),
    ));
    let execution_inputs = ExecutionInputs::new_prepared(PreparedExecutionInputContext {
        runtime: &runtime,
        plan: &prepared.logical_plan,
        executable_access: prepared.logical_plan.access.executable_contract(),
        stream_bindings: AccessStreamBindings::new(
            prepared.index_prefix_specs.as_ref(),
            prepared.index_range_specs.as_ref(),
            AccessScanContinuationInput::new(None, prepared.route_plan.direction()),
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

// Resolve delete candidates and record scanned-row attribution against the
// selected entity path in one place for typed and structural delete callers.
pub(in crate::db::executor::delete) fn resolve_delete_candidate_rows_recorded_as<T>(
    store: StoreHandle,
    prepared: &PreparedDeleteExecutionState,
    map_row: impl FnMut(DataRow) -> Result<T, InternalError>,
) -> Result<Vec<T>, InternalError> {
    let (rows, rows_scanned) = resolve_delete_candidate_rows_as(store, prepared, map_row)?;
    record_rows_scanned_for_path(prepared.authority.entity.entity_path(), rows_scanned);

    Ok(rows)
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
    let mut rows = Vec::with_capacity(
        key_stream
            .exact_key_count_hint()
            .unwrap_or(0)
            .min(crate::db::executor::ACCESS_SCAN_CHUNK_ENTRIES),
    );
    let mut rows_loaded = 0usize;

    while let Some(key) = key_stream.next_key()? {
        if let Some(row) = read_owned_data_row_with_consistency_from_store(store, key, consistency)?
        {
            rows.push(map_row(row)?);
            rows_loaded = rows_loaded.saturating_add(1);
        }
    }

    Ok((rows, rows_loaded))
}

// Apply delete post-access selection and then let the caller package the
// surviving rows. Typed and structural delete cores share this leaf boundary
// while keeping row decoding, response shaping, and rollback packaging local.
pub(in crate::db::executor::delete) fn prepare_delete_leaf_rows<R, T>(
    prepared: &PreparedDeleteExecutionState,
    mut rows: Vec<R>,
    package_rows: impl FnOnce(Vec<R>) -> Result<T, InternalError>,
) -> Result<T, InternalError>
where
    R: OrderReadableRow,
{
    apply_delete_post_access_rows(prepared, &mut rows)?;

    package_rows(rows)
}

// Prepare relation validation and commit row ops for one already selected
// delete leaf. Typed and structural delete callers share this final
// pre-commit payload assembly before their wrapper applies the commit window.
pub(in crate::db::executor::delete) fn prepare_delete_output_from_leaf<C, T>(
    db: &Db<C>,
    store: StoreHandle,
    prepared: &PreparedDeleteExecutionState,
    leaf: DeleteLeaf<T>,
) -> Result<Option<PreparedDeleteOutput<T>>, InternalError>
where
    C: crate::traits::CanisterKind,
{
    if leaf.row_count == 0 {
        return Ok(None);
    }

    let commit = prepare_delete_commit(db, store, &prepared.authority, leaf.rollback_rows)?;

    Ok(Some(PreparedDeleteOutput {
        output: leaf.output,
        commit,
        row_count: leaf.row_count,
    }))
}
