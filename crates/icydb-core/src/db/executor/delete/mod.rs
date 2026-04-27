//! Module: executor::delete
//! Responsibility: delete-plan execution and commit-window handoff.
//! Does not own: logical planning, relation semantics, or cursor protocol details.
//! Boundary: delete-specific preflight/decode/apply flow over executable plans.

#[cfg(feature = "sql")]
use crate::db::executor::{
    projection::MaterializedProjectionRows,
    terminal::{KernelRow, RowDecoder},
};
use crate::{
    db::{
        Db,
        commit::{CommitRowOp, CommitSchemaFingerprint},
        data::{DataKey, DataRow, PersistedRow, RawDataKey, RawRow, decode_raw_row_for_entity_key},
        executor::{
            AccessScanContinuationInput, AccessStreamBindings, EntityAuthority, ExecutionKernel,
            ExecutionPlan, ExecutionPreparation, OrderReadableRow, PreparedExecutionPlan,
            TraversalRuntime,
            mutation::{
                commit_delete_row_ops_with_window, commit_delete_row_ops_with_window_for_path,
                mutation_write_context, preflight_mutation_plan_for_authority,
            },
            pipeline::contracts::{
                ExecutionInputs, ExecutionRuntimeAdapter, PreparedExecutionInputParts,
                PreparedExecutionProjection, ProjectionMaterializationMode,
            },
            pipeline::runtime::ExecutionAttemptKernel,
            plan_metrics::{record_plan_metrics, record_rows_scanned_for_path, set_rows_from_len},
            planning::preparation::slot_map_for_model_plan,
            read_data_row_with_consistency_from_store,
            route::{RoutePlanRequest, build_execution_route_plan},
            traversal::row_read_consistency_for_plan,
        },
        index::IndexCompilePolicy,
        predicate::MissingRowPolicy,
        query::plan::AccessPlannedQuery,
        registry::StoreHandle,
        response::{EntityResponse, Row},
        schema::commit_schema_fingerprint_for_entity,
    },
    error::InternalError,
    metrics::sink::{ExecKind, Span},
    traits::{CanisterKind, EntityKind, EntityValue, Storable},
    types::Id,
};
use std::collections::BTreeSet;

///
/// DeleteRow
/// Row wrapper used during delete planning and execution.
///

pub(in crate::db::executor) struct DeleteRow<E>
where
    E: EntityKind,
{
    pub(super) key: DataKey,
    pub(super) raw: Option<RawRow>,
    pub(super) entity: E,
}

impl<E: EntityKind> DeleteRow<E> {
    pub(in crate::db::executor) const fn entity_ref(&self) -> &E {
        &self.entity
    }
}

///
/// DeleteExecutionAuthority
///
/// Authority bundle for nongeneric delete planning and commit
/// preparation phases.
///

struct DeleteExecutionAuthority {
    entity: EntityAuthority,
    schema_fingerprint: CommitSchemaFingerprint,
}

impl DeleteExecutionAuthority {
    /// Lower one entity type into the authority used by delete execution.
    fn for_type<E>() -> Self
    where
        E: EntityKind,
    {
        Self {
            entity: EntityAuthority::for_type::<E>(),
            schema_fingerprint: commit_schema_fingerprint_for_entity::<E>(),
        }
    }
}

///
/// PreparedDeleteExecutionState
///
/// Generic-free delete execution payload after typed `PreparedExecutionPlan<E>` is
/// consumed into structural planner and compilation state.
///

struct PreparedDeleteExecutionState {
    authority: DeleteExecutionAuthority,
    logical_plan: AccessPlannedQuery,
    route_plan: ExecutionPlan,
    execution_preparation: ExecutionPreparation,
    index_prefix_specs: Vec<crate::db::access::LoweredIndexPrefixSpec>,
    index_range_specs: Vec<crate::db::access::LoweredIndexRangeSpec>,
}

impl PreparedDeleteExecutionState {
    /// Return row-read missing-row policy for this delete execution.
    const fn consistency(&self) -> MissingRowPolicy {
        row_read_consistency_for_plan(&self.logical_plan)
    }
}

/// Validate the plan-shape invariants shared by all delete executor entrypoints.
fn validate_delete_plan_shape<E>(plan: &PreparedExecutionPlan<E>) -> Result<(), InternalError>
where
    E: EntityKind,
{
    if plan.is_grouped() {
        return Err(InternalError::delete_executor_grouped_unsupported());
    }

    if !plan.mode().is_delete() {
        return Err(InternalError::delete_executor_delete_plan_required());
    }

    Ok(())
}

///
/// TypedDeleteLeaf
///
/// TypedDeleteLeaf carries one typed delete output after shared selection has
/// completed.
/// The generic output lets response-row and count-only callers share the same
/// rollback and commit-preparation path without duplicating row selection.
///

struct TypedDeleteLeaf<T> {
    output: T,
    row_count: usize,
    rollback_rows: Vec<(RawDataKey, RawRow)>,
}

///
/// DeleteProjection
///
/// Structural delete payload after row resolution, delete-only post-access
/// filtering, and commit-window apply.
/// Carries executor-materialized projection rows so adapter layers do not see
/// structural kernel row internals.
///

#[cfg(feature = "sql")]
pub(in crate::db) struct DeleteProjection {
    rows: MaterializedProjectionRows,
    row_count: u32,
}

#[cfg(feature = "sql")]
impl DeleteProjection {
    #[must_use]
    const fn new(rows: MaterializedProjectionRows, row_count: u32) -> Self {
        Self { rows, row_count }
    }

    #[must_use]
    pub(in crate::db) fn into_parts(self) -> (MaterializedProjectionRows, u32) {
        (self.rows, self.row_count)
    }
}

///
/// DeletePreparation
///
/// Structural delete leaf output carrying structural kernel rows plus the
/// rollback rows required by structural commit preparation.
///

#[cfg(feature = "sql")]
struct DeletePreparation {
    response_rows: Vec<KernelRow>,
    rollback_rows: Vec<(RawDataKey, RawRow)>,
}

///
/// PreparedDeleteCommit
///
/// Generic-free delete commit payload after structural relation validation and
/// rollback-row materialization.
///

struct PreparedDeleteCommit {
    row_ops: Vec<CommitRowOp>,
}

///
/// PreparedTypedDelete
///
/// PreparedTypedDelete pairs a caller-specific typed delete output with the
/// already assembled commit operations.
/// It is the typed equivalent of `PreparedDeleteProjection`, keeping commit
/// application out of the row-selection and packaging helpers.
///

struct PreparedTypedDelete<T> {
    output: T,
    commit: PreparedDeleteCommit,
    row_count: usize,
}

///
/// PreparedDeleteProjection
///
/// Structural delete payload paired with its already prepared delete
/// commit operations.
/// Keeps the heavy row-resolution and commit-preparation flow on one
/// nongeneric helper so the typed executor wrapper only handles context,
/// metrics, and final commit-window application.
///

#[cfg(feature = "sql")]
struct PreparedDeleteProjection {
    projection: DeleteProjection,
    commit: PreparedDeleteCommit,
}

#[cfg(feature = "sql")]
type DeleteCommitApplyFn<C> =
    fn(&Db<C>, EntityAuthority, Vec<CommitRowOp>, &'static str) -> Result<(), InternalError>;

// Prepare one generic-free delete execution state after the typed plan shell is consumed.
fn prepare_delete_execution_state(
    authority: DeleteExecutionAuthority,
    logical_plan: AccessPlannedQuery,
    index_prefix_specs: Vec<crate::db::access::LoweredIndexPrefixSpec>,
    index_range_specs: Vec<crate::db::access::LoweredIndexRangeSpec>,
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
fn prepare_delete_runtime<E>(
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
fn resolve_delete_candidate_rows(
    store: StoreHandle,
    prepared: &PreparedDeleteExecutionState,
) -> Result<Vec<DataRow>, InternalError> {
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
            prepared.index_prefix_specs.as_slice(),
            prepared.index_range_specs.as_slice(),
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
    collect_delete_rows_from_key_stream(store, resolved.key_stream_mut(), prepared.consistency())
}

// Materialize ordered delete rows from one structural key stream.
fn collect_delete_rows_from_key_stream<S>(
    store: StoreHandle,
    key_stream: &mut S,
    consistency: MissingRowPolicy,
) -> Result<Vec<DataRow>, InternalError>
where
    S: crate::db::executor::OrderedKeyStream + ?Sized,
{
    let mut rows = Vec::with_capacity(key_stream.exact_key_count_hint().unwrap_or(0));

    while let Some(key) = key_stream.next_key()? {
        if let Some(row) = read_data_row_with_consistency_from_store(store, &key, consistency)? {
            rows.push(row);
        }
    }

    Ok(rows)
}

// Apply the shared delete-only post-access contract once after the caller has
// chosen its row representation.
fn apply_delete_post_access_rows<R>(
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

// Decode typed delete candidates, apply the shared delete post-access flow,
// and then let the caller package the surviving rows.
fn prepare_typed_delete_leaf<E, T>(
    prepared: &PreparedDeleteExecutionState,
    data_rows: Vec<DataRow>,
    package_rows: impl FnOnce(Vec<DeleteRow<E>>) -> Result<T, InternalError>,
) -> Result<T, InternalError>
where
    E: PersistedRow + EntityValue,
{
    // Phase 1: decode structural access rows into typed delete candidates.
    let mut rows = data_rows
        .into_iter()
        .map(|row| {
            let (key, raw) = row;
            let (_, entity) = decode_raw_row_for_entity_key::<E>(&key, &raw)?;

            Ok(DeleteRow {
                key,
                raw: Some(raw),
                entity,
            })
        })
        .collect::<Result<Vec<DeleteRow<E>>, InternalError>>()?;

    // Phase 2: apply typed delete post-access filtering and ordering once.
    apply_delete_post_access_rows(prepared, &mut rows)?;

    // Phase 3: package the already-filtered typed delete rows for the caller.
    package_rows(rows)
}

// Decode structural delete rows, apply the shared delete post-access flow,
// and then let the caller package the surviving kernel rows.
#[cfg(feature = "sql")]
fn prepare_structural_delete_leaf<T>(
    prepared: &PreparedDeleteExecutionState,
    data_rows: Vec<DataRow>,
    package_rows: impl FnOnce(Vec<KernelRow>) -> Result<T, InternalError>,
) -> Result<T, InternalError> {
    // Phase 1: decode structural access rows directly into slot-indexed kernel rows.
    let row_layout = prepared.authority.entity.row_layout();
    let row_decoder = RowDecoder::structural();
    let mut rows = data_rows
        .into_iter()
        .map(|data_row| row_decoder.decode(&row_layout, data_row))
        .collect::<Result<Vec<KernelRow>, InternalError>>()?;

    // Phase 2: apply delete-only post-access semantics on the structural row shape.
    apply_delete_post_access_rows(prepared, &mut rows)?;

    // Phase 3: package the already-filtered structural delete rows for the caller.
    package_rows(rows)
}

// Package surviving typed delete rows into outward response rows plus
// rollback rows for commit preparation.
fn package_typed_delete_rows<E>(
    rows: Vec<DeleteRow<E>>,
) -> Result<TypedDeleteLeaf<Vec<Row<E>>>, InternalError>
where
    E: PersistedRow + EntityValue,
{
    let mut response_rows = Vec::with_capacity(rows.len());
    let mut rollback_rows = Vec::with_capacity(rows.len());

    for mut row in rows {
        let response_id = Id::from_key(row.key.try_key::<E>()?);
        let rollback_row = row
            .raw
            .take()
            .ok_or_else(InternalError::delete_rollback_row_required)?;
        let rollback_key = row.key.to_raw()?;

        response_rows.push(Row::new(response_id, row.entity));
        rollback_rows.push((rollback_key, rollback_row));
    }

    Ok(TypedDeleteLeaf {
        output: response_rows,
        row_count: rollback_rows.len(),
        rollback_rows,
    })
}

// Package surviving typed delete rows into rollback rows only when the caller
// needs the affected-row count without response-row materialization.
fn package_typed_delete_count<E>(
    rows: Vec<DeleteRow<E>>,
) -> Result<TypedDeleteLeaf<u32>, InternalError>
where
    E: PersistedRow + EntityValue,
{
    let row_count = rows.len();
    let mut rollback_rows = Vec::with_capacity(rows.len());

    for mut row in rows {
        let rollback_row = row
            .raw
            .take()
            .ok_or_else(InternalError::delete_rollback_row_required)?;
        let rollback_key = row.key.to_raw()?;

        rollback_rows.push((rollback_key, rollback_row));
    }

    Ok(TypedDeleteLeaf {
        output: u32::try_from(row_count).unwrap_or(u32::MAX),
        row_count,
        rollback_rows,
    })
}

// Resolve, filter, and package one typed delete result before the outer
// entrypoint applies the final commit window.
fn prepare_typed_delete_core<C, E, T>(
    db: &Db<C>,
    store: StoreHandle,
    prepared: &PreparedDeleteExecutionState,
    package_rows: impl FnOnce(Vec<DeleteRow<E>>) -> Result<TypedDeleteLeaf<T>, InternalError>,
) -> Result<Option<PreparedTypedDelete<T>>, InternalError>
where
    C: CanisterKind,
    E: PersistedRow + EntityValue,
{
    // Phase 1: resolve structural access rows once through the shared executor
    // key-stream seam and record the real candidate count for metrics.
    let data_rows = resolve_delete_candidate_rows(store, prepared)?;
    record_rows_scanned_for_path(prepared.authority.entity.entity_path(), data_rows.len());

    // Phase 2: run typed delete post-access selection and package the caller's
    // desired output shape alongside rollback rows.
    let typed = prepare_typed_delete_leaf(prepared, data_rows, package_rows)?;
    if typed.row_count == 0 {
        return Ok(None);
    }

    // Phase 3: prepare relation validation and commit row ops once for the
    // already-selected delete targets.
    let commit = prepare_delete_commit(db, store, &prepared.authority, typed.rollback_rows)?;

    Ok(Some(PreparedTypedDelete {
        output: typed.output,
        commit,
        row_count: typed.row_count,
    }))
}

// Package surviving structural delete kernel rows plus rollback rows for
// commit preparation.
#[cfg(feature = "sql")]
fn package_structural_delete_rows(
    rows: Vec<KernelRow>,
) -> Result<DeletePreparation, InternalError> {
    let mut response_rows = Vec::with_capacity(rows.len());
    let mut rollback_rows = Vec::with_capacity(rows.len());

    for row in rows {
        let (data_row, slots) = row.into_parts()?;
        let (key, raw) = data_row;
        let rollback_key = key.to_raw()?;

        response_rows.push(KernelRow::new((key, raw.clone()), slots));
        rollback_rows.push((rollback_key, raw));
    }

    Ok(DeletePreparation {
        response_rows,
        rollback_rows,
    })
}

// Prepare the nongeneric delete commit payload from structural rollback rows.
#[inline(never)]
fn prepare_delete_commit<C>(
    db: &Db<C>,
    _store: StoreHandle,
    authority: &DeleteExecutionAuthority,
    rollback_rows: Vec<(RawDataKey, RawRow)>,
) -> Result<PreparedDeleteCommit, InternalError>
where
    C: CanisterKind,
{
    // Phase 1: reject target deletes that are still strongly referenced.
    let deleted_target_keys = rollback_rows
        .iter()
        .map(|(raw_key, _)| *raw_key)
        .collect::<BTreeSet<_>>();
    db.validate_delete_strong_relations(authority.entity.entity_path(), &deleted_target_keys)?;

    // Phase 2: assemble mechanical delete commit row ops.
    let row_ops = rollback_rows
        .into_iter()
        .map(|(raw_key, raw_row)| {
            Ok(CommitRowOp::new(
                authority.entity.entity_path(),
                raw_key,
                Some(raw_row.into_bytes()),
                None,
                authority.schema_fingerprint,
            ))
        })
        .collect::<Result<Vec<_>, InternalError>>()?;

    Ok(PreparedDeleteCommit { row_ops })
}

// Resolve, filter, and package one structural delete result before the
// outer typed wrapper applies the final commit window.
#[cfg(feature = "sql")]
fn prepare_structural_delete_projection<C>(
    db: &Db<C>,
    store: StoreHandle,
    prepared: &PreparedDeleteExecutionState,
) -> Result<PreparedDeleteProjection, InternalError>
where
    C: CanisterKind,
{
    // Phase 1: resolve structural access rows once and record the scanned
    // count against the real authority path.
    let data_rows = resolve_delete_candidate_rows(store, prepared)?;
    record_rows_scanned_for_path(prepared.authority.entity.entity_path(), data_rows.len());

    // Phase 2: keep delete filtering, ordering, and rollback packaging on the
    // structural kernel-row boundary.
    let structural =
        prepare_structural_delete_leaf(prepared, data_rows, package_structural_delete_rows)?;
    if structural.response_rows.is_empty() {
        return Ok(PreparedDeleteProjection {
            projection: DeleteProjection::new(MaterializedProjectionRows::empty(), 0),
            commit: PreparedDeleteCommit {
                row_ops: Vec::new(),
            },
        });
    }

    // Phase 3: prepare the structural delete commit payload before the typed
    // wrapper enters the mechanical commit-window apply step.
    let commit = prepare_delete_commit(db, store, &prepared.authority, structural.rollback_rows)?;
    let row_count = u32::try_from(structural.response_rows.len()).unwrap_or(u32::MAX);
    let rows = MaterializedProjectionRows::from_kernel_rows(structural.response_rows)?;

    Ok(PreparedDeleteProjection {
        projection: DeleteProjection::new(rows, row_count),
        commit,
    })
}

// Execute one structural delete projection through the shared delete core
// while leaving only the final typed commit-window bridge to the caller.
#[cfg(feature = "sql")]
fn execute_structural_delete_projection_core<C>(
    db: &Db<C>,
    store: StoreHandle,
    prepared: &PreparedDeleteExecutionState,
    apply_delete_commit: DeleteCommitApplyFn<C>,
) -> Result<DeleteProjection, InternalError>
where
    C: CanisterKind,
{
    // Phase 1: run the shared structural delete projection core.
    let prepared_projection = prepare_structural_delete_projection(db, store, prepared)?;
    if prepared_projection.projection.row_count == 0 {
        return Ok(DeleteProjection::new(
            MaterializedProjectionRows::empty(),
            0,
        ));
    }

    // Phase 2: apply the already prepared delete commit payload through the
    // caller-provided commit-window bridge.
    apply_delete_commit(
        db,
        prepared.authority.entity,
        prepared_projection.commit.row_ops,
        "delete_row_apply",
    )?;

    Ok(prepared_projection.projection)
}

// Bridge the final delete commit apply through the existing typed fallback
// only at the wrapper edge so the structural delete core stays shared.
fn apply_delete_commit_window_for_type<E>(
    db: &Db<E::Canister>,
    authority: EntityAuthority,
    row_ops: Vec<CommitRowOp>,
    apply_phase: &'static str,
) -> Result<(), InternalError>
where
    E: EntityKind + EntityValue,
{
    if db.has_runtime_hooks() {
        commit_delete_row_ops_with_window_for_path(
            db,
            authority.entity_path(),
            row_ops,
            apply_phase,
        )
    } else {
        commit_delete_row_ops_with_window::<E>(db, row_ops, apply_phase)
    }
}

///
/// DeleteExecutor
///
/// Atomicity invariant:
/// All fallible validation and planning completes before the commit boundary.
/// After `begin_commit`, mutations are applied mechanically from a
/// prevalidated commit marker. Rollback exists as a safety net but is
/// not relied upon for correctness.
///

#[derive(Clone, Copy)]
pub(in crate::db) struct DeleteExecutor<E>
where
    E: PersistedRow,
{
    db: Db<E::Canister>,
}

impl<E> DeleteExecutor<E>
where
    E: PersistedRow + EntityValue,
{
    /// Construct one delete executor bound to a database handle.
    #[must_use]
    pub(in crate::db) const fn new(db: Db<E::Canister>) -> Self {
        Self { db }
    }

    // ─────────────────────────────────────────────
    // Plan-based delete
    // ─────────────────────────────────────────────

    /// Execute one delete plan and return deleted entities in response order.
    pub(in crate::db) fn execute(
        self,
        plan: PreparedExecutionPlan<E>,
    ) -> Result<EntityResponse<E>, InternalError> {
        (|| {
            // Phase 1: prepare authority, store access, and delete execution inputs once.
            let (prepared, store) = prepare_delete_runtime(&self.db, plan)?;
            let mut span = Span::<E>::new(ExecKind::Delete);
            record_plan_metrics(&prepared.logical_plan.access);

            // Phase 2: run the shared typed delete core and package response rows.
            let Some(typed) = prepare_typed_delete_core(
                &self.db,
                store,
                &prepared,
                package_typed_delete_rows::<E>,
            )?
            else {
                set_rows_from_len(&mut span, 0);
                return Ok(EntityResponse::new(Vec::new()));
            };

            // Phase 3: apply the already prepared delete commit payload.
            apply_delete_commit_window_for_type::<E>(
                &self.db,
                prepared.authority.entity,
                typed.commit.row_ops,
                "delete_row_apply",
            )?;

            // Phase 4: return the already-prepared typed delete response rows.
            set_rows_from_len(&mut span, typed.row_count);

            Ok(EntityResponse::new(typed.output))
        })()
    }

    /// Execute one structural delete projection plan and return structural row
    /// values for one outer projection/rendering surface.
    #[cfg(feature = "sql")]
    pub(in crate::db) fn execute_structural_projection(
        self,
        plan: PreparedExecutionPlan<E>,
    ) -> Result<DeleteProjection, InternalError> {
        (|| {
            // Phase 1: prepare authority, store access, and delete execution inputs once.
            let (prepared, store) = prepare_delete_runtime(&self.db, plan)?;
            let mut span = Span::<E>::new(ExecKind::Delete);
            record_plan_metrics(&prepared.logical_plan.access);

            // Phase 2: run the shared structural delete core and apply the
            // final typed commit-window bridge only at the boundary.
            let projection = execute_structural_delete_projection_core(
                &self.db,
                store,
                &prepared,
                apply_delete_commit_window_for_type::<E>,
            )?;
            if projection.row_count == 0 {
                set_rows_from_len(&mut span, 0);
                return Ok(DeleteProjection::new(
                    MaterializedProjectionRows::empty(),
                    0,
                ));
            }

            // Phase 3: return the already prepared structural delete projection.
            set_rows_from_len(
                &mut span,
                usize::try_from(projection.row_count).unwrap_or(usize::MAX),
            );

            Ok(projection)
        })()
    }

    /// Execute one delete plan and return only the affected-row count.
    pub(in crate::db) fn execute_count(
        self,
        plan: PreparedExecutionPlan<E>,
    ) -> Result<u32, InternalError> {
        (|| {
            // Phase 1: prepare authority, store access, and delete execution inputs once.
            let (prepared, store) = prepare_delete_runtime(&self.db, plan)?;
            let mut span = Span::<E>::new(ExecKind::Delete);
            record_plan_metrics(&prepared.logical_plan.access);

            // Phase 2: run the shared typed delete core while skipping response
            // row materialization.
            let Some(counted) = prepare_typed_delete_core(
                &self.db,
                store,
                &prepared,
                package_typed_delete_count::<E>,
            )?
            else {
                set_rows_from_len(&mut span, 0);
                return Ok(0);
            };

            // Phase 3: apply the already prepared delete commit payload.
            apply_delete_commit_window_for_type::<E>(
                &self.db,
                prepared.authority.entity,
                counted.commit.row_ops,
                "delete_row_apply",
            )?;

            // Phase 4: return only the final affected-row count.
            set_rows_from_len(&mut span, counted.row_count);

            Ok(counted.output)
        })()
    }
}
