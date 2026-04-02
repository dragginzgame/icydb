//! Module: executor::delete
//! Responsibility: delete-plan execution and commit-window handoff.
//! Does not own: logical planning, relation semantics, or cursor protocol details.
//! Boundary: delete-specific preflight/decode/apply flow over executable plans.

#[cfg(feature = "sql")]
use crate::db::executor::terminal::{KernelRow, RowDecoder, RowLayout};
#[cfg(feature = "sql")]
use crate::db::schema::commit_schema_fingerprint_for_model;
use crate::{
    db::{
        Db,
        commit::{CommitRowOp, CommitSchemaFingerprint},
        data::{
            DataKey, DataRow, PersistedEntityRow, PersistedRow, RawDataKey, RawRow,
            decode_raw_row_for_entity_key,
        },
        executor::{
            AccessScanContinuationInput, EntityAuthority, ExecutableAccess, ExecutablePlan,
            ExecutionKernel, ExecutionPreparation, KeyStreamLoopControl, TraversalRuntime,
            drive_key_stream_with_control_flow,
            mutation::{
                commit_delete_row_ops_with_window, commit_delete_row_ops_with_window_for_path,
                mutation_write_context, preflight_mutation_plan_for_authority,
            },
            plan_metrics::{record_plan_metrics, record_rows_scanned_for_path, set_rows_from_len},
            preparation::slot_map_for_model_plan,
            read_data_row_with_consistency_from_store,
            traversal::row_read_consistency_for_plan,
        },
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
/// Generic-free delete execution payload after typed `ExecutablePlan<E>` is
/// consumed into structural planner and compilation state.
///

struct PreparedDeleteExecutionState {
    authority: DeleteExecutionAuthority,
    logical_plan: AccessPlannedQuery,
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

///
/// TypedDeletePreparation
///
/// Typed delete leaf output containing only the entity-shaped response rows
/// plus rollback rows needed by structural commit preparation.
///

struct TypedDeletePreparation<E>
where
    E: EntityKind,
{
    response_rows: Vec<Row<E>>,
    rollback_rows: Vec<(RawDataKey, RawRow)>,
}

///
/// DeleteCountPreparation
///
/// Delete leaf output for count-only execution.
/// Keeps rollback rows for commit preparation while avoiding typed response-row
/// materialization when the caller only needs the affected-row count.
///

struct DeleteCountPreparation {
    row_count: u32,
    rollback_rows: Vec<(RawDataKey, RawRow)>,
}

///
/// DeleteProjection
///
/// Structural SQL delete payload after row resolution, delete-only post-access
/// filtering, and commit-window apply.
/// Keeps SQL DELETE result rendering on structural kernel rows so the executor
/// stays on the slot-based runtime boundary.
///

#[cfg(feature = "sql")]
pub(in crate::db) struct DeleteProjection {
    rows: Vec<KernelRow>,
    row_count: u32,
}

#[cfg(feature = "sql")]
impl DeleteProjection {
    #[must_use]
    const fn new(rows: Vec<KernelRow>, row_count: u32) -> Self {
        Self { rows, row_count }
    }

    #[must_use]
    pub(in crate::db) fn into_parts(self) -> (Vec<KernelRow>, u32) {
        (self.rows, self.row_count)
    }
}

///
/// DeletePreparation
///
/// Structural delete leaf output carrying structural SQL kernel rows plus the
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
/// PreparedDeleteSqlProjection
///
/// Structural SQL delete payload paired with its already prepared delete
/// commit operations.
/// Keeps the heavy row-resolution and commit-preparation flow on one
/// nongeneric helper so the typed executor wrapper only handles context,
/// metrics, and final commit-window application.
///

#[cfg(feature = "sql")]
struct PreparedDeleteSqlProjection {
    projection: DeleteProjection,
    commit: PreparedDeleteCommit,
}

#[cfg(feature = "sql")]
type DeleteCommitApplyFn<C> =
    fn(&Db<C>, EntityAuthority, Vec<CommitRowOp>, &'static str) -> Result<(), InternalError>;

/// Decode raw access rows into typed delete rows with key/entity checks.
pub(super) fn decode_rows<E: PersistedRow + EntityValue>(
    rows: Vec<DataRow>,
) -> Result<Vec<DeleteRow<E>>, InternalError> {
    rows.into_iter()
        .map(|row| {
            let row = PersistedEntityRow::from_data_row(row);
            let (key, raw) = row.into_parts();
            let (_, entity) = decode_raw_row_for_entity_key::<E>(&key, &raw)?;

            Ok(DeleteRow {
                key,
                raw: Some(raw),
                entity,
            })
        })
        .collect()
}

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
    let execution_preparation = ExecutionPreparation::from_plan(
        authority.entity.model(),
        &logical_plan,
        slot_map_for_model_plan(authority.entity.model(), &logical_plan),
    );

    Ok(PreparedDeleteExecutionState {
        authority,
        logical_plan,
        execution_preparation,
        index_prefix_specs,
        index_range_specs,
    })
}

// Resolve structural access rows for one delete execution without carrying `Context<E>`.
fn resolve_delete_candidate_rows(
    store: StoreHandle,
    prepared: &PreparedDeleteExecutionState,
) -> Result<Vec<DataRow>, InternalError> {
    // Phase 1: resolve the structural access plan into one ordered key stream.
    let runtime = TraversalRuntime::new(store, prepared.authority.entity.entity_tag());
    let bindings = crate::db::executor::AccessStreamBindings::new(
        prepared.index_prefix_specs.as_slice(),
        prepared.index_range_specs.as_slice(),
        AccessScanContinuationInput::initial_asc(),
    );
    let executable_access =
        ExecutableAccess::new(&prepared.logical_plan.access, bindings, None, None);
    let mut key_stream = runtime.ordered_key_stream_from_runtime_access(executable_access)?;

    // Phase 2: materialize rows through the structural consistency boundary.
    collect_delete_rows_from_key_stream(store, key_stream.as_mut(), prepared.consistency())
}

// Materialize ordered delete rows from one structural key stream.
fn collect_delete_rows_from_key_stream(
    store: StoreHandle,
    key_stream: &mut dyn crate::db::executor::OrderedKeyStream,
    consistency: MissingRowPolicy,
) -> Result<Vec<DataRow>, InternalError> {
    let mut rows = Vec::with_capacity(key_stream.exact_key_count_hint().unwrap_or(0));

    drive_key_stream_with_control_flow(
        key_stream,
        &mut || KeyStreamLoopControl::Emit,
        &mut |key| {
            if let Some(row) = read_data_row_with_consistency_from_store(store, &key, consistency)?
            {
                rows.push(row);
            }

            Ok(KeyStreamLoopControl::Emit)
        },
    )?;

    Ok(rows)
}

// Decode, filter, and format typed delete rows while returning structural rollback data.
fn prepare_typed_delete_rows<E>(
    prepared: &PreparedDeleteExecutionState,
    data_rows: Vec<DataRow>,
) -> Result<TypedDeletePreparation<E>, InternalError>
where
    E: PersistedRow + EntityValue,
{
    // Phase 1: decode structural access rows into typed delete candidates.
    let mut rows = decode_rows::<E>(data_rows)?;

    // Phase 2: apply typed delete post-access filtering and ordering.
    let stats = ExecutionKernel::apply_delete_post_access_with_compiled_predicate(
        prepared.authority.entity.model(),
        &prepared.logical_plan,
        &mut rows,
        prepared.execution_preparation.compiled_predicate(),
    )?;
    let _ = stats.delete_was_limited;
    let _ = stats.rows_after_cursor;

    // Phase 3: package typed responses and structural rollback rows separately.
    let mut response_rows = Vec::with_capacity(rows.len());
    let mut rollback_rows = Vec::with_capacity(rows.len());
    for mut row in rows {
        let response_id = Id::from_key(row.key.try_key::<E>()?);
        let rollback_row = row.raw.take().ok_or_else(|| {
            InternalError::store_internal("missing raw row for delete rollback".to_string())
        })?;
        let rollback_key = row.key.to_raw()?;

        response_rows.push(Row::new(response_id, row.entity));
        rollback_rows.push((rollback_key, rollback_row));
    }

    Ok(TypedDeletePreparation {
        response_rows,
        rollback_rows,
    })
}

// Decode, filter, and package only rollback rows when the caller needs delete
// mutation effects without typed response-row materialization.
fn prepare_typed_delete_count<E>(
    prepared: &PreparedDeleteExecutionState,
    data_rows: Vec<DataRow>,
) -> Result<DeleteCountPreparation, InternalError>
where
    E: PersistedRow + EntityValue,
{
    // Phase 1: decode structural access rows into typed delete candidates.
    let mut rows = decode_rows::<E>(data_rows)?;

    // Phase 2: apply typed delete post-access filtering and ordering.
    let stats = ExecutionKernel::apply_delete_post_access_with_compiled_predicate(
        prepared.authority.entity.model(),
        &prepared.logical_plan,
        &mut rows,
        prepared.execution_preparation.compiled_predicate(),
    )?;
    let _ = stats.delete_was_limited;
    let _ = stats.rows_after_cursor;

    // Phase 3: retain only rollback rows and the final affected-row count.
    let row_count = u32::try_from(rows.len()).unwrap_or(u32::MAX);
    let mut rollback_rows = Vec::with_capacity(rows.len());
    for mut row in rows {
        let rollback_row = row.raw.take().ok_or_else(|| {
            InternalError::store_internal("missing raw row for delete rollback".to_string())
        })?;
        let rollback_key = row.key.to_raw()?;

        rollback_rows.push((rollback_key, rollback_row));
    }

    Ok(DeleteCountPreparation {
        row_count,
        rollback_rows,
    })
}

// Decode, filter, and package structural delete rows for SQL projection payloads.
#[cfg(feature = "sql")]
fn prepare_structural_delete_rows(
    prepared: &PreparedDeleteExecutionState,
    data_rows: Vec<DataRow>,
) -> Result<DeletePreparation, InternalError> {
    // Phase 1: decode structural access rows directly into slot-indexed kernel rows.
    let row_layout = RowLayout::from_model(prepared.authority.entity.model());
    let row_decoder = RowDecoder::structural();
    let mut rows = data_rows
        .into_iter()
        .map(|data_row| row_decoder.decode(&row_layout, data_row))
        .collect::<Result<Vec<KernelRow>, InternalError>>()?;

    // Phase 2: apply delete-only post-access semantics on the structural row shape.
    let stats = ExecutionKernel::apply_delete_post_access_with_compiled_predicate(
        prepared.authority.entity.model(),
        &prepared.logical_plan,
        &mut rows,
        prepared.execution_preparation.compiled_predicate(),
    )?;
    let _ = stats.delete_was_limited;
    let _ = stats.rows_after_cursor;

    // Phase 3: package kernel rows and rollback rows from the same payload.
    let mut response_rows = Vec::with_capacity(rows.len());
    let mut rollback_rows = Vec::with_capacity(rows.len());
    for row in rows {
        let (data_row, slots) = row.into_parts();
        let (key, raw) = PersistedEntityRow::from_data_row(data_row).into_parts();
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

// Resolve, filter, and package one structural SQL delete result before the
// outer typed wrapper applies the final commit window.
#[cfg(feature = "sql")]
fn prepare_structural_delete_sql_projection<C>(
    db: &Db<C>,
    store: StoreHandle,
    prepared: &PreparedDeleteExecutionState,
) -> Result<PreparedDeleteSqlProjection, InternalError>
where
    C: CanisterKind,
{
    // Phase 1: resolve structural access rows once and record the scanned
    // count against the real authority path.
    let data_rows = resolve_delete_candidate_rows(store, prepared)?;
    record_rows_scanned_for_path(prepared.authority.entity.entity_path(), data_rows.len());

    // Phase 2: keep SQL delete filtering, ordering, and rollback packaging on
    // the structural kernel-row boundary.
    let structural = prepare_structural_delete_rows(prepared, data_rows)?;
    if structural.response_rows.is_empty() {
        return Ok(PreparedDeleteSqlProjection {
            projection: DeleteProjection::new(Vec::new(), 0),
            commit: PreparedDeleteCommit {
                row_ops: Vec::new(),
            },
        });
    }

    // Phase 3: prepare the structural delete commit payload before the typed
    // wrapper enters the mechanical commit-window apply step.
    let commit = prepare_delete_commit(db, store, &prepared.authority, structural.rollback_rows)?;
    let row_count = u32::try_from(structural.response_rows.len()).unwrap_or(u32::MAX);

    Ok(PreparedDeleteSqlProjection {
        projection: DeleteProjection::new(structural.response_rows, row_count),
        commit,
    })
}

// Execute one structural SQL delete projection through the shared delete core
// while leaving only the final typed commit-window bridge to the caller.
#[cfg(feature = "sql")]
fn execute_sql_projection_core<C>(
    db: &Db<C>,
    store: StoreHandle,
    prepared: &PreparedDeleteExecutionState,
    apply_delete_commit: DeleteCommitApplyFn<C>,
) -> Result<DeleteProjection, InternalError>
where
    C: CanisterKind,
{
    // Phase 1: run the shared structural delete projection core.
    let prepared_projection = prepare_structural_delete_sql_projection(db, store, prepared)?;
    if prepared_projection.projection.row_count == 0 {
        return Ok(DeleteProjection::new(Vec::new(), 0));
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
// only at the wrapper edge so the structural SQL delete core stays shared.
#[cfg(feature = "sql")]
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

// Prepare one structural delete execution state from runtime-hook authority
// once the typed SQL dispatch shell has already resolved the concrete entity.
#[cfg(feature = "sql")]
fn prepare_delete_execution_state_for_runtime_hooks<C>(
    db: &Db<C>,
    authority: EntityAuthority,
    logical_plan: AccessPlannedQuery,
) -> Result<PreparedDeleteExecutionState, InternalError>
where
    C: CanisterKind,
{
    let hooks = db.runtime_hook_for_entity_path(authority.entity_path())?;
    let authority = DeleteExecutionAuthority {
        entity: authority,
        schema_fingerprint: commit_schema_fingerprint_for_model(hooks.entity_path, hooks.model),
    };
    let index_prefix_specs = crate::db::access::lower_index_prefix_specs(
        authority.entity.entity_tag(),
        &logical_plan.access,
    )?;
    let index_range_specs = crate::db::access::lower_index_range_specs(
        authority.entity.entity_tag(),
        &logical_plan.access,
    )?;

    prepare_delete_execution_state(
        authority,
        logical_plan,
        index_prefix_specs,
        index_range_specs,
    )
}

// Apply delete commit ops through the structural runtime-hook commit window.
#[cfg(feature = "sql")]
fn apply_delete_commit_window_for_path<C>(
    db: &Db<C>,
    authority: EntityAuthority,
    row_ops: Vec<CommitRowOp>,
    apply_phase: &'static str,
) -> Result<(), InternalError>
where
    C: CanisterKind,
{
    commit_delete_row_ops_with_window_for_path(db, authority.entity_path(), row_ops, apply_phase)
}

/// Execute one structural SQL delete plan for canister SQL dispatch.
///
/// This keeps lowered SQL DELETE routing on resolved authority once the
/// entity path has already been resolved by the canister dispatch surface.
#[inline(never)]
#[cfg(feature = "sql")]
pub(in crate::db) fn execute_sql_delete_projection_for_canister<C>(
    db: &Db<C>,
    authority: EntityAuthority,
    plan: AccessPlannedQuery,
) -> Result<DeleteProjection, InternalError>
where
    C: CanisterKind,
{
    // Phase 1: lower structural delete authority and reusable execution state
    // from the runtime hook table once the route has been fixed.
    let prepared = prepare_delete_execution_state_for_runtime_hooks(db, authority, plan)?;
    let store =
        db.with_store_registry(|reg| reg.try_get_store(prepared.authority.entity.store_path()))?;

    // Phase 2: execute the shared structural SQL delete core and commit
    // through the runtime-hook path-only bridge.
    execute_sql_projection_core(
        db,
        store,
        &prepared,
        apply_delete_commit_window_for_path::<C>,
    )
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
    pub(in crate::db) const fn new(db: Db<E::Canister>, _debug: bool) -> Self {
        Self { db }
    }

    // ─────────────────────────────────────────────
    // Plan-based delete
    // ─────────────────────────────────────────────

    /// Execute one delete plan and return deleted entities in response order.
    pub(in crate::db) fn execute(
        self,
        plan: ExecutablePlan<E>,
    ) -> Result<EntityResponse<E>, InternalError> {
        // Phase 1: enforce delete entrypoint plan-shape invariants immediately.
        if plan.is_grouped() {
            return Err(InternalError::delete_executor_grouped_unsupported());
        }

        if !plan.mode().is_delete() {
            return Err(InternalError::delete_executor_delete_plan_required());
        }
        (|| {
            // Phase 2: prepare authority and delete execution inputs once.
            let authority = DeleteExecutionAuthority::for_type::<E>();
            let index_prefix_specs = plan.index_prefix_specs()?.to_vec();
            let index_range_specs = plan.index_range_specs()?.to_vec();
            let logical_plan = plan.into_plan();
            let prepared = prepare_delete_execution_state(
                authority,
                logical_plan,
                index_prefix_specs,
                index_range_specs,
            )?;
            let ctx = mutation_write_context::<E>(&self.db)?;
            let store = ctx.structural_store()?;

            let mut span = Span::<E>::new(ExecKind::Delete);
            record_plan_metrics(&prepared.logical_plan.access);

            // Phase 3: resolve structural access rows before typed delete semantics run.
            let data_rows = resolve_delete_candidate_rows(store, &prepared)?;
            record_rows_scanned_for_path(prepared.authority.entity.entity_path(), data_rows.len());

            // Phase 4: run the typed delete leaf and package structural rollback rows.
            let typed = prepare_typed_delete_rows::<E>(&prepared, data_rows)?;
            if typed.response_rows.is_empty() {
                set_rows_from_len(&mut span, 0);
                return Ok(EntityResponse::new(Vec::new()));
            }

            // Phase 5: keep relation validation and commit assembly on the structural path.
            let commit =
                prepare_delete_commit(&self.db, store, &prepared.authority, typed.rollback_rows)?;
            if self.db.has_runtime_hooks() {
                commit_delete_row_ops_with_window_for_path(
                    &self.db,
                    prepared.authority.entity.entity_path(),
                    commit.row_ops,
                    "delete_row_apply",
                )?;
            } else {
                commit_delete_row_ops_with_window::<E>(
                    &self.db,
                    commit.row_ops,
                    "delete_row_apply",
                )?;
            }

            // Phase 6: return the already-prepared typed delete response rows.
            set_rows_from_len(&mut span, typed.response_rows.len());

            Ok(EntityResponse::new(typed.response_rows))
        })()
    }

    /// Execute one delete plan and return structural row values for SQL projection rendering.
    #[cfg(feature = "sql")]
    pub(in crate::db) fn execute_sql_projection(
        self,
        plan: ExecutablePlan<E>,
    ) -> Result<DeleteProjection, InternalError> {
        // Phase 1: enforce delete entrypoint plan-shape invariants immediately.
        if plan.is_grouped() {
            return Err(InternalError::delete_executor_grouped_unsupported());
        }

        if !plan.mode().is_delete() {
            return Err(InternalError::delete_executor_delete_plan_required());
        }

        (|| {
            // Phase 2: prepare authority and delete execution inputs once.
            let authority = DeleteExecutionAuthority::for_type::<E>();
            let index_prefix_specs = plan.index_prefix_specs()?.to_vec();
            let index_range_specs = plan.index_range_specs()?.to_vec();
            let logical_plan = plan.into_plan();
            let prepared = prepare_delete_execution_state(
                authority,
                logical_plan,
                index_prefix_specs,
                index_range_specs,
            )?;
            let ctx = mutation_write_context::<E>(&self.db)?;
            let store = ctx.structural_store()?;

            let mut span = Span::<E>::new(ExecKind::Delete);
            record_plan_metrics(&prepared.logical_plan.access);

            // Phase 3: run the shared structural SQL delete core and apply the
            // final typed commit-window bridge only at the boundary.
            let projection = execute_sql_projection_core(
                &self.db,
                store,
                &prepared,
                apply_delete_commit_window_for_type::<E>,
            )?;
            if projection.row_count == 0 {
                set_rows_from_len(&mut span, 0);
                return Ok(DeleteProjection::new(Vec::new(), 0));
            }

            // Phase 4: return the already prepared structural SQL projection.
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
        plan: ExecutablePlan<E>,
    ) -> Result<u32, InternalError> {
        // Phase 1: enforce delete entrypoint plan-shape invariants immediately.
        if plan.is_grouped() {
            return Err(InternalError::delete_executor_grouped_unsupported());
        }

        if !plan.mode().is_delete() {
            return Err(InternalError::delete_executor_delete_plan_required());
        }

        (|| {
            // Phase 2: prepare authority and delete execution inputs once.
            let authority = DeleteExecutionAuthority::for_type::<E>();
            let index_prefix_specs = plan.index_prefix_specs()?.to_vec();
            let index_range_specs = plan.index_range_specs()?.to_vec();
            let logical_plan = plan.into_plan();
            let prepared = prepare_delete_execution_state(
                authority,
                logical_plan,
                index_prefix_specs,
                index_range_specs,
            )?;
            let ctx = mutation_write_context::<E>(&self.db)?;
            let store = ctx.structural_store()?;

            let mut span = Span::<E>::new(ExecKind::Delete);
            record_plan_metrics(&prepared.logical_plan.access);

            // Phase 3: resolve structural access rows before typed delete semantics run.
            let data_rows = resolve_delete_candidate_rows(store, &prepared)?;
            record_rows_scanned_for_path(prepared.authority.entity.entity_path(), data_rows.len());

            // Phase 4: keep relation validation and commit assembly while skipping
            // typed response-row materialization.
            let counted = prepare_typed_delete_count::<E>(&prepared, data_rows)?;
            if counted.row_count == 0 {
                set_rows_from_len(&mut span, 0);
                return Ok(0);
            }

            let commit =
                prepare_delete_commit(&self.db, store, &prepared.authority, counted.rollback_rows)?;
            if self.db.has_runtime_hooks() {
                commit_delete_row_ops_with_window_for_path(
                    &self.db,
                    prepared.authority.entity.entity_path(),
                    commit.row_ops,
                    "delete_row_apply",
                )?;
            } else {
                commit_delete_row_ops_with_window::<E>(
                    &self.db,
                    commit.row_ops,
                    "delete_row_apply",
                )?;
            }

            // Phase 5: return only the final affected-row count.
            set_rows_from_len(
                &mut span,
                usize::try_from(counted.row_count).unwrap_or(usize::MAX),
            );

            Ok(counted.row_count)
        })()
    }
}
