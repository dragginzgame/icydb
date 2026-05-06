//! Module: executor::delete::types
//! Responsibility: delete executor state, commit payload, and output DTOs.
//! Does not own: row resolution, post-access filtering, or commit application.
//! Boundary: shared contracts used by delete core and executor entrypoints.

#[cfg(feature = "sql")]
use crate::db::executor::projection::MaterializedProjectionRows;
use crate::{
    db::{
        commit::{CommitRowOp, CommitSchemaFingerprint},
        data::{DataKey, RawDataKey, RawRow},
        executor::{
            EntityAuthority, ExecutionPlan, ExecutionPreparation, PreparedExecutionPlan,
            saturating_u32_len, traversal::row_read_consistency_for_plan,
        },
        predicate::MissingRowPolicy,
        query::plan::AccessPlannedQuery,
        schema::commit_schema_fingerprint_for_entity,
    },
    error::InternalError,
    traits::EntityKind,
};
use std::sync::Arc;

///
/// DeleteRow
///
/// Row wrapper used during delete planning and execution.
///

pub(in crate::db::executor) struct DeleteRow<E>
where
    E: EntityKind,
{
    pub(in crate::db::executor::delete) key: DataKey,
    pub(in crate::db::executor::delete) raw: Option<RawRow>,
    pub(in crate::db::executor::delete) entity: E,
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

pub(in crate::db::executor::delete) struct DeleteExecutionAuthority {
    pub(in crate::db::executor::delete) entity: EntityAuthority,
    pub(in crate::db::executor::delete) schema_fingerprint: CommitSchemaFingerprint,
}

impl DeleteExecutionAuthority {
    /// Preserve one prepared-plan entity authority for delete execution.
    ///
    /// Accepted-schema delete plans carry a frozen row layout in the authority.
    /// Delete must keep that layout when decoding old physical rows and when
    /// staging rollback bytes for commit preflight.
    pub(in crate::db::executor::delete) fn from_entity_authority<E>(entity: EntityAuthority) -> Self
    where
        E: EntityKind,
    {
        Self {
            entity,
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

pub(in crate::db::executor::delete) struct PreparedDeleteExecutionState {
    pub(in crate::db::executor::delete) authority: DeleteExecutionAuthority,
    pub(in crate::db::executor::delete) logical_plan: Arc<AccessPlannedQuery>,
    pub(in crate::db::executor::delete) route_plan: ExecutionPlan,
    pub(in crate::db::executor::delete) execution_preparation: ExecutionPreparation,
    pub(in crate::db::executor::delete) index_prefix_specs:
        Arc<[crate::db::access::LoweredIndexPrefixSpec]>,
    pub(in crate::db::executor::delete) index_range_specs:
        Arc<[crate::db::access::LoweredIndexRangeSpec]>,
}

impl PreparedDeleteExecutionState {
    /// Return row-read missing-row policy for this delete execution.
    pub(in crate::db::executor::delete) fn consistency(&self) -> MissingRowPolicy {
        row_read_consistency_for_plan(&self.logical_plan)
    }
}

/// Validate the plan-shape invariants shared by all delete executor entrypoints.
pub(in crate::db::executor::delete) fn validate_delete_plan_shape<E>(
    plan: &PreparedExecutionPlan<E>,
) -> Result<(), InternalError>
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

pub(in crate::db::executor::delete) struct TypedDeleteLeaf<T> {
    pub(in crate::db::executor::delete) output: T,
    pub(in crate::db::executor::delete) row_count: usize,
    pub(in crate::db::executor::delete) rollback_rows: Vec<(RawDataKey, RawRow)>,
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
}

#[cfg(feature = "sql")]
impl DeleteProjection {
    #[must_use]
    pub(in crate::db::executor::delete) const fn new(rows: MaterializedProjectionRows) -> Self {
        Self { rows }
    }

    #[must_use]
    pub(in crate::db::executor::delete) fn row_count(&self) -> u32 {
        saturating_u32_len(self.rows.len())
    }

    #[must_use]
    pub(in crate::db) fn into_parts(self) -> (MaterializedProjectionRows, u32) {
        let row_count = self.row_count();

        (self.rows, row_count)
    }
}

///
/// DeletePreparation
///
/// Structural delete leaf output carrying already materialized projection rows
/// plus the rollback rows required by structural commit preparation.
///

#[cfg(feature = "sql")]
pub(in crate::db::executor::delete) struct DeletePreparation {
    pub(in crate::db::executor::delete) response_rows: MaterializedProjectionRows,
    pub(in crate::db::executor::delete) rollback_rows: Vec<(RawDataKey, RawRow)>,
}

///
/// DeleteCountPreparation
///
/// Structural delete-count payload carrying only the affected-row count plus
/// rollback rows for commit preparation.
/// This keeps count-only deletes on the accepted structural row-decode path
/// without materializing typed response entities or SQL projection rows.
///

pub(in crate::db::executor::delete) struct DeleteCountPreparation {
    pub(in crate::db::executor::delete) row_count: usize,
    pub(in crate::db::executor::delete) rollback_rows: Vec<(RawDataKey, RawRow)>,
}

///
/// PreparedDeleteCommit
///
/// Generic-free delete commit payload after structural relation validation and
/// rollback-row materialization.
///

pub(in crate::db::executor::delete) struct PreparedDeleteCommit {
    pub(in crate::db::executor::delete) row_ops: Vec<CommitRowOp>,
}

///
/// PreparedTypedDelete
///
/// PreparedTypedDelete pairs a caller-specific typed delete output with the
/// already assembled commit operations.
/// It is the typed equivalent of `PreparedDeleteProjection`, keeping commit
/// application out of the row-selection and packaging helpers.
///

pub(in crate::db::executor::delete) struct PreparedTypedDelete<T> {
    pub(in crate::db::executor::delete) output: T,
    pub(in crate::db::executor::delete) commit: PreparedDeleteCommit,
    pub(in crate::db::executor::delete) row_count: usize,
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
pub(in crate::db::executor::delete) struct PreparedDeleteProjection {
    pub(in crate::db::executor::delete) projection: DeleteProjection,
    pub(in crate::db::executor::delete) commit: PreparedDeleteCommit,
}

#[cfg(feature = "sql")]
pub(in crate::db::executor::delete) type DeleteCommitApplyFn<C> = fn(
    &crate::db::Db<C>,
    EntityAuthority,
    Vec<CommitRowOp>,
    &'static str,
) -> Result<(), InternalError>;
