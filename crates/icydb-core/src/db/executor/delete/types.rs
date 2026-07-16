//! Module: executor::delete::types
//! Responsibility: delete executor state, commit payload, and output DTOs.
//! Does not own: row resolution, post-access filtering, or commit application.
//! Boundary: shared contracts used by delete core and executor entrypoints.

use crate::{
    db::{
        commit::{CommitRowOp, CommitSchemaFingerprint},
        data::{DecodedDataStoreKey, RawDataStoreKey, RawRow},
        executor::{
            EntityAuthority, ExecutionPreparation, ExecutionRoutePlan, PreparedExecutionPlan,
            traversal::row_read_consistency_for_plan,
        },
        predicate::MissingRowPolicy,
        query::plan::AccessPlannedQuery,
    },
    entity::EntityKind,
    error::InternalError,
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
    pub(in crate::db::executor::delete) key: DecodedDataStoreKey,
    pub(in crate::db::executor::delete) raw: Option<RawRow>,
    pub(in crate::db::executor::delete) entity: E,
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
    pub(in crate::db::executor::delete) const fn from_entity_authority(
        entity: EntityAuthority,
        schema_fingerprint: CommitSchemaFingerprint,
    ) -> Self {
        Self {
            entity,
            schema_fingerprint,
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
    pub(in crate::db::executor::delete) route_plan: ExecutionRoutePlan,
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
/// DeleteLeaf
///
/// DeleteLeaf carries one caller-specific delete output after shared selection
/// has completed.
/// The generic output lets typed, structural projection, and count-only
/// callers share the same rollback and commit-preparation path without
/// duplicating row selection.
///

pub(in crate::db::executor::delete) struct DeleteLeaf<T> {
    pub(in crate::db::executor::delete) output: T,
    pub(in crate::db::executor::delete) row_count: usize,
    pub(in crate::db::executor::delete) rollback_rows: Vec<(RawDataStoreKey, RawRow)>,
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
/// PreparedDeleteOutput
///
/// PreparedDeleteOutput pairs a caller-specific delete output with the already
/// assembled commit operations.
/// This keeps commit application out of typed and structural row-selection and
/// packaging helpers.
///

pub(in crate::db::executor::delete) struct PreparedDeleteOutput<T> {
    pub(in crate::db::executor::delete) output: T,
    pub(in crate::db::executor::delete) commit: PreparedDeleteCommit,
    pub(in crate::db::executor::delete) row_count: usize,
}
