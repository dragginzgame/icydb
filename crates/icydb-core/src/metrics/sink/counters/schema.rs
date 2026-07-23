//! Schema reconciliation and transition counter mutation helpers.
//! Does not own schema policy or metrics event dispatch.

use crate::metrics::{
    sink::{SchemaReconcileOutcome, SchemaTransitionOutcome},
    state as metrics,
};

// Schema reconciliation is a startup/metadata trust boundary, not normal query
// execution. Count the check plus the stable outcome bucket so operators can
// distinguish expected first-contact writes from fail-closed drift.
#[remain::check]
pub(in crate::metrics::sink) const fn record_global_schema_reconcile_outcome(
    ops: &mut metrics::EventOps,
    outcome: SchemaReconcileOutcome,
) {
    ops.schema_reconcile_checks = ops.schema_reconcile_checks.saturating_add(1);

    #[remain::sorted]
    match outcome {
        SchemaReconcileOutcome::ExactMatch => {
            ops.schema_reconcile_exact_match = ops.schema_reconcile_exact_match.saturating_add(1);
        }
        SchemaReconcileOutcome::FirstCreate => {
            ops.schema_reconcile_first_create = ops.schema_reconcile_first_create.saturating_add(1);
        }
        SchemaReconcileOutcome::LatestSnapshotCorrupt => {
            ops.schema_reconcile_latest_snapshot_corrupt = ops
                .schema_reconcile_latest_snapshot_corrupt
                .saturating_add(1);
        }
        SchemaReconcileOutcome::RejectedFieldSlot => {
            ops.schema_reconcile_rejected_field_slot =
                ops.schema_reconcile_rejected_field_slot.saturating_add(1);
        }
        SchemaReconcileOutcome::RejectedOther => {
            ops.schema_reconcile_rejected_other =
                ops.schema_reconcile_rejected_other.saturating_add(1);
        }
        SchemaReconcileOutcome::RejectedRowLayout => {
            ops.schema_reconcile_rejected_row_layout =
                ops.schema_reconcile_rejected_row_layout.saturating_add(1);
        }
        SchemaReconcileOutcome::RejectedSchemaVersion => {
            ops.schema_reconcile_rejected_schema_version = ops
                .schema_reconcile_rejected_schema_version
                .saturating_add(1);
        }
        SchemaReconcileOutcome::StoreWriteError => {
            ops.schema_reconcile_store_write_error =
                ops.schema_reconcile_store_write_error.saturating_add(1);
        }
    }
}

// Mirror schema reconciliation outcomes into the entity summary because one
// drifting entity schema should be visible without inspecting global totals.
#[remain::check]
pub(in crate::metrics::sink) const fn record_entity_schema_reconcile_outcome(
    ops: &mut metrics::EntityCounters,
    outcome: SchemaReconcileOutcome,
) {
    ops.schema_reconcile_checks = ops.schema_reconcile_checks.saturating_add(1);

    #[remain::sorted]
    match outcome {
        SchemaReconcileOutcome::ExactMatch => {
            ops.schema_reconcile_exact_match = ops.schema_reconcile_exact_match.saturating_add(1);
        }
        SchemaReconcileOutcome::FirstCreate => {
            ops.schema_reconcile_first_create = ops.schema_reconcile_first_create.saturating_add(1);
        }
        SchemaReconcileOutcome::LatestSnapshotCorrupt => {
            ops.schema_reconcile_latest_snapshot_corrupt = ops
                .schema_reconcile_latest_snapshot_corrupt
                .saturating_add(1);
        }
        SchemaReconcileOutcome::RejectedFieldSlot => {
            ops.schema_reconcile_rejected_field_slot =
                ops.schema_reconcile_rejected_field_slot.saturating_add(1);
        }
        SchemaReconcileOutcome::RejectedOther => {
            ops.schema_reconcile_rejected_other =
                ops.schema_reconcile_rejected_other.saturating_add(1);
        }
        SchemaReconcileOutcome::RejectedRowLayout => {
            ops.schema_reconcile_rejected_row_layout =
                ops.schema_reconcile_rejected_row_layout.saturating_add(1);
        }
        SchemaReconcileOutcome::RejectedSchemaVersion => {
            ops.schema_reconcile_rejected_schema_version = ops
                .schema_reconcile_rejected_schema_version
                .saturating_add(1);
        }
        SchemaReconcileOutcome::StoreWriteError => {
            ops.schema_reconcile_store_write_error =
                ops.schema_reconcile_store_write_error.saturating_add(1);
        }
    }
}

// Schema transition outcomes are narrower than reconciliation outcomes: they
// count only policy decisions for an existing accepted snapshot.
#[remain::check]
pub(in crate::metrics::sink) const fn record_global_schema_transition_outcome(
    ops: &mut metrics::EventOps,
    outcome: SchemaTransitionOutcome,
) {
    ops.schema_transition_checks = ops.schema_transition_checks.saturating_add(1);

    #[remain::sorted]
    match outcome {
        SchemaTransitionOutcome::AddExpressionIndex => {
            ops.schema_transition_add_expression_index =
                ops.schema_transition_add_expression_index.saturating_add(1);
        }
        SchemaTransitionOutcome::AddFieldPathIndex => {
            ops.schema_transition_add_field_path_index =
                ops.schema_transition_add_field_path_index.saturating_add(1);
        }
        SchemaTransitionOutcome::AppendOnlyFields => {
            ops.schema_transition_append_only_fields =
                ops.schema_transition_append_only_fields.saturating_add(1);
        }
        SchemaTransitionOutcome::ConstraintActivation => {
            ops.schema_transition_constraint_activation = ops
                .schema_transition_constraint_activation
                .saturating_add(1);
        }
        SchemaTransitionOutcome::ExactMatch => {
            ops.schema_transition_exact_match = ops.schema_transition_exact_match.saturating_add(1);
        }
        SchemaTransitionOutcome::MetadataOnlyFieldDefault => {
            ops.schema_transition_metadata_only_field_default = ops
                .schema_transition_metadata_only_field_default
                .saturating_add(1);
        }
        SchemaTransitionOutcome::MetadataOnlyIndexRename => {
            ops.schema_transition_metadata_only_index_rename = ops
                .schema_transition_metadata_only_index_rename
                .saturating_add(1);
        }
        SchemaTransitionOutcome::RejectedEntityIdentity => {
            ops.schema_transition_rejected_entity_identity = ops
                .schema_transition_rejected_entity_identity
                .saturating_add(1);
        }
        SchemaTransitionOutcome::RejectedFieldContract => {
            ops.schema_transition_rejected_field_contract = ops
                .schema_transition_rejected_field_contract
                .saturating_add(1);
        }
        SchemaTransitionOutcome::RejectedFieldSlot => {
            ops.schema_transition_rejected_field_slot =
                ops.schema_transition_rejected_field_slot.saturating_add(1);
        }
        SchemaTransitionOutcome::RejectedRowLayout => {
            ops.schema_transition_rejected_row_layout =
                ops.schema_transition_rejected_row_layout.saturating_add(1);
        }
        SchemaTransitionOutcome::RejectedSchemaVersion => {
            ops.schema_transition_rejected_schema_version = ops
                .schema_transition_rejected_schema_version
                .saturating_add(1);
        }
        SchemaTransitionOutcome::RejectedSnapshot => {
            ops.schema_transition_rejected_snapshot =
                ops.schema_transition_rejected_snapshot.saturating_add(1);
        }
    }
}

// Mirror transition decisions into entity summaries so one drifting entity can
// be found without conflating policy rejection with store/recovery failures.
#[remain::check]
pub(in crate::metrics::sink) const fn record_entity_schema_transition_outcome(
    ops: &mut metrics::EntityCounters,
    outcome: SchemaTransitionOutcome,
) {
    ops.schema_transition_checks = ops.schema_transition_checks.saturating_add(1);

    #[remain::sorted]
    match outcome {
        SchemaTransitionOutcome::AddExpressionIndex => {
            ops.schema_transition_add_expression_index =
                ops.schema_transition_add_expression_index.saturating_add(1);
        }
        SchemaTransitionOutcome::AddFieldPathIndex => {
            ops.schema_transition_add_field_path_index =
                ops.schema_transition_add_field_path_index.saturating_add(1);
        }
        SchemaTransitionOutcome::AppendOnlyFields => {
            ops.schema_transition_append_only_fields =
                ops.schema_transition_append_only_fields.saturating_add(1);
        }
        SchemaTransitionOutcome::ConstraintActivation => {
            ops.schema_transition_constraint_activation = ops
                .schema_transition_constraint_activation
                .saturating_add(1);
        }
        SchemaTransitionOutcome::ExactMatch => {
            ops.schema_transition_exact_match = ops.schema_transition_exact_match.saturating_add(1);
        }
        SchemaTransitionOutcome::MetadataOnlyFieldDefault => {
            ops.schema_transition_metadata_only_field_default = ops
                .schema_transition_metadata_only_field_default
                .saturating_add(1);
        }
        SchemaTransitionOutcome::MetadataOnlyIndexRename => {
            ops.schema_transition_metadata_only_index_rename = ops
                .schema_transition_metadata_only_index_rename
                .saturating_add(1);
        }
        SchemaTransitionOutcome::RejectedEntityIdentity => {
            ops.schema_transition_rejected_entity_identity = ops
                .schema_transition_rejected_entity_identity
                .saturating_add(1);
        }
        SchemaTransitionOutcome::RejectedFieldContract => {
            ops.schema_transition_rejected_field_contract = ops
                .schema_transition_rejected_field_contract
                .saturating_add(1);
        }
        SchemaTransitionOutcome::RejectedFieldSlot => {
            ops.schema_transition_rejected_field_slot =
                ops.schema_transition_rejected_field_slot.saturating_add(1);
        }
        SchemaTransitionOutcome::RejectedRowLayout => {
            ops.schema_transition_rejected_row_layout =
                ops.schema_transition_rejected_row_layout.saturating_add(1);
        }
        SchemaTransitionOutcome::RejectedSchemaVersion => {
            ops.schema_transition_rejected_schema_version = ops
                .schema_transition_rejected_schema_version
                .saturating_add(1);
        }
        SchemaTransitionOutcome::RejectedSnapshot => {
            ops.schema_transition_rejected_snapshot =
                ops.schema_transition_rejected_snapshot.saturating_add(1);
        }
    }
}
