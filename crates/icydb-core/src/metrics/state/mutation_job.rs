//! Module: metrics::state::mutation_job
//! Responsibility: bounded mutation-job lifecycle and resource metrics DTO.
//! Does not own: instrumentation call sites or durable mutation-job state.
//! Boundary: mutation-job metrics events -> rich aggregate metrics report.

use crate::db::{MutationJobRestartReason, MutationJobTargetFailureReason};
use candid::CandidType;
use serde::Deserialize;

/// Aggregate bounded mutation-job lifecycle and resource observations.
#[derive(CandidType, Clone, Debug, Default, Deserialize)]
pub struct MutationJobMetrics {
    pub(crate) starts_inserted: u64,
    pub(crate) starts_exact_replayed: u64,
    pub(crate) states_loaded: u64,
    pub(crate) advances_exact_replayed: u64,
    pub(crate) cancellations: u64,
    pub(crate) terminal_acknowledgements: u64,
    pub(crate) inventories_loaded: u64,
    pub(crate) forward_to_verify_transitions: u64,
    pub(crate) verify_restarts_revision_drift: u64,
    pub(crate) verify_restarts_residual_work: u64,
    pub(crate) completions: u64,
    pub(crate) forward_steps_committed: u64,
    pub(crate) verify_steps_committed: u64,
    pub(crate) keys_scanned: u64,
    pub(crate) rows_updated: u64,
    pub(crate) scan_bytes: u64,
    pub(crate) staged_bytes: u64,
    pub(crate) keys_scanned_cumulative_max: u64,
    pub(crate) rows_updated_cumulative_max: u64,
    pub(crate) verify_restarts_cumulative_max: u64,
    pub(crate) restart_accepted_schema_changed: u64,
    pub(crate) restart_batch_policy_changed: u64,
    pub(crate) restart_candidate_exceeds_batch_policy: u64,
    pub(crate) restart_execution_budget_policy_exceeded: u64,
    pub(crate) restart_intent_ineligible: u64,
    pub(crate) restart_managed_timestamp_regression: u64,
    pub(crate) restart_target_allocation_changed: u64,
    pub(crate) restart_unsupported_continuation: u64,
    pub(crate) target_failure_staging_byte_budget_exceeded: u64,
    pub(crate) target_failure_other: u64,
    pub(crate) retained_count: u64,
    pub(crate) hard_limit: u64,
    pub(crate) reserved_integrity_headroom: u64,
    pub(crate) integrity_count: u64,
    pub(crate) resumable_count: u64,
    pub(crate) mutation_count: u64,
    pub(crate) retained_record_bytes: u64,
}

impl MutationJobMetrics {
    #[must_use]
    pub const fn starts_inserted(&self) -> u64 {
        self.starts_inserted
    }

    #[must_use]
    pub const fn starts_exact_replayed(&self) -> u64 {
        self.starts_exact_replayed
    }

    #[must_use]
    pub const fn states_loaded(&self) -> u64 {
        self.states_loaded
    }

    #[must_use]
    pub const fn advances_exact_replayed(&self) -> u64 {
        self.advances_exact_replayed
    }

    #[must_use]
    pub const fn cancellations(&self) -> u64 {
        self.cancellations
    }

    #[must_use]
    pub const fn terminal_acknowledgements(&self) -> u64 {
        self.terminal_acknowledgements
    }

    #[must_use]
    pub const fn inventories_loaded(&self) -> u64 {
        self.inventories_loaded
    }

    #[must_use]
    pub const fn forward_to_verify_transitions(&self) -> u64 {
        self.forward_to_verify_transitions
    }

    #[must_use]
    pub const fn verify_restarts_revision_drift(&self) -> u64 {
        self.verify_restarts_revision_drift
    }

    #[must_use]
    pub const fn verify_restarts_residual_work(&self) -> u64 {
        self.verify_restarts_residual_work
    }

    #[must_use]
    pub const fn completions(&self) -> u64 {
        self.completions
    }

    #[must_use]
    pub const fn forward_steps_committed(&self) -> u64 {
        self.forward_steps_committed
    }

    #[must_use]
    pub const fn verify_steps_committed(&self) -> u64 {
        self.verify_steps_committed
    }

    #[must_use]
    pub const fn keys_scanned(&self) -> u64 {
        self.keys_scanned
    }

    #[must_use]
    pub const fn rows_updated(&self) -> u64 {
        self.rows_updated
    }

    #[must_use]
    pub const fn scan_bytes(&self) -> u64 {
        self.scan_bytes
    }

    #[must_use]
    pub const fn staged_bytes(&self) -> u64 {
        self.staged_bytes
    }

    #[must_use]
    pub const fn keys_scanned_cumulative_max(&self) -> u64 {
        self.keys_scanned_cumulative_max
    }

    #[must_use]
    pub const fn rows_updated_cumulative_max(&self) -> u64 {
        self.rows_updated_cumulative_max
    }

    #[must_use]
    pub const fn verify_restarts_cumulative_max(&self) -> u64 {
        self.verify_restarts_cumulative_max
    }

    #[must_use]
    pub const fn terminal_restarts(&self) -> u64 {
        self.restart_accepted_schema_changed
            .saturating_add(self.restart_batch_policy_changed)
            .saturating_add(self.restart_candidate_exceeds_batch_policy)
            .saturating_add(self.restart_execution_budget_policy_exceeded)
            .saturating_add(self.restart_intent_ineligible)
            .saturating_add(self.restart_managed_timestamp_regression)
            .saturating_add(self.restart_target_allocation_changed)
            .saturating_add(self.restart_unsupported_continuation)
    }

    #[must_use]
    pub const fn restart_count(&self, reason: MutationJobRestartReason) -> u64 {
        match reason {
            MutationJobRestartReason::AcceptedSchemaChanged => self.restart_accepted_schema_changed,
            MutationJobRestartReason::BatchPolicyChanged => self.restart_batch_policy_changed,
            MutationJobRestartReason::CandidateExceedsBatchPolicy => {
                self.restart_candidate_exceeds_batch_policy
            }
            MutationJobRestartReason::ExecutionBudgetPolicyExceeded => {
                self.restart_execution_budget_policy_exceeded
            }
            MutationJobRestartReason::IntentIneligible => self.restart_intent_ineligible,
            MutationJobRestartReason::ManagedTimestampRegression => {
                self.restart_managed_timestamp_regression
            }
            MutationJobRestartReason::TargetAllocationChanged => {
                self.restart_target_allocation_changed
            }
            MutationJobRestartReason::UnsupportedContinuation => {
                self.restart_unsupported_continuation
            }
        }
    }

    #[must_use]
    pub const fn target_failure_count(&self, reason: MutationJobTargetFailureReason) -> u64 {
        match reason {
            MutationJobTargetFailureReason::StagingByteBudgetExceeded => {
                self.target_failure_staging_byte_budget_exceeded
            }
            MutationJobTargetFailureReason::Other => self.target_failure_other,
        }
    }

    #[must_use]
    pub const fn retained_count(&self) -> u64 {
        self.retained_count
    }

    #[must_use]
    pub const fn hard_limit(&self) -> u64 {
        self.hard_limit
    }

    #[must_use]
    pub const fn reserved_integrity_headroom(&self) -> u64 {
        self.reserved_integrity_headroom
    }

    #[must_use]
    pub const fn integrity_count(&self) -> u64 {
        self.integrity_count
    }

    #[must_use]
    pub const fn resumable_count(&self) -> u64 {
        self.resumable_count
    }

    #[must_use]
    pub const fn mutation_count(&self) -> u64 {
        self.mutation_count
    }

    #[must_use]
    pub const fn retained_record_bytes(&self) -> u64 {
        self.retained_record_bytes
    }
}
