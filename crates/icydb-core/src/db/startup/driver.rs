//! Module: db::startup::driver
//! Responsibility: advance one bounded replicated startup page and memoize terminal failures.
//! Does not own: journal replay semantics, generated schema lowering, or timer lifecycle.
//! Boundary: existing recovery engine plus fresh durable bindings -> one coordinator step.

use std::thread::LocalKey;

use icydb_diagnostic_code::{Diagnostic, DiagnosticFactTag};
use icydb_schema::ExpectedAcceptedHead;

use crate::{
    db::{
        DbSession, StoreRegistry,
        commit::{CommitControlObservation, database_incarnation_id, observe_commit_control},
        schema::generated_schema_authority,
        startup::{
            DatabaseStartupState, GeneratedStartupDriverStep, StartupFailureKind,
            classify_terminal_failure, classify_terminal_failure_parts, observe,
            receipt::{
                AcceptedHeadBinding, StartupFailureBinding, StartupFailureReceipt,
                StoreAllocationIdentityOwned,
            },
        },
    },
    error::InternalError,
    traits::CanisterKind,
};

pub(super) fn drive_recovery_page<C: CanisterKind>(
    session: &DbSession<C>,
    stores: &'static LocalKey<StoreRegistry>,
    submission_key: &str,
) -> Result<GeneratedStartupDriverStep, InternalError> {
    match observe::observe::<C>(stores, submission_key) {
        Ok(DatabaseStartupState::Ready) => {
            super::receipt::clear::<C>()?;
            return Ok(GeneratedStartupDriverStep::Terminal);
        }
        Err(_) => return Ok(GeneratedStartupDriverStep::Terminal),
        Ok(DatabaseStartupState::Recovering) => {}
    }

    match session.__continue_startup_recovery() {
        Ok(true) => Ok(GeneratedStartupDriverStep::ApplyGeneratedSchema),
        Ok(false) => Ok(GeneratedStartupDriverStep::Recovering),
        Err(error) => record_recovery_failure::<C>(stores, submission_key, error),
    }
}

fn record_recovery_failure<C: CanisterKind>(
    stores: &'static LocalKey<StoreRegistry>,
    submission_key: &str,
    error: InternalError,
) -> Result<GeneratedStartupDriverStep, InternalError> {
    if observe::observe::<C>(stores, submission_key).is_err() {
        return Ok(GeneratedStartupDriverStep::Terminal);
    }

    let Some(failure) = classify_terminal_failure(StartupFailureKind::JournalRecovery, &error)
    else {
        return Err(error);
    };
    let Some(binding) = capture_journal_binding(stores)? else {
        return Err(error);
    };
    let receipt = StartupFailureReceipt::new(failure, binding)?;
    super::receipt::publish::<C>(&receipt)?;
    Ok(GeneratedStartupDriverStep::Terminal)
}

fn capture_journal_binding(
    stores: &'static LocalKey<StoreRegistry>,
) -> Result<Option<StartupFailureBinding>, InternalError> {
    let CommitControlObservation::Present { incarnation, .. } = observe_commit_control()? else {
        return Ok(None);
    };
    let mut candidates = stores.with(|registry| {
        registry
            .iter()
            .filter_map(|(path, handle)| {
                handle
                    .journal_tail_store()
                    .zip(handle.journal_allocation())
                    .map(|(journal, allocation)| (path, journal, allocation))
            })
            .collect::<Vec<_>>()
    });
    candidates.sort_unstable_by_key(|(path, _, _)| *path);
    let candidate = candidates
        .iter()
        .find(|(_, journal, _)| {
            journal.with_borrow(|store| store.has_stored_batch() || store.has_fold_record_cursor())
        })
        .or_else(|| candidates.first());
    let Some((_, journal, allocation)) = candidate else {
        return Ok(None);
    };
    let (proof, cursor) = journal.with_borrow(|store| {
        Ok::<_, InternalError>((store.proof_identity()?, store.fold_record_cursor()?))
    })?;
    Ok(Some(StartupFailureBinding::JournalRecovery {
        incarnation,
        allocation: StoreAllocationIdentityOwned::from_identity(*allocation),
        proof,
        cursor,
    }))
}

pub(super) fn record_schema_failure<C: CanisterKind>(
    stores: &'static LocalKey<StoreRegistry>,
    submission_key: &str,
    diagnostic: Diagnostic,
    facts: Vec<(DiagnosticFactTag, u64)>,
) -> Result<bool, InternalError> {
    let Some(failure) = classify_terminal_failure_parts(
        StartupFailureKind::SchemaReconciliation,
        diagnostic,
        facts,
    ) else {
        return Ok(false);
    };
    let incarnation = database_incarnation_id()?;
    let (_, accepted_head) = generated_schema_authority(stores, incarnation)?;
    let binding = StartupFailureBinding::SchemaReconciliation {
        incarnation,
        submission_key: submission_key.to_string(),
        accepted_head: accepted_head_binding(&accepted_head),
    };
    let receipt = StartupFailureReceipt::new(failure, binding)?;
    super::receipt::publish::<C>(&receipt)?;
    Ok(true)
}

const fn accepted_head_binding(head: &ExpectedAcceptedHead) -> AcceptedHeadBinding {
    match head {
        ExpectedAcceptedHead::Empty => AcceptedHeadBinding::Empty,
        ExpectedAcceptedHead::Exact {
            revision,
            fingerprint,
        } => AcceptedHeadBinding::Exact {
            revision: *revision,
            fingerprint: fingerprint.to_bytes(),
        },
    }
}
