//! Module: db::startup::observe
//! Responsibility: compute readiness from the fixed startup-control read set.
//! Does not own: receipt persistence, recovery execution, or generated scheduling.
//! Boundary: generated registry and submission identity -> pure readiness observation.

use crate::{
    db::{
        StoreRegistry,
        commit::{
            CommitControlObservation, configure_commit_memory_id, observe_commit_control,
            startup_recovery_witness,
        },
        database_format::{DatabaseFormatObservation, observe_database_format},
        schema::generated_schema_reconciled,
        startup::{
            DatabaseStartupState, StartupFailure, StartupFailureKind,
            receipt::{AcceptedHeadBinding, StartupFailureBinding, StartupFailureReceipt},
        },
    },
    error::{ErrorOrigin, InternalError},
    traits::CanisterKind,
};
use icydb_schema::ExpectedAcceptedHead;

pub(super) fn observe<C: CanisterKind>(
    stores: &'static std::thread::LocalKey<StoreRegistry>,
    submission_key: &str,
) -> Result<DatabaseStartupState, StartupFailure> {
    configure_commit_memory_id(C::COMMIT_MEMORY_ID, C::COMMIT_STABLE_KEY)
        .map_err(database_control_failure)?;
    let receipt = super::receipt::load::<C>().map_err(database_control_failure)?;
    let format = observe_database_format(stores)
        .map_err(|error| database_control_or_allocation_failure::<C>(receipt.as_ref(), &error))?;
    if format == DatabaseFormatObservation::Uninitialized {
        return matching_allocation_failure::<C>(receipt.as_ref())
            .map_or(Ok(DatabaseStartupState::Recovering), Err);
    }

    let control = observe_commit_control()
        .map_err(|error| database_control_or_allocation_failure::<C>(receipt.as_ref(), &error))?;
    let CommitControlObservation::Present {
        incarnation,
        empty_control_proof,
        marker_present,
    } = control
    else {
        return Ok(DatabaseStartupState::Recovering);
    };
    if let Some(receipt) = receipt.as_ref() {
        let matches = if empty_control_proof.is_none() {
            allocation_receipt_matches::<C>(receipt)
        } else {
            database_control_receipt_matches(receipt, incarnation, empty_control_proof)
        };
        if matches {
            return Err(receipt.failure().clone());
        }
    }

    let (recovered, in_progress) =
        startup_recovery_witness(stores).map_err(database_control_failure)?;
    let (cursor_present, matching_journal_failure) =
        observe_journal_control(stores, receipt.as_ref(), incarnation)?;
    if let Some(failure) = matching_journal_failure {
        return Err(failure);
    }
    // A matching receipt has priority over pending controls. Schema failures
    // are captured after a normally returned reconciliation error, which may
    // itself have retained a marker. Requiring recovery completion before
    // checking the receipt would clear the only watchdog and strand that
    // terminal failure behind `Recovering`.
    let schema_observation = if receipt
        .as_ref()
        .is_some_and(|receipt| receipt.failure().kind() == StartupFailureKind::SchemaReconciliation)
    {
        let observation = generated_schema_reconciled(stores, incarnation, submission_key)
            .map_err(|error| {
                StartupFailure::from_internal(StartupFailureKind::SchemaReconciliation, &error)
            })?;
        if let Some(receipt) = receipt.as_ref()
            && schema_receipt_matches(receipt, incarnation, submission_key, &observation.1)
        {
            return Err(receipt.failure().clone());
        }
        Some(observation)
    } else {
        None
    };
    if marker_present || cursor_present || in_progress || !recovered {
        return Ok(DatabaseStartupState::Recovering);
    }

    let (reconciled, accepted_head) = match schema_observation {
        Some(observation) => observation,
        None => {
            generated_schema_reconciled(stores, incarnation, submission_key).map_err(|error| {
                StartupFailure::from_internal(StartupFailureKind::SchemaReconciliation, &error)
            })?
        }
    };
    if let Some(receipt) = receipt.as_ref()
        && schema_receipt_matches(receipt, incarnation, submission_key, &accepted_head)
    {
        return Err(receipt.failure().clone());
    }
    Ok(if reconciled {
        DatabaseStartupState::Ready
    } else {
        DatabaseStartupState::Recovering
    })
}

fn observe_journal_control(
    stores: &'static std::thread::LocalKey<StoreRegistry>,
    receipt: Option<&StartupFailureReceipt>,
    incarnation: crate::db::DatabaseIncarnationId,
) -> Result<(bool, Option<StartupFailure>), StartupFailure> {
    let mut cursor_present = false;
    let mut matching_journal_failure = None;
    let journal_failure_receipt =
        receipt.filter(|receipt| receipt.failure().kind() == StartupFailureKind::JournalRecovery);
    stores
        .with(|registry| {
            for (_, handle) in registry.iter() {
                let Some(journal) = handle.journal_tail_store() else {
                    continue;
                };
                let Some(allocation) = handle.journal_allocation() else {
                    return Err(InternalError::store_invariant());
                };
                let (proof, cursor) = journal.with_borrow(|journal| {
                    // Watermark and cursor are always part of the fixed readiness
                    // read set. The remaining proof fields exist only to bind an
                    // already-persisted journal failure, so avoid the reverse tail
                    // lookup when no such receipt can match.
                    let proof = if journal_failure_receipt.is_some() {
                        Some(journal.proof_identity()?)
                    } else {
                        let _watermark = journal.fold_watermark()?;
                        None
                    };
                    Ok::<_, InternalError>((proof, journal.fold_record_cursor()?))
                })?;
                cursor_present |= cursor.is_some();
                if let (Some(receipt), Some(proof)) = (journal_failure_receipt, proof)
                    && journal_receipt_matches(receipt, incarnation, allocation, proof, cursor)
                {
                    matching_journal_failure = Some(receipt.failure().clone());
                }
            }
            Ok::<(), InternalError>(())
        })
        .map_err(|error| {
            StartupFailure::from_internal(StartupFailureKind::JournalRecovery, &error)
        })?;
    Ok((cursor_present, matching_journal_failure))
}

fn database_control_failure(error: InternalError) -> StartupFailure {
    let error = error.with_origin(ErrorOrigin::Recovery);
    StartupFailure::from_internal(StartupFailureKind::DatabaseControl, &error)
}

fn database_control_or_allocation_failure<C: CanisterKind>(
    receipt: Option<&StartupFailureReceipt>,
    error: &InternalError,
) -> StartupFailure {
    matching_allocation_failure::<C>(receipt).unwrap_or_else(|| {
        StartupFailure::from_internal(StartupFailureKind::DatabaseControl, error)
    })
}

fn matching_allocation_failure<C: CanisterKind>(
    receipt: Option<&StartupFailureReceipt>,
) -> Option<StartupFailure> {
    let receipt = receipt?;
    allocation_receipt_matches::<C>(receipt).then(|| receipt.failure().clone())
}

fn allocation_receipt_matches<C: CanisterKind>(receipt: &StartupFailureReceipt) -> bool {
    matches!(
        receipt.binding(),
        StartupFailureBinding::DatabaseControl {
            commit_memory_id,
            commit_stable_key,
            control: None,
        } if *commit_memory_id == C::COMMIT_MEMORY_ID
            && commit_stable_key == C::COMMIT_STABLE_KEY
    )
}

fn database_control_receipt_matches(
    receipt: &StartupFailureReceipt,
    incarnation: crate::db::DatabaseIncarnationId,
    proof: Option<[u8; 32]>,
) -> bool {
    matches!(
        receipt.binding(),
        StartupFailureBinding::DatabaseControl {
            control: Some(control),
            ..
        } if control.incarnation == incarnation && Some(control.proof) == proof
    )
}

fn journal_receipt_matches(
    receipt: &StartupFailureReceipt,
    incarnation: crate::db::DatabaseIncarnationId,
    allocation: crate::db::StoreAllocationIdentity,
    proof: crate::db::journal::JournalTailProofIdentity,
    cursor: Option<crate::db::journal::FoldRecordCursor>,
) -> bool {
    matches!(
        receipt.binding(),
        StartupFailureBinding::JournalRecovery {
            incarnation: bound_incarnation,
            allocation: bound_allocation,
            proof: bound_proof,
            cursor: bound_cursor,
        } if *bound_incarnation == incarnation
            && bound_allocation.memory_id == allocation.memory_id()
            && bound_allocation.stable_key == allocation.stable_key()
            && *bound_proof == proof
            && *bound_cursor == cursor
    )
}

fn schema_receipt_matches(
    receipt: &StartupFailureReceipt,
    incarnation: crate::db::DatabaseIncarnationId,
    submission_key: &str,
    accepted_head: &ExpectedAcceptedHead,
) -> bool {
    let accepted_head = match accepted_head {
        ExpectedAcceptedHead::Empty => AcceptedHeadBinding::Empty,
        ExpectedAcceptedHead::Exact {
            revision,
            fingerprint,
        } => AcceptedHeadBinding::Exact {
            revision: *revision,
            fingerprint: fingerprint.to_bytes(),
        },
    };
    matches!(
        receipt.binding(),
        StartupFailureBinding::SchemaReconciliation {
            incarnation: bound_incarnation,
            submission_key: bound_submission_key,
            accepted_head: bound_head,
        } if *bound_incarnation == incarnation
            && bound_submission_key == submission_key
            && *bound_head == accepted_head
    )
}
