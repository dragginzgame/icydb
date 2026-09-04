//! Module: db::startup::driver
//! Responsibility: advance one bounded replicated startup page and memoize terminal failures.
//! Does not own: journal replay semantics, generated schema lowering, or timer lifecycle.
//! Boundary: existing recovery engine plus fresh durable bindings -> one coordinator step.

use std::thread::LocalKey;

use icydb_diagnostic_code::{Diagnostic, DiagnosticConstraintKind, DiagnosticFactTag, ErrorCode};
use icydb_schema::{ExpectedAcceptedHead, SchemaSubmissionKey};

use crate::{
    db::{
        DbSession, JournalTailStore, StoreRegistry,
        commit::{
            CommitControlObservation, StartupRecoveryFailure, StartupRecoveryFailureAuthority,
            database_incarnation_id, observe_commit_control,
        },
        schema::{
            SchemaChangeOutcome, SchemaChangeProgressStatus,
            accepted_schema_cache_fingerprint_method_version,
            cardinality_build::{
                CardinalityBuildAuthority, CardinalityGenerationPageOutcome,
                drive_cardinality_generation_page,
            },
            generated_schema_authority,
        },
        startup::{
            DatabaseStartupState, GeneratedStartupDriverStep, StartupFailureKind,
            classify_terminal_failure, classify_terminal_failure_parts, observe,
            receipt::{
                AcceptedHeadBinding, DatabaseControlBinding, StartupFailureBinding,
                StartupFailureReceipt, StoreAllocationIdentityOwned,
            },
        },
    },
    error::{AcceptedConstraintFactContext, ConstraintValidationFindingOutput, InternalError},
    traits::CanisterKind,
};

pub(super) fn drive_recovery_page<C: CanisterKind>(
    session: &DbSession<C>,
    stores: &'static LocalKey<StoreRegistry>,
    submission_key: &str,
) -> Result<GeneratedStartupDriverStep, InternalError> {
    let startup_ready = match observe::observe::<C>(stores, submission_key) {
        Ok(DatabaseStartupState::Ready) => true,
        Err(_) => return Ok(GeneratedStartupDriverStep::Terminal),
        Ok(DatabaseStartupState::Recovering) => false,
    };

    match session.drive_startup_recovery_page_with_failure_authority() {
        Ok(true) if startup_ready => {
            super::receipt::clear::<C>()?;
            match drive_cardinality_page(stores) {
                Ok(step) => Ok(step),
                Err(_) => Ok(GeneratedStartupDriverStep::Terminal),
            }
        }
        Ok(true) => drive_generated_schema_application::<C>(session, stores, submission_key),
        Ok(false) => Ok(GeneratedStartupDriverStep::Recovering),
        Err(error) => record_recovery_failure::<C>(stores, submission_key, error),
    }
}

/// Reuse the existing watchdog for one optional store-local cardinality page.
///
/// Malformed derived evidence stops optional construction without changing
/// startup readiness. The maintained integrity and planner fallback paths keep
/// row, index, and accepted-schema authority usable.
fn drive_cardinality_page(
    stores: &'static LocalKey<StoreRegistry>,
) -> Result<GeneratedStartupDriverStep, InternalError> {
    let incarnation = database_incarnation_id()?;
    let mut registered = stores.with(|registry| registry.iter().collect::<Vec<_>>());
    icydb_schema::compact_sort_unstable_by(&mut registered, |left, right| left.0.cmp(right.0));
    for (_path, handle) in registered {
        let Some(journal) = handle.journal_tail_store() else {
            continue;
        };
        let outcome = handle.with_data(|data| {
            handle.with_index(|index| {
                handle.with_schema_mut(|schema| {
                    drive_cardinality_generation_page(data, index, schema, |schema| {
                        let watermark = journal.with_borrow(JournalTailStore::fold_watermark)?;
                        CardinalityBuildAuthority::derive(
                            schema,
                            incarnation,
                            handle.allocation_identities(),
                            watermark,
                        )
                    })
                })
            })
        })?;
        if outcome != CardinalityGenerationPageOutcome::Quiescent {
            return Ok(GeneratedStartupDriverStep::Recovering);
        }
    }
    Ok(GeneratedStartupDriverStep::Terminal)
}

fn drive_generated_schema_application<C: CanisterKind>(
    session: &DbSession<C>,
    stores: &'static LocalKey<StoreRegistry>,
    submission_key: &str,
) -> Result<GeneratedStartupDriverStep, InternalError> {
    let submission_key = SchemaSubmissionKey::try_new(submission_key.to_string())
        .map_err(|_| InternalError::store_invariant())?;
    let incarnation = database_incarnation_id()?;
    let (database_identity, _) = generated_schema_authority(stores, incarnation)?;
    let Some(receipt) = session.schema_application_receipt(database_identity, &submission_key)?
    else {
        return Ok(GeneratedStartupDriverStep::ApplyGeneratedSchema);
    };

    let job_id = match receipt.outcome() {
        SchemaChangeOutcome::NoOp { .. } | SchemaChangeOutcome::Applied { .. } => {
            super::receipt::clear::<C>()?;
            return Ok(GeneratedStartupDriverStep::Recovering);
        }
        SchemaChangeOutcome::Pending { job, .. } => job.id(),
        SchemaChangeOutcome::Aborted { .. } => {
            return record_schema_application_failure::<C>(
                stores,
                submission_key.as_str(),
                InternalError::schema_application_conflict(),
            );
        }
    };
    let progress = match session.continue_schema_application(job_id, None) {
        Ok(progress) => progress,
        Err(error) => {
            return record_schema_application_failure::<C>(stores, submission_key.as_str(), error);
        }
    };
    match progress.status() {
        SchemaChangeProgressStatus::Started
        | SchemaChangeProgressStatus::Advanced { .. }
        | SchemaChangeProgressStatus::Restarted { .. } => {
            Ok(GeneratedStartupDriverStep::Recovering)
        }
        SchemaChangeProgressStatus::Applied => {
            super::receipt::clear::<C>()?;
            Ok(GeneratedStartupDriverStep::Recovering)
        }
        SchemaChangeProgressStatus::Aborted => record_schema_application_failure::<C>(
            stores,
            submission_key.as_str(),
            InternalError::schema_application_conflict(),
        ),
        SchemaChangeProgressStatus::Findings { findings, .. } => {
            let finding = findings
                .first()
                .ok_or_else(InternalError::store_corruption)?;
            record_schema_application_failure::<C>(
                stores,
                submission_key.as_str(),
                generated_schema_finding_error(finding)?,
            )
        }
    }
}

fn generated_schema_finding_error(
    finding: &ConstraintValidationFindingOutput,
) -> Result<InternalError, InternalError> {
    if finding.error_code() != ErrorCode::RUNTIME_BOUNDARY_CONSTRAINT_VIOLATION {
        return Err(InternalError::store_corruption());
    }
    let constraint_kind = if finding.value_path().is_some() {
        DiagnosticConstraintKind::TargetedRule
    } else {
        DiagnosticConstraintKind::Check
    };
    Ok(InternalError::mutation_constraint_violation(
        AcceptedConstraintFactContext::write_admission(
            accepted_schema_cache_fingerprint_method_version(),
            finding.accepted_schema_fingerprint(),
            finding.entity_tag(),
            finding.constraint_id(),
            constraint_kind,
            None,
            finding.value_path().cloned(),
        ),
    ))
}

fn record_schema_application_failure<C: CanisterKind>(
    stores: &'static LocalKey<StoreRegistry>,
    submission_key: &str,
    error: InternalError,
) -> Result<GeneratedStartupDriverStep, InternalError> {
    if record_schema_failure::<C>(
        stores,
        submission_key,
        error.diagnostic(),
        error.diagnostic_facts(),
    )? {
        Ok(GeneratedStartupDriverStep::Terminal)
    } else {
        Err(error)
    }
}

fn record_recovery_failure<C: CanisterKind>(
    stores: &'static LocalKey<StoreRegistry>,
    submission_key: &str,
    recovery_failure: StartupRecoveryFailure,
) -> Result<GeneratedStartupDriverStep, InternalError> {
    if observe::observe::<C>(stores, submission_key).is_err() {
        return Ok(GeneratedStartupDriverStep::Terminal);
    }

    let kind = match recovery_failure.authority() {
        StartupRecoveryFailureAuthority::DatabaseControl => StartupFailureKind::DatabaseControl,
        StartupRecoveryFailureAuthority::JournalStore(_) => StartupFailureKind::JournalRecovery,
    };
    let Some(failure) = classify_terminal_failure(kind, recovery_failure.error()) else {
        return Err(recovery_failure.into_error());
    };
    let binding = match recovery_failure.authority() {
        StartupRecoveryFailureAuthority::DatabaseControl => {
            capture_database_control_binding::<C>()?
        }
        StartupRecoveryFailureAuthority::JournalStore(store_path) => {
            capture_journal_binding(stores, store_path)?
        }
    };
    let receipt = StartupFailureReceipt::new(failure, binding)?;
    super::receipt::publish::<C>(&receipt)?;
    Ok(GeneratedStartupDriverStep::Terminal)
}

fn capture_journal_binding(
    stores: &'static LocalKey<StoreRegistry>,
    store_path: &'static str,
) -> Result<StartupFailureBinding, InternalError> {
    let CommitControlObservation::Present { incarnation, .. } = observe_commit_control()? else {
        return Err(InternalError::store_invariant());
    };
    let candidate = stores.with(|registry| {
        registry.iter().find_map(|(path, handle)| {
            (path == store_path)
                .then(|| handle.journal_tail_store().zip(handle.journal_allocation()))?
        })
    });
    let (journal, allocation) = candidate.ok_or_else(InternalError::store_invariant)?;
    let proof = journal.with_borrow(JournalTailStore::proof_identity)?;
    Ok(StartupFailureBinding::JournalRecovery {
        incarnation,
        allocation: StoreAllocationIdentityOwned::from_identity(allocation),
        proof,
    })
}

fn capture_database_control_binding<C: CanisterKind>()
-> Result<StartupFailureBinding, InternalError> {
    let control = match observe_commit_control()? {
        CommitControlObservation::Uninitialized => None,
        CommitControlObservation::Present {
            incarnation,
            empty_control_proof,
            ..
        } => empty_control_proof.map(|proof| DatabaseControlBinding::new(incarnation, proof)),
    };
    Ok(StartupFailureBinding::DatabaseControl {
        commit_memory_id: C::COMMIT_MEMORY_ID,
        commit_stable_key: C::COMMIT_STABLE_KEY.to_string(),
        control,
    })
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

#[cfg(test)]
mod tests {
    use super::generated_schema_finding_error;
    use crate::error::ConstraintValidationFindingOutput;
    use icydb_diagnostic_code::{DiagnosticFactTag, ErrorCode};

    #[test]
    fn generated_schema_finding_becomes_canonical_terminal_constraint_failure() {
        let finding = ConstraintValidationFindingOutput::new(
            [0x11; 16],
            7,
            9,
            vec![0x01],
            vec![2],
            None,
            ErrorCode::RUNTIME_BOUNDARY_CONSTRAINT_VIOLATION.raw(),
        );

        let error = generated_schema_finding_error(&finding)
            .expect("a generated check finding should map to E210");
        assert_eq!(
            error.diagnostic().error_code(),
            ErrorCode::RUNTIME_BOUNDARY_CONSTRAINT_VIOLATION,
        );
        assert_eq!(
            error
                .diagnostic_facts()
                .iter()
                .map(|(tag, _)| *tag)
                .collect::<Vec<_>>(),
            vec![
                DiagnosticFactTag::AcceptedSchemaFingerprintMethod,
                DiagnosticFactTag::AcceptedSchemaFingerprintHigh,
                DiagnosticFactTag::AcceptedSchemaFingerprintLow,
                DiagnosticFactTag::EntityTag,
                DiagnosticFactTag::ConstraintId,
                DiagnosticFactTag::ConstraintKind,
                DiagnosticFactTag::ConstraintContext,
            ],
        );
    }
}
