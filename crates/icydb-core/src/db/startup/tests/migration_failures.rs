//! Migration planning failures stop startup through the existing bound receipt.

use super::*;
use icydb_diagnostic_code::SchemaMigrationCode;

const TERMINAL_MIGRATION_REASONS: [SchemaMigrationCode; 14] = [
    SchemaMigrationCode::Unadopted,
    SchemaMigrationCode::MissingMigration,
    SchemaMigrationCode::VersionGap,
    SchemaMigrationCode::Downgrade,
    SchemaMigrationCode::EmptyEntityVersionBump,
    SchemaMigrationCode::UnknownFromObject,
    SchemaMigrationCode::UnknownToObject,
    SchemaMigrationCode::KindMismatch,
    SchemaMigrationCode::IdentityConflict,
    SchemaMigrationCode::UnexplainedSchemaDifference,
    SchemaMigrationCode::UnsupportedTransform,
    SchemaMigrationCode::PhysicalRunnerMissing,
    SchemaMigrationCode::ProgressCorrupt,
    SchemaMigrationCode::CandidateMismatch,
];

#[test]
fn migration_classification_preserves_lifecycle_conflicts_and_failure_owners() {
    for reason in TERMINAL_MIGRATION_REASONS {
        let error = InternalError::schema_migration(reason);
        let internal = classify_terminal_failure(StartupFailureKind::SchemaReconciliation, &error)
            .expect("deterministic migration rejection must stop generated startup");
        let public = classify_terminal_failure_parts(
            StartupFailureKind::SchemaReconciliation,
            error.diagnostic(),
            error.diagnostic_facts(),
        )
        .expect("the facade diagnostic must retain the same terminal policy");
        assert_eq!(internal, public);
        assert_eq!(*public.diagnostic(), error.diagnostic());
        for kind in [
            StartupFailureKind::DatabaseControl,
            StartupFailureKind::JournalRecovery,
        ] {
            assert!(classify_terminal_failure(kind, &error).is_none());
        }
    }

    // Controller progress and publication conflicts keep their existing retry
    // classification; a generated-plan rejection must not capture those lanes.
    for reason in [
        SchemaMigrationCode::StaleAcceptedHead,
        SchemaMigrationCode::PlanChanged,
        SchemaMigrationCode::MigrationInProgress,
        SchemaMigrationCode::AbortTooLate,
        SchemaMigrationCode::PublicationRaceLost,
    ] {
        let error = InternalError::schema_migration(reason);
        assert!(
            classify_terminal_failure(StartupFailureKind::SchemaReconciliation, &error).is_none()
        );
        assert!(
            classify_terminal_failure_parts(
                StartupFailureKind::SchemaReconciliation,
                error.diagnostic(),
                error.diagnostic_facts(),
            )
            .is_none()
        );
    }
}

#[test]
fn migration_planning_receipt_stops_replanning_and_retains_exact_diagnostics() {
    const SUBMISSION: &str = "generated/migration-planning-failure";
    select_commit_memory_allocation(
        DriverCanister::commit_memory_id().unwrap(),
        DriverCanister::COMMIT_STABLE_KEY,
    );
    let memory = commit_memory_handle(current_commit_memory_allocation().unwrap()).unwrap();
    initialize_current_database_control_for_tests(&memory);
    let request = RequestExecutionRoot::__new_runtime_root();
    let session = crate::db::DbSession::<DriverCanister>::new(&DRIVER_STORES, &request);
    assert_eq!(
        drive_generated_startup_recovery_page(&session, &DRIVER_STORES, SUBMISSION).unwrap(),
        GeneratedStartupDriverStep::ApplyGeneratedSchema,
    );

    // Migration planning currently carries no numeric facts. Keep the existing
    // receipt validator authoritative: invented tags must still be rejected.
    let invalid = InternalError::schema_migration(SchemaMigrationCode::VersionGap);
    let refusal = record_generated_schema_startup_failure::<DriverCanister>(
        &DRIVER_STORES,
        SUBMISSION,
        invalid.diagnostic(),
        vec![(DiagnosticFactTag::EntityTag, 7)],
    )
    .unwrap_err();
    assert_eq!(
        refusal.diagnostic().error_code(),
        ErrorCode::STORE_INVARIANT_VIOLATION
    );
    assert_eq!(
        receipt::startup_memory::<DriverCanister>().unwrap().size(),
        0
    );

    // Exercise the diagnostic-parts boundary used by generated handoff code,
    // not just the internal-error classifier. Each receipt must stop the next
    // attempt before it can request another generated-schema application.
    for reason in TERMINAL_MIGRATION_REASONS {
        let error = InternalError::schema_migration(reason);
        let facts = error.diagnostic_facts();
        assert!(
            record_generated_schema_startup_failure::<DriverCanister>(
                &DRIVER_STORES,
                SUBMISSION,
                error.diagnostic(),
                facts.clone(),
            )
            .unwrap(),
            "a deterministic migration rejection must publish a terminal receipt"
        );
        let failure = observe_generated_startup_state::<DriverCanister>(&DRIVER_STORES, SUBMISSION)
            .unwrap_err();
        assert_eq!(failure.kind(), StartupFailureKind::SchemaReconciliation);
        assert_eq!(*failure.diagnostic(), error.diagnostic());
        assert_eq!(failure.facts(), facts);
        for _ in 0..2 {
            assert_eq!(
                drive_generated_startup_recovery_page(&session, &DRIVER_STORES, SUBMISSION)
                    .unwrap(),
                GeneratedStartupDriverStep::Terminal,
            );
        }

        // A corrected deployment has a different generated submission identity.
        // Reuse the maintained binding rather than making the failure permanent.
        assert_eq!(
            observe_generated_startup_state::<DriverCanister>(
                &DRIVER_STORES,
                "generated/corrected-migration",
            ),
            Ok(DatabaseStartupState::Recovering),
        );
        assert!(clear_generated_startup_failure::<DriverCanister>().unwrap());
        assert_eq!(
            drive_generated_startup_recovery_page(&session, &DRIVER_STORES, SUBMISSION).unwrap(),
            GeneratedStartupDriverStep::ApplyGeneratedSchema,
        );
    }
}
