use super::*;

#[test]
fn field_path_runner_orchestrates_staging_to_publication_handoff() {
    let first = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Ada".to_string()))],
    };
    let second = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Grace".to_string()))],
    };
    let (before, after, execution_plan) = field_path_index_runner_context();
    let input = super::SchemaMutationRunnerInput::new(&before, &after, execution_plan)
        .expect("same-entity accepted snapshots should build runner input");
    let mut index_store = initialized_index_store(234);
    let mut invalidation_sink = RecordingRuntimeInvalidationSink::default();
    let mut publication_sink = RecordingAcceptedSnapshotPublicationSink::default();

    let report = super::SchemaFieldPathIndexRunner::run(
        &input,
        EntityTag::new(7),
        accepted_name_field_path_target(),
        [
            super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Nat(2), &second),
            super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Nat(1), &first),
        ],
        &mut index_store,
        &mut invalidation_sink,
        &mut publication_sink,
    )
    .expect("accepted field-path execution plan should complete the runner handoff");

    assert_eq!(report.store(), "test::mutation::by_name");
    assert_eq!(report.write_report().store(), report.store());
    assert_eq!(report.write_report().intended_entries(), 2);
    assert_eq!(report.validation().store(), report.store());
    assert_eq!(report.validation().entry_count(), 2);
    assert_eq!(report.validation().index_state(), IndexState::Building);
    assert_eq!(
        report.validation().store_visibility(),
        super::SchemaMutationStoreVisibility::StagedOnly,
    );
    assert_eq!(report.invalidation_report().invalidated_epochs(), 1);
    assert_eq!(
        report.publication_report().store_visibility(),
        super::SchemaMutationStoreVisibility::StagedOnly,
    );
    assert!(
        !report
            .publication_report()
            .runner_report()
            .physical_work_allows_publication(),
    );
    assert_eq!(report.published_store_report().store(), report.store());
    assert_eq!(report.published_store_report().entry_count(), 2);
    assert_eq!(
        report.published_store_report().index_state(),
        IndexState::Ready,
    );
    assert_eq!(
        report.published_store_report().store_visibility(),
        super::SchemaMutationStoreVisibility::Published,
    );
    assert_eq!(index_store.len(), 2);
    assert_eq!(index_store.state(), IndexState::Ready);
    assert_eq!(invalidation_sink.invalidations.len(), 1);
    assert_eq!(invalidation_sink.invalidations[0].0, report.store());
    assert_eq!(publication_sink.publications.len(), 1);
    assert_eq!(publication_sink.publications[0].0, report.store());
    assert_eq!(publication_sink.publications[0].1, after);
    assert_eq!(report.runner_report().rows_scanned(), 2);
    assert_eq!(report.runner_report().rows_skipped(), 0);
    assert_eq!(report.runner_report().index_keys_written(), 2);
    assert!(
        report
            .runner_report()
            .has_completed_phase(super::SchemaMutationRunnerPhase::PublishSnapshot),
    );
    assert!(
        report
            .runner_report()
            .has_completed_phase(super::SchemaMutationRunnerPhase::PublishPhysicalStore),
    );
    assert!(report.runner_report().physical_work_allows_publication());
    assert!(report.publication_readiness().allows_publication());
}

#[test]
fn field_path_runner_orchestrates_handoff_with_unrelated_index_entries() {
    let first = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Ada".to_string()))],
    };
    let second = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Grace".to_string()))],
    };
    let (before, after, execution_plan) = field_path_index_runner_context();
    let input = super::SchemaMutationRunnerInput::new(&before, &after, execution_plan)
        .expect("same-entity accepted snapshots should build runner input");
    let mut index_store = initialized_index_store(233);
    let other_index_id = IndexId::new(EntityTag::new(7), 99);
    let other_key = IndexKey::empty_with_kind(&other_index_id, IndexKeyKind::User).to_raw();
    let other_entry =
        RawIndexEntry::try_from_keys([StorageKey::Nat(99)]).expect("other entry should encode");
    index_store.insert(other_key, other_entry);
    let mut invalidation_sink = RecordingRuntimeInvalidationSink::default();
    let mut publication_sink = RecordingAcceptedSnapshotPublicationSink::default();

    let report = super::SchemaFieldPathIndexRunner::run(
        &input,
        EntityTag::new(7),
        accepted_name_field_path_target(),
        [
            super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Nat(2), &second),
            super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Nat(1), &first),
        ],
        &mut index_store,
        &mut invalidation_sink,
        &mut publication_sink,
    )
    .expect("target-scoped runner validation should ignore unrelated index entries");

    assert_eq!(report.validation().entry_count(), 2);
    assert_eq!(report.published_store_report().entry_count(), 2);
    assert_eq!(index_store.len(), 3);
    assert_eq!(index_store.state(), IndexState::Ready);
    assert!(report.runner_report().physical_work_allows_publication());
    assert!(report.publication_readiness().allows_publication());
}

#[test]
fn field_path_runner_rejects_target_mismatch_before_physical_work() {
    let mismatched_index = PersistedIndexSnapshot::new(
        9,
        "by_alias".to_string(),
        "test::mutation::by_alias".to_string(),
        false,
        PersistedIndexKeySnapshot::FieldPath(vec![name_key_path()]),
        Some("name IS NOT NULL".to_string()),
    );
    let request =
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&mismatched_index)
            .expect("mismatched field-path index should lower to a rebuild target");
    let SchemaMutationRequest::AddNonUniqueFieldPathIndex {
        target: mismatched_target,
    } = request
    else {
        panic!("field-path request should carry a rebuild target");
    };
    let (before, after, execution_plan) = field_path_index_runner_context();
    let input = super::SchemaMutationRunnerInput::new(&before, &after, execution_plan)
        .expect("same-entity accepted snapshots should build runner input");
    let mut index_store = initialized_index_store(236);
    let mut invalidation_sink = RecordingRuntimeInvalidationSink::default();
    let mut publication_sink = RecordingAcceptedSnapshotPublicationSink::default();

    let failure = super::SchemaFieldPathIndexRunner::run(
        &input,
        EntityTag::new(7),
        mismatched_target,
        std::iter::empty(),
        &mut index_store,
        &mut invalidation_sink,
        &mut publication_sink,
    )
    .expect_err("mismatched field-path target should reject before physical work");

    assert_eq!(
        failure.error(),
        super::SchemaFieldPathIndexRunnerError::TargetMismatch,
    );
    assert_eq!(failure.rollback_report(), None);
    assert_eq!(index_store.len(), 0);
    assert_eq!(index_store.state(), IndexState::Ready);
    assert!(invalidation_sink.invalidations.is_empty());
    assert!(publication_sink.publications.is_empty());
}

#[test]
fn field_path_runner_rejects_non_field_path_execution_plan_before_physical_work() {
    let before = base_snapshot();
    let after = snapshot_with_indexes(&before, vec![expression_name_index()]);
    let expression_plan =
        SchemaMutationRequest::from_accepted_expression_index(&expression_name_index())
            .expect("accepted expression index should lower")
            .lower_to_plan();
    let input =
        super::SchemaMutationRunnerInput::new(&before, &after, expression_plan.execution_plan())
            .expect("same-entity accepted snapshots should build runner input");
    let mut index_store = initialized_index_store(235);
    let mut invalidation_sink = RecordingRuntimeInvalidationSink::default();
    let mut publication_sink = RecordingAcceptedSnapshotPublicationSink::default();

    let failure = super::SchemaFieldPathIndexRunner::run(
        &input,
        EntityTag::new(7),
        accepted_name_field_path_target(),
        std::iter::empty(),
        &mut index_store,
        &mut invalidation_sink,
        &mut publication_sink,
    )
    .expect_err("non-field-path execution should reject before physical work");

    assert_eq!(
        failure.error(),
        super::SchemaFieldPathIndexRunnerError::UnsupportedExecutionPlan,
    );
    assert_eq!(failure.rollback_report(), None);
    assert_eq!(index_store.len(), 0);
    assert_eq!(index_store.state(), IndexState::Ready);
    assert!(invalidation_sink.invalidations.is_empty());
    assert!(publication_sink.publications.is_empty());
}

#[test]
fn field_path_runner_rolls_back_staged_writes_after_isolated_validation_failure() {
    let first = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Ada".to_string()))],
    };
    let second = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Grace".to_string()))],
    };
    let (before, after, execution_plan) = field_path_index_runner_context();
    let input = super::SchemaMutationRunnerInput::new(&before, &after, execution_plan)
        .expect("same-entity accepted snapshots should build runner input");
    let mut index_store = initialized_index_store(229);
    let extra_entry = extra_staged_name_index_entry();
    index_store.insert(extra_entry.key().clone(), extra_entry.entry().clone());
    let mut invalidation_sink = RecordingRuntimeInvalidationSink::default();
    let mut publication_sink = RecordingAcceptedSnapshotPublicationSink::default();

    let failure = super::SchemaFieldPathIndexRunner::run(
        &input,
        EntityTag::new(7),
        accepted_name_field_path_target(),
        [
            super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Nat(2), &second),
            super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Nat(1), &first),
        ],
        &mut index_store,
        &mut invalidation_sink,
        &mut publication_sink,
    )
    .expect_err("unexpected isolated store entries should fail validation and roll back writes");

    let rollback_report = failure
        .rollback_report()
        .expect("validation failure after writes should carry rollback diagnostics");
    assert_eq!(
        failure.error(),
        super::SchemaFieldPathIndexRunnerError::IsolatedStoreValidationFailed,
    );
    assert_eq!(rollback_report.store(), "test::mutation::by_name");
    assert_eq!(rollback_report.actions_applied(), 2);
    assert_eq!(rollback_report.restored_entries(), 0);
    assert_eq!(rollback_report.removed_entries(), 2);
    assert_eq!(
        rollback_report.store_visibility(),
        super::SchemaMutationStoreVisibility::StagedOnly,
    );
    assert_eq!(index_store.len(), 1);
    assert_eq!(index_store.state(), IndexState::Building);
    assert_eq!(
        index_store.get(extra_entry.key()),
        Some(extra_entry.entry().clone())
    );
    assert!(invalidation_sink.invalidations.is_empty());
    assert!(publication_sink.publications.is_empty());
}
