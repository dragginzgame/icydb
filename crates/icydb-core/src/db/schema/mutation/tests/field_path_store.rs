use super::*;

#[test]
fn field_path_rebuild_writer_reports_staged_write_intents_without_physical_mutation() {
    let plan =
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&non_unique_name_index())
            .expect("non-unique field-path index should lower")
            .lower_to_plan();
    let request =
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&non_unique_name_index())
            .expect("non-unique field-path index should lower to a rebuild target");
    let SchemaMutationRequest::AddNonUniqueFieldPathIndex { target } = request else {
        panic!("field-path index request should preserve rebuild target");
    };
    let first = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Ada".to_string()))],
    };
    let second = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Grace".to_string()))],
    };
    let staged = super::SchemaFieldPathIndexStagedRebuild::from_rows(
        "test::mutation::entity",
        EntityTag::new(7),
        target,
        [
            super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Uint(2), &second),
            super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Uint(1), &first),
        ],
    )
    .expect("field-path rebuild rows should stage into raw index entries");
    let buffer =
        super::SchemaFieldPathIndexStagedStore::from_rebuild(&staged, &plan.execution_plan())
            .expect("valid staged rebuild should write into an in-memory staged store buffer");
    let mut writer = RecordingStagedStoreWriter::default();

    let report = buffer.write_to(&mut writer);

    assert_eq!(report.store(), "test::mutation::by_name");
    assert_eq!(report.intended_entries(), 2);
    assert_eq!(
        report.store_visibility(),
        super::SchemaMutationStoreVisibility::StagedOnly,
    );
    assert_eq!(report.runner_report().rows_scanned(), 2);
    assert_eq!(report.runner_report().index_keys_written(), 2);
    assert!(!report.runner_report().physical_work_allows_publication());
    assert_eq!(writer.writes.len(), 2);
    for ((store, key, entry), staged_entry) in writer.writes.iter().zip(buffer.entries()) {
        assert_eq!(store, "test::mutation::by_name");
        assert_eq!(key, staged_entry.key());
        assert_eq!(entry, staged_entry.entry());
    }
    assert!(!buffer.physical_work_allows_publication());
}

#[test]
fn field_path_rebuild_write_batch_snapshots_physical_rollback_without_publication() {
    let plan =
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&non_unique_name_index())
            .expect("non-unique field-path index should lower")
            .lower_to_plan();
    let request =
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&non_unique_name_index())
            .expect("non-unique field-path index should lower to a rebuild target");
    let SchemaMutationRequest::AddNonUniqueFieldPathIndex { target } = request else {
        panic!("field-path index request should preserve rebuild target");
    };
    let first = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Ada".to_string()))],
    };
    let second = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Grace".to_string()))],
    };
    let staged = super::SchemaFieldPathIndexStagedRebuild::from_rows(
        "test::mutation::entity",
        EntityTag::new(7),
        target,
        [
            super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Uint(2), &second),
            super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Uint(1), &first),
        ],
    )
    .expect("field-path rebuild rows should stage into raw index entries");
    let buffer =
        super::SchemaFieldPathIndexStagedStore::from_rebuild(&staged, &plan.execution_plan())
            .expect("valid staged rebuild should write into an in-memory staged store buffer");
    let previous_entry =
        RawIndexEntry::try_from_keys([StorageKey::Uint(99)]).expect("previous entry should encode");
    let mut read_view = RecordingStagedStoreReadView::default();
    read_view.insert(
        buffer.store(),
        buffer.entries()[0].key().clone(),
        previous_entry.clone(),
    );

    let batch = buffer.write_batch(&read_view);

    assert_eq!(batch.store(), "test::mutation::by_name");
    assert_eq!(batch.entries(), buffer.entries());
    assert_eq!(batch.rollback_snapshots().len(), 2);
    assert_eq!(
        batch.store_visibility(),
        super::SchemaMutationStoreVisibility::StagedOnly,
    );
    assert_eq!(batch.runner_report().index_keys_written(), 2);
    assert_eq!(batch.rollback_snapshots()[0].store(), buffer.store());
    assert_eq!(
        batch.rollback_snapshots()[0].key(),
        buffer.entries()[0].key(),
    );
    assert_eq!(
        batch.rollback_snapshots()[0].previous_entry(),
        Some(&previous_entry),
    );
    assert_eq!(batch.rollback_snapshots()[1].store(), buffer.store());
    assert_eq!(
        batch.rollback_snapshots()[1].key(),
        buffer.entries()[1].key(),
    );
    assert_eq!(batch.rollback_snapshots()[1].previous_entry(), None);

    let mut writer = RecordingStagedStoreWriter::default();
    let report = batch.write_to(&mut writer);

    assert_eq!(report.intended_entries(), 2);
    assert_eq!(
        report.store_visibility(),
        super::SchemaMutationStoreVisibility::StagedOnly,
    );
    assert!(!report.runner_report().physical_work_allows_publication());
    assert_eq!(writer.writes.len(), 2);
}

#[test]
fn field_path_rebuild_write_batch_derives_reverse_rollback_plan() {
    let plan =
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&non_unique_name_index())
            .expect("non-unique field-path index should lower")
            .lower_to_plan();
    let request =
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&non_unique_name_index())
            .expect("non-unique field-path index should lower to a rebuild target");
    let SchemaMutationRequest::AddNonUniqueFieldPathIndex { target } = request else {
        panic!("field-path index request should preserve rebuild target");
    };
    let first = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Ada".to_string()))],
    };
    let second = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Grace".to_string()))],
    };
    let staged = super::SchemaFieldPathIndexStagedRebuild::from_rows(
        "test::mutation::entity",
        EntityTag::new(7),
        target,
        [
            super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Uint(2), &second),
            super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Uint(1), &first),
        ],
    )
    .expect("field-path rebuild rows should stage into raw index entries");
    let buffer =
        super::SchemaFieldPathIndexStagedStore::from_rebuild(&staged, &plan.execution_plan())
            .expect("valid staged rebuild should write into an in-memory staged store buffer");
    let previous_entry =
        RawIndexEntry::try_from_keys([StorageKey::Uint(99)]).expect("previous entry should encode");
    let mut read_view = RecordingStagedStoreReadView::default();
    read_view.insert(
        buffer.store(),
        buffer.entries()[0].key().clone(),
        previous_entry.clone(),
    );
    let batch = buffer.write_batch(&read_view);

    let rollback_plan = batch.rollback_plan();

    assert_eq!(rollback_plan.store(), "test::mutation::by_name");
    assert_eq!(rollback_plan.actions().len(), 2);
    assert_eq!(
        rollback_plan.store_visibility(),
        super::SchemaMutationStoreVisibility::StagedOnly,
    );
    assert_eq!(rollback_plan.runner_report().index_keys_written(), 2);
    assert_eq!(rollback_plan.actions()[0].store(), buffer.store());
    assert_eq!(rollback_plan.actions()[0].key(), buffer.entries()[1].key());
    assert_eq!(rollback_plan.actions()[0].restore_entry(), None);
    assert_eq!(rollback_plan.actions()[1].store(), buffer.store());
    assert_eq!(rollback_plan.actions()[1].key(), buffer.entries()[0].key());
    assert_eq!(
        rollback_plan.actions()[1].restore_entry(),
        Some(&previous_entry),
    );
    assert!(
        !rollback_plan
            .runner_report()
            .physical_work_allows_publication()
    );
}

#[test]
fn field_path_rebuild_rollback_plan_reports_mocked_restore_and_remove_actions() {
    let plan =
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&non_unique_name_index())
            .expect("non-unique field-path index should lower")
            .lower_to_plan();
    let request =
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&non_unique_name_index())
            .expect("non-unique field-path index should lower to a rebuild target");
    let SchemaMutationRequest::AddNonUniqueFieldPathIndex { target } = request else {
        panic!("field-path index request should preserve rebuild target");
    };
    let first = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Ada".to_string()))],
    };
    let second = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Grace".to_string()))],
    };
    let staged = super::SchemaFieldPathIndexStagedRebuild::from_rows(
        "test::mutation::entity",
        EntityTag::new(7),
        target,
        [
            super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Uint(2), &second),
            super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Uint(1), &first),
        ],
    )
    .expect("field-path rebuild rows should stage into raw index entries");
    let buffer =
        super::SchemaFieldPathIndexStagedStore::from_rebuild(&staged, &plan.execution_plan())
            .expect("valid staged rebuild should write into an in-memory staged store buffer");
    let previous_entry =
        RawIndexEntry::try_from_keys([StorageKey::Uint(99)]).expect("previous entry should encode");
    let mut read_view = RecordingStagedStoreReadView::default();
    read_view.insert(
        buffer.store(),
        buffer.entries()[0].key().clone(),
        previous_entry.clone(),
    );
    let rollback_plan = buffer.write_batch(&read_view).rollback_plan();
    let mut writer = RecordingStagedStoreRollbackWriter::default();

    let report = rollback_plan.rollback_to(&mut writer);

    assert_eq!(report.store(), "test::mutation::by_name");
    assert_eq!(report.actions_applied(), 2);
    assert_eq!(report.restored_entries(), 1);
    assert_eq!(report.removed_entries(), 1);
    assert_eq!(
        report.store_visibility(),
        super::SchemaMutationStoreVisibility::StagedOnly,
    );
    assert!(!report.runner_report().physical_work_allows_publication());
    assert_eq!(writer.actions.len(), 2);
    assert_eq!(writer.actions[0].0, buffer.store());
    assert_eq!(writer.actions[0].1, *buffer.entries()[1].key());
    assert_eq!(writer.actions[0].2, None);
    assert_eq!(writer.actions[1].0, buffer.store());
    assert_eq!(writer.actions[1].1, *buffer.entries()[0].key());
    assert_eq!(writer.actions[1].2, Some(previous_entry));
}

#[test]
fn field_path_rebuild_isolated_index_store_writer_writes_and_rolls_back() {
    let buffer = staged_name_index_store();
    let mut index_store = initialized_index_store(239);
    let mut writer =
        super::SchemaFieldPathIndexIsolatedIndexStoreWriter::new(buffer.store(), &mut index_store);

    let batch = buffer.write_batch(&writer);
    let write_report = batch.write_to(&mut writer);
    let validation = writer
        .validate_batch(&batch)
        .expect("isolated IndexStore should validate against staged batch");

    assert_eq!(writer.store(), buffer.store());
    assert_eq!(
        writer.store_visibility(),
        super::SchemaMutationStoreVisibility::StagedOnly
    );
    assert_eq!(writer.index_state(), IndexState::Building);
    assert_eq!(writer.len(), 2);
    assert_eq!(writer.generation(), writer.generation_before() + 2);
    assert_eq!(write_report.intended_entries(), 2);
    assert_eq!(
        write_report.store_visibility(),
        super::SchemaMutationStoreVisibility::StagedOnly,
    );
    assert!(
        !write_report
            .runner_report()
            .physical_work_allows_publication()
    );
    assert_eq!(
        writer.get(buffer.entries()[0].key()),
        Some(buffer.entries()[0].entry().clone()),
    );
    assert_eq!(
        writer.get(buffer.entries()[1].key()),
        Some(buffer.entries()[1].entry().clone()),
    );
    assert_eq!(validation.store(), buffer.store());
    assert_eq!(validation.entry_count(), 2);
    assert_eq!(validation.index_state(), IndexState::Building);
    assert_eq!(validation.generation_before(), writer.generation_before());
    assert_eq!(validation.generation_after(), writer.generation());
    assert_eq!(
        validation.store_visibility(),
        super::SchemaMutationStoreVisibility::StagedOnly,
    );
    let publication_readiness = validation.publication_readiness();
    assert_eq!(
        publication_readiness.blockers(),
        &[
            super::SchemaFieldPathIndexStagedStorePublicationBlocker::StoreStillStaged,
            super::SchemaFieldPathIndexStagedStorePublicationBlocker::RuntimeStateNotInvalidated,
            super::SchemaFieldPathIndexStagedStorePublicationBlocker::SnapshotNotPublished,
        ],
    );
    assert!(!publication_readiness.allows_publication());

    let rollback_report = batch.rollback_plan().rollback_to(&mut writer);

    assert_eq!(rollback_report.actions_applied(), 2);
    assert_eq!(rollback_report.removed_entries(), 2);
    assert_eq!(rollback_report.restored_entries(), 0);
    assert_eq!(writer.len(), 0);
    assert_eq!(writer.generation(), writer.generation_before() + 4);
    assert_eq!(writer.index_state(), IndexState::Building);
    assert!(
        !rollback_report
            .runner_report()
            .physical_work_allows_publication()
    );
}

#[test]
fn field_path_rebuild_isolated_index_store_validation_fails_closed() {
    let buffer = staged_name_index_store();
    let extra_entry = extra_staged_name_index_entry();
    let previous_entry =
        RawIndexEntry::try_from_keys([StorageKey::Uint(99)]).expect("previous entry should encode");
    let batch = buffer.write_batch(&super::SchemaFieldPathIndexStagedStoreOverlay::new(
        buffer.store(),
    ));

    let mut wrong_store = initialized_index_store(238);
    let writer =
        super::SchemaFieldPathIndexIsolatedIndexStoreWriter::new("wrong::store", &mut wrong_store);
    assert_eq!(
        writer.validate_batch(&batch),
        Err(super::SchemaFieldPathIndexIsolatedIndexStoreValidationError::StoreMismatch),
    );

    let mut published_store = initialized_index_store(237);
    let mut writer = super::SchemaFieldPathIndexIsolatedIndexStoreWriter::new(
        buffer.store(),
        &mut published_store,
    );
    writer.store_visibility = super::SchemaMutationStoreVisibility::Published;
    assert_eq!(
        writer.validate_batch(&batch),
        Err(super::SchemaFieldPathIndexIsolatedIndexStoreValidationError::PublishedVisibility),
    );

    let mut ready_store = initialized_index_store(236);
    let writer =
        super::SchemaFieldPathIndexIsolatedIndexStoreWriter::new(buffer.store(), &mut ready_store);
    writer.index_store.mark_ready();
    assert_eq!(
        writer.validate_batch(&batch),
        Err(super::SchemaFieldPathIndexIsolatedIndexStoreValidationError::StoreNotBuilding),
    );

    let mut partial_store = initialized_index_store(235);
    let writer = super::SchemaFieldPathIndexIsolatedIndexStoreWriter::new(
        buffer.store(),
        &mut partial_store,
    );
    assert_eq!(
        writer.validate_batch(&batch),
        Err(super::SchemaFieldPathIndexIsolatedIndexStoreValidationError::EntryCountMismatch),
    );

    let mut missing_store = initialized_index_store(234);
    let writer = super::SchemaFieldPathIndexIsolatedIndexStoreWriter::new(
        buffer.store(),
        &mut missing_store,
    );
    writer.index_store.insert(
        buffer.entries()[0].key().clone(),
        buffer.entries()[0].entry().clone(),
    );
    writer
        .index_store
        .insert(extra_entry.key().clone(), extra_entry.entry().clone());
    assert_eq!(
        writer.validate_batch(&batch),
        Err(super::SchemaFieldPathIndexIsolatedIndexStoreValidationError::MissingEntry),
    );

    let mut mismatch_store = initialized_index_store(233);
    let mut writer = super::SchemaFieldPathIndexIsolatedIndexStoreWriter::new(
        buffer.store(),
        &mut mismatch_store,
    );
    let _ = batch.write_to(&mut writer);
    writer
        .index_store
        .insert(buffer.entries()[0].key().clone(), previous_entry);
    assert_eq!(
        writer.validate_batch(&batch),
        Err(super::SchemaFieldPathIndexIsolatedIndexStoreValidationError::EntryMismatch),
    );
}

#[test]
fn field_path_rebuild_runtime_invalidation_records_epoch_handoff_without_publication() {
    let buffer = staged_name_index_store();
    let mut index_store = initialized_index_store(232);
    let mut writer =
        super::SchemaFieldPathIndexIsolatedIndexStoreWriter::new(buffer.store(), &mut index_store);
    let batch = buffer.write_batch(&writer);
    let _ = batch.write_to(&mut writer);
    let validation = writer
        .validate_batch(&batch)
        .expect("isolated IndexStore should validate before invalidation");
    let before = base_snapshot();
    let after = snapshot_with_indexes(&before, vec![non_unique_name_index()]);
    let plan =
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&non_unique_name_index())
            .expect("non-unique field-path index should lower")
            .lower_to_plan();
    let input = super::SchemaMutationRunnerInput::new(&before, &after, plan.execution_plan())
        .expect("same-entity accepted snapshots should build runner input");

    let invalidation_plan =
        super::SchemaFieldPathIndexRuntimeInvalidationPlan::from_isolated_index_store_validation(
            &validation,
            &input,
        )
        .expect("validated staged store should bind runtime invalidation epochs");
    let mut sink = RecordingRuntimeInvalidationSink::default();
    let report = invalidation_plan.invalidate_runtime_state(&mut sink);

    assert_eq!(invalidation_plan.store(), buffer.store());
    assert_eq!(invalidation_plan.entry_count(), 2);
    assert!(invalidation_plan.requires_invalidation());
    assert_eq!(
        invalidation_plan.store_visibility(),
        super::SchemaMutationStoreVisibility::StagedOnly,
    );
    assert_eq!(sink.invalidations.len(), 1);
    assert_eq!(sink.invalidations[0].0, buffer.store());
    assert_eq!(
        &sink.invalidations[0].1,
        invalidation_plan.publication_identity().before_epoch(),
    );
    assert_eq!(
        &sink.invalidations[0].2,
        invalidation_plan.publication_identity().after_epoch(),
    );
    assert_eq!(report.store(), buffer.store());
    assert_eq!(report.entry_count(), 2);
    assert_eq!(report.invalidated_epochs(), 1);
    assert_eq!(
        report.store_visibility(),
        super::SchemaMutationStoreVisibility::StagedOnly,
    );
    assert_eq!(
        report.publication_identity().visible_epoch(),
        invalidation_plan.publication_identity().before_epoch(),
    );
    assert!(
        report
            .runner_report()
            .has_completed_phase(super::SchemaMutationRunnerPhase::InvalidateRuntimeState),
    );
    assert!(
        !report
            .runner_report()
            .has_completed_phase(super::SchemaMutationRunnerPhase::PublishSnapshot),
    );
    assert!(!report.runner_report().physical_work_allows_publication());
    let readiness = report.publication_readiness();
    assert_eq!(
        readiness.blockers(),
        &[
            super::SchemaFieldPathIndexStagedStorePublicationBlocker::StoreStillStaged,
            super::SchemaFieldPathIndexStagedStorePublicationBlocker::SnapshotNotPublished,
        ],
    );
    assert!(!readiness.allows_publication());
}

#[test]
fn field_path_rebuild_snapshot_publication_handoff_reports_publishable_runner_state() {
    let validation = validated_isolated_name_index_store(231);
    let (before, after, execution_plan) = field_path_index_runner_context();
    let input = super::SchemaMutationRunnerInput::new(&before, &after, execution_plan)
        .expect("same-entity accepted snapshots should build runner input");
    let invalidation_plan =
        super::SchemaFieldPathIndexRuntimeInvalidationPlan::from_isolated_index_store_validation(
            &validation,
            &input,
        )
        .expect("validated staged store should bind runtime invalidation epochs");
    let mut invalidation_sink = RecordingRuntimeInvalidationSink::default();
    let invalidation_report = invalidation_plan.invalidate_runtime_state(&mut invalidation_sink);

    let publication_plan =
        super::SchemaFieldPathIndexSnapshotPublicationPlan::from_runtime_invalidation_report(
            &invalidation_report,
            &input,
        )
        .expect("runtime invalidation should allow snapshot publication planning");
    let mut publication_sink = RecordingAcceptedSnapshotPublicationSink::default();
    let publication_report = publication_plan.publish_snapshot(&mut publication_sink);

    assert_eq!(publication_plan.store(), validation.store());
    assert_eq!(publication_plan.entry_count(), validation.entry_count());
    assert_eq!(publication_plan.accepted_after(), &after);
    assert_eq!(publication_sink.publications.len(), 1);
    assert_eq!(publication_sink.publications[0].0, validation.store());
    assert_eq!(publication_sink.publications[0].1, after);
    assert_eq!(
        &publication_sink.publications[0].2,
        publication_plan.publication_identity().before_epoch(),
    );
    assert_eq!(
        &publication_sink.publications[0].3,
        publication_plan.publication_identity().after_epoch(),
    );
    assert_eq!(
        publication_report.store_visibility(),
        super::SchemaMutationStoreVisibility::Published,
    );
    assert_eq!(
        publication_report.publication_identity().visible_epoch(),
        publication_plan.publication_identity().after_epoch(),
    );
    assert_eq!(
        publication_report.publication_identity().published_epoch(),
        Some(publication_plan.publication_identity().after_epoch()),
    );
    assert_eq!(
        publication_report.accepted_after(),
        publication_plan.accepted_after()
    );
    assert!(
        publication_report
            .runner_report()
            .has_completed_phase(super::SchemaMutationRunnerPhase::PublishSnapshot),
    );
    assert!(
        publication_report
            .runner_report()
            .physical_work_allows_publication()
    );
    let readiness = publication_report.publication_readiness();
    assert!(readiness.blockers().is_empty());
    assert!(readiness.allows_publication());
}

#[test]
fn field_path_rebuild_staged_overlay_writes_and_rolls_back_without_index_store() {
    let buffer = staged_name_index_store();
    let previous_entry =
        RawIndexEntry::try_from_keys([StorageKey::Uint(99)]).expect("previous entry should encode");
    let mut overlay = super::SchemaFieldPathIndexStagedStoreOverlay::from_entries(
        buffer.store(),
        [(buffer.entries()[0].key().clone(), previous_entry.clone())],
    );

    let batch = buffer.write_batch(&overlay);
    let write_report = batch.write_to(&mut overlay);
    let overlay_validation = overlay
        .validate_batch(&batch)
        .expect("overlay should validate against the staged write batch");

    assert_eq!(overlay.store(), buffer.store());
    assert_eq!(overlay.len(), 2);
    assert_eq!(
        overlay.store_visibility(),
        super::SchemaMutationStoreVisibility::StagedOnly,
    );
    assert_eq!(write_report.intended_entries(), 2);
    assert_eq!(
        overlay.get(buffer.entries()[0].key()),
        Some(buffer.entries()[0].entry()),
    );
    assert_eq!(
        overlay.get(buffer.entries()[1].key()),
        Some(buffer.entries()[1].entry()),
    );
    assert_eq!(overlay_validation.store(), buffer.store());
    assert_eq!(overlay_validation.entry_count(), 2);
    assert_eq!(
        overlay_validation.store_visibility(),
        super::SchemaMutationStoreVisibility::StagedOnly,
    );
    assert_eq!(overlay_validation.runner_report().index_keys_written(), 2);
    assert!(
        !overlay_validation
            .runner_report()
            .physical_work_allows_publication()
    );
    let publication_readiness = overlay_validation.publication_readiness();
    assert_eq!(publication_readiness.store(), buffer.store());
    assert_eq!(publication_readiness.entry_count(), 2);
    assert_eq!(
        publication_readiness.blockers(),
        &[
            super::SchemaFieldPathIndexStagedStorePublicationBlocker::StoreStillStaged,
            super::SchemaFieldPathIndexStagedStorePublicationBlocker::RuntimeStateNotInvalidated,
            super::SchemaFieldPathIndexStagedStorePublicationBlocker::SnapshotNotPublished,
        ],
    );
    assert!(!publication_readiness.allows_publication());
    assert!(
        !publication_readiness
            .runner_report()
            .physical_work_allows_publication()
    );

    let rollback_report = batch.rollback_plan().rollback_to(&mut overlay);

    assert_eq!(rollback_report.actions_applied(), 2);
    assert_eq!(rollback_report.restored_entries(), 1);
    assert_eq!(rollback_report.removed_entries(), 1);
    assert_eq!(overlay.len(), 1);
    assert_eq!(
        overlay.get(buffer.entries()[0].key()),
        Some(&previous_entry)
    );
    assert_eq!(overlay.get(buffer.entries()[1].key()), None);
    assert!(
        !rollback_report
            .runner_report()
            .physical_work_allows_publication()
    );
}

#[test]
fn field_path_rebuild_staged_overlay_validation_fails_closed() {
    let buffer = staged_name_index_store();
    let extra_entry = extra_staged_name_index_entry();
    let previous_entry =
        RawIndexEntry::try_from_keys([StorageKey::Uint(99)]).expect("previous entry should encode");
    let batch = buffer.write_batch(&super::SchemaFieldPathIndexStagedStoreOverlay::new(
        buffer.store(),
    ));

    let wrong_store = super::SchemaFieldPathIndexStagedStoreOverlay::new("wrong::store");
    assert_eq!(
        wrong_store.validate_batch(&batch),
        Err(super::SchemaFieldPathIndexStagedStoreOverlayValidationError::StoreMismatch),
    );

    let mut published_overlay = super::SchemaFieldPathIndexStagedStoreOverlay::new(buffer.store());
    published_overlay.store_visibility = super::SchemaMutationStoreVisibility::Published;
    assert_eq!(
        published_overlay.validate_batch(&batch),
        Err(super::SchemaFieldPathIndexStagedStoreOverlayValidationError::PublishedVisibility),
    );

    let partial_overlay = super::SchemaFieldPathIndexStagedStoreOverlay::from_entries(
        buffer.store(),
        [(
            buffer.entries()[0].key().clone(),
            buffer.entries()[0].entry().clone(),
        )],
    );
    assert_eq!(
        partial_overlay.validate_batch(&batch),
        Err(super::SchemaFieldPathIndexStagedStoreOverlayValidationError::EntryCountMismatch),
    );

    let missing_entry_overlay = super::SchemaFieldPathIndexStagedStoreOverlay::from_entries(
        buffer.store(),
        [
            (
                buffer.entries()[0].key().clone(),
                buffer.entries()[0].entry().clone(),
            ),
            (extra_entry.key().clone(), extra_entry.entry().clone()),
        ],
    );
    assert_eq!(
        missing_entry_overlay.validate_batch(&batch),
        Err(super::SchemaFieldPathIndexStagedStoreOverlayValidationError::MissingEntry),
    );

    let mismatched_entry_overlay = super::SchemaFieldPathIndexStagedStoreOverlay::from_entries(
        buffer.store(),
        [
            (buffer.entries()[0].key().clone(), previous_entry),
            (
                buffer.entries()[1].key().clone(),
                buffer.entries()[1].entry().clone(),
            ),
        ],
    );
    assert_eq!(
        mismatched_entry_overlay.validate_batch(&batch),
        Err(super::SchemaFieldPathIndexStagedStoreOverlayValidationError::EntryMismatch),
    );
}
