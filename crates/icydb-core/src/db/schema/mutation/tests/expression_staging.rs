use super::*;

impl super::SchemaExpressionIndexStagedStoreReadView for RecordingStagedStoreReadView {
    fn read_staged_entry(&self, store: &str, key: &RawIndexKey) -> Option<RawIndexEntry> {
        self.entries.get(&(store.to_string(), key.clone())).cloned()
    }
}

impl super::SchemaExpressionIndexStagedStoreWriter for RecordingStagedStoreWriter {
    fn write_staged_entry(&mut self, store: &str, key: &RawIndexKey, entry: &RawIndexEntry) {
        self.writes
            .push((store.to_string(), key.clone(), entry.clone()));
    }
}

impl super::SchemaExpressionIndexStagedStoreRollbackWriter for RecordingStagedStoreRollbackWriter {
    fn restore_staged_entry(&mut self, store: &str, key: &RawIndexKey, entry: &RawIndexEntry) {
        self.actions
            .push((store.to_string(), key.clone(), Some(entry.clone())));
    }

    fn remove_staged_entry(&mut self, store: &str, key: &RawIndexKey) {
        self.actions.push((store.to_string(), key.clone(), None));
    }
}

#[test]
fn expression_rebuild_stages_sorted_entries_without_publication() {
    let first = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Ada".to_string()))],
    };
    let skipped = RebuildSlotReader {
        values: vec![None, Some(Value::Null)],
    };
    let second = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Grace".to_string()))],
    };

    let staged = super::SchemaExpressionIndexStagedRebuild::from_rows(
        "test::mutation::entity",
        EntityTag::new(7),
        accepted_lower_name_expression_target(),
        None,
        [
            super::SchemaExpressionIndexRebuildRow::new(StorageKey::Nat(2), &second),
            super::SchemaExpressionIndexRebuildRow::new(StorageKey::Nat(1), &first),
            super::SchemaExpressionIndexRebuildRow::new(StorageKey::Nat(3), &skipped),
        ],
    )
    .expect("expression rebuild rows should stage into raw index entries");

    assert_eq!(staged.target().name(), "by_lower_name");
    assert_eq!(staged.source_rows(), 3);
    assert_eq!(staged.skipped_rows(), 1);
    assert_eq!(staged.entries().len(), 2);
    assert!(
        staged
            .entries()
            .windows(2)
            .all(|pair| pair[0].key() < pair[1].key()),
        "staged expression entries should be raw-key sorted"
    );
    assert_eq!(
        staged.store_visibility(),
        super::SchemaMutationStoreVisibility::StagedOnly,
    );
}

#[test]
fn expression_rebuild_writer_reports_staged_write_intents_without_physical_mutation() {
    let buffer = staged_lower_name_expression_store();
    let mut writer = RecordingStagedStoreWriter::default();

    let report = buffer.write_to(&mut writer);

    assert_eq!(report.store(), "test::mutation::by_lower_name");
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
        assert_eq!(store, "test::mutation::by_lower_name");
        assert_eq!(key, staged_entry.key());
        assert_eq!(entry, staged_entry.entry());
    }
    assert!(!buffer.physical_work_allows_publication());
}

#[test]
fn expression_rebuild_write_batch_snapshots_physical_rollback_without_publication() {
    let buffer = staged_lower_name_expression_store();
    let previous_entry =
        RawIndexEntry::try_from_keys([StorageKey::Nat(99)]).expect("previous entry should encode");
    let mut read_view = RecordingStagedStoreReadView::default();
    read_view.insert(
        buffer.store(),
        buffer.entries()[0].key().clone(),
        previous_entry.clone(),
    );

    let batch = buffer.write_batch(&read_view);

    assert_eq!(batch.store(), "test::mutation::by_lower_name");
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
}

#[test]
fn expression_rebuild_write_batch_derives_reverse_rollback_plan() {
    let buffer = staged_lower_name_expression_store();
    let previous_entry =
        RawIndexEntry::try_from_keys([StorageKey::Nat(99)]).expect("previous entry should encode");
    let mut read_view = RecordingStagedStoreReadView::default();
    read_view.insert(
        buffer.store(),
        buffer.entries()[0].key().clone(),
        previous_entry.clone(),
    );
    let batch = buffer.write_batch(&read_view);

    let rollback_plan = batch.rollback_plan();
    let mut writer = RecordingStagedStoreRollbackWriter::default();
    let report = rollback_plan.rollback_to(&mut writer);

    assert_eq!(rollback_plan.store(), "test::mutation::by_lower_name");
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
    assert_eq!(report.store(), "test::mutation::by_lower_name");
    assert_eq!(report.actions_applied(), 2);
    assert_eq!(report.restored_entries(), 1);
    assert_eq!(report.removed_entries(), 1);
    assert_eq!(writer.actions.len(), 2);
    assert_eq!(writer.actions[0].0, buffer.store());
    assert_eq!(writer.actions[0].1, *buffer.entries()[1].key());
    assert_eq!(writer.actions[0].2, None);
    assert_eq!(writer.actions[1].0, buffer.store());
    assert_eq!(writer.actions[1].1, *buffer.entries()[0].key());
    assert_eq!(writer.actions[1].2, Some(previous_entry));
}

fn staged_lower_name_expression_store() -> super::SchemaExpressionIndexStagedStore {
    let plan = SchemaMutationRequest::from_accepted_expression_index(&expression_name_index())
        .expect("accepted expression index should lower")
        .lower_to_plan();
    let first = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Ada".to_string()))],
    };
    let second = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Grace".to_string()))],
    };
    let staged = super::SchemaExpressionIndexStagedRebuild::from_rows(
        "test::mutation::entity",
        EntityTag::new(7),
        accepted_lower_name_expression_target(),
        None,
        [
            super::SchemaExpressionIndexRebuildRow::new(StorageKey::Nat(2), &second),
            super::SchemaExpressionIndexRebuildRow::new(StorageKey::Nat(1), &first),
        ],
    )
    .expect("expression rebuild rows should stage into raw index entries");

    super::SchemaExpressionIndexStagedStore::from_rebuild(&staged, &plan.execution_plan())
        .expect("valid staged expression rebuild should write into a staged store buffer")
}

#[test]
fn expression_rebuild_validation_reports_runner_diagnostics_without_publication() {
    let plan = SchemaMutationRequest::from_accepted_expression_index(&expression_name_index())
        .expect("accepted expression index should lower")
        .lower_to_plan();
    let first = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Ada".to_string()))],
    };
    let second = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Grace".to_string()))],
    };
    let staged = super::SchemaExpressionIndexStagedRebuild::from_rows(
        "test::mutation::entity",
        EntityTag::new(7),
        accepted_lower_name_expression_target(),
        None,
        [
            super::SchemaExpressionIndexRebuildRow::new(StorageKey::Nat(2), &second),
            super::SchemaExpressionIndexRebuildRow::new(StorageKey::Nat(1), &first),
        ],
    )
    .expect("expression rebuild rows should stage into raw index entries");

    let validation = staged
        .validate()
        .expect("valid expression staged rebuild should validate");
    let report = staged
        .validated_runner_report(&plan.execution_plan())
        .expect("validated expression staged rebuild should produce runner diagnostics");

    assert_eq!(validation.entry_count(), 2);
    assert_eq!(validation.source_rows(), 2);
    assert_eq!(validation.skipped_rows(), 0);
    assert_eq!(
        validation.store_visibility(),
        super::SchemaMutationStoreVisibility::StagedOnly,
    );
    assert_eq!(report.rows_scanned(), 2);
    assert_eq!(report.rows_skipped(), 0);
    assert_eq!(report.index_keys_written(), 2);
    assert_eq!(
        report.required_capabilities(),
        &[
            super::SchemaMutationRunnerCapability::BuildExpressionIndex,
            super::SchemaMutationRunnerCapability::ValidatePhysicalWork,
            super::SchemaMutationRunnerCapability::InvalidateRuntimeState,
        ],
    );
    assert!(
        !report.physical_work_allows_publication(),
        "staged expression rebuilds should not become publishable before runner publication phases"
    );
}
