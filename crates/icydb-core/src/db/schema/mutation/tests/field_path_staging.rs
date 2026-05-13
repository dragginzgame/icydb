use super::*;

#[test]
fn field_path_rebuild_key_materializes_from_accepted_target_slots() {
    let request =
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&non_unique_name_index())
            .expect("non-unique field-path index should lower to a rebuild target");
    let SchemaMutationRequest::AddNonUniqueFieldPathIndex { target } = request else {
        panic!("field-path index request should preserve rebuild target");
    };
    let slots = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Ada".to_string()))],
    };
    let storage_key = crate::db::data::StorageKey::Nat(42);

    let key = IndexKey::new_from_slots_with_field_path_rebuild_target(
        EntityTag::new(7),
        storage_key,
        &target,
        &slots,
    )
    .expect("accepted field-path target should build index key")
    .expect("text key component should be indexable");

    assert_eq!(key.index_id(), &IndexId::new(EntityTag::new(7), 1));
    assert_eq!(key.component_count(), 1);
    assert_eq!(
        key.primary_storage_key()
            .expect("index key should carry primary storage key"),
        storage_key,
    );
}

#[test]
fn field_path_rebuild_stages_sorted_entries_without_publication() {
    let request =
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&non_unique_name_index())
            .expect("non-unique field-path index should lower to a rebuild target");
    let SchemaMutationRequest::AddNonUniqueFieldPathIndex { target } = request else {
        panic!("field-path index request should preserve rebuild target");
    };
    let first = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Ada".to_string()))],
    };
    let skipped = RebuildSlotReader {
        values: vec![None, Some(Value::Null)],
    };
    let second = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Grace".to_string()))],
    };

    let staged = super::SchemaFieldPathIndexStagedRebuild::from_rows(
        "test::mutation::entity",
        EntityTag::new(7),
        target,
        [
            super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Nat(2), &second),
            super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Nat(1), &first),
            super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Nat(3), &skipped),
        ],
    )
    .expect("field-path rebuild rows should stage into raw index entries");

    assert_eq!(staged.target().name(), "by_name");
    assert_eq!(staged.source_rows(), 3);
    assert_eq!(staged.skipped_rows(), 1);
    assert_eq!(staged.entries().len(), 2);
    assert_eq!(
        staged.store_visibility(),
        super::SchemaMutationStoreVisibility::StagedOnly,
    );
    assert!(
        staged
            .entries()
            .windows(2)
            .all(|pair| pair[0].key() <= pair[1].key())
    );
    let staged_members = staged
        .entries()
        .iter()
        .map(|entry| {
            entry
                .entry()
                .try_decode()
                .expect("staged entry should decode")
                .iter_ids()
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    assert_eq!(
        staged_members,
        vec![vec![StorageKey::Nat(1)], vec![StorageKey::Nat(2)]],
    );

    let validation = staged
        .validate()
        .expect("fresh staged rebuild output should validate");
    assert_eq!(validation.entry_count(), 2);
    assert_eq!(validation.source_rows(), 3);
    assert_eq!(validation.skipped_rows(), 1);
    assert_eq!(
        validation.store_visibility(),
        super::SchemaMutationStoreVisibility::StagedOnly,
    );
}

#[test]
fn field_path_rebuild_validation_fails_closed_for_mutated_staged_state() {
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
            super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Nat(1), &first),
            super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Nat(2), &second),
        ],
    )
    .expect("field-path rebuild rows should stage into raw index entries");

    let mut duplicate = staged.clone();
    duplicate.entries[1] = duplicate.entries[0].clone();
    assert_eq!(
        duplicate.validate(),
        Err(super::SchemaFieldPathIndexStagedValidationError::UnsortedOrDuplicateEntries),
    );
    let plan =
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&non_unique_name_index())
            .expect("non-unique field-path index should lower to a rebuild target")
            .lower_to_plan();
    let rejection = duplicate
        .validated_runner_report(&plan.execution_plan())
        .expect_err("invalid staged state should reject runner reporting");
    assert_eq!(
        rejection.phase(),
        super::SchemaMutationRunnerPhase::ValidatePhysicalState,
    );
    assert_eq!(
        rejection.kind(),
        super::SchemaMutationRunnerRejectionKind::ValidationFailed,
    );
    assert_eq!(
        rejection.requirement(),
        Some(RebuildRequirement::IndexRebuildRequired),
    );

    let mut mismatched_count = staged.clone();
    mismatched_count.skipped_rows = 1;
    assert_eq!(
        mismatched_count.validate(),
        Err(super::SchemaFieldPathIndexStagedValidationError::EntryCountMismatch),
    );

    let mut published = staged;
    published.store_visibility = super::SchemaMutationStoreVisibility::Published;
    assert_eq!(
        published.validate(),
        Err(super::SchemaFieldPathIndexStagedValidationError::PublishedVisibility),
    );
}

#[test]
fn field_path_rebuild_validation_reports_runner_diagnostics_without_publication() {
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
    let skipped = RebuildSlotReader {
        values: vec![None, Some(Value::Null)],
    };
    let second = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Grace".to_string()))],
    };
    let staged = super::SchemaFieldPathIndexStagedRebuild::from_rows(
        "test::mutation::entity",
        EntityTag::new(7),
        target,
        [
            super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Nat(2), &second),
            super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Nat(3), &skipped),
            super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Nat(1), &first),
        ],
    )
    .expect("field-path rebuild rows should stage into raw index entries");

    let report = staged
        .validated_runner_report(&plan.execution_plan())
        .expect("valid staged rebuild output should produce runner diagnostics");

    assert_eq!(report.step_count(), 3);
    assert_eq!(
        report.required_capabilities(),
        &[
            super::SchemaMutationRunnerCapability::BuildFieldPathIndex,
            super::SchemaMutationRunnerCapability::ValidatePhysicalWork,
            super::SchemaMutationRunnerCapability::InvalidateRuntimeState,
        ],
    );
    assert_eq!(
        report.completed_phases(),
        &[
            super::SchemaMutationRunnerPhase::Preflight,
            super::SchemaMutationRunnerPhase::StageStores,
            super::SchemaMutationRunnerPhase::BuildPhysicalState,
            super::SchemaMutationRunnerPhase::ValidatePhysicalState,
        ],
    );
    assert_eq!(
        report.store_visibility(),
        Some(super::SchemaMutationStoreVisibility::StagedOnly),
    );
    assert_eq!(report.rows_scanned(), 3);
    assert_eq!(report.rows_skipped(), 1);
    assert_eq!(report.index_keys_written(), 2);
    assert!(report.has_completed_phase(super::SchemaMutationRunnerPhase::ValidatePhysicalState));
    assert!(!report.has_completed_phase(super::SchemaMutationRunnerPhase::InvalidateRuntimeState));
    assert!(!report.physical_work_allows_publication());
}

#[test]
fn field_path_rebuild_writes_validated_entries_to_staged_store_buffer() {
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
            super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Nat(2), &second),
            super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Nat(1), &first),
        ],
    )
    .expect("field-path rebuild rows should stage into raw index entries");

    let buffer =
        super::SchemaFieldPathIndexStagedStore::from_rebuild(&staged, &plan.execution_plan())
            .expect("valid staged rebuild should write into an in-memory staged store buffer");

    assert_eq!(buffer.store(), "test::mutation::by_name");
    assert_eq!(buffer.entries(), staged.entries());
    assert_eq!(buffer.validation().entry_count(), 2);
    assert_eq!(
        buffer.store_visibility(),
        super::SchemaMutationStoreVisibility::StagedOnly,
    );
    assert_eq!(buffer.report().rows_scanned(), 2);
    assert_eq!(buffer.report().index_keys_written(), 2);
    assert!(!buffer.physical_work_allows_publication());

    let discard = buffer.discard();
    assert_eq!(discard.store(), "test::mutation::by_name");
    assert_eq!(discard.discarded_entries(), 2);
    assert_eq!(
        discard.store_visibility(),
        super::SchemaMutationStoreVisibility::StagedOnly,
    );
}
