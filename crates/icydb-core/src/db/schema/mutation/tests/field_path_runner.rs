use super::*;

#[test]
fn field_path_runner_marks_validated_physical_work_ready() {
    let first = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Ada".to_string()))],
    };
    let second = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Grace".to_string()))],
    };
    let (before, after, mutation_plan) = field_path_index_runner_context();
    let input = super::SchemaMutationRunnerInput::new(&before, &after, mutation_plan)
        .expect("same-entity accepted snapshots should build runner input");
    let mut index_store = initialized_index_store(234);

    let report = super::SchemaFieldPathIndexRunner::run(
        &input,
        EntityTag::new(7),
        accepted_name_field_path_target(),
        None,
        [
            super::SchemaFieldPathIndexRebuildRow::new(PrimaryKeyComponent::Nat64(2), &second),
            super::SchemaFieldPathIndexRebuildRow::new(PrimaryKeyComponent::Nat64(1), &first),
        ],
        &mut index_store,
    )
    .expect("accepted field-path mutation plan should complete physical work");

    assert_eq!(report.store(), "test::mutation::by_name");
    assert_eq!(report.validation().store(), report.store());
    assert_eq!(report.validation().entry_count(), 2);
    assert_eq!(report.validation().index_state(), IndexState::Building);
    assert_eq!(report.ready_store_report().store(), report.store());
    assert_eq!(report.ready_store_report().entry_count(), 2);
    assert_eq!(report.ready_store_report().index_state(), IndexState::Ready);
    assert_eq!(index_store.len(), 2);
    assert_eq!(index_store.state(), IndexState::Ready);
    assert_eq!(report.staged_validation().source_rows(), 2);
    assert_eq!(report.staged_validation().skipped_rows(), 0);
    assert_eq!(report.staged_validation().entry_count(), 2);

    assert_field_path_success_metrics(&report);
}

#[test]
fn field_path_runner_preserves_unrelated_index_entries() {
    let first = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Ada".to_string()))],
    };
    let second = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Grace".to_string()))],
    };
    let (before, after, mutation_plan) = field_path_index_runner_context();
    let input = super::SchemaMutationRunnerInput::new(&before, &after, mutation_plan)
        .expect("same-entity accepted snapshots should build runner input");
    let mut index_store = initialized_index_store(233);
    let other_index_id = IndexId::new(EntityTag::new(7), 99);
    let other_key = IndexKey::empty_with_kind(&other_index_id, IndexKeyKind::User)
        .to_raw()
        .expect("test index key should encode");
    index_store.insert(other_key, IndexEntryValue::presence());

    let report = super::SchemaFieldPathIndexRunner::run(
        &input,
        EntityTag::new(7),
        accepted_name_field_path_target(),
        None,
        [
            super::SchemaFieldPathIndexRebuildRow::new(PrimaryKeyComponent::Nat64(2), &second),
            super::SchemaFieldPathIndexRebuildRow::new(PrimaryKeyComponent::Nat64(1), &first),
        ],
        &mut index_store,
    )
    .expect("target-scoped validation should ignore unrelated index entries");

    assert_eq!(report.validation().entry_count(), 2);
    assert_eq!(report.ready_store_report().entry_count(), 2);
    assert_eq!(index_store.len(), 3);
    assert_eq!(index_store.state(), IndexState::Ready);
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
    let request = SchemaMutationRequest::from_accepted_field_path_index(&mismatched_index)
        .expect("mismatched field-path index should lower to a rebuild target");
    let SchemaMutationRequest::AddFieldPathIndex {
        target: mismatched_target,
    } = request
    else {
        panic!("field-path request should carry a rebuild target");
    };
    let (before, after, mutation_plan) = field_path_index_runner_context();
    let input = super::SchemaMutationRunnerInput::new(&before, &after, mutation_plan)
        .expect("same-entity accepted snapshots should build runner input");
    let mut index_store = initialized_index_store(236);

    let failure = super::SchemaFieldPathIndexRunner::run(
        &input,
        EntityTag::new(7),
        mismatched_target,
        None,
        std::iter::empty(),
        &mut index_store,
    )
    .expect_err("mismatched field-path target should reject before physical work");

    assert_eq!(
        failure,
        super::SchemaFieldPathIndexRunnerError::TargetMismatch
    );
    assert_eq!(index_store.len(), 0);
    assert_eq!(index_store.state(), IndexState::Ready);
    assert_eq!(
        failure.into_internal_error().class(),
        crate::error::ErrorClass::InvariantViolation,
    );
}

#[test]
fn field_path_runner_rejects_non_field_path_plan_before_physical_work() {
    let before = base_snapshot();
    let after = snapshot_with_indexes(&before, vec![expression_name_index()]);
    let expression_plan: MutationPlan =
        SchemaMutationRequest::from_accepted_expression_index(&expression_name_index())
            .expect("accepted expression index should lower")
            .into();
    let input = super::SchemaMutationRunnerInput::new(&before, &after, expression_plan)
        .expect("same-entity accepted snapshots should build runner input");
    let mut index_store = initialized_index_store(235);

    let failure = super::SchemaFieldPathIndexRunner::run(
        &input,
        EntityTag::new(7),
        accepted_name_field_path_target(),
        None,
        std::iter::empty(),
        &mut index_store,
    )
    .expect_err("non-field-path execution should reject before physical work");

    assert_eq!(
        failure,
        super::SchemaFieldPathIndexRunnerError::UnsupportedMutationPlan,
    );
    assert_eq!(index_store.len(), 0);
    assert_eq!(index_store.state(), IndexState::Ready);
    assert_eq!(
        failure.into_internal_error().class(),
        crate::error::ErrorClass::Unsupported,
    );
}

#[test]
fn field_path_runner_rolls_back_validation_failure_and_restores_ready_state() {
    let first = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Ada".to_string()))],
    };
    let second = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Grace".to_string()))],
    };
    let (before, after, mutation_plan) = field_path_index_runner_context();
    let input = super::SchemaMutationRunnerInput::new(&before, &after, mutation_plan)
        .expect("same-entity accepted snapshots should build runner input");
    let mut index_store = initialized_index_store(229);
    let extra_entry = extra_staged_name_index_entry();
    index_store.insert(extra_entry.key().clone(), extra_entry.entry().clone());

    let failure = super::SchemaFieldPathIndexRunner::run(
        &input,
        EntityTag::new(7),
        accepted_name_field_path_target(),
        None,
        [
            super::SchemaFieldPathIndexRebuildRow::new(PrimaryKeyComponent::Nat64(2), &second),
            super::SchemaFieldPathIndexRebuildRow::new(PrimaryKeyComponent::Nat64(1), &first),
        ],
        &mut index_store,
    )
    .expect_err("unexpected target entries should fail validation and roll back writes");

    assert_eq!(
        failure,
        super::SchemaFieldPathIndexRunnerError::IsolatedStoreValidationFailed,
    );
    assert_eq!(index_store.len(), 1);
    assert_eq!(index_store.state(), IndexState::Ready);
    assert_eq!(
        index_store.get(extra_entry.key()),
        Some(extra_entry.entry().clone())
    );
}

#[test]
fn field_path_runner_report_can_roll_back_later_publication_failure() {
    let row = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Ada".to_string()))],
    };
    let (before, after, mutation_plan) = field_path_index_runner_context();
    let input = super::SchemaMutationRunnerInput::new(&before, &after, mutation_plan)
        .expect("same-entity accepted snapshots should build runner input");
    let mut index_store = initialized_index_store(228);

    let report = super::SchemaFieldPathIndexRunner::run(
        &input,
        EntityTag::new(7),
        accepted_name_field_path_target(),
        None,
        [super::SchemaFieldPathIndexRebuildRow::new(
            PrimaryKeyComponent::Nat64(1),
            &row,
        )],
        &mut index_store,
    )
    .expect("physical work should succeed before publication");
    assert_eq!(index_store.len(), 1);

    report.rollback_physical_work(&mut index_store);

    assert_eq!(index_store.len(), 0);
    assert_eq!(index_store.state(), IndexState::Ready);
}

fn assert_field_path_success_metrics(report: &super::SchemaFieldPathIndexRunnerReport) {
    let metrics = report.mutation_metrics("test::MutationEntity");
    assert_eq!(metrics.entity_path(), "test::MutationEntity");
    assert_eq!(metrics.rows_scanned(), 2);
    assert_eq!(metrics.index_keys_written(), 2);
}
