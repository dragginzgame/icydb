use super::*;

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
            super::SchemaExpressionIndexRebuildRow::new(PrimaryKeyComponent::Nat64(2), &second),
            super::SchemaExpressionIndexRebuildRow::new(PrimaryKeyComponent::Nat64(1), &first),
            super::SchemaExpressionIndexRebuildRow::new(PrimaryKeyComponent::Nat64(3), &skipped),
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
}

#[test]
fn expression_rebuild_validation_reports_staged_counts_without_publication() {
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
            super::SchemaExpressionIndexRebuildRow::new(PrimaryKeyComponent::Nat64(2), &second),
            super::SchemaExpressionIndexRebuildRow::new(PrimaryKeyComponent::Nat64(1), &first),
        ],
    )
    .expect("expression rebuild rows should stage into raw index entries");

    let validation = staged
        .validate()
        .expect("valid expression staged rebuild should validate");
    assert_eq!(validation.entry_count(), 2);
    assert_eq!(validation.source_rows(), 2);
    assert_eq!(validation.skipped_rows(), 0);
}

#[test]
fn expression_unique_rebuild_validation_rejects_duplicate_components() {
    let first = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Ada".to_string()))],
    };
    let duplicate = RebuildSlotReader {
        values: vec![None, Some(Value::Text("ada".to_string()))],
    };
    let staged = super::SchemaExpressionIndexStagedRebuild::from_rows(
        "test::mutation::entity",
        EntityTag::new(7),
        unique_lower_name_expression_target(),
        None,
        [
            super::SchemaExpressionIndexRebuildRow::new(PrimaryKeyComponent::Nat64(1), &first),
            super::SchemaExpressionIndexRebuildRow::new(PrimaryKeyComponent::Nat64(2), &duplicate),
        ],
    )
    .expect("unique expression rebuild rows should stage before validation");

    assert_eq!(staged.entries().len(), 2);
    assert_eq!(
        staged.validate(),
        Err(super::SchemaExpressionIndexStagedValidationError::DuplicateUniqueKey),
        "unique expression rebuild validation must reject duplicate indexed values before publication",
    );
}

#[test]
fn expression_unique_rebuild_validation_accepts_distinct_components() {
    let first = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Ada".to_string()))],
    };
    let second = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Grace".to_string()))],
    };
    let staged = super::SchemaExpressionIndexStagedRebuild::from_rows(
        "test::mutation::entity",
        EntityTag::new(7),
        unique_lower_name_expression_target(),
        None,
        [
            super::SchemaExpressionIndexRebuildRow::new(PrimaryKeyComponent::Nat64(2), &second),
            super::SchemaExpressionIndexRebuildRow::new(PrimaryKeyComponent::Nat64(1), &first),
        ],
    )
    .expect("distinct unique expression rebuild rows should stage");

    let validation = staged
        .validate()
        .expect("distinct unique expression values should validate");
    assert_eq!(validation.entry_count(), 2);
    assert_eq!(validation.source_rows(), 2);
    assert_eq!(validation.skipped_rows(), 0);
}
