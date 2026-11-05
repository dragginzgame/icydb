use super::*;

#[test]
fn source_budget_matches_the_maintained_complete_domain_caps() {
    let mut rows = SchemaTransitionSourceBudget::standard();
    for _ in 0..MAX_SOURCE_ROWS {
        rows.consume_source_row(0)
            .expect("rows through the exact cap should admit");
    }
    assert_eq!(
        rows.consume_source_row(0),
        Err(SchemaTransitionBudgetResource::SourceRows),
    );

    let mut bytes = SchemaTransitionSourceBudget::standard();
    assert_eq!(
        bytes.consume_source_row(MAX_SOURCE_ROW_BYTES + 1),
        Err(SchemaTransitionBudgetResource::SourceRowBytes),
    );
}
