use super::*;

#[test]
fn update_selector_primary_key_order_fallback_appends_missing_composite_components() {
    let mut order_by = vec![SqlOrderTerm {
        field: SqlExpr::Field("tenant_id".to_string()),
        direction: SqlOrderDirection::Asc,
    }];
    let primary_key_names = vec!["tenant_id".to_string(), "local_id".to_string()];

    append_primary_key_order_fallback(&mut order_by, &primary_key_names);

    assert_eq!(order_by.len(), 2);
    assert_eq!(order_by[0].direct_field_name(), Some("tenant_id"));
    assert_eq!(order_by[1].direct_field_name(), Some("local_id"));
}
