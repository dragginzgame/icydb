use super::{FieldRef, FilterExpr};

#[test]
fn field_ref_text_starts_with_uses_strict_prefix_compare() {
    assert_eq!(
        FieldRef::new("name").text_starts_with("Al"),
        FilterExpr::starts_with("name", "Al"),
    );
}

#[test]
fn field_ref_text_starts_with_ci_uses_casefold_prefix_compare() {
    assert_eq!(
        FieldRef::new("name").text_starts_with_ci("AL"),
        FilterExpr::starts_with_ci("name", "AL"),
    );
}

#[test]
fn field_ref_gt_field_builds_field_compare_filter_expr() {
    assert_eq!(
        FieldRef::new("age").gt_field("rank"),
        FilterExpr::gt_field("age", "rank"),
    );
}

#[test]
fn field_ref_not_between_builds_outside_range_filter_expr() {
    assert_eq!(
        FieldRef::new("age").not_between(10_u64, 20_u64),
        FilterExpr::or(vec![
            FilterExpr::lt("age", 10_u64),
            FilterExpr::gt("age", 20_u64),
        ])
    );
}

#[test]
fn field_ref_between_fields_builds_field_bound_range_filter_expr() {
    assert_eq!(
        FieldRef::new("age").between_fields("min_age", "max_age"),
        FilterExpr::and(vec![
            FilterExpr::gte_field("age", "min_age"),
            FilterExpr::lte_field("age", "max_age"),
        ])
    );
}
