use crate::node::index::{Index, IndexExpression, IndexKeyItem, IndexKeyItemsRef};

#[test]
fn index_with_predicate_reports_conditional_shape() {
    let index = Index::new_with_predicate(
        "idx_user__email",
        &["email"],
        false,
        Some("active = true"),
        None,
    );

    assert_eq!(index.predicate(), Some("active = true"));
    assert_eq!(index.to_string(), "(email) WHERE active = true");
}

#[test]
fn index_without_predicate_preserves_unconditional_shape() {
    let index = Index::new("uidx_user__email", &["email"], true);

    assert_eq!(index.predicate(), None);
    assert_eq!(index.to_string(), "UNIQUE (email)");
}

#[test]
fn index_with_explicit_key_items_exposes_expression_items() {
    static KEY_ITEMS: [IndexKeyItem; 2] = [
        IndexKeyItem::Field("tenant_id"),
        IndexKeyItem::Expression(IndexExpression::Lower("email")),
    ];
    let index = Index::new_with_key_items(
        "idx_user__tenant_id__lower_email",
        &["tenant_id"],
        &KEY_ITEMS,
        false,
    );

    assert!(index.has_expression_key_items());
    assert_eq!(index.to_string(), "(tenant_id, LOWER(email))");
    std::assert_matches!(
        index.key_items(),
        IndexKeyItemsRef::Items(items)
            if items == KEY_ITEMS.as_slice()
    );
}
