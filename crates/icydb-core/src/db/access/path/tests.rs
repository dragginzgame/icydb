use super::{
    SemanticIndexAccessContract, SemanticIndexAccessContractInner, SemanticIndexKeyItem,
    SemanticIndexKeyItemRef,
};
use crate::db::{index::SemanticIndexExpression, schema::PersistedIndexExpressionOp};
use std::sync::Arc;

#[test]
fn semantic_index_key_items_preserve_one_ordered_field_expression_contract() {
    let contract = SemanticIndexAccessContract {
        inner: Arc::new(SemanticIndexAccessContractInner {
            ordinal: 4,
            physical_generation: 7,
            name: "by_tenant_lower_email".to_string(),
            store_path: "ByTenantLowerEmail".to_string(),
            key_items: vec![
                SemanticIndexKeyItem::Field("tenant".to_string()),
                SemanticIndexKeyItem::Expression(SemanticIndexExpression::new(
                    PersistedIndexExpressionOp::Lower,
                    "email".to_string(),
                )),
            ],
            unique: false,
            predicate_semantics: None,
        }),
    };

    assert_eq!(contract.key_arity(), 2);
    assert_eq!(
        contract.key_item_at(0),
        Some(SemanticIndexKeyItemRef::Field("tenant"))
    );
    let Some(SemanticIndexKeyItemRef::AcceptedExpression(expression)) = contract.key_item_at(1)
    else {
        panic!("second semantic key item should remain an expression");
    };
    assert_eq!(expression.canonical_order_text(), "LOWER(email)");
    assert!(contract.has_expression_key_items());
}
