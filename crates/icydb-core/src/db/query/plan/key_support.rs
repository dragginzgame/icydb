use crate::{
    db::{
        access::{SemanticIndexAccessContract, SemanticIndexKeyItemRef, SemanticIndexKeyItemsRef},
        predicate::CompareOp,
    },
    model::index::{IndexKeyItem, IndexKeyItemsRef},
};

pub(in crate::db::query::plan) fn field_key_contract_supports_operator(
    index_contract: &SemanticIndexAccessContract,
    field: &str,
    op: CompareOp,
) -> bool {
    if index_contract.has_expression_key_items() {
        return false;
    }
    if !contract_contains_field_key(index_contract, field) {
        return false;
    }

    matches!(
        op,
        CompareOp::Eq
            | CompareOp::In
            | CompareOp::Gt
            | CompareOp::Gte
            | CompareOp::Lt
            | CompareOp::Lte
            | CompareOp::StartsWith
    )
}

fn contract_contains_field_key(index_contract: &SemanticIndexAccessContract, field: &str) -> bool {
    match index_contract.key_items() {
        SemanticIndexKeyItemsRef::Fields(fields) => {
            fields.iter().any(|key_field| key_field == field)
        }
        SemanticIndexKeyItemsRef::Accepted(items) => items
            .iter()
            .any(|item| matches!(item.as_ref(), SemanticIndexKeyItemRef::Field(key_field) if key_field == field)),
        SemanticIndexKeyItemsRef::Static(IndexKeyItemsRef::Fields(fields)) => {
            fields.contains(&field)
        }
        SemanticIndexKeyItemsRef::Static(IndexKeyItemsRef::Items(items)) => items
            .iter()
            .any(|item| matches!(item, IndexKeyItem::Field(key_field) if key_field == &field)),
    }
}
