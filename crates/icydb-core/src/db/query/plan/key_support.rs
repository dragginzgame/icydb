use crate::db::{
    access::{SemanticIndexAccessContract, SemanticIndexKeyItemRef},
    predicate::CompareOp,
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
    index_contract
        .key_items()
        .iter()
        .any(|item| matches!(item.as_ref(), SemanticIndexKeyItemRef::Field(key_field) if key_field == field))
}
