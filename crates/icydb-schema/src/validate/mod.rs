//! Schema validation orchestration and shared helpers.

pub mod naming;
pub mod relation;
pub mod reserved;

use crate::{
    MAX_ENTITY_NAME_LEN, MAX_FIELD_NAME_LEN, MAX_INDEX_NAME_LEN,
    error::ErrorTree,
    node::{Schema, VisitableNode},
    visit::ValidateVisitor,
};
use reserved::WORDS;

/// Run full schema validation in a staged, deterministic order.
pub(crate) fn validate_schema(schema: &Schema) -> Result<(), ErrorTree> {
    // Phase 1: validate each node (structural + local invariants).
    let mut errors = validate_nodes(schema);

    // Phase 2: enforce schema-wide invariants.
    validate_global(schema, &mut errors);

    errors.result()
}

// Validate all nodes via a visitor to retain route-aware error aggregation.
fn validate_nodes(schema: &Schema) -> ErrorTree {
    let mut visitor = ValidateVisitor::new();
    schema.accept(&mut visitor);

    visitor.errors
}

// Run global validation passes that require a full schema view.
fn validate_global(schema: &Schema, errors: &mut ErrorTree) {
    naming::validate_entity_naming(schema, errors);
    relation::validate_same_canister_relations(schema, errors);
}

/// Ensure an identifier is non-empty and not a reserved keyword.
pub(crate) fn validate_ident(ident: &str) -> Result<(), String> {
    if ident.is_empty() {
        return Err("ident is empty".to_string());
    }

    // reserved?
    is_reserved(ident)?;

    Ok(())
}

/// Ensure entity names are non-empty, ASCII, and within the maximum length.
pub(crate) fn validate_entity_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("entity name is empty".to_string());
    }
    if name.len() > MAX_ENTITY_NAME_LEN {
        return Err(format!(
            "entity name '{name}' exceeds max length {MAX_ENTITY_NAME_LEN}"
        ));
    }
    if !name.is_ascii() {
        return Err(format!("entity name '{name}' must be ASCII"));
    }

    Ok(())
}

/// Ensure field names are within the maximum length.
pub(crate) fn validate_field_name_len(name: &str) -> Result<(), String> {
    if name.len() > MAX_FIELD_NAME_LEN {
        return Err(format!(
            "field name '{name}' exceeds max length {MAX_FIELD_NAME_LEN}"
        ));
    }

    Ok(())
}

pub(crate) fn validate_index_name_len(entity_name: &str, fields: &[&str]) -> Result<(), String> {
    let mut len = entity_name.len();
    for field in fields {
        len = len.saturating_add(1 + field.len());
    }

    if len > MAX_INDEX_NAME_LEN {
        return Err(format!(
            "index name '{entity_name}|{fields:?}' exceeds max length {MAX_INDEX_NAME_LEN}"
        ));
    }

    Ok(())
}

fn is_reserved(word: &str) -> Result<(), String> {
    if WORDS.contains(word) {
        return Err(format!("the word '{word}' is reserved"));
    }

    Ok(())
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_empty_and_reserved_words() {
        assert!(validate_ident("").is_err(), "empty identifiers should fail");
        assert!(
            validate_ident("record").is_err(),
            "reserved keywords should be rejected"
        );
    }

    #[test]
    fn accepts_non_reserved_identifier() {
        assert!(validate_ident("custom_ident").is_ok());
    }

    #[test]
    fn rejects_field_name_over_limit() {
        let long_name = "a".repeat(MAX_FIELD_NAME_LEN + 1);
        assert!(validate_field_name_len(&long_name).is_err());
    }

    #[test]
    fn accepts_index_name_at_max_len() {
        let entity = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let field = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let fields = [field, field, field, field];

        assert_eq!(
            entity.len() + fields.len() * (1 + field.len()),
            MAX_INDEX_NAME_LEN
        );
        assert!(validate_index_name_len(entity, &fields).is_ok());
    }

    #[test]
    fn rejects_index_name_over_max_len() {
        let entity = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let field = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let long_field = format!("{field}a");
        let fields = [long_field.as_str(), field, field, field];

        assert!(validate_index_name_len(entity, &fields).is_err());
    }
}
