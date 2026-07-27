use crate::{
    node::{Entity, Enum, List, Map, Newtype, Record, Schema, Set, Tuple},
    prelude::*,
};
use std::collections::BTreeMap;

/// Enforce graph-wide uniqueness in each top-level source-identity namespace.
pub(super) fn validate_definition_source_keys(schema: &Schema, errors: &mut ErrorTree) {
    let mut entities = BTreeMap::new();
    for (path, entity) in schema.get_nodes::<Entity>() {
        record_unique_key(&mut entities, entity.source_key(), path, "entity", errors);
    }

    let mut types = BTreeMap::new();
    for (path, node) in schema.get_nodes::<Record>() {
        record_unique_key(&mut types, node.source_key(), path, "type", errors);
    }
    for (path, node) in schema.get_nodes::<Enum>() {
        record_unique_key(&mut types, node.source_key(), path, "type", errors);
    }
    for (path, node) in schema.get_nodes::<Newtype>() {
        record_unique_key(&mut types, node.source_key(), path, "type", errors);
    }
    for (path, node) in schema.get_nodes::<List>() {
        record_unique_key(&mut types, node.source_key(), path, "type", errors);
    }
    for (path, node) in schema.get_nodes::<Set>() {
        record_unique_key(&mut types, node.source_key(), path, "type", errors);
    }
    for (path, node) in schema.get_nodes::<Map>() {
        record_unique_key(&mut types, node.source_key(), path, "type", errors);
    }
    for (path, node) in schema.get_nodes::<Tuple>() {
        record_unique_key(&mut types, node.source_key(), path, "type", errors);
    }
}

fn record_unique_key<'a>(
    seen: &mut BTreeMap<&'a str, &'a str>,
    source_key: &'a str,
    path: &'a str,
    kind: &str,
    errors: &mut ErrorTree,
) {
    if let Some(previous_path) = seen.insert(source_key, path) {
        err!(
            errors,
            "duplicate {kind} source key '{source_key}' for '{previous_path}' and '{path}'",
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::{Def, SchemaNode, Type};

    #[test]
    fn rejects_duplicate_entity_source_keys() {
        let mut schema = Schema::new();
        for ident in ["One", "Two"] {
            schema.insert_node(SchemaNode::Entity(Entity::new(
                Def::new("source_key_test", ident),
                "entity/shared",
                "source_key_test::Store",
                1,
                crate::node::PrimaryKey::new(&["id"], crate::node::PrimaryKeySource::Internal),
                Some(ident),
                &[],
                &[],
                &[],
                crate::node::FieldList::new(&[]),
                Type::new(&[], &[]),
            )));
        }

        let mut errors = ErrorTree::new();
        validate_definition_source_keys(&schema, &mut errors);
        let rendered = errors
            .result()
            .expect_err("duplicate entity source identities must fail")
            .to_string();

        assert!(rendered.contains("duplicate entity source key 'entity/shared'"));
        assert!(rendered.contains("source_key_test::One"));
        assert!(rendered.contains("source_key_test::Two"));
    }

    #[test]
    fn rejects_duplicate_type_source_keys_across_type_kinds() {
        let mut schema = Schema::new();
        schema.insert_node(SchemaNode::Record(Record::new(
            Def::new("source_key_test", "Names"),
            "type/shared",
            crate::node::FieldList::new(&[]),
            Type::new(&[], &[]),
        )));
        schema.insert_node(SchemaNode::Tuple(Tuple::new(
            Def::new("source_key_test", "Pair"),
            "type/shared",
            &[],
            Type::new(&[], &[]),
        )));

        let mut errors = ErrorTree::new();
        validate_definition_source_keys(&schema, &mut errors);
        let rendered = errors
            .result()
            .expect_err("duplicate type source identities must fail")
            .to_string();

        assert!(rendered.contains("duplicate type source key 'type/shared'"));
        assert!(rendered.contains("source_key_test::Names"));
        assert!(rendered.contains("source_key_test::Pair"));
    }
}
