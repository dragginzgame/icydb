use super::*;
use crate::node::{Def, SchemaNode, Type};

#[test]
fn rejects_duplicate_type_names_across_type_kinds() {
    let mut schema = Schema::new();
    schema.insert_node(SchemaNode::Record(Record::new(
        Def::new("definition_name_test", "Shared"),
        "Shared",
        crate::node::FieldList::new(&[]),
        Type::new(&[], &[], &[]),
    )));
    schema.insert_node(SchemaNode::Tuple(Tuple::new(
        Def::new("definition_name_test_other", "Shared"),
        "Shared",
        &[],
        Type::new(&[], &[], &[]),
    )));

    let mut errors = ErrorTree::new();
    validate_type_names(&schema, &mut errors);
    let rendered = errors
        .result()
        .expect_err("duplicate type names must fail")
        .to_string();

    assert!(rendered.contains("duplicate type name 'Shared'"));
    assert!(rendered.contains("definition_name_test::Shared"));
    assert!(rendered.contains("definition_name_test_other::Shared"));
}
