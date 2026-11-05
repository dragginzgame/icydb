use super::Map;
use crate::prelude::*;

fn map_node() -> Map {
    Map {
        def: Def::new(
            syn::parse_quote!(
                struct TestMap;
            ),
            crate::crate_path::CratePaths::test(),
        ),
        name: None,
        key: Item {
            primitive: Some(Primitive::Text),
            unbounded: true,
            ..Default::default()
        },
        value: Value {
            item: Item {
                primitive: Some(Primitive::Nat32),
                ..Default::default()
            },
            ..Default::default()
        },
        ty: Type::default(),
        traits: TraitBuilder::default(),
    }
}

#[test]
fn map_value_relation_is_admitted() {
    let mut node = map_node();
    node.value.item.relation = Some(syn::parse_quote!(SomeEntity));

    node.validate()
        .expect("map relation values should validate");
}

#[test]
fn map_value_indirect_is_rejected() {
    let mut node = map_node();
    node.value.item.indirect = true;

    let err = node
        .validate()
        .expect_err("indirect map values should fail");
    assert!(
        err.to_string().contains("map value cannot be indirect"),
        "unexpected error: {err}"
    );
}
