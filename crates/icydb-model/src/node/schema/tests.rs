use crate::node::{Def, Schema, SchemaGraphError, SchemaNode, Validator};

fn validator(path: &'static str, ident: &'static str) -> SchemaNode {
    SchemaNode::Validator(Validator::new(Def::new(path, ident)))
}

#[test]
fn sealing_is_deterministic_and_idempotent() {
    let mut left = Schema::new();
    left.insert_node(validator("test::beta", "Beta"));
    left.insert_node(validator("test::alpha", "Alpha"));

    let mut right = Schema::new();
    right.insert_node(validator("test::alpha", "Alpha"));
    right.insert_node(validator("test::beta", "Beta"));

    let left_digest = left.seal().expect("left graph should seal");
    let right_digest = right.seal().expect("right graph should seal");

    assert_eq!(left_digest, right_digest);
    assert_eq!(
        left.seal().expect("sealed graph should reuse its digest"),
        left_digest,
    );
}

#[test]
fn sealing_digest_changes_with_graph_content() {
    let mut before = Schema::new();
    before.insert_node(validator("test", "Before"));

    let mut after = Schema::new();
    after.insert_node(validator("test", "After"));

    assert_ne!(
        before.seal().expect("before graph should seal"),
        after.seal().expect("after graph should seal"),
    );
}

#[test]
fn duplicate_registration_fails_without_replacing_the_first_node() {
    let mut schema = Schema::new();
    schema.insert_node(validator("test", "Duplicate"));
    schema.insert_node(validator("test", "Duplicate"));

    assert_eq!(
        schema.seal(),
        Err(SchemaGraphError::DuplicateRegistration(
            "test::Duplicate".to_string(),
        )),
    );
    assert_eq!(schema.nodes().len(), 1);
}

#[test]
fn duplicate_registration_diagnostic_is_constructor_order_independent() {
    let mut left = Schema::new();
    left.insert_node(validator("test", "Beta"));
    left.insert_node(validator("test", "Beta"));
    left.insert_node(validator("test", "Alpha"));
    left.insert_node(validator("test", "Alpha"));

    let mut right = Schema::new();
    right.insert_node(validator("test", "Alpha"));
    right.insert_node(validator("test", "Alpha"));
    right.insert_node(validator("test", "Beta"));
    right.insert_node(validator("test", "Beta"));

    let expected = Err(SchemaGraphError::DuplicateRegistration(
        "test::Alpha".to_string(),
    ));
    assert_eq!(left.seal(), expected);
    assert_eq!(right.seal(), expected);
}

#[test]
fn late_registration_fails_without_mutating_the_snapshot() {
    let mut schema = Schema::new();
    schema.insert_node(validator("test", "BeforeSeal"));
    let digest = schema.seal().expect("initial graph should seal");

    schema.insert_node(validator("test", "AfterSeal"));

    assert_eq!(
        schema.seal(),
        Err(SchemaGraphError::LateRegistration(
            "test::AfterSeal".to_string(),
        )),
    );
    assert_eq!(schema.digest(), Some(digest));
    assert_eq!(schema.nodes().len(), 1);
}
