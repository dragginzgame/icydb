//! Generated checks and filtered indexes share total Boolean-test semantics.

use icydb_model::{build::get_schema, node::Entity, prelude::*};
use icydb_schema::{ScalarLiteral, SourceCheckExpr, SourceCheckInstruction};

#[canister(memory_namespace = "boolean_truth")]
pub struct TruthCanister;

#[store(canister = "TruthCanister", storage(journaled(key = "main")))]
pub struct TruthStore;

#[entity(
    store = "TruthStore",
    version = 1,
    pk(field = "id"),
    constraint(name = "is_true", check = "flag IS TRUE"),
    constraint(name = "is_false", check = "flag IS FALSE"),
    constraint(name = "not_true", check = "flag IS NOT TRUE"),
    constraint(name = "not_false", check = "flag IS NOT FALSE"),
    index(field = "a", predicate = "flag IS TRUE"),
    index(field = "b", predicate = "flag IS FALSE"),
    index(field = "c", predicate = "flag IS NOT TRUE"),
    index(field = "d", predicate = "flag IS NOT FALSE"),
    fields(
        field(name = "id", value(item(prim = "Ulid"))),
        field(name = "flag", value(opt, item(prim = "Bool"))),
        field(name = "a", value(item(prim = "Nat64"))),
        field(name = "b", value(item(prim = "Nat64"))),
        field(name = "c", value(item(prim = "Nat64"))),
        field(name = "d", value(item(prim = "Nat64")))
    )
)]
pub struct TruthRow;

// Interpret the documented public Boolean instruction semantics independently
// of macro parsing. Equivalent generated programs may use different layouts.
fn evaluate(expression: &SourceCheckExpr, flag: Option<bool>) -> Option<bool> {
    use SourceCheckInstruction as I;

    let mut stack = Vec::new();
    for instruction in expression.instructions() {
        match instruction {
            I::Field(field) => {
                assert_eq!(field.as_str(), "flag");
                stack.push(flag);
            }
            I::Literal(ScalarLiteral::Bool(value)) => stack.push(Some(*value)),
            I::Equal | I::NotEqual | I::And | I::Or => {
                let right = stack.pop().unwrap();
                let left = stack.pop().unwrap();
                let truth = match instruction {
                    I::Equal => left.zip(right).map(|(a, b)| a == b),
                    I::NotEqual => left.zip(right).map(|(a, b)| a != b),
                    I::And if left == Some(false) || right == Some(false) => Some(false),
                    I::Or if left == Some(true) || right == Some(true) => Some(true),
                    I::And => left.zip(right).map(|(a, b)| a && b),
                    I::Or => left.zip(right).map(|(a, b)| a || b),
                    _ => unreachable!("matched Boolean binary instruction"),
                };
                stack.push(truth);
            }
            I::Not | I::IsNull | I::IsNotNull => {
                let value = stack.pop().unwrap();
                stack.push(match instruction {
                    I::Not => value.map(|value| !value),
                    I::IsNull => Some(value.is_none()),
                    I::IsNotNull => Some(value.is_some()),
                    _ => unreachable!("matched Boolean unary instruction"),
                });
            }
            other => panic!("unexpected Boolean fixture instruction: {other:?}"),
        }
    }
    assert_eq!(stack.len(), 1);
    stack.pop().unwrap()
}

#[test]
fn generated_boolean_tests_preserve_totality_in_public_source_programs() {
    let schema = get_schema().expect("Boolean-test declarations should seal");
    let (_, entity) = schema
        .get_nodes::<Entity>()
        .find(|(_, entity)| entity.def().ident() == "TruthRow")
        .expect("generated entity should register");
    for (name, sql, expected) in [
        ("is_true", "flag IS TRUE", [true, false, false]),
        ("is_false", "flag IS FALSE", [false, true, false]),
        ("not_true", "flag IS NOT TRUE", [false, true, true]),
        ("not_false", "flag IS NOT FALSE", [true, false, true]),
    ] {
        let check = entity
            .constraints()
            .iter()
            .find(|check| check.name() == name)
            .unwrap()
            .source_expression(&schema)
            .unwrap();
        let index = entity
            .indexes()
            .iter()
            .find(|index| index.predicate() == Some(sql))
            .unwrap()
            .source_predicate(&schema)
            .unwrap()
            .unwrap();
        for (value, expected) in [Some(true), Some(false), None].into_iter().zip(expected) {
            assert_eq!(
                evaluate(&check, value),
                Some(expected),
                "CHECK {sql} for {value:?}"
            );
            assert_eq!(
                evaluate(&index, value),
                Some(expected),
                "index {sql} for {value:?}"
            );
        }
    }
    drop(schema);
}
