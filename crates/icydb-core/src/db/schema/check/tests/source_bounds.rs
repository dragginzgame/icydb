//! Source instructions must reach accepted bounds without first building a deep input tree.

use super::*;
use crate::types::EntityTag;
use icydb_schema::{
    FieldSourceKey, MAX_SOURCE_CHECK_INSTRUCTIONS, SourceCheckExpr, SourceCheckInstruction,
};

fn bind_source(
    instructions: Vec<SourceCheckInstruction>,
) -> Result<AcceptedCheckExprV1, AcceptedCheckExprV1Error> {
    let snapshot = snapshot();
    let catalog = value_catalog();
    let entity = EntityTag::new(1);
    let bindings = AcceptedSourceBindingCatalog::initial(
        BTreeMap::new(),
        snapshot
            .fields()
            .iter()
            .map(|field| {
                (
                    (entity, FieldSourceKey::try_new(field.name()).unwrap()),
                    field.id(),
                )
            })
            .collect(),
        BTreeMap::new(),
        BTreeMap::new(),
        BTreeMap::new(),
    );
    bind_source_check_expr(
        &SourceCheckExpr::try_new(instructions).unwrap(),
        entity,
        &bindings,
        &snapshot,
        catalog.enum_catalog(),
        catalog.composite_catalog(),
    )
}

fn score_comparison(value: i128) -> Vec<SourceCheckInstruction> {
    vec![
        SourceCheckInstruction::Field(FieldSourceKey::try_new("score").unwrap()),
        SourceCheckInstruction::Literal(ScalarLiteral::Int(value)),
        SourceCheckInstruction::GreaterThanOrEqual,
    ]
}

#[test]
fn source_nesting_admits_exact_depth_and_refuses_maximum_input_safely() {
    let seeds = [
        score_comparison(0),
        vec![
            SourceCheckInstruction::Field(FieldSourceKey::try_new("nickname").unwrap()),
            SourceCheckInstruction::IsNull,
        ],
        vec![
            SourceCheckInstruction::Field(FieldSourceKey::try_new("payload").unwrap()),
            SourceCheckInstruction::Length,
            SourceCheckInstruction::Literal(ScalarLiteral::Nat(0)),
            SourceCheckInstruction::GreaterThanOrEqual,
        ],
        vec![SourceCheckInstruction::Literal(ScalarLiteral::Bool(true))],
    ];
    let max_depth = usize::from(MAX_CHECK_EXPR_V1_DEPTH);
    for seed in seeds {
        let leaf = bind_source(seed.clone()).unwrap();
        for not_count in [
            max_depth - 1,
            max_depth,
            MAX_SOURCE_CHECK_INSTRUCTIONS - seed.len(),
        ] {
            let mut instructions = seed.clone();
            instructions.extend(std::iter::repeat_n(SourceCheckInstruction::Not, not_count));
            let result = bind_source(instructions);
            if not_count < max_depth {
                let mut expected = leaf.clone();
                for _ in 0..not_count {
                    expected = AcceptedCheckExprV1::Not(Box::new(expected));
                }
                assert_eq!(result.unwrap(), expected);
            } else {
                assert_eq!(result, Err(AcceptedCheckExprV1Error::DepthExceeded));
            }
        }
    }
}

#[test]
fn source_long_boolean_chains_keep_canonical_flattening_and_deduplication() {
    for operator in [SourceCheckInstruction::And, SourceCheckInstruction::Or] {
        for seed in [
            vec![SourceCheckInstruction::Literal(ScalarLiteral::Bool(true))],
            score_comparison(7),
        ] {
            let expected = bind_source(seed.clone()).unwrap();
            let mut instructions = seed.clone();
            while instructions.len() + seed.len() < MAX_SOURCE_CHECK_INSTRUCTIONS {
                instructions.extend(seed.iter().cloned());
                instructions.push(operator.clone());
            }
            assert_eq!(bind_source(instructions).unwrap(), expected);
        }
    }
}

#[test]
fn source_boolean_nodes_retain_the_accepted_exact_node_budget() {
    for exact in [true, false] {
        let mut instructions = Vec::new();
        for index in 0..MAX_CHECK_EXPR_V1_CHILDREN {
            instructions.extend(score_comparison(i128::try_from(index).unwrap()));
            let not_count = if exact && index + 1 == MAX_CHECK_EXPR_V1_CHILDREN {
                2
            } else {
                3
            };
            instructions.extend(std::iter::repeat_n(SourceCheckInstruction::Not, not_count));
            if index > 0 {
                instructions.push(SourceCheckInstruction::And);
            }
        }
        let result = bind_source(instructions);
        if exact {
            let accepted = result.unwrap();
            let AcceptedCheckExprV1::And(children) = &accepted else {
                panic!("distinct source clauses must retain their conjunction");
            };
            assert_eq!(children.len(), MAX_CHECK_EXPR_V1_CHILDREN);
            accepted
                .validate(&snapshot(), value_catalog().composite_catalog())
                .unwrap();
        } else {
            assert_eq!(result, Err(AcceptedCheckExprV1Error::NodeCountExceeded));
        }
    }
}

#[test]
fn source_binding_retains_typed_operand_refusals() {
    for (instructions, expected) in [
        (
            vec![
                SourceCheckInstruction::Field(FieldSourceKey::try_new("absent").unwrap()),
                SourceCheckInstruction::IsNull,
            ],
            AcceptedCheckExprV1Error::UnknownField,
        ),
        (
            vec![
                SourceCheckInstruction::Field(FieldSourceKey::try_new("score").unwrap()),
                SourceCheckInstruction::Not,
            ],
            AcceptedCheckExprV1Error::UnsupportedOperator,
        ),
        (
            vec![
                SourceCheckInstruction::Literal(ScalarLiteral::Int(1)),
                SourceCheckInstruction::Length,
            ],
            AcceptedCheckExprV1Error::UnsupportedOperator,
        ),
    ] {
        assert_eq!(bind_source(instructions), Err(expected));
    }
}
