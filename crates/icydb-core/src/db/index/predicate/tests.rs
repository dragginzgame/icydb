//! Borrowed predicate components retain byte ordering and short-circuit errors.

use super::*;
use crate::{
    db::{
        index::{IndexId, decode_index_key_frame},
        test_support::index::nat64_index_key,
    },
    types::EntityTag,
};

#[test]
fn borrowed_predicates_preserve_comparisons_and_short_circuit_refusals() {
    let raw = nat64_index_key(&IndexId::new(EntityTag::new(7), 1), b"bb", 7)
        .to_raw()
        .unwrap();
    let frame = decode_index_key_frame(&raw).unwrap();
    for (op, expected) in [
        (IndexCompareOp::Eq, [false, true, false]),
        (IndexCompareOp::Ne, [true, false, true]),
        (IndexCompareOp::Lt, [false, false, true]),
        (IndexCompareOp::Lte, [false, true, true]),
        (IndexCompareOp::Gt, [true, false, false]),
        (IndexCompareOp::Gte, [true, true, false]),
    ] {
        for (literal, expected) in [b"b".as_slice(), b"bb", b"bbb"].into_iter().zip(expected) {
            let program = IndexPredicateProgram::Compare {
                component_index: 0,
                op,
                literal: IndexLiteral::One(literal.to_vec()),
            };
            assert_eq!(
                eval_index_program_on_frame(&frame, &program).unwrap(),
                expected
            );
        }
    }
    for literal in [
        IndexLiteral::Many(vec![b"bb".to_vec(), b"a".to_vec()]),
        IndexLiteral::ManySorted(vec![b"a".to_vec(), b"bb".to_vec()]),
    ] {
        for (op, expected) in [(IndexCompareOp::In, true), (IndexCompareOp::NotIn, false)] {
            let program = IndexPredicateProgram::Compare {
                component_index: 0,
                op,
                literal: literal.clone(),
            };
            assert_eq!(
                eval_index_program_on_frame(&frame, &program).unwrap(),
                expected
            );
        }
    }
    let missing = IndexPredicateProgram::Compare {
        component_index: 1,
        op: IndexCompareOp::Eq,
        literal: IndexLiteral::One(vec![1]),
    };
    let error = eval_index_program_on_frame(&frame, &missing).unwrap_err();
    let expected = InternalError::index_only_predicate_component_required();
    assert_eq!(error.diagnostic(), expected.diagnostic());
    assert_eq!(error.diagnostic_facts(), expected.diagnostic_facts());
    for (program, expected) in [
        (
            IndexPredicateProgram::And(vec![IndexPredicateProgram::False, missing.clone()]),
            false,
        ),
        (
            IndexPredicateProgram::Or(vec![IndexPredicateProgram::True, missing]),
            true,
        ),
        (
            IndexPredicateProgram::Not(Box::new(IndexPredicateProgram::False)),
            true,
        ),
    ] {
        assert_eq!(
            eval_index_program_on_frame(&frame, &program).unwrap(),
            expected
        );
    }
}
