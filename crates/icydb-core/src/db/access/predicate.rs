use crate::{db::index::EncodedValue, value::Value};

///
/// IndexPredicateProgram
///
/// Index-only predicate program compiled against index component positions.
/// This is a conservative subset used for raw-index-key predicate evaluation.
///

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) enum IndexPredicateProgram {
    True,
    False,
    And(Vec<Self>),
    Or(Vec<Self>),
    Not(Box<Self>),
    Compare {
        component_index: usize,
        op: IndexCompareOp,
        literal: IndexLiteral,
    },
}

///
/// IndexCompareOp
///
/// Operator subset that can be evaluated directly on canonical encoded index bytes.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) enum IndexCompareOp {
    Eq,
    Ne,
    Lt,
    Lte,
    Gt,
    Gte,
    In,
    NotIn,
}

///
/// IndexLiteral
///
/// Encoded literal payload used by one index-only compare operation.
///

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) enum IndexLiteral {
    One(Vec<u8>),
    Many(Vec<Vec<u8>>),
}

/// Encode one literal value to canonical index-component bytes.
#[must_use]
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn encode_index_literal(value: &Value) -> Option<Vec<u8>> {
    let encoded = EncodedValue::try_from_ref(value).ok()?;
    Some(encoded.encoded().to_vec())
}

// Compare one encoded index component against one compiled literal payload.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn eval_index_compare(
    component: &[u8],
    op: IndexCompareOp,
    literal: &IndexLiteral,
) -> bool {
    match (op, literal) {
        (IndexCompareOp::Eq, IndexLiteral::One(expected)) => component == expected.as_slice(),
        (IndexCompareOp::Ne, IndexLiteral::One(expected)) => component != expected.as_slice(),
        (IndexCompareOp::Lt, IndexLiteral::One(expected)) => component < expected.as_slice(),
        (IndexCompareOp::Lte, IndexLiteral::One(expected)) => component <= expected.as_slice(),
        (IndexCompareOp::Gt, IndexLiteral::One(expected)) => component > expected.as_slice(),
        (IndexCompareOp::Gte, IndexLiteral::One(expected)) => component >= expected.as_slice(),
        (IndexCompareOp::In, IndexLiteral::Many(candidates)) => {
            candidates.iter().any(|candidate| component == candidate)
        }
        (IndexCompareOp::NotIn, IndexLiteral::Many(candidates)) => {
            candidates.iter().all(|candidate| component != candidate)
        }
        (
            IndexCompareOp::Eq
            | IndexCompareOp::Ne
            | IndexCompareOp::Lt
            | IndexCompareOp::Lte
            | IndexCompareOp::Gt
            | IndexCompareOp::Gte,
            IndexLiteral::Many(_),
        )
        | (IndexCompareOp::In | IndexCompareOp::NotIn, IndexLiteral::One(_)) => false,
    }
}
