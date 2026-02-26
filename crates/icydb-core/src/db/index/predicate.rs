use crate::{
    db::index::{EncodedValue, IndexKey},
    error::InternalError,
    value::Value,
};
use std::cell::Cell;

///
/// IndexPredicateProgram
///
/// Index-only predicate program compiled against index component positions.
/// This is a conservative subset used for raw-index-key predicate evaluation.
///
#[derive(Clone, Debug, Eq, PartialEq)]
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
pub(crate) enum IndexLiteral {
    One(Vec<u8>),
    Many(Vec<Vec<u8>>),
}

// Compare one encoded index component against one compiled literal payload.
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

///
/// IndexPredicateExecution
///
/// Execution-time wrapper for one compiled index predicate program.
/// Carries optional observability counters used by load execution tracing.
///

#[derive(Clone, Copy)]
pub(in crate::db) struct IndexPredicateExecution<'a> {
    pub(in crate::db) program: &'a IndexPredicateProgram,
    pub(in crate::db) rejected_keys_counter: Option<&'a Cell<u64>>,
}

// Evaluate one compiled index-only program against one decoded index key.
pub(in crate::db) fn eval_index_program_on_decoded_key(
    key: &IndexKey,
    program: &IndexPredicateProgram,
) -> Result<bool, InternalError> {
    match program {
        IndexPredicateProgram::True => Ok(true),
        IndexPredicateProgram::False => Ok(false),
        IndexPredicateProgram::And(children) => {
            for child in children {
                if !eval_index_program_on_decoded_key(key, child)? {
                    return Ok(false);
                }
            }

            Ok(true)
        }
        IndexPredicateProgram::Or(children) => {
            for child in children {
                if eval_index_program_on_decoded_key(key, child)? {
                    return Ok(true);
                }
            }

            Ok(false)
        }
        IndexPredicateProgram::Not(inner) => Ok(!eval_index_program_on_decoded_key(key, inner)?),
        IndexPredicateProgram::Compare {
            component_index,
            op,
            literal,
        } => {
            let Some(component) = key.component(*component_index) else {
                return Err(InternalError::query_executor_invariant(
                    "index-only predicate program referenced missing index component",
                ));
            };

            Ok(eval_index_compare(component, *op, literal))
        }
    }
}

/// Evaluate one compiled index-only execution request and update observability
/// counters when a key is rejected by index-only filtering.
pub(in crate::db) fn eval_index_execution_on_decoded_key(
    key: &IndexKey,
    execution: IndexPredicateExecution<'_>,
) -> Result<bool, InternalError> {
    let passed = eval_index_program_on_decoded_key(key, execution.program)?;
    if !passed && let Some(counter) = execution.rejected_keys_counter {
        counter.set(counter.get().saturating_add(1));
    }

    Ok(passed)
}

/// Encode one literal value to canonical index-component bytes.
#[must_use]
pub(in crate::db) fn encode_index_literal(value: &Value) -> Option<Vec<u8>> {
    let encoded = EncodedValue::try_from_ref(value).ok()?;

    Some(encoded.encoded().to_vec())
}

/// Convert one literal value to canonical index-component bytes.
///
/// This alias keeps executor/runtime callers free from direct encoding helper
/// naming while preserving one canonical implementation here in `db::index`.
#[must_use]
pub(in crate::db) fn literal_index_component_bytes(value: &Value) -> Option<Vec<u8>> {
    encode_index_literal(value)
}
