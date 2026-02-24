use crate::{db::index::IndexKey, error::InternalError};
use std::cell::Cell;

///
/// IndexPredicateProgram
///
/// Index-only predicate program compiled against index component positions.
/// This is a conservative subset used for raw-index-key predicate evaluation.
///

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::db) enum IndexPredicateProgram {
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
/// IndexPredicateExecution
///
/// Execution-time wrapper for one compiled index predicate program.
/// Carries optional observability counters used by load execution tracing.
///

#[derive(Clone, Copy)]
#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::db) struct IndexPredicateExecution<'a> {
    pub(in crate::db) program: &'a IndexPredicateProgram,
    pub(in crate::db) rejected_keys_counter: Option<&'a Cell<u64>>,
}

///
/// IndexCompareOp
///
/// Operator subset that can be evaluated directly on canonical encoded index bytes.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::db) enum IndexCompareOp {
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
pub(in crate::db) enum IndexLiteral {
    One(Vec<u8>),
    Many(Vec<Vec<u8>>),
}

// Evaluate one compiled index-only program against one decoded index key.
#[cfg_attr(not(test), allow(dead_code))]
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

// Compare one encoded index component against one compiled literal payload.
#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::db) fn eval_index_compare(
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
