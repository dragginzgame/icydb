use crate::{
    db::{access::eval_index_compare, index::IndexKey},
    error::InternalError,
};
use std::cell::Cell;

#[allow(unused_imports)]
pub(in crate::db) use crate::db::access::{IndexCompareOp, IndexLiteral, IndexPredicateProgram};

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
