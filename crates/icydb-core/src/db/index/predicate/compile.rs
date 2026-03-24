//! Module: index::predicate::compile
//! Responsibility: compile resolved semantic predicates into index-only programs.
//! Does not own: predicate resolution or runtime key scanning.
//! Boundary: planner/load uses this at compile/preflight time.

use crate::{
    db::{
        index::{
            IndexCompareOp, IndexLiteral, IndexPredicateProgram, next_text_prefix,
            predicate::literal_index_component_bytes,
        },
        predicate::{
            CompareOp, ExecutableComparePredicate, ExecutablePredicate, IndexPredicateCapability,
            PredicateCapabilityContext, classify_index_compare_component,
            classify_predicate_capabilities,
        },
    },
    value::Value,
};

///
/// IndexCompilePolicy
///
/// Predicate compile policy for index-only prefilter programs.
/// `ConservativeSubset` keeps load behavior by compiling safe AND-subsets.
/// `StrictAllOrNone` compiles only when every predicate node is supported.
///

#[derive(Clone, Copy)]
pub(crate) enum IndexCompilePolicy {
    ConservativeSubset,
    StrictAllOrNone,
}

/// Compile one optional index-only predicate program from one resolved predicate.
/// This is the single compile-mode switch boundary for subset vs strict policy.
#[must_use]
pub(crate) fn compile_index_program(
    predicate: &ExecutablePredicate,
    index_slots: &[usize],
    mode: IndexCompilePolicy,
) -> Option<IndexPredicateProgram> {
    // Single policy switch boundary for conservative vs strict compilation.
    match mode {
        IndexCompilePolicy::ConservativeSubset => {
            compile_index_program_from_resolved(predicate, index_slots)
        }
        IndexCompilePolicy::StrictAllOrNone => {
            let capabilities = classify_predicate_capabilities(
                predicate,
                PredicateCapabilityContext::index_compile(index_slots),
            );
            match capabilities.index() {
                IndexPredicateCapability::FullyIndexable => {
                    compile_index_program_from_resolved_full(predicate, index_slots)
                }
                IndexPredicateCapability::PartiallyIndexable
                | IndexPredicateCapability::RequiresFullScan => None,
            }
        }
    }
}

/// Compile one resolved predicate tree into one index-only program.
fn compile_index_program_from_resolved(
    predicate: &ExecutablePredicate,
    index_slots: &[usize],
) -> Option<IndexPredicateProgram> {
    // Compile a safe AND-subset: unsupported AND children are dropped so
    // index-only filtering remains conservative (no false negatives).
    if let ExecutablePredicate::And(children) = predicate {
        return compile_index_program_and_subset(children, index_slots);
    }

    compile_index_program_from_resolved_full(predicate, index_slots)
}

/// Compile an AND node by retaining only safely compilable children.
fn compile_index_program_and_subset(
    children: &[ExecutablePredicate],
    index_slots: &[usize],
) -> Option<IndexPredicateProgram> {
    let mut compiled = Vec::new();
    for child in children {
        let child_program = if let ExecutablePredicate::And(nested) = child {
            // Nested AND nodes can also be safely reduced to a conjunction subset.
            compile_index_program_and_subset(nested, index_slots)
        } else {
            let capabilities = classify_predicate_capabilities(
                child,
                PredicateCapabilityContext::index_compile(index_slots),
            );
            match capabilities.index() {
                IndexPredicateCapability::FullyIndexable => {
                    compile_index_program_from_resolved_full(child, index_slots)
                }
                IndexPredicateCapability::PartiallyIndexable
                | IndexPredicateCapability::RequiresFullScan => None,
            }
        };

        let Some(child_program) = child_program else {
            continue;
        };
        match child_program {
            IndexPredicateProgram::True => {}
            IndexPredicateProgram::False => return Some(IndexPredicateProgram::False),
            other => compiled.push(other),
        }
    }

    match compiled.len() {
        0 => None,
        1 => compiled.pop(),
        _ => Some(IndexPredicateProgram::And(compiled)),
    }
}

/// Compile one resolved predicate tree only when every node is supported.
fn compile_index_program_from_resolved_full(
    predicate: &ExecutablePredicate,
    index_slots: &[usize],
) -> Option<IndexPredicateProgram> {
    match predicate {
        ExecutablePredicate::True => Some(IndexPredicateProgram::True),
        ExecutablePredicate::False => Some(IndexPredicateProgram::False),
        ExecutablePredicate::And(children) => Some(IndexPredicateProgram::And(
            children
                .iter()
                .map(|child| compile_index_program_from_resolved_full(child, index_slots))
                .collect::<Option<Vec<_>>>()?,
        )),
        ExecutablePredicate::Or(children) => Some(IndexPredicateProgram::Or(
            children
                .iter()
                .map(|child| compile_index_program_from_resolved_full(child, index_slots))
                .collect::<Option<Vec<_>>>()?,
        )),
        ExecutablePredicate::Not(inner) => Some(IndexPredicateProgram::Not(Box::new(
            compile_index_program_from_resolved_full(inner, index_slots)?,
        ))),
        ExecutablePredicate::Compare(cmp) => compile_compare_index_node(cmp, index_slots),
        ExecutablePredicate::IsNull { .. }
        | ExecutablePredicate::IsNotNull { .. }
        | ExecutablePredicate::IsMissing { .. }
        | ExecutablePredicate::IsEmpty { .. }
        | ExecutablePredicate::IsNotEmpty { .. }
        | ExecutablePredicate::TextContains { .. }
        | ExecutablePredicate::TextContainsCi { .. } => None,
    }
}

/// Compile one resolved compare node into index-only compare bytes.
fn compile_compare_index_node(
    cmp: &ExecutableComparePredicate,
    index_slots: &[usize],
) -> Option<IndexPredicateProgram> {
    // Capability classification owns index eligibility; translation only runs
    // once the compare node is known to be indexable for this slot projection.
    let component_index = classify_index_compare_component(cmp, index_slots)?;

    match cmp.op {
        CompareOp::Eq
        | CompareOp::Ne
        | CompareOp::Lt
        | CompareOp::Lte
        | CompareOp::Gt
        | CompareOp::Gte => {
            let literal = literal_index_component_bytes(&cmp.value)?;
            let op = match cmp.op {
                CompareOp::Eq => IndexCompareOp::Eq,
                CompareOp::Ne => IndexCompareOp::Ne,
                CompareOp::Lt => IndexCompareOp::Lt,
                CompareOp::Lte => IndexCompareOp::Lte,
                CompareOp::Gt => IndexCompareOp::Gt,
                CompareOp::Gte => IndexCompareOp::Gte,
                CompareOp::In
                | CompareOp::NotIn
                | CompareOp::Contains
                | CompareOp::StartsWith
                | CompareOp::EndsWith => {
                    unreachable!("op branch must match index compare subset")
                }
            };

            Some(IndexPredicateProgram::Compare {
                component_index,
                op,
                literal: IndexLiteral::One(literal),
            })
        }
        CompareOp::In | CompareOp::NotIn => {
            let Value::List(items) = &cmp.value else {
                return None;
            };
            if items.is_empty() {
                return None;
            }
            let literals = items
                .iter()
                .map(literal_index_component_bytes)
                .collect::<Option<Vec<_>>>()?;
            let op = match cmp.op {
                CompareOp::In => IndexCompareOp::In,
                CompareOp::NotIn => IndexCompareOp::NotIn,
                CompareOp::Eq
                | CompareOp::Ne
                | CompareOp::Lt
                | CompareOp::Lte
                | CompareOp::Gt
                | CompareOp::Gte
                | CompareOp::Contains
                | CompareOp::StartsWith
                | CompareOp::EndsWith => unreachable!("op branch must match index compare subset"),
            };

            Some(IndexPredicateProgram::Compare {
                component_index,
                op,
                literal: IndexLiteral::Many(literals),
            })
        }
        CompareOp::StartsWith => compile_starts_with_index_node(component_index, &cmp.value),
        CompareOp::Contains | CompareOp::EndsWith => None,
    }
}

fn compile_starts_with_index_node(
    component_index: usize,
    value: &Value,
) -> Option<IndexPredicateProgram> {
    let Value::Text(prefix) = value else {
        return None;
    };
    if prefix.is_empty() {
        return None;
    }

    let lower_literal = literal_index_component_bytes(&Value::Text(prefix.clone()))?;
    let lower = IndexPredicateProgram::Compare {
        component_index,
        op: IndexCompareOp::Gte,
        literal: IndexLiteral::One(lower_literal),
    };

    let Some(upper_prefix) = next_text_prefix(prefix) else {
        return Some(lower);
    };

    let upper_literal = literal_index_component_bytes(&Value::Text(upper_prefix))?;
    let upper = IndexPredicateProgram::Compare {
        component_index,
        op: IndexCompareOp::Lt,
        literal: IndexLiteral::One(upper_literal),
    };

    Some(IndexPredicateProgram::And(vec![lower, upper]))
}
