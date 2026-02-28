//! Module: index::predicate::compile
//! Responsibility: compile resolved semantic predicates into index-only programs.
//! Does not own: predicate resolution or runtime key scanning.
//! Boundary: planner/load uses this at compile/preflight time.

use crate::{
    db::{
        index::{
            IndexCompareOp, IndexLiteral, IndexPredicateProgram,
            predicate::literal_index_component_bytes,
        },
        predicate::{CoercionId, CompareOp, ResolvedComparePredicate, ResolvedPredicate},
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
    predicate: &ResolvedPredicate,
    index_slots: &[usize],
    mode: IndexCompilePolicy,
) -> Option<IndexPredicateProgram> {
    // Single policy switch boundary for conservative vs strict compilation.
    match mode {
        IndexCompilePolicy::ConservativeSubset => {
            compile_index_program_from_resolved(predicate, index_slots)
        }
        IndexCompilePolicy::StrictAllOrNone => {
            compile_index_program_from_resolved_full(predicate, index_slots)
        }
    }
}

// Compile one resolved predicate tree into one index-only program.
fn compile_index_program_from_resolved(
    predicate: &ResolvedPredicate,
    index_slots: &[usize],
) -> Option<IndexPredicateProgram> {
    // Compile a safe AND-subset: unsupported AND children are dropped so
    // index-only filtering remains conservative (no false negatives).
    if let ResolvedPredicate::And(children) = predicate {
        return compile_index_program_and_subset(children, index_slots);
    }

    compile_index_program_from_resolved_full(predicate, index_slots)
}

// Compile an AND node by retaining only safely compilable children.
fn compile_index_program_and_subset(
    children: &[ResolvedPredicate],
    index_slots: &[usize],
) -> Option<IndexPredicateProgram> {
    let mut compiled = Vec::new();
    for child in children {
        let child_program = match child {
            // Nested AND nodes can also be safely reduced to a conjunction subset.
            ResolvedPredicate::And(nested) => compile_index_program_and_subset(nested, index_slots),
            _ => compile_index_program_from_resolved_full(child, index_slots),
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

// Compile one resolved predicate tree only when every node is supported.
fn compile_index_program_from_resolved_full(
    predicate: &ResolvedPredicate,
    index_slots: &[usize],
) -> Option<IndexPredicateProgram> {
    match predicate {
        ResolvedPredicate::True => Some(IndexPredicateProgram::True),
        ResolvedPredicate::False => Some(IndexPredicateProgram::False),
        ResolvedPredicate::And(children) => Some(IndexPredicateProgram::And(
            children
                .iter()
                .map(|child| compile_index_program_from_resolved_full(child, index_slots))
                .collect::<Option<Vec<_>>>()?,
        )),
        ResolvedPredicate::Or(children) => Some(IndexPredicateProgram::Or(
            children
                .iter()
                .map(|child| compile_index_program_from_resolved_full(child, index_slots))
                .collect::<Option<Vec<_>>>()?,
        )),
        ResolvedPredicate::Not(inner) => Some(IndexPredicateProgram::Not(Box::new(
            compile_index_program_from_resolved_full(inner, index_slots)?,
        ))),
        ResolvedPredicate::Compare(cmp) => compile_compare_index_node(cmp, index_slots),
        ResolvedPredicate::IsNull { .. }
        | ResolvedPredicate::IsMissing { .. }
        | ResolvedPredicate::IsEmpty { .. }
        | ResolvedPredicate::IsNotEmpty { .. }
        | ResolvedPredicate::TextContains { .. }
        | ResolvedPredicate::TextContainsCi { .. } => None,
    }
}

// Compile one resolved compare node into index-only compare bytes.
fn compile_compare_index_node(
    cmp: &ResolvedComparePredicate,
    index_slots: &[usize],
) -> Option<IndexPredicateProgram> {
    // Index-only compare compilation requires strict coercion and a mapped index slot.
    if cmp.coercion.id != CoercionId::Strict {
        return None;
    }
    let field_slot = cmp.field_slot?;
    let component_index = index_slots.iter().position(|slot| *slot == field_slot)?;

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
                | CompareOp::EndsWith => unreachable!("op branch must match index compare subset"),
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
        CompareOp::Contains | CompareOp::StartsWith | CompareOp::EndsWith => None,
    }
}
