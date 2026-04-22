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
            CompareOp, ExecutableComparePredicate, ExecutablePredicate, IndexCompileTarget,
            IndexPredicateCapability, PredicateCapabilityContext, classify_index_compare_component,
            classify_index_compare_target, classify_predicate_capabilities,
            classify_predicate_capabilities_for_targets, lower_index_compare_literal_for_target,
            lower_index_starts_with_prefix_for_target,
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

/// Compile one optional index-only predicate program from one resolved
/// predicate using key-item-aware compile targets.
#[must_use]
pub(crate) fn compile_index_program_for_targets(
    predicate: &ExecutablePredicate,
    compile_targets: &[IndexCompileTarget],
    mode: IndexCompilePolicy,
) -> Option<IndexPredicateProgram> {
    match mode {
        IndexCompilePolicy::ConservativeSubset => {
            compile_index_program_from_resolved_for_targets(predicate, compile_targets)
        }
        IndexCompilePolicy::StrictAllOrNone => {
            let capabilities =
                classify_predicate_capabilities_for_targets(predicate, compile_targets);
            match capabilities.index() {
                IndexPredicateCapability::FullyIndexable => {
                    compile_index_program_from_resolved_full_for_targets(predicate, compile_targets)
                }
                IndexPredicateCapability::PartiallyIndexable
                | IndexPredicateCapability::RequiresFullScan => None,
            }
        }
    }
}

// Map one predicate compare operator to the equivalent index compare opcode
// when the index compiler can represent it directly.
const fn index_compare_op(op: CompareOp) -> Option<IndexCompareOp> {
    match op {
        CompareOp::Eq => Some(IndexCompareOp::Eq),
        CompareOp::Ne => Some(IndexCompareOp::Ne),
        CompareOp::Lt => Some(IndexCompareOp::Lt),
        CompareOp::Lte => Some(IndexCompareOp::Lte),
        CompareOp::Gt => Some(IndexCompareOp::Gt),
        CompareOp::Gte => Some(IndexCompareOp::Gte),
        CompareOp::In => Some(IndexCompareOp::In),
        CompareOp::NotIn => Some(IndexCompareOp::NotIn),
        CompareOp::Contains | CompareOp::StartsWith | CompareOp::EndsWith => None,
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

// Compile one resolved predicate tree into one index-only program using
// key-item-aware compile targets.
fn compile_index_program_from_resolved_for_targets(
    predicate: &ExecutablePredicate,
    compile_targets: &[IndexCompileTarget],
) -> Option<IndexPredicateProgram> {
    if let ExecutablePredicate::And(children) = predicate {
        return compile_index_program_and_subset_for_targets(children, compile_targets);
    }

    compile_index_program_from_resolved_full_for_targets(predicate, compile_targets)
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

// Compile an AND node by retaining only safely compilable children for one
// key-item-aware compile target set.
fn compile_index_program_and_subset_for_targets(
    children: &[ExecutablePredicate],
    compile_targets: &[IndexCompileTarget],
) -> Option<IndexPredicateProgram> {
    let mut compiled = Vec::new();
    for child in children {
        let child_program = if let ExecutablePredicate::And(nested) = child {
            compile_index_program_and_subset_for_targets(nested, compile_targets)
        } else {
            let capabilities = classify_predicate_capabilities_for_targets(child, compile_targets);
            match capabilities.index() {
                IndexPredicateCapability::FullyIndexable => {
                    compile_index_program_from_resolved_full_for_targets(child, compile_targets)
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

// Compile one resolved predicate tree only when every node is supported for
// one key-item-aware compile target set.
fn compile_index_program_from_resolved_full_for_targets(
    predicate: &ExecutablePredicate,
    compile_targets: &[IndexCompileTarget],
) -> Option<IndexPredicateProgram> {
    match predicate {
        ExecutablePredicate::True => Some(IndexPredicateProgram::True),
        ExecutablePredicate::False => Some(IndexPredicateProgram::False),
        ExecutablePredicate::And(children) => Some(IndexPredicateProgram::And(
            children
                .iter()
                .map(|child| {
                    compile_index_program_from_resolved_full_for_targets(child, compile_targets)
                })
                .collect::<Option<Vec<_>>>()?,
        )),
        ExecutablePredicate::Or(children) => Some(IndexPredicateProgram::Or(
            children
                .iter()
                .map(|child| {
                    compile_index_program_from_resolved_full_for_targets(child, compile_targets)
                })
                .collect::<Option<Vec<_>>>()?,
        )),
        ExecutablePredicate::Not(inner) => Some(IndexPredicateProgram::Not(Box::new(
            compile_index_program_from_resolved_full_for_targets(inner, compile_targets)?,
        ))),
        ExecutablePredicate::Compare(cmp) => {
            compile_compare_index_node_for_targets(cmp, compile_targets)
        }
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
    let literal_value = cmp.right_literal()?;

    if cmp.op.is_equality_family() || cmp.op.is_ordering_family() {
        let literal = literal_index_component_bytes(literal_value)?;

        Some(IndexPredicateProgram::Compare {
            component_index,
            op: index_compare_op(cmp.op)?,
            literal: IndexLiteral::One(literal),
        })
    } else if cmp.op.is_membership_family() {
        let Value::List(items) = literal_value else {
            return None;
        };
        if items.is_empty() {
            return None;
        }
        let literals = items
            .iter()
            .map(literal_index_component_bytes)
            .collect::<Option<Vec<_>>>()?;

        Some(IndexPredicateProgram::Compare {
            component_index,
            op: index_compare_op(cmp.op)?,
            literal: IndexLiteral::Many(literals),
        })
    } else if matches!(cmp.op, CompareOp::StartsWith) {
        compile_starts_with_index_node(component_index, literal_value)
    } else {
        None
    }
}

// Compile one resolved compare node into index-only compare bytes for one
// key-item-aware target set.
fn compile_compare_index_node_for_targets(
    cmp: &ExecutableComparePredicate,
    compile_targets: &[IndexCompileTarget],
) -> Option<IndexPredicateProgram> {
    let target = classify_index_compare_target(cmp, compile_targets)?;
    let literal_value = cmp.right_literal()?;

    if cmp.op.is_equality_family() || cmp.op.is_ordering_family() {
        let lowered =
            lower_index_compare_literal_for_target(target, literal_value, cmp.coercion.id)?;
        let literal = literal_index_component_bytes(&lowered)?;

        Some(IndexPredicateProgram::Compare {
            component_index: target.component_index,
            op: index_compare_op(cmp.op)?,
            literal: IndexLiteral::One(literal),
        })
    } else if cmp.op.is_membership_family() {
        let Value::List(values) = literal_value else {
            return None;
        };
        let literals = values
            .iter()
            .map(|value| {
                let lowered =
                    lower_index_compare_literal_for_target(target, value, cmp.coercion.id)?;
                literal_index_component_bytes(&lowered)
            })
            .collect::<Option<Vec<_>>>()?;
        if literals.is_empty() {
            return None;
        }

        Some(IndexPredicateProgram::Compare {
            component_index: target.component_index,
            op: index_compare_op(cmp.op)?,
            literal: IndexLiteral::Many(literals),
        })
    } else if matches!(cmp.op, CompareOp::StartsWith) {
        compile_starts_with_index_node_for_target(literal_value, cmp.coercion.id, target)
    } else {
        None
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

// Compile one starts-with compare node into one canonical bounded text range
// for one key-item-aware compile target.
fn compile_starts_with_index_node_for_target(
    value: &Value,
    coercion: crate::db::predicate::CoercionId,
    target: IndexCompileTarget,
) -> Option<IndexPredicateProgram> {
    let prefix = lower_index_starts_with_prefix_for_target(target, value, coercion)?;
    let lower_literal = literal_index_component_bytes(&Value::Text(prefix.clone()))?;
    let lower = IndexPredicateProgram::Compare {
        component_index: target.component_index,
        op: IndexCompareOp::Gte,
        literal: IndexLiteral::One(lower_literal),
    };

    let Some(upper_prefix) = next_text_prefix(&prefix) else {
        return Some(lower);
    };

    let upper_literal = literal_index_component_bytes(&Value::Text(upper_prefix))?;
    let upper = IndexPredicateProgram::Compare {
        component_index: target.component_index,
        op: IndexCompareOp::Lt,
        literal: IndexLiteral::One(upper_literal),
    };

    Some(IndexPredicateProgram::And(vec![lower, upper]))
}
