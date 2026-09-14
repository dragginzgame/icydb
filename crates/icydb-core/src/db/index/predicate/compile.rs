//! Module: index::predicate::compile
//! Responsibility: compile resolved semantic predicates into index-only programs.
//! Does not own: predicate resolution or runtime key scanning.
//! Boundary: planner/load uses this at compile/preflight time.

#[cfg(test)]
mod tests;

use crate::{
    db::{
        index::{
            IndexCompareOp, IndexLiteral, IndexPredicateProgram, TextPrefixBoundMode,
            admit_text_prefix_bounds, predicate::literal_index_component_bytes,
            starts_with_component_bounds,
        },
        predicate::{
            CompareOp, ExecutableComparePredicate, ExecutablePredicate, IndexCompileTarget,
            IndexPredicateCapability, PredicateCapabilityContext,
            admit_index_compare_literal_for_kind, classify_index_compare_component,
            classify_index_compare_target, classify_predicate_capabilities,
            classify_predicate_capabilities_for_targets, lower_index_compare_literal_for_kind,
            lower_index_starts_with_prefix_for_target,
        },
        query::construction::ConstructionBudget,
    },
    error::InternalError,
    value::Value,
};
use std::ops::Bound;

const INDEX_MANY_BINARY_SEARCH_MIN_CANDIDATES: usize = 16;

///
/// IndexCompilePolicy
///
/// Predicate compile policy for index-only prefilter programs.
/// `ConservativeSubset` keeps load behavior by compiling safe AND-subsets.
/// `StrictAllOrNone` compiles only when every predicate node is supported.
///

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum IndexCompilePolicy {
    ConservativeSubset,
    StrictAllOrNone,
}

///
/// ComponentBoundProgram
///
/// Result of compiling one text-prefix range bound into an index-only
/// predicate node. The enum keeps unsupported compilation as the outer
/// `Option` return while naming the valid unbounded-vs-program states.
///

enum ComponentBoundProgram {
    Unbounded,
    Program(IndexPredicateProgram),
}

impl ComponentBoundProgram {
    // Convert a named bound state back into the optional program shape needed
    // by the surrounding two-sided prefix interval assembly.
    fn into_program(self) -> Option<IndexPredicateProgram> {
        match self {
            Self::Unbounded => None,
            Self::Program(program) => Some(program),
        }
    }
}

/// Compile using structural slots. Unsupported syntax is a successful absence;
/// construction failure must never be treated as an optional optimization miss.
pub(crate) fn compile_index_program(
    predicate: &ExecutablePredicate,
    index_slots: &[usize],
    mode: IndexCompilePolicy,
    budget: &dyn ConstructionBudget,
) -> Result<Option<IndexPredicateProgram>, InternalError> {
    if mode == IndexCompilePolicy::StrictAllOrNone
        && classify_predicate_capabilities(
            predicate,
            PredicateCapabilityContext::index_compile(index_slots),
        )
        .index()
            != IndexPredicateCapability::FullyIndexable
    {
        return Ok(None);
    }

    compile_tree(predicate, mode, &mut |cmp| {
        let Some(component_index) = classify_index_compare_component(cmp, index_slots) else {
            return Ok(None);
        };
        compile_compare(
            cmp,
            component_index,
            &mut |value| literal_index_component_bytes(value, budget),
            budget,
        )
    })
}

/// Compile using planner-frozen key-item targets, sharing the boolean traversal
/// and canonical scalar encoder with structural-slot compilation.
pub(crate) fn compile_index_program_for_targets(
    predicate: &ExecutablePredicate,
    compile_targets: &[IndexCompileTarget],
    mode: IndexCompilePolicy,
    budget: &dyn ConstructionBudget,
) -> Result<Option<IndexPredicateProgram>, InternalError> {
    if mode == IndexCompilePolicy::StrictAllOrNone
        && classify_predicate_capabilities_for_targets(predicate, compile_targets).index()
            != IndexPredicateCapability::FullyIndexable
    {
        return Ok(None);
    }

    compile_tree(predicate, mode, &mut |cmp| {
        let Some(target) = classify_index_compare_target(cmp, compile_targets) else {
            return Ok(None);
        };
        if cmp.op == CompareOp::StartsWith {
            let Some(value) = cmp.right_literal() else {
                return Ok(None);
            };
            admit_index_compare_literal_for_kind(target.kind, value, cmp.coercion.id, budget)?;
            let Some(prefix) =
                lower_index_starts_with_prefix_for_target(target, value, cmp.coercion.id)
            else {
                return Ok(None);
            };
            return compile_text_prefix_bounds_for_component(
                target.component_index,
                &prefix,
                budget,
            );
        }

        compile_compare(
            cmp,
            target.component_index,
            &mut |value| {
                admit_index_compare_literal_for_kind(target.kind, value, cmp.coercion.id, budget)?;
                let Some(lowered) =
                    lower_index_compare_literal_for_kind(target.kind, value, cmp.coercion.id)
                else {
                    return Ok(None);
                };
                literal_index_component_bytes(&lowered, budget)
            },
            budget,
        )
    })
}

// Map semantic compare operators to the existing index opcodes.
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

// One traversal for both slot and target compilers. Subset policy only flows
// through AND: OR and NOT always require every child to compile.
fn compile_tree(
    predicate: &ExecutablePredicate,
    policy: IndexCompilePolicy,
    compare: &mut impl FnMut(
        &ExecutableComparePredicate,
    ) -> Result<Option<IndexPredicateProgram>, InternalError>,
) -> Result<Option<IndexPredicateProgram>, InternalError> {
    let program = match predicate {
        ExecutablePredicate::True => Some(IndexPredicateProgram::True),
        ExecutablePredicate::False => Some(IndexPredicateProgram::False),
        ExecutablePredicate::And(children) | ExecutablePredicate::Or(children) => {
            let is_and = matches!(predicate, ExecutablePredicate::And(_));
            let subset = is_and && policy == IndexCompilePolicy::ConservativeSubset;
            let mut compiled = Vec::new();
            for child in children {
                let child_policy = if subset {
                    policy
                } else {
                    IndexCompilePolicy::StrictAllOrNone
                };
                let Some(program) = compile_tree(child, child_policy, compare)? else {
                    if subset {
                        continue;
                    }
                    return Ok(None);
                };
                match program {
                    IndexPredicateProgram::True if subset => {}
                    IndexPredicateProgram::False if subset => {
                        return Ok(Some(IndexPredicateProgram::False));
                    }
                    program => compiled.push(program),
                }
            }
            if subset && compiled.len() <= 1 {
                compiled.pop()
            } else if is_and {
                Some(IndexPredicateProgram::And(compiled))
            } else {
                Some(IndexPredicateProgram::Or(compiled))
            }
        }
        ExecutablePredicate::Not(inner) => {
            compile_tree(inner, IndexCompilePolicy::StrictAllOrNone, compare)?
                .map(|program| IndexPredicateProgram::Not(Box::new(program)))
        }
        ExecutablePredicate::Compare(cmp) => return compare(cmp),
        ExecutablePredicate::IsNull { .. }
        | ExecutablePredicate::IsNotNull { .. }
        | ExecutablePredicate::IsMissing { .. }
        | ExecutablePredicate::IsEmpty { .. }
        | ExecutablePredicate::IsNotEmpty { .. }
        | ExecutablePredicate::TextContains { .. }
        | ExecutablePredicate::TextContainsCi { .. } => None,
    };

    Ok(program)
}

// Scalar and membership assembly share one fallible literal translation owner.
// Target coercion remains in the predicate subsystem, not this encoder.
fn compile_compare(
    cmp: &ExecutableComparePredicate,
    component_index: usize,
    encode: &mut impl FnMut(&Value) -> Result<Option<Vec<u8>>, InternalError>,
    budget: &dyn ConstructionBudget,
) -> Result<Option<IndexPredicateProgram>, InternalError> {
    let Some(value) = cmp.right_literal() else {
        return Ok(None);
    };
    if cmp.op == CompareOp::StartsWith {
        let Value::Text(prefix) = value else {
            return Ok(None);
        };
        return compile_text_prefix_bounds_for_component(component_index, prefix, budget);
    }
    let Some(op) = index_compare_op(cmp.op) else {
        return Ok(None);
    };
    let literal = if cmp.op.is_membership_family() {
        let Value::List(items) = value else {
            return Ok(None);
        };
        let mut literals = Vec::with_capacity(items.len());
        for item in items {
            let Some(bytes) = encode(item)? else {
                return Ok(None);
            };
            literals.push(bytes);
        }
        compile_index_many_literal(literals)
    } else {
        encode(value)?.map(IndexLiteral::One)
    };

    Ok(literal.map(|literal| IndexPredicateProgram::Compare {
        component_index,
        op,
        literal,
    }))
}

fn compile_index_many_literal(mut literals: Vec<Vec<u8>>) -> Option<IndexLiteral> {
    if literals.is_empty() {
        return None;
    }
    if literals.len() < INDEX_MANY_BINARY_SEARCH_MIN_CANDIDATES {
        return Some(IndexLiteral::Many(literals));
    }
    literals.sort_unstable();
    literals.dedup();

    Some(IndexLiteral::ManySorted(literals))
}

// Admit semantic strings/scan first, then each separately encoded scalar buffer.
fn compile_text_prefix_bounds_for_component(
    component_index: usize,
    prefix: &str,
    budget: &dyn ConstructionBudget,
) -> Result<Option<IndexPredicateProgram>, InternalError> {
    admit_text_prefix_bounds(prefix, TextPrefixBoundMode::Strict, budget)?;
    let Some((lower, upper)) = starts_with_component_bounds(prefix, TextPrefixBoundMode::Strict)
    else {
        return Ok(None);
    };
    let Some(lower) = compile_component_bound(component_index, &lower, true, budget)? else {
        return Ok(None);
    };
    let Some(upper) = compile_component_bound(component_index, &upper, false, budget)? else {
        return Ok(None);
    };

    Ok(match (lower.into_program(), upper.into_program()) {
        (None, None) => None,
        (Some(program), None) | (None, Some(program)) => Some(program),
        (Some(lower), Some(upper)) => Some(IndexPredicateProgram::And(vec![lower, upper])),
    })
}

// Convert semantic bounds without swallowing construction failures.
fn compile_component_bound(
    component_index: usize,
    bound: &Bound<Value>,
    lower: bool,
    budget: &dyn ConstructionBudget,
) -> Result<Option<ComponentBoundProgram>, InternalError> {
    let (value, op) = match (bound, lower) {
        (Bound::Unbounded, _) => return Ok(Some(ComponentBoundProgram::Unbounded)),
        (Bound::Included(value), true) => (value, IndexCompareOp::Gte),
        (Bound::Excluded(value), true) => (value, IndexCompareOp::Gt),
        (Bound::Included(value), false) => (value, IndexCompareOp::Lte),
        (Bound::Excluded(value), false) => (value, IndexCompareOp::Lt),
    };

    Ok(
        literal_index_component_bytes(value, budget)?.map(|literal| {
            ComponentBoundProgram::Program(IndexPredicateProgram::Compare {
                component_index,
                op,
                literal: IndexLiteral::One(literal),
            })
        }),
    )
}
