use crate::{
    db::{
        index::{EncodedValue, IndexKey},
        query::predicate::{
            CompareOp, ComparePredicate, Predicate,
            coercion::{CoercionId, CoercionSpec, TextOp, compare_eq, compare_order, compare_text},
        },
    },
    error::InternalError,
    model::entity::resolve_field_slot,
    traits::{EntityKind, EntityValue},
    value::{TextMode, Value},
};
use std::{cell::Cell, cmp::Ordering, collections::BTreeSet};

///
/// PredicateFieldSlots
///
/// Slot-resolved predicate program for runtime row filtering.
/// Field names are resolved once during setup; evaluation is slot-only.
///

#[derive(Clone, Debug)]
pub(crate) struct PredicateFieldSlots {
    resolved: ResolvedPredicate,
    #[cfg_attr(not(test), allow(dead_code))]
    required_slots: Vec<usize>,
}

///
/// ResolvedComparePredicate
///
/// One comparison node with a pre-resolved field slot.
///

#[derive(Clone, Debug, Eq, PartialEq)]
struct ResolvedComparePredicate {
    field_slot: Option<usize>,
    op: CompareOp,
    value: Value,
    coercion: CoercionSpec,
}

///
/// ResolvedPredicate
///
/// Predicate AST compiled to field slots for execution hot paths.
///

#[derive(Clone, Debug, Eq, PartialEq)]
enum ResolvedPredicate {
    True,
    False,
    And(Vec<Self>),
    Or(Vec<Self>),
    Not(Box<Self>),
    Compare(ResolvedComparePredicate),
    IsNull {
        field_slot: Option<usize>,
    },
    IsMissing {
        field_slot: Option<usize>,
    },
    IsEmpty {
        field_slot: Option<usize>,
    },
    IsNotEmpty {
        field_slot: Option<usize>,
    },
    TextContains {
        field_slot: Option<usize>,
        value: Value,
    },
    TextContainsCi {
        field_slot: Option<usize>,
        value: Value,
    },
}

impl PredicateFieldSlots {
    /// Resolve a predicate into a slot-based executable form.
    #[must_use]
    pub(crate) fn resolve<E: EntityKind>(predicate: &Predicate) -> Self {
        let resolved = resolve_predicate_slots::<E>(predicate);
        let required_slots = collect_required_slots(&resolved);

        Self {
            resolved,
            required_slots,
        }
    }

    /// Return all unique field slots referenced by this compiled predicate.
    ///
    /// Contract:
    /// - sorted ascending
    /// - deduplicated
    /// - excludes unresolved/missing field references
    #[must_use]
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) const fn required_slots(&self) -> &[usize] {
        self.required_slots.as_slice()
    }

    // Compile this predicate into an index-component evaluator program for one
    // concrete index field-slot ordering.
    #[must_use]
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn compile_index_program(
        &self,
        index_slots: &[usize],
    ) -> Option<IndexPredicateProgram> {
        compile_index_program_from_resolved(&self.resolved, index_slots)
    }

    // Compile this predicate into an index-component evaluator program only
    // when every predicate node is supported by index-only evaluation.
    #[must_use]
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn compile_index_program_strict(
        &self,
        index_slots: &[usize],
    ) -> Option<IndexPredicateProgram> {
        compile_index_program_from_resolved_full(&self.resolved, index_slots)
    }
}

// Collect every resolved field slot referenced by one compiled predicate tree.
fn collect_required_slots(predicate: &ResolvedPredicate) -> Vec<usize> {
    let mut slots = BTreeSet::new();
    collect_required_slots_into(predicate, &mut slots);

    slots.into_iter().collect()
}

// Recursively gather field-slot references from one compiled predicate node.
fn collect_required_slots_into(predicate: &ResolvedPredicate, slots: &mut BTreeSet<usize>) {
    match predicate {
        ResolvedPredicate::True | ResolvedPredicate::False => {}
        ResolvedPredicate::And(children) | ResolvedPredicate::Or(children) => {
            for child in children {
                collect_required_slots_into(child, slots);
            }
        }
        ResolvedPredicate::Not(inner) => collect_required_slots_into(inner, slots),
        ResolvedPredicate::Compare(cmp) => {
            if let Some(field_slot) = cmp.field_slot {
                slots.insert(field_slot);
            }
        }
        ResolvedPredicate::IsNull { field_slot }
        | ResolvedPredicate::IsMissing { field_slot }
        | ResolvedPredicate::IsEmpty { field_slot }
        | ResolvedPredicate::IsNotEmpty { field_slot }
        | ResolvedPredicate::TextContains { field_slot, .. }
        | ResolvedPredicate::TextContainsCi { field_slot, .. } => {
            if let Some(field_slot) = field_slot {
                slots.insert(*field_slot);
            }
        }
    }
}

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
/// IndexPredicateExecution
///
/// Execution-time wrapper for one compiled index predicate program.
/// Carries optional observability counters used by load execution tracing.
///

#[derive(Clone, Copy)]
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct IndexPredicateExecution<'a> {
    pub(crate) program: &'a IndexPredicateProgram,
    pub(crate) rejected_keys_counter: Option<&'a Cell<u64>>,
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

// Compile one resolved predicate tree into one index-only program.
#[cfg_attr(not(test), allow(dead_code))]
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
#[cfg_attr(not(test), allow(dead_code))]
fn compile_compare_index_node(
    cmp: &ResolvedComparePredicate,
    index_slots: &[usize],
) -> Option<IndexPredicateProgram> {
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
            let literal = encode_index_literal(&cmp.value)?;
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
            let encoded = items
                .iter()
                .map(encode_index_literal)
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
                literal: IndexLiteral::Many(encoded),
            })
        }
        CompareOp::Contains | CompareOp::StartsWith | CompareOp::EndsWith => None,
    }
}

// Encode one literal to canonical index-component bytes.
#[cfg_attr(not(test), allow(dead_code))]
fn encode_index_literal(value: &Value) -> Option<Vec<u8>> {
    let encoded = EncodedValue::try_from_ref(value).ok()?;
    Some(encoded.encoded().to_vec())
}

// Evaluate one compiled index-only program against one decoded index key.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn eval_index_program_on_decoded_key(
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
pub(crate) fn eval_index_execution_on_decoded_key(
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
fn eval_index_compare(component: &[u8], op: IndexCompareOp, literal: &IndexLiteral) -> bool {
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

fn resolve_predicate_slots<E: EntityKind>(predicate: &Predicate) -> ResolvedPredicate {
    fn resolve_field<E: EntityKind>(field_name: &str) -> Option<usize> {
        resolve_field_slot(E::MODEL, field_name)
    }

    // Compile field-name predicates to stable field-slot predicates once per query.
    match predicate {
        Predicate::True => ResolvedPredicate::True,
        Predicate::False => ResolvedPredicate::False,
        Predicate::And(children) => ResolvedPredicate::And(
            children
                .iter()
                .map(resolve_predicate_slots::<E>)
                .collect::<Vec<_>>(),
        ),
        Predicate::Or(children) => ResolvedPredicate::Or(
            children
                .iter()
                .map(resolve_predicate_slots::<E>)
                .collect::<Vec<_>>(),
        ),
        Predicate::Not(inner) => {
            ResolvedPredicate::Not(Box::new(resolve_predicate_slots::<E>(inner)))
        }
        Predicate::Compare(ComparePredicate {
            field,
            op,
            value,
            coercion,
        }) => ResolvedPredicate::Compare(ResolvedComparePredicate {
            field_slot: resolve_field::<E>(field),
            op: *op,
            value: value.clone(),
            coercion: coercion.clone(),
        }),
        Predicate::IsNull { field } => ResolvedPredicate::IsNull {
            field_slot: resolve_field::<E>(field),
        },
        Predicate::IsMissing { field } => ResolvedPredicate::IsMissing {
            field_slot: resolve_field::<E>(field),
        },
        Predicate::IsEmpty { field } => ResolvedPredicate::IsEmpty {
            field_slot: resolve_field::<E>(field),
        },
        Predicate::IsNotEmpty { field } => ResolvedPredicate::IsNotEmpty {
            field_slot: resolve_field::<E>(field),
        },
        Predicate::TextContains { field, value } => ResolvedPredicate::TextContains {
            field_slot: resolve_field::<E>(field),
            value: value.clone(),
        },
        Predicate::TextContainsCi { field, value } => ResolvedPredicate::TextContainsCi {
            field_slot: resolve_field::<E>(field),
            value: value.clone(),
        },
    }
}

///
/// FieldPresence
///
/// Result of attempting to read a field from a row during predicate
/// evaluation. This distinguishes between a missing field and a
/// present field whose value may be `None`.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum FieldPresence {
    /// Field exists and has a value (including `Value::Null`).
    Present(Value),
    /// Field is not present on the row.
    Missing,
}

///
/// Row
///
/// Abstraction over a row-like value that can expose fields by name.
/// This decouples predicate evaluation from concrete entity types.
///

#[cfg(test)]
pub(crate) trait Row {
    fn field(&self, name: &str) -> FieldPresence;
}

///
/// Default `Row` implementation for runtime entity values.
///

#[cfg(test)]
impl<T: EntityKind + EntityValue> Row for T {
    fn field(&self, name: &str) -> FieldPresence {
        let value = resolve_field_slot(T::MODEL, name)
            .and_then(|field_index| self.get_value_by_index(field_index));

        match value {
            Some(value) => FieldPresence::Present(value),
            None => FieldPresence::Missing,
        }
    }
}

///
/// FieldLookup
///
/// Runtime field-read capability used by predicate evaluation.
///

#[cfg(test)]
trait FieldLookup {
    fn field(&self, name: &str) -> FieldPresence;
}

#[cfg(test)]
impl<R: Row + ?Sized> FieldLookup for R {
    fn field(&self, name: &str) -> FieldPresence {
        Row::field(self, name)
    }
}

// Evaluate a field predicate only when the field is present.
#[cfg(test)]
fn on_present<R: FieldLookup + ?Sized>(
    row: &R,
    field: &str,
    f: impl FnOnce(&Value) -> bool,
) -> bool {
    match row.field(field) {
        FieldPresence::Present(value) => f(&value),
        FieldPresence::Missing => false,
    }
}

///
/// Evaluate a predicate against a single row.
///
/// This function performs **pure runtime evaluation**:
/// - no schema access
/// - no planning or index logic
/// - no validation
///
/// Any unsupported comparison simply evaluates to `false`.
/// CONTRACT: internal-only; predicates must be validated before evaluation.
///
#[must_use]
#[cfg(test)]
pub(crate) fn eval<R: Row + ?Sized>(row: &R, predicate: &Predicate) -> bool {
    eval_lookup(row, predicate)
}

/// Evaluate one predicate against one entity using pre-resolved field slots.
#[must_use]
pub(crate) fn eval_with_slots<E: EntityValue>(entity: &E, slots: &PredicateFieldSlots) -> bool {
    eval_with_resolved_slots(entity, &slots.resolved)
}

#[must_use]
#[expect(clippy::match_like_matches_macro)]
#[cfg(test)]
fn eval_lookup<R: FieldLookup + ?Sized>(row: &R, predicate: &Predicate) -> bool {
    match predicate {
        Predicate::True => true,
        Predicate::False => false,

        Predicate::And(children) => children.iter().all(|child| eval_lookup(row, child)),
        Predicate::Or(children) => children.iter().any(|child| eval_lookup(row, child)),
        Predicate::Not(inner) => !eval_lookup(row, inner),

        Predicate::Compare(cmp) => eval_compare(row, cmp),

        Predicate::IsNull { field } => match row.field(field) {
            FieldPresence::Present(Value::Null) => true,
            _ => false,
        },

        Predicate::IsMissing { field } => matches!(row.field(field), FieldPresence::Missing),

        Predicate::IsEmpty { field } => on_present(row, field, is_empty_value),

        Predicate::IsNotEmpty { field } => on_present(row, field, |value| !is_empty_value(value)),
        Predicate::TextContains { field, value } => on_present(row, field, |actual| {
            // NOTE: Invalid text comparisons are treated as non-matches.
            actual.text_contains(value, TextMode::Cs).unwrap_or(false)
        }),
        Predicate::TextContainsCi { field, value } => on_present(row, field, |actual| {
            // NOTE: Invalid text comparisons are treated as non-matches.
            actual.text_contains(value, TextMode::Ci).unwrap_or(false)
        }),
    }
}

// Read one field from an entity by pre-resolved slot.
fn field_from_slot<E: EntityValue>(entity: &E, field_slot: Option<usize>) -> FieldPresence {
    let value = field_slot.and_then(|slot| entity.get_value_by_index(slot));

    match value {
        Some(value) => FieldPresence::Present(value),
        None => FieldPresence::Missing,
    }
}

// Evaluate one slot-based field predicate only when the field is present.
fn on_present_slot<E: EntityValue>(
    entity: &E,
    field_slot: Option<usize>,
    f: impl FnOnce(&Value) -> bool,
) -> bool {
    match field_from_slot(entity, field_slot) {
        FieldPresence::Present(value) => f(&value),
        FieldPresence::Missing => false,
    }
}

// Evaluate one slot-resolved predicate against one entity.
#[must_use]
fn eval_with_resolved_slots<E: EntityValue>(entity: &E, predicate: &ResolvedPredicate) -> bool {
    match predicate {
        ResolvedPredicate::True => true,
        ResolvedPredicate::False => false,
        ResolvedPredicate::And(children) => children
            .iter()
            .all(|child| eval_with_resolved_slots(entity, child)),
        ResolvedPredicate::Or(children) => children
            .iter()
            .any(|child| eval_with_resolved_slots(entity, child)),
        ResolvedPredicate::Not(inner) => !eval_with_resolved_slots(entity, inner),
        ResolvedPredicate::Compare(cmp) => eval_compare_with_resolved_slots(entity, cmp),
        ResolvedPredicate::IsNull { field_slot } => {
            matches!(
                field_from_slot(entity, *field_slot),
                FieldPresence::Present(Value::Null)
            )
        }
        ResolvedPredicate::IsMissing { field_slot } => {
            matches!(field_from_slot(entity, *field_slot), FieldPresence::Missing)
        }
        ResolvedPredicate::IsEmpty { field_slot } => {
            on_present_slot(entity, *field_slot, is_empty_value)
        }
        ResolvedPredicate::IsNotEmpty { field_slot } => {
            on_present_slot(entity, *field_slot, |value| !is_empty_value(value))
        }
        ResolvedPredicate::TextContains { field_slot, value } => {
            on_present_slot(entity, *field_slot, |actual| {
                actual.text_contains(value, TextMode::Cs).unwrap_or(false)
            })
        }
        ResolvedPredicate::TextContainsCi { field_slot, value } => {
            on_present_slot(entity, *field_slot, |actual| {
                actual.text_contains(value, TextMode::Ci).unwrap_or(false)
            })
        }
    }
}

///
/// Evaluate a single comparison predicate against a row.
///
/// Returns `false` if:
/// - the field is missing
/// - the comparison is not defined under the given coercion
///
#[cfg(test)]
fn eval_compare<R: FieldLookup + ?Sized>(row: &R, cmp: &ComparePredicate) -> bool {
    let ComparePredicate {
        field,
        op,
        value,
        coercion,
    } = cmp;

    let FieldPresence::Present(actual) = row.field(field) else {
        return false;
    };

    eval_compare_values(&actual, *op, value, coercion)
}

// Evaluate a slot-resolved comparison predicate against one entity.
fn eval_compare_with_resolved_slots<E: EntityValue>(
    entity: &E,
    cmp: &ResolvedComparePredicate,
) -> bool {
    let FieldPresence::Present(actual) = field_from_slot(entity, cmp.field_slot) else {
        return false;
    };

    eval_compare_values(&actual, cmp.op, &cmp.value, &cmp.coercion)
}

// Shared compare-op semantics for test-path and runtime slot-path evaluation.
fn eval_compare_values(
    actual: &Value,
    op: CompareOp,
    value: &Value,
    coercion: &CoercionSpec,
) -> bool {
    // NOTE: Comparison helpers return None when a comparison is invalid; eval treats that as false.
    match op {
        CompareOp::Eq => compare_eq(actual, value, coercion).unwrap_or(false),
        CompareOp::Ne => compare_eq(actual, value, coercion).is_some_and(|v| !v),

        CompareOp::Lt => compare_order(actual, value, coercion).is_some_and(Ordering::is_lt),
        CompareOp::Lte => compare_order(actual, value, coercion).is_some_and(Ordering::is_le),
        CompareOp::Gt => compare_order(actual, value, coercion).is_some_and(Ordering::is_gt),
        CompareOp::Gte => compare_order(actual, value, coercion).is_some_and(Ordering::is_ge),

        CompareOp::In => in_list(actual, value, coercion).unwrap_or(false),
        CompareOp::NotIn => in_list(actual, value, coercion).is_some_and(|matched| !matched),

        CompareOp::Contains => contains(actual, value, coercion),

        CompareOp::StartsWith => {
            compare_text(actual, value, coercion, TextOp::StartsWith).unwrap_or(false)
        }
        CompareOp::EndsWith => {
            compare_text(actual, value, coercion, TextOp::EndsWith).unwrap_or(false)
        }
    }
}

///
/// Determine whether a value is considered empty for `IsEmpty` checks.
///
const fn is_empty_value(value: &Value) -> bool {
    match value {
        Value::Text(text) => text.is_empty(),
        Value::List(items) => items.is_empty(),
        _ => false,
    }
}

///
/// Check whether a value equals any element in a list.
///
fn in_list(actual: &Value, list: &Value, coercion: &CoercionSpec) -> Option<bool> {
    let Value::List(items) = list else {
        return None;
    };

    let mut saw_valid = false;
    for item in items {
        match compare_eq(actual, item, coercion) {
            Some(true) => return Some(true),
            Some(false) => saw_valid = true,
            None => {}
        }
    }

    saw_valid.then_some(false)
}

///
/// Check whether a collection contains another value.
///
/// CONTRACT: text substring matching uses TextContains/TextContainsCi only.
///
fn contains(actual: &Value, needle: &Value, coercion: &CoercionSpec) -> bool {
    if matches!(actual, Value::Text(_)) {
        // CONTRACT: text substring matching uses TextContains/TextContainsCi.
        return false;
    }

    let Value::List(items) = actual else {
        return false;
    };

    items
        .iter()
        // Invalid comparisons are treated as non-matches.
        .any(|item| compare_eq(item, needle, coercion).unwrap_or(false))
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{
        IndexCompareOp, IndexLiteral, IndexPredicateProgram, PredicateFieldSlots,
        ResolvedComparePredicate, ResolvedPredicate, collect_required_slots,
        compile_index_program_from_resolved, eval_index_compare,
    };
    use crate::{
        db::{
            index::EncodedValue,
            query::predicate::{
                CompareOp,
                coercion::{CoercionId, CoercionSpec},
            },
        },
        value::Value,
    };

    #[test]
    fn collect_required_slots_dedups_and_sorts_slots() {
        let predicate = ResolvedPredicate::And(vec![
            ResolvedPredicate::Compare(ResolvedComparePredicate {
                field_slot: Some(4),
                op: CompareOp::Eq,
                value: Value::Uint(42),
                coercion: CoercionSpec::default(),
            }),
            ResolvedPredicate::Or(vec![
                ResolvedPredicate::IsNull {
                    field_slot: Some(1),
                },
                ResolvedPredicate::IsMissing {
                    field_slot: Some(4),
                },
            ]),
            ResolvedPredicate::Not(Box::new(ResolvedPredicate::TextContains {
                field_slot: Some(2),
                value: Value::Text("x".to_string()),
            })),
            ResolvedPredicate::IsEmpty { field_slot: None },
        ]);

        let slots = collect_required_slots(&predicate);
        assert_eq!(slots, vec![1, 2, 4]);
    }

    #[test]
    fn required_slots_excludes_unresolved_field_references() {
        let resolved = ResolvedPredicate::And(vec![
            ResolvedPredicate::Compare(ResolvedComparePredicate {
                field_slot: None,
                op: CompareOp::Eq,
                value: Value::Uint(9),
                coercion: CoercionSpec::default(),
            }),
            ResolvedPredicate::TextContainsCi {
                field_slot: None,
                value: Value::Text("x".to_string()),
            },
        ]);
        let required_slots = collect_required_slots(&resolved);
        let slots = PredicateFieldSlots {
            resolved,
            required_slots,
        };

        assert!(slots.required_slots().is_empty());
    }

    #[test]
    fn compile_index_program_maps_field_slot_to_component_index() {
        let predicate = ResolvedPredicate::Compare(ResolvedComparePredicate {
            field_slot: Some(7),
            op: CompareOp::Eq,
            value: Value::Uint(11),
            coercion: CoercionSpec::new(CoercionId::Strict),
        });

        let program = compile_index_program_from_resolved(&predicate, &[3, 7, 9])
            .expect("strict EQ over indexed slot should compile");
        let expected = EncodedValue::try_from_ref(&Value::Uint(11))
            .expect("uint literal should encode")
            .encoded()
            .to_vec();

        assert_eq!(
            program,
            IndexPredicateProgram::Compare {
                component_index: 1,
                op: IndexCompareOp::Eq,
                literal: IndexLiteral::One(expected),
            }
        );
    }

    #[test]
    fn compile_index_program_rejects_non_strict_coercion() {
        let predicate = ResolvedPredicate::Compare(ResolvedComparePredicate {
            field_slot: Some(1),
            op: CompareOp::Eq,
            value: Value::Uint(11),
            coercion: CoercionSpec::new(CoercionId::NumericWiden),
        });

        let program = compile_index_program_from_resolved(&predicate, &[1]);
        assert!(program.is_none());
    }

    #[test]
    fn eval_index_compare_applies_membership_semantics() {
        let component = &[1_u8, 2_u8, 3_u8][..];
        let in_literal = IndexLiteral::Many(vec![vec![9_u8], vec![1_u8, 2_u8, 3_u8]]);
        let not_in_literal = IndexLiteral::Many(vec![vec![0_u8], vec![4_u8]]);

        assert!(eval_index_compare(
            component,
            IndexCompareOp::In,
            &in_literal
        ));
        assert!(eval_index_compare(
            component,
            IndexCompareOp::NotIn,
            &not_in_literal
        ));
    }

    #[test]
    fn compile_index_program_operator_matrix_matches_strict_subset() {
        let eligible = [
            (CompareOp::Eq, Value::Uint(11)),
            (CompareOp::Ne, Value::Uint(11)),
            (CompareOp::Lt, Value::Uint(11)),
            (CompareOp::Lte, Value::Uint(11)),
            (CompareOp::Gt, Value::Uint(11)),
            (CompareOp::Gte, Value::Uint(11)),
            (
                CompareOp::In,
                Value::List(vec![Value::Uint(11), Value::Uint(12)]),
            ),
            (
                CompareOp::NotIn,
                Value::List(vec![Value::Uint(11), Value::Uint(12)]),
            ),
        ];
        for (op, value) in eligible {
            let predicate = ResolvedPredicate::Compare(ResolvedComparePredicate {
                field_slot: Some(1),
                op,
                value,
                coercion: CoercionSpec::new(CoercionId::Strict),
            });
            let program = compile_index_program_from_resolved(&predicate, &[1]);

            assert!(
                program.is_some(),
                "strict compare op {op:?} should compile into an index predicate program",
            );
        }

        let ineligible = [
            (CompareOp::Contains, Value::Text("x".to_string())),
            (CompareOp::StartsWith, Value::Text("x".to_string())),
            (CompareOp::EndsWith, Value::Text("x".to_string())),
        ];
        for (op, value) in ineligible {
            let predicate = ResolvedPredicate::Compare(ResolvedComparePredicate {
                field_slot: Some(1),
                op,
                value,
                coercion: CoercionSpec::new(CoercionId::Strict),
            });
            let program = compile_index_program_from_resolved(&predicate, &[1]);

            assert!(
                program.is_none(),
                "op {op:?} should stay on fallback execution",
            );
        }
    }

    #[test]
    fn compile_index_program_rejects_non_strict_coercion_across_operator_subset() {
        let operators = [
            (CompareOp::Eq, Value::Uint(11)),
            (CompareOp::Ne, Value::Uint(11)),
            (CompareOp::Lt, Value::Uint(11)),
            (CompareOp::Lte, Value::Uint(11)),
            (CompareOp::Gt, Value::Uint(11)),
            (CompareOp::Gte, Value::Uint(11)),
            (
                CompareOp::In,
                Value::List(vec![Value::Uint(11), Value::Uint(12)]),
            ),
            (
                CompareOp::NotIn,
                Value::List(vec![Value::Uint(11), Value::Uint(12)]),
            ),
        ];

        for (op, value) in operators {
            let predicate = ResolvedPredicate::Compare(ResolvedComparePredicate {
                field_slot: Some(1),
                op,
                value,
                coercion: CoercionSpec::new(CoercionId::NumericWiden),
            });
            let program = compile_index_program_from_resolved(&predicate, &[1]);

            assert!(
                program.is_none(),
                "non-strict coercion should reject index-only compile for op {op:?}",
            );
        }
    }

    #[test]
    fn compile_index_program_keeps_safe_and_subset_when_residual_is_uncompilable() {
        let predicate = ResolvedPredicate::And(vec![
            ResolvedPredicate::Compare(ResolvedComparePredicate {
                field_slot: Some(1),
                op: CompareOp::Eq,
                value: Value::Uint(7),
                coercion: CoercionSpec::new(CoercionId::Strict),
            }),
            ResolvedPredicate::TextContains {
                field_slot: Some(9),
                value: Value::Text("residual".to_string()),
            },
            ResolvedPredicate::Compare(ResolvedComparePredicate {
                field_slot: Some(2),
                op: CompareOp::In,
                value: Value::List(vec![Value::Uint(10), Value::Uint(20)]),
                coercion: CoercionSpec::new(CoercionId::Strict),
            }),
        ]);

        let program = compile_index_program_from_resolved(&predicate, &[1, 2]);
        assert!(
            program.is_some(),
            "AND predicates should keep index-only-safe children as a subset",
        );
    }

    #[test]
    fn compile_index_program_rejects_or_with_uncompilable_child() {
        let predicate = ResolvedPredicate::Or(vec![
            ResolvedPredicate::Compare(ResolvedComparePredicate {
                field_slot: Some(1),
                op: CompareOp::Eq,
                value: Value::Uint(7),
                coercion: CoercionSpec::new(CoercionId::Strict),
            }),
            ResolvedPredicate::TextContains {
                field_slot: Some(9),
                value: Value::Text("residual".to_string()),
            },
        ]);

        let program = compile_index_program_from_resolved(&predicate, &[1, 2]);
        assert!(
            program.is_none(),
            "OR predicates must fail closed when any child is not index-only-safe",
        );
    }
}
