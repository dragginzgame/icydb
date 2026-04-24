//! Module: db::predicate::capability
//! Defines the predicate capability classifiers used to choose scalar,
//! index-backed, or full-scan evaluation paths.

use crate::{
    db::{
        index::derive_index_expression_value,
        predicate::{CoercionId, CompareOp, ExecutableComparePredicate, ExecutablePredicate},
    },
    model::{entity::EntityModel, field::LeafCodec, index::IndexKeyItem},
    value::Value,
};

///
/// ScalarPredicateCapability
///
/// Scalar execution capability derived from the canonical executable predicate
/// tree. Runtime uses this to decide whether the predicate can stay on the
/// scalar slot seam or must fall back to generic value evaluation.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum ScalarPredicateCapability {
    ScalarSafe,
    RequiresGenericEvaluation,
}

///
/// IndexPredicateCapability
///
/// Index compilation capability derived from the canonical executable
/// predicate tree. `PartiallyIndexable` is reserved for conservative AND-subset
/// retention; callers that require exact full-tree index compilation must
/// demand `FullyIndexable`.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum IndexPredicateCapability {
    FullyIndexable,
    PartiallyIndexable,
    RequiresFullScan,
}

///
/// PredicateCapabilityContext
///
/// Optional capability inputs available at one predicate boundary.
/// Runtime classification needs a model to prove scalar-slot execution.
/// Index classification needs the active index slot projection.
///
#[derive(Clone, Copy, Debug, Default)]
pub(in crate::db) struct PredicateCapabilityContext<'a> {
    compile_targets: Option<&'a [IndexCompileTarget]>,
    model: Option<&'a EntityModel>,
    index_slots: Option<&'a [usize]>,
}

impl<'a> PredicateCapabilityContext<'a> {
    /// Construct one runtime capability context.
    #[must_use]
    pub(in crate::db) const fn runtime(model: &'a EntityModel) -> Self {
        Self {
            compile_targets: None,
            model: Some(model),
            index_slots: None,
        }
    }

    /// Construct one index-compilation capability context.
    #[must_use]
    pub(in crate::db) const fn index_compile(index_slots: &'a [usize]) -> Self {
        Self {
            compile_targets: None,
            model: None,
            index_slots: Some(index_slots),
        }
    }
}

///
/// IndexCompileTarget
///
/// Key-item-aware index compile target for one resolved access component.
/// This keeps expression-index predicate capability and compile lowering on
/// one shared boundary instead of pretending raw field slots are sufficient.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct IndexCompileTarget {
    pub(in crate::db) component_index: usize,
    pub(in crate::db) field_slot: usize,
    pub(in crate::db) key_item: IndexKeyItem,
}

///
/// PredicateCapabilityProfile
///
/// Capability snapshot derived once from the canonical executable predicate tree.
/// This profile keeps scalar and index capability as explicit classified
/// states instead of collapsing the boundary back into booleans. That preserves
/// the reasons callers need when strict compilation, conservative subset
/// retention, and generic runtime fallback diverge.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct PredicateCapabilityProfile {
    scalar: ScalarPredicateCapability,
    index: IndexPredicateCapability,
}

impl PredicateCapabilityProfile {
    /// Return scalar execution capability for this predicate snapshot.
    #[must_use]
    pub(in crate::db) const fn scalar(self) -> ScalarPredicateCapability {
        self.scalar
    }

    /// Return index compilation capability for this predicate snapshot.
    #[must_use]
    pub(in crate::db) const fn index(self) -> IndexPredicateCapability {
        self.index
    }
}

/// Derive one capability snapshot from the canonical executable predicate tree.
#[must_use]
pub(in crate::db) fn classify_predicate_capabilities(
    predicate: &ExecutablePredicate,
    context: PredicateCapabilityContext<'_>,
) -> PredicateCapabilityProfile {
    PredicateCapabilityProfile {
        scalar: context.model.map_or(
            ScalarPredicateCapability::RequiresGenericEvaluation,
            |model| classify_scalar_capability(model, predicate),
        ),
        index: if let Some(compile_targets) = context.compile_targets {
            classify_index_capability_for_targets(predicate, compile_targets)
        } else {
            context
                .index_slots
                .map_or(IndexPredicateCapability::RequiresFullScan, |index_slots| {
                    classify_index_capability(predicate, index_slots)
                })
        },
    }
}

/// Derive one capability snapshot from the canonical executable predicate tree
/// using key-item-aware index compile targets.
#[must_use]
pub(in crate::db) fn classify_predicate_capabilities_for_targets(
    predicate: &ExecutablePredicate,
    compile_targets: &[IndexCompileTarget],
) -> PredicateCapabilityProfile {
    PredicateCapabilityProfile {
        scalar: ScalarPredicateCapability::RequiresGenericEvaluation,
        index: classify_index_capability_for_targets(predicate, compile_targets),
    }
}

/// Resolve one compare node to the index component it is allowed to target.
#[must_use]
pub(in crate::db) fn classify_index_compare_component(
    cmp: &ExecutableComparePredicate,
    index_slots: &[usize],
) -> Option<usize> {
    if !compare_is_indexable(cmp, index_slots) {
        return None;
    }

    let field_slot = cmp.left_field_slot()?;
    index_slots.iter().position(|slot| *slot == field_slot)
}

/// Resolve one compare node to the key-item-aware compile target it may use.
#[must_use]
pub(in crate::db) fn classify_index_compare_target(
    cmp: &ExecutableComparePredicate,
    compile_targets: &[IndexCompileTarget],
) -> Option<IndexCompileTarget> {
    let field_slot = cmp.left_field_slot()?;

    compile_targets.iter().copied().find(|target| {
        target.field_slot == field_slot && compare_is_indexable_for_target(cmp, *target)
    })
}

/// Lower one compare literal onto the canonical bytes expected by one compile target.
#[must_use]
pub(in crate::db) fn lower_index_compare_literal_for_target(
    target: IndexCompileTarget,
    value: &Value,
    coercion: CoercionId,
) -> Option<Value> {
    match target.key_item {
        IndexKeyItem::Field(_) => (coercion == CoercionId::Strict).then(|| value.clone()),
        IndexKeyItem::Expression(expression) => {
            if coercion != CoercionId::TextCasefold || !expression.supports_text_casefold_lookup() {
                return None;
            }

            derive_index_expression_value(expression, value.clone())
                .ok()
                .flatten()
        }
    }
}

/// Lower one starts-with predicate prefix onto the canonical bytes expected by one compile target.
#[must_use]
pub(in crate::db) fn lower_index_starts_with_prefix_for_target(
    target: IndexCompileTarget,
    value: &Value,
    coercion: CoercionId,
) -> Option<String> {
    let lowered = lower_index_compare_literal_for_target(target, value, coercion)?;
    let Value::Text(prefix) = lowered else {
        return None;
    };
    if prefix.is_empty() {
        return None;
    }

    Some(prefix)
}

// Classify whether one executable predicate can stay on the scalar slot seam.
fn classify_scalar_capability(
    model: &EntityModel,
    predicate: &ExecutablePredicate,
) -> ScalarPredicateCapability {
    if predicate_is_scalar_safe(model, predicate) {
        ScalarPredicateCapability::ScalarSafe
    } else {
        ScalarPredicateCapability::RequiresGenericEvaluation
    }
}

// Classify how much of one executable predicate can stay on the index-only seam.
fn classify_index_capability(
    predicate: &ExecutablePredicate,
    index_slots: &[usize],
) -> IndexPredicateCapability {
    classify_index_capability_with_compare(predicate, |cmp| compare_is_indexable(cmp, index_slots))
}

// Classify index capability when the chosen access route carries key-item-aware
// compile targets instead of raw field-slot membership alone.
fn classify_index_capability_for_targets(
    predicate: &ExecutablePredicate,
    compile_targets: &[IndexCompileTarget],
) -> IndexPredicateCapability {
    classify_index_capability_with_compare(predicate, |cmp| {
        classify_index_compare_target(cmp, compile_targets).is_some()
    })
}

// Keep the index-capability recursion on one shared tree walk and vary only
// the compare-leaf admission rule between slot-based and target-based callers.
fn classify_index_capability_with_compare(
    predicate: &ExecutablePredicate,
    compare_is_fully_indexable: impl Fn(&ExecutableComparePredicate) -> bool + Copy,
) -> IndexPredicateCapability {
    match predicate {
        ExecutablePredicate::True | ExecutablePredicate::False => {
            IndexPredicateCapability::FullyIndexable
        }
        ExecutablePredicate::And(children) => {
            merge_and_index_capability(children.iter().map(|child| {
                classify_index_capability_with_compare(child, compare_is_fully_indexable)
            }))
        }
        ExecutablePredicate::Or(children) => {
            if children.iter().all(|child| {
                classify_index_capability_with_compare(child, compare_is_fully_indexable)
                    == IndexPredicateCapability::FullyIndexable
            }) {
                IndexPredicateCapability::FullyIndexable
            } else {
                IndexPredicateCapability::RequiresFullScan
            }
        }
        ExecutablePredicate::Not(inner) => {
            if classify_index_capability_with_compare(inner, compare_is_fully_indexable)
                == IndexPredicateCapability::FullyIndexable
            {
                IndexPredicateCapability::FullyIndexable
            } else {
                IndexPredicateCapability::RequiresFullScan
            }
        }
        ExecutablePredicate::Compare(cmp) => {
            if compare_is_fully_indexable(cmp) {
                IndexPredicateCapability::FullyIndexable
            } else {
                IndexPredicateCapability::RequiresFullScan
            }
        }
        ExecutablePredicate::IsNull { .. }
        | ExecutablePredicate::IsNotNull { .. }
        | ExecutablePredicate::IsMissing { .. }
        | ExecutablePredicate::IsEmpty { .. }
        | ExecutablePredicate::IsNotEmpty { .. }
        | ExecutablePredicate::TextContains { .. }
        | ExecutablePredicate::TextContainsCi { .. } => IndexPredicateCapability::RequiresFullScan,
    }
}

// AND trees can retain conservative indexable subsets even when not all
// children are individually index-compilable.
fn merge_and_index_capability(
    children: impl Iterator<Item = IndexPredicateCapability>,
) -> IndexPredicateCapability {
    let mut all_full = true;
    let mut any_retainable = false;

    for capability in children {
        match capability {
            IndexPredicateCapability::FullyIndexable => {
                any_retainable = true;
            }
            IndexPredicateCapability::PartiallyIndexable => {
                all_full = false;
                any_retainable = true;
            }
            IndexPredicateCapability::RequiresFullScan => {
                all_full = false;
            }
        }
    }

    if all_full {
        IndexPredicateCapability::FullyIndexable
    } else if any_retainable {
        IndexPredicateCapability::PartiallyIndexable
    } else {
        IndexPredicateCapability::RequiresFullScan
    }
}

// Predicate classification remains exhaustive over the canonical executable tree.
fn predicate_is_scalar_safe(model: &EntityModel, predicate: &ExecutablePredicate) -> bool {
    match predicate {
        ExecutablePredicate::True
        | ExecutablePredicate::False
        | ExecutablePredicate::IsMissing { .. } => true,
        ExecutablePredicate::And(children) | ExecutablePredicate::Or(children) => children
            .iter()
            .all(|child| predicate_is_scalar_safe(model, child)),
        ExecutablePredicate::Not(inner) => predicate_is_scalar_safe(model, inner),
        ExecutablePredicate::Compare(cmp) => compare_is_scalar_safe(model, cmp),
        ExecutablePredicate::IsNull { field_slot }
        | ExecutablePredicate::IsNotNull { field_slot }
        | ExecutablePredicate::IsEmpty { field_slot }
        | ExecutablePredicate::IsNotEmpty { field_slot } => {
            scalar_field_slot_supported(model, *field_slot)
        }
        ExecutablePredicate::TextContains { field_slot, value }
        | ExecutablePredicate::TextContainsCi { field_slot, value } => {
            scalar_field_slot_supported(model, *field_slot) && matches!(value, Value::Text(_))
        }
    }
}

// Classify whether one compare node can stay on the scalar slot seam.
fn compare_is_scalar_safe(model: &EntityModel, cmp: &ExecutableComparePredicate) -> bool {
    match (
        cmp.left_field_slot(),
        cmp.right_literal(),
        cmp.right_field_slot(),
    ) {
        (Some(left_field_slot), Some(value), None) => {
            scalar_field_slot_supported(model, Some(left_field_slot))
                && scalar_compare_op_supported(cmp.op)
                && scalar_compare_literal_coercion_supported(cmp.coercion.id)
                && scalar_compare_literal_supported(cmp.op, value)
        }
        (Some(left_field_slot), None, Some(right_field_slot)) => {
            scalar_field_slot_supported(model, Some(left_field_slot))
                && scalar_field_slot_supported(model, Some(right_field_slot))
                && scalar_field_compare_op_supported(cmp.op)
                && scalar_compare_field_coercion_supported(cmp.coercion.id)
        }
        _ => false,
    }
}

// Classify whether one compare node is index-compilable for one slot projection.
fn compare_is_indexable(cmp: &ExecutableComparePredicate, index_slots: &[usize]) -> bool {
    if cmp.coercion.id != CoercionId::Strict {
        return false;
    }

    let Some(field_slot) = cmp.left_field_slot() else {
        return false;
    };
    let Some(value) = cmp.right_literal() else {
        return false;
    };
    if !index_slots.contains(&field_slot) {
        return false;
    }

    if cmp.op.is_equality_family() || cmp.op.is_ordering_family() {
        value_is_index_literal(value)
    } else if cmp.op.is_membership_family() {
        list_value_is_non_empty_index_literal(value)
    } else if matches!(cmp.op, CompareOp::StartsWith) {
        matches!(value, Value::Text(prefix) if !prefix.is_empty())
    } else {
        false
    }
}

// Classify whether one compare node is index-compilable for one key-item-aware
// compile target.
fn compare_is_indexable_for_target(
    cmp: &ExecutableComparePredicate,
    target: IndexCompileTarget,
) -> bool {
    let Some(value) = cmp.right_literal() else {
        return false;
    };

    if cmp.op.is_equality_family() || cmp.op.is_ordering_family() {
        lower_index_compare_literal_for_target(target, value, cmp.coercion.id)
            .is_some_and(|value| value_is_index_literal(&value))
    } else if cmp.op.is_membership_family() {
        let Value::List(items) = value else {
            return false;
        };
        !items.is_empty()
            && items.iter().all(|value| {
                lower_index_compare_literal_for_target(target, value, cmp.coercion.id)
                    .is_some_and(|value| value_is_index_literal(&value))
            })
    } else if matches!(cmp.op, CompareOp::StartsWith) {
        lower_index_starts_with_prefix_for_target(target, value, cmp.coercion.id).is_some()
    } else {
        false
    }
}

// Keep scalar fast-path operators centralized under the capability boundary.
const fn scalar_compare_op_supported(op: CompareOp) -> bool {
    op.is_equality_family()
        || op.is_ordering_family()
        || op.is_membership_family()
        || op.is_text_pattern_family()
}

// Numeric widening still requires generic runtime comparison.
const fn scalar_compare_literal_coercion_supported(coercion: CoercionId) -> bool {
    !matches!(coercion, CoercionId::NumericWiden)
}

// Field-vs-field scalar fast path shares the generic compare semantics layer,
// so numeric widening is still allowed even though literal fast paths reject it.
const fn scalar_compare_field_coercion_supported(coercion: CoercionId) -> bool {
    !matches!(coercion, CoercionId::CollectionElement)
}

// Field-vs-field compare leaves are intentionally bounded to ordinary ordered
// comparison operators in the current slice.
const fn scalar_field_compare_op_supported(op: CompareOp) -> bool {
    matches!(
        op,
        CompareOp::Eq
            | CompareOp::Ne
            | CompareOp::Lt
            | CompareOp::Lte
            | CompareOp::Gt
            | CompareOp::Gte
    )
}

// Scalar fast-path execution is only valid for scalar leaf codecs.
fn scalar_field_slot_supported(model: &EntityModel, field_slot: Option<usize>) -> bool {
    let Some(field_slot) = field_slot else {
        return false;
    };
    let Some(field_model) = model.fields().get(field_slot) else {
        return false;
    };

    matches!(field_model.leaf_codec(), LeafCodec::Scalar(_))
}

// Scalar comparison literals must stay within the direct scalar value subset.
fn scalar_compare_literal_supported(op: CompareOp, value: &Value) -> bool {
    match op {
        CompareOp::In | CompareOp::NotIn => match value {
            Value::List(items) => items.iter().all(value_is_scalar_literal_supported),
            _ => false,
        },
        _ => value_is_scalar_literal_supported(value),
    }
}

// Admit only direct scalar value literals into the scalar fast path.
const fn value_is_scalar_literal_supported(value: &Value) -> bool {
    matches!(
        value,
        Value::Null
            | Value::Blob(_)
            | Value::Bool(_)
            | Value::Date(_)
            | Value::Duration(_)
            | Value::Float32(_)
            | Value::Float64(_)
            | Value::Int(_)
            | Value::Principal(_)
            | Value::Subaccount(_)
            | Value::Text(_)
            | Value::Timestamp(_)
            | Value::Uint(_)
            | Value::Ulid(_)
            | Value::Unit
    )
}

// Admit only index-encodable single values into direct index comparisons.
const fn value_is_index_literal(value: &Value) -> bool {
    matches!(
        value,
        Value::Blob(_)
            | Value::Bool(_)
            | Value::Date(_)
            | Value::Duration(_)
            | Value::Float32(_)
            | Value::Float64(_)
            | Value::Int(_)
            | Value::Principal(_)
            | Value::Subaccount(_)
            | Value::Text(_)
            | Value::Timestamp(_)
            | Value::Uint(_)
            | Value::Ulid(_)
            | Value::Unit
    )
}

// `IN`/`NOT IN` index compares require a non-empty all-literal list.
fn list_value_is_non_empty_index_literal(value: &Value) -> bool {
    let Value::List(items) = value else {
        return false;
    };

    !items.is_empty() && items.iter().all(value_is_index_literal)
}
