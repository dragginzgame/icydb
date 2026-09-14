//! Module: db::predicate::capability
//! Defines the predicate capability classifiers used to choose scalar,
//! index-backed, or full-scan evaluation paths.

use crate::{
    db::{
        index::{derive_index_expression_value, index_expression_supports_text_casefold_lookup},
        predicate::{CoercionId, CompareOp, ExecutableComparePredicate, ExecutablePredicate},
        query::construction::ConstructionBudget,
        schema::{PersistedIndexExpressionOp, SchemaInfo},
    },
    error::InternalError,
    value::{Value, lower_text_construction_allowance},
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;
use std::borrow::Cow;

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
/// Runtime classification needs schema info to prove scalar-slot execution.
/// Index classification needs the active index slot projection.
///
#[derive(Clone, Copy, Debug, Default)]
pub(in crate::db) struct PredicateCapabilityContext<'a> {
    compile_targets: Option<&'a [IndexCompileTarget]>,
    schema_info: Option<&'a SchemaInfo>,
    index_slots: Option<&'a [usize]>,
}

impl<'a> PredicateCapabilityContext<'a> {
    /// Construct one runtime capability context from explicit schema authority.
    #[must_use]
    pub(in crate::db) const fn runtime_schema(schema_info: &'a SchemaInfo) -> Self {
        Self {
            compile_targets: None,
            schema_info: Some(schema_info),
            index_slots: None,
        }
    }

    /// Construct one index-compilation capability context.
    #[must_use]
    pub(in crate::db) const fn index_compile(index_slots: &'a [usize]) -> Self {
        Self {
            compile_targets: None,
            schema_info: None,
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
    pub(in crate::db) kind: IndexCompileTargetKind,
}

/// Reduced accepted index-key semantics needed by predicate compilation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum IndexCompileTargetKind {
    Field,
    Expression(PersistedIndexExpressionOp),
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
        scalar: context.schema_info.map_or(
            ScalarPredicateCapability::RequiresGenericEvaluation,
            |schema_info| classify_scalar_capability(schema_info, predicate),
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

/// Lower one compare literal using the accepted field/expression semantics.
/// Identity lowering borrows; expression lowering owns only its derived result.
#[must_use]
pub(in crate::db) fn lower_index_compare_literal_for_kind(
    kind: IndexCompileTargetKind,
    value: &Value,
    coercion: CoercionId,
) -> Option<Cow<'_, Value>> {
    if !index_kind_supports_coercion(kind, coercion) {
        return None;
    }

    match kind {
        IndexCompileTargetKind::Field => Some(Cow::Borrowed(value)),
        IndexCompileTargetKind::Expression(op) => derive_index_expression_value(op, value)
            .ok()
            .flatten()
            .map(Cow::Owned),
    }
}

/// Admit the supported expression conversion before allocating its result.
/// Identity literals and unsupported target/source pairs perform no conversion.
pub(in crate::db) fn admit_index_compare_literal_for_kind(
    kind: IndexCompileTargetKind,
    value: &Value,
    coercion: CoercionId,
    budget: &dyn ConstructionBudget,
) -> Result<(), InternalError> {
    if !matches!(kind, IndexCompileTargetKind::Expression(_))
        || !index_kind_supports_coercion(kind, coercion)
    {
        return Ok(());
    }
    if let Value::Text(text) = value {
        // The current accepted lookup conversion is LOWER only. Its canonical
        // text owner supplies the allowance; no second transformation is run.
        let (backing, work) = lower_text_construction_allowance(text.len());
        budget.charge(Resource::TemporaryBytes, backing)?;
        budget.charge(Resource::PredicateExpressionSteps, work)?;
    }
    Ok(())
}

// Classification and lowering must agree on the accepted target/coercion pair.
fn index_kind_supports_coercion(kind: IndexCompileTargetKind, coercion: CoercionId) -> bool {
    match kind {
        IndexCompileTargetKind::Field => coercion == CoercionId::Strict,
        IndexCompileTargetKind::Expression(op) => {
            coercion == CoercionId::TextCasefold
                && index_expression_supports_text_casefold_lookup(op)
        }
    }
}

/// Lower one starts-with prefix without copying an unchanged field literal.
#[must_use]
pub(in crate::db) fn lower_index_starts_with_prefix_for_target(
    target: IndexCompileTarget,
    value: &Value,
    coercion: CoercionId,
) -> Option<Cow<'_, str>> {
    let lowered = lower_index_compare_literal_for_kind(target.kind, value, coercion)?;
    let prefix = match lowered {
        Cow::Borrowed(Value::Text(prefix)) => Cow::Borrowed(prefix.as_str()),
        Cow::Owned(Value::Text(prefix)) => Cow::Owned(prefix),
        _ => return None,
    };
    if prefix.is_empty() {
        return None;
    }

    Some(prefix)
}

// Classify whether one executable predicate can stay on the scalar slot seam.
fn classify_scalar_capability(
    schema_info: &SchemaInfo,
    predicate: &ExecutablePredicate,
) -> ScalarPredicateCapability {
    if predicate_is_scalar_safe(schema_info, predicate) {
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
        ExecutablePredicate::Compare(cmp) => {
            if compare_is_fully_indexable(cmp) {
                IndexPredicateCapability::FullyIndexable
            } else {
                IndexPredicateCapability::RequiresFullScan
            }
        }
        // A negated indexable child is still not index-covering-safe unless
        // the access planner has an explicit complement-scan contract.
        ExecutablePredicate::Not(_)
        | ExecutablePredicate::IsNull { .. }
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
fn predicate_is_scalar_safe(schema_info: &SchemaInfo, predicate: &ExecutablePredicate) -> bool {
    match predicate {
        ExecutablePredicate::True
        | ExecutablePredicate::False
        | ExecutablePredicate::IsMissing { .. } => true,
        ExecutablePredicate::And(children) | ExecutablePredicate::Or(children) => children
            .iter()
            .all(|child| predicate_is_scalar_safe(schema_info, child)),
        ExecutablePredicate::Not(inner) => predicate_is_scalar_safe(schema_info, inner),
        ExecutablePredicate::Compare(cmp) => compare_is_scalar_safe(schema_info, cmp),
        ExecutablePredicate::IsNull { field_slot }
        | ExecutablePredicate::IsNotNull { field_slot }
        | ExecutablePredicate::IsEmpty { field_slot }
        | ExecutablePredicate::IsNotEmpty { field_slot } => {
            scalar_field_slot_supported(schema_info, *field_slot)
        }
        ExecutablePredicate::TextContains { field_slot, value }
        | ExecutablePredicate::TextContainsCi { field_slot, value } => {
            scalar_field_slot_supported(schema_info, *field_slot) && matches!(value, Value::Text(_))
        }
    }
}

// Classify whether one compare node can stay on the scalar slot seam.
fn compare_is_scalar_safe(schema_info: &SchemaInfo, cmp: &ExecutableComparePredicate) -> bool {
    match (
        cmp.left_field_slot(),
        cmp.right_literal(),
        cmp.right_field_slot(),
    ) {
        (Some(left_field_slot), Some(value), None) => {
            scalar_field_slot_supported(schema_info, Some(left_field_slot))
                && scalar_compare_op_supported(cmp.op)
                && scalar_compare_literal_coercion_supported(cmp.coercion.id)
                && scalar_compare_literal_supported(cmp.op, value)
        }
        (Some(left_field_slot), None, Some(right_field_slot)) => {
            scalar_field_slot_supported(schema_info, Some(left_field_slot))
                && scalar_field_slot_supported(schema_info, Some(right_field_slot))
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
    if !index_kind_supports_coercion(target.kind, cmp.coercion.id) {
        return false;
    }
    let Some(value) = cmp.right_literal() else {
        return false;
    };

    // The admitted expression lookup is LOWER over text: it always returns
    // indexable text and preserves emptiness. Inspect the source shape instead
    // of allocating transformed values during every capability walk. Actual
    // compilation still uses the canonical scalar transformation owner.
    let literal_supported = |value: &Value| match target.kind {
        IndexCompileTargetKind::Field => value_is_index_literal(value),
        IndexCompileTargetKind::Expression(_) => matches!(value, Value::Text(_)),
    };

    if cmp.op.is_equality_family() || cmp.op.is_ordering_family() {
        literal_supported(value)
    } else if cmp.op.is_membership_family() {
        let Value::List(items) = value else {
            return false;
        };
        !items.is_empty() && items.iter().all(literal_supported)
    } else if matches!(cmp.op, CompareOp::StartsWith) {
        matches!(value, Value::Text(prefix) if !prefix.is_empty())
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
fn scalar_field_slot_supported(schema_info: &SchemaInfo, field_slot: Option<usize>) -> bool {
    let Some(field_slot) = field_slot else {
        return false;
    };

    schema_info.field_slot_has_scalar_leaf(field_slot)
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
            | Value::Int64(_)
            | Value::Principal(_)
            | Value::Subaccount(_)
            | Value::Text(_)
            | Value::Timestamp(_)
            | Value::Nat64(_)
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
            | Value::Int64(_)
            | Value::Principal(_)
            | Value::Subaccount(_)
            | Value::Text(_)
            | Value::Timestamp(_)
            | Value::Nat64(_)
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

#[cfg(test)]
mod tests {
    use super::{
        IndexCompileTarget, IndexCompileTargetKind, classify_index_compare_target,
        lower_index_compare_literal_for_kind, lower_index_starts_with_prefix_for_target,
        value_is_index_literal,
    };
    use crate::{
        db::{
            predicate::{CoercionId, CoercionSpec, CompareOp, ExecutableComparePredicate},
            schema::PersistedIndexExpressionOp,
        },
        types::{IntBig, NatBig},
        value::Value,
    };
    use std::borrow::Cow;

    fn expression_target(op: PersistedIndexExpressionOp) -> IndexCompileTarget {
        IndexCompileTarget {
            component_index: 0,
            field_slot: 0,
            kind: IndexCompileTargetKind::Expression(op),
        }
    }

    #[test]
    fn target_classification_agrees_with_canonical_literal_lowering() {
        let field = IndexCompileTarget {
            kind: IndexCompileTargetKind::Field,
            ..expression_target(PersistedIndexExpressionOp::Lower)
        };
        let expressions = [
            PersistedIndexExpressionOp::Lower,
            PersistedIndexExpressionOp::Upper,
            PersistedIndexExpressionOp::Trim,
            PersistedIndexExpressionOp::LowerTrim,
            PersistedIndexExpressionOp::Date,
            PersistedIndexExpressionOp::Year,
            PersistedIndexExpressionOp::Month,
            PersistedIndexExpressionOp::Day,
        ];
        let values = [
            Value::Text(String::new()),
            Value::Text(" \u{2003}İΣß\0 ".repeat(128)),
            Value::Null,
            Value::Unit,
            Value::Nat64(1),
            Value::IntBig(IntBig::from(-256)),
            Value::List(vec![]),
            Value::List(vec![Value::Text(String::new()), Value::Text("ÄBC".into())]),
            Value::List(vec![Value::Text("ÄBC".into()), Value::Null]),
            Value::List(vec![Value::Text("ÄBC".into()), Value::Nat64(1)]),
            Value::List(vec![Value::List(vec![Value::Text("ÄBC".into())])]),
        ];
        for target in std::iter::once(field).chain(expressions.map(expression_target)) {
            for coercion in [
                CoercionId::Strict,
                CoercionId::TextCasefold,
                CoercionId::NumericWiden,
                CoercionId::CollectionElement,
            ] {
                let scalar_supported = |value| {
                    lower_index_compare_literal_for_kind(target.kind, value, coercion)
                        .is_some_and(|value| value_is_index_literal(&value))
                };
                for op in [
                    CompareOp::Eq,
                    CompareOp::Ne,
                    CompareOp::Lt,
                    CompareOp::Lte,
                    CompareOp::Gt,
                    CompareOp::Gte,
                    CompareOp::In,
                    CompareOp::NotIn,
                    CompareOp::StartsWith,
                    CompareOp::EndsWith,
                    CompareOp::Contains,
                ] {
                    for value in &values {
                        let expected = match op {
                            CompareOp::Eq
                            | CompareOp::Ne
                            | CompareOp::Lt
                            | CompareOp::Lte
                            | CompareOp::Gt
                            | CompareOp::Gte => scalar_supported(value),
                            CompareOp::In | CompareOp::NotIn => match value {
                                Value::List(items) => {
                                    !items.is_empty() && items.iter().all(scalar_supported)
                                }
                                _ => false,
                            },
                            CompareOp::StartsWith => {
                                lower_index_starts_with_prefix_for_target(target, value, coercion)
                                    .is_some()
                            }
                            CompareOp::EndsWith | CompareOp::Contains => false,
                        };
                        let cmp = ExecutableComparePredicate::field_literal(
                            Some(target.field_slot),
                            op,
                            value.clone(),
                            CoercionSpec::new(coercion),
                        );
                        assert_eq!(
                            classify_index_compare_target(&cmp, &[target]),
                            expected.then_some(target),
                            "target={target:?}, coercion={coercion:?}, op={op:?}",
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn text_casefold_literals_only_lower_through_matching_expression_keys() {
        let literal = Value::Text("ßeta".to_string());

        assert_eq!(
            lower_index_compare_literal_for_kind(
                expression_target(PersistedIndexExpressionOp::Lower).kind,
                &literal,
                CoercionId::TextCasefold,
            )
            .as_deref(),
            Some(&literal),
        );
        assert_eq!(
            lower_index_compare_literal_for_kind(
                expression_target(PersistedIndexExpressionOp::Upper).kind,
                &literal,
                CoercionId::TextCasefold,
            ),
            None,
        );
    }

    #[test]
    fn identity_target_lowering_borrows_operands_and_prefixes() {
        let target = IndexCompileTarget {
            component_index: 0,
            field_slot: 0,
            kind: IndexCompileTargetKind::Field,
        };
        let values = [
            Value::Text("x".repeat(4096)),
            Value::IntBig(IntBig::from(-256)),
            Value::NatBig(NatBig::from(256u64)),
            Value::Null,
            Value::List(vec![Value::Text("member".into())]),
        ];
        for value in &values {
            let lowered =
                lower_index_compare_literal_for_kind(target.kind, value, CoercionId::Strict)
                    .unwrap();
            let Cow::Borrowed(borrowed) = lowered else {
                panic!("identity lowering must borrow");
            };
            assert!(std::ptr::eq(borrowed, value));
            for coercion in [
                CoercionId::NumericWiden,
                CoercionId::TextCasefold,
                CoercionId::CollectionElement,
            ] {
                assert!(
                    lower_index_compare_literal_for_kind(target.kind, value, coercion).is_none()
                );
            }
        }
        let Value::Text(text) = &values[0] else {
            unreachable!();
        };
        let prefix =
            lower_index_starts_with_prefix_for_target(target, &values[0], CoercionId::Strict)
                .unwrap();
        let Cow::Borrowed(prefix) = prefix else {
            panic!("identity prefix must borrow");
        };
        assert!(std::ptr::eq(prefix, text.as_str()));
        for value in [Value::Text(String::new()), Value::Nat64(1), Value::Null] {
            assert!(
                lower_index_starts_with_prefix_for_target(target, &value, CoercionId::Strict)
                    .is_none()
            );
        }
    }

    #[test]
    fn expression_target_lowering_keeps_derived_values_owned() {
        let target = expression_target(PersistedIndexExpressionOp::Lower);
        let value = Value::Text("ÄBC".into());
        let lowered =
            lower_index_compare_literal_for_kind(target.kind, &value, CoercionId::TextCasefold)
                .unwrap();
        assert!(matches!(lowered, Cow::Owned(_)));
        assert_eq!(lowered.as_ref(), &Value::Text("äbc".into()));
        let prefix =
            lower_index_starts_with_prefix_for_target(target, &value, CoercionId::TextCasefold)
                .unwrap();
        assert!(matches!(prefix, Cow::Owned(_)));
        assert_eq!(prefix, "äbc");
        assert_eq!(value, Value::Text("ÄBC".into()));
    }
}

// Exhaustive cache-retention coverage; new owned fields require accounting.
crate::retained::retained_copy!(IndexCompileTarget);
crate::retained::retained_copy!(PredicateCapabilityProfile);
