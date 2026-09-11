//! Module: db::query::plan::planner::index_select
//! Selects and orders candidate indexes for predicate-backed access planning.

#[cfg(test)]
mod implication_tests;

use crate::{
    db::{
        access::{AccessPath, SemanticIndexAccessContract},
        index::{TextPrefixBoundMode, starts_with_component_bounds},
        numeric::compare_numeric_or_strict_order,
        predicate::{CoercionId, CompareOp, ComparePredicate, Predicate},
        schema::{FieldType, SchemaInfo, literal_matches_type},
    },
    value::Value,
};
use std::{
    borrow::Cow,
    cmp::Ordering,
    convert::Infallible,
    ops::{Bound, ControlFlow},
};

pub(in crate::db::query) fn eligible_sorted_index_contracts(
    indexes: &[SemanticIndexAccessContract],
    schema: &SchemaInfo,
    query_predicate: &Predicate,
) -> Vec<SemanticIndexAccessContract> {
    debug_assert!(index_contracts_are_sorted(indexes));
    indexes
        .iter()
        .filter(|index| {
            index_contract_predicate_implied_by_query(index, query_predicate)
                && index_stream_is_complete_for_query(schema, index, query_predicate)
        })
        .cloned()
        .collect()
}

/// Prove that every matching row has all physical index components. Nullable
/// trailing fields and nullable path ancestors can otherwise omit whole rows,
/// even when the constrained leading prefix is non-null.
pub(in crate::db::query::plan) fn index_stream_is_complete_for_query(
    schema: &SchemaInfo,
    index: &SemanticIndexAccessContract,
    query_predicate: &Predicate,
) -> bool {
    (0..index.key_arity()).all(|slot| {
        index.key_item_at(slot).is_some_and(|key_item| {
            let field = key_item.field();
            !schema
                .accepted_query_field_is_omittable(field)
                .unwrap_or(true)
                || predicate_implies_clause_for_planner(
                    query_predicate,
                    ImplicationClause::NonNull(field),
                )
        })
    })
}

fn index_contracts_are_sorted(indexes: &[SemanticIndexAccessContract]) -> bool {
    indexes
        .windows(2)
        .all(|pair| pair[0].name() <= pair[1].name())
}

pub(in crate::db::query) fn index_literal_matches_schema(
    schema: &SchemaInfo,
    field: &str,
    value: &Value,
) -> bool {
    index_field_literal_matcher(schema, field).matches(value)
}

pub(in crate::db::query) struct IndexFieldLiteralMatcher<'a> {
    field_type: Option<Cow<'a, FieldType>>,
}

impl IndexFieldLiteralMatcher<'_> {
    #[must_use]
    pub(in crate::db::query) fn matches(&self, value: &Value) -> bool {
        self.field_type
            .as_ref()
            .is_some_and(|field_type| literal_matches_type(value, field_type))
    }
}

#[must_use]
pub(in crate::db::query) fn index_field_literal_matcher<'a>(
    schema: &'a SchemaInfo,
    field: &str,
) -> IndexFieldLiteralMatcher<'a> {
    IndexFieldLiteralMatcher {
        field_type: schema.accepted_query_field_type(field),
    }
}

// Filtered indexes are eligible only when the full query predicate implies the
// index predicate. This check is intentionally conservative and fail-closed:
// unsupported predicate forms are treated as non-implying.
fn index_contract_predicate_implied_by_query(
    index: &SemanticIndexAccessContract,
    query_predicate: &Predicate,
) -> bool {
    let Some(index_predicate) = index.predicate_semantics() else {
        return true;
    };

    predicate_implies_predicate_for_planner(query_predicate, index_predicate)
}

pub(in crate::db) fn residual_query_predicate_after_filtered_access_contract(
    index: SemanticIndexAccessContract,
    query_predicate: Predicate,
) -> Option<Predicate> {
    let Some(index_predicate) = index.predicate_semantics() else {
        return Some(query_predicate);
    };

    if !predicate_implies_predicate_for_planner(&query_predicate, index_predicate) {
        return Some(query_predicate);
    }

    strip_query_clauses_satisfied_by_filtered_guard(query_predicate, index_predicate)
}

pub(in crate::db) fn residual_query_predicate_after_access_path_bounds(
    access_path: Option<&AccessPath<Value>>,
    query_predicate: Predicate,
) -> Option<Predicate> {
    let Some(access_path) = access_path else {
        return Some(query_predicate);
    };

    // Borrow only clauses guaranteed by this concrete path. Proof construction
    // must not copy field labels, operands or comparison-vector backing.
    let Some(implied_bounds) = AccessBoundClauses::from_path(access_path) else {
        return Some(query_predicate);
    };
    if implied_bounds.is_empty() {
        return Some(query_predicate);
    }

    // Remove only clauses guaranteed by these bounds, preserving stricter
    // siblings that still require runtime filtering.
    strip_query_clauses_satisfied_by_access_bounds(query_predicate, &implied_bounds)
}

pub(in crate::db::query::plan) fn predicate_implies_predicate_for_planner(
    implying: &Predicate,
    required: &Predicate,
) -> bool {
    if let Predicate::Or(children) = implying {
        return children
            .iter()
            .all(|child| predicate_implies_predicate_for_planner(child, required));
    }
    // Required validity precedes query contradiction. A top-level FALSE is a
    // supported requirement; FALSE nested inside AND remains unsupported.
    let required_classification = if matches!(required, Predicate::False) {
        ImplicationClassification::Unsatisfiable
    } else {
        classify_implication_clauses(required, CompareClauseMode::Required)
    };
    if matches!(required_classification, ImplicationClassification::Unknown) {
        return false;
    }

    match classify_implication_clauses(implying, CompareClauseMode::Query) {
        ImplicationClassification::Unsatisfiable => true,
        ImplicationClassification::Unknown => false,
        ImplicationClassification::Known => match required_classification {
            ImplicationClassification::Unsatisfiable | ImplicationClassification::Unknown => false,
            ImplicationClassification::Known => {
                visit_implication_clauses(required, CompareClauseMode::Required, &mut |required| {
                    if query_clauses_imply_clause(implying, required) {
                        ControlFlow::Continue(())
                    } else {
                        ControlFlow::Break(())
                    }
                })
                .is_continue()
            }
        },
    }
}

fn strip_query_clauses_satisfied_by_filtered_guard(
    query_predicate: Predicate,
    index_predicate: &Predicate,
) -> Option<Predicate> {
    strip_query_clauses(
        query_predicate,
        |cmp| {
            compare_clause_supported(cmp.into())
                && predicate_implies_clause_for_planner(
                    index_predicate,
                    ImplicationClause::Compare(cmp),
                )
        },
        |field| {
            predicate_implies_clause_for_planner(index_predicate, ImplicationClause::NonNull(field))
        },
    )
}

/// Borrowed comparison facts shared by authored predicates and access proofs.
/// Access bounds are strict; authored clauses keep their original coercion.
#[derive(Clone, Copy)]
struct ComparisonRef<'a> {
    field: &'a str,
    op: CompareOp,
    value: &'a Value,
    coercion: CoercionId,
}

impl<'a> ComparisonRef<'a> {
    const fn strict(field: &'a str, op: CompareOp, value: &'a Value) -> Self {
        Self {
            field,
            op,
            value,
            coercion: CoercionId::Strict,
        }
    }
}

impl<'a> From<&'a ComparePredicate> for ComparisonRef<'a> {
    fn from(compare: &'a ComparePredicate) -> Self {
        Self {
            field: compare.field(),
            op: compare.op(),
            value: compare.value(),
            coercion: compare.coercion().id,
        }
    }
}

struct AccessBoundBranchIn<'a> {
    field: &'a str,
    values: &'a [Value],
}

struct AccessBoundClauses<'a> {
    index: &'a SemanticIndexAccessContract,
    equalities: &'a [Value],
    ranges: [Option<ComparisonRef<'a>>; 2],
    branch_in: Option<AccessBoundBranchIn<'a>>,
}

impl<'a> AccessBoundClauses<'a> {
    fn from_path(path: &'a AccessPath<Value>) -> Option<Self> {
        let (index, equalities, ranges, branch_in) = match path {
            AccessPath::IndexPrefix { index, values } => {
                (index, values.as_slice(), [None, None], None)
            }
            AccessPath::IndexMultiLookup { index, values } => (
                index,
                &[][..],
                [None, None],
                access_bound_branch_in(index, 0, values),
            ),
            AccessPath::IndexBranchSet { spec } => (
                spec.index_ref(),
                spec.fixed_values(),
                [None, None],
                access_bound_branch_in(spec.index_ref(), spec.branch_slot(), spec.branch_values()),
            ),
            AccessPath::IndexRange { spec } => {
                let index = spec.index_ref();
                let field = spec
                    .field_slots()
                    .last()
                    .and_then(|slot| index.key_field_at(*slot));
                let ranges = field.map_or([None, None], |field| {
                    [
                        access_bound_lower_range_clause(field, spec.lower()),
                        access_bound_upper_range_clause(field, spec.upper()),
                    ]
                });
                (index, spec.prefix_values(), ranges, None)
            }
            AccessPath::ByKey(_)
            | AccessPath::ByKeys(_)
            | AccessPath::KeyRange { .. }
            | AccessPath::FullScan => return None,
        };

        Some(Self {
            index,
            equalities,
            ranges,
            branch_in,
        })
    }

    fn equalities(&self) -> impl Iterator<Item = ComparisonRef<'a>> + '_ {
        self.equalities
            .iter()
            .enumerate()
            .filter_map(|(slot, value)| {
                self.index
                    .key_field_at(slot)
                    .map(|field| ComparisonRef::strict(field, CompareOp::Eq, value))
            })
    }

    fn is_empty(&self) -> bool {
        self.equalities().next().is_none()
            && self.ranges.iter().all(Option::is_none)
            && self.branch_in.is_none()
    }
}

const fn access_bound_lower_range_clause<'a>(
    field: &'a str,
    bound: &'a Bound<Value>,
) -> Option<ComparisonRef<'a>> {
    match bound {
        Bound::Included(value) => Some(ComparisonRef::strict(field, CompareOp::Gte, value)),
        Bound::Excluded(value) => Some(ComparisonRef::strict(field, CompareOp::Gt, value)),
        Bound::Unbounded => None,
    }
}

const fn access_bound_upper_range_clause<'a>(
    field: &'a str,
    bound: &'a Bound<Value>,
) -> Option<ComparisonRef<'a>> {
    match bound {
        Bound::Included(value) => Some(ComparisonRef::strict(field, CompareOp::Lte, value)),
        Bound::Excluded(value) => Some(ComparisonRef::strict(field, CompareOp::Lt, value)),
        Bound::Unbounded => None,
    }
}

fn access_bound_branch_in<'a>(
    index: &'a SemanticIndexAccessContract,
    branch_slot: usize,
    branch_values: &'a [Value],
) -> Option<AccessBoundBranchIn<'a>> {
    Some(AccessBoundBranchIn {
        field: index.key_field_at(branch_slot)?,
        values: branch_values,
    })
}

fn strip_query_clauses_satisfied_by_access_bounds(
    query_predicate: Predicate,
    implied_bounds: &AccessBoundClauses,
) -> Option<Predicate> {
    strip_query_clauses(
        query_predicate,
        |cmp| access_bound_clauses_imply_required(implied_bounds, cmp),
        |_| false,
    )
}

fn access_bound_clauses_imply_required(
    implied_bounds: &AccessBoundClauses,
    cmp: &ComparePredicate,
) -> bool {
    access_bound_text_prefix_range_implies_required(implied_bounds, cmp)
        || branch_in_clause_implies_required(implied_bounds.branch_in.as_ref(), cmp)
        || implied_bounds
            .equalities()
            .any(|bound| equality_bound_implies_required(bound, cmp))
        || implied_bounds
            .ranges
            .iter()
            .flatten()
            .any(|bound| range_bound_implies_required(*bound, cmp.into()))
}

fn access_bound_text_prefix_range_implies_required(
    implied_bounds: &AccessBoundClauses,
    cmp: &ComparePredicate,
) -> bool {
    if cmp.op() != CompareOp::StartsWith || cmp.coercion().id != CoercionId::Strict {
        return false;
    }
    let Value::Text(prefix) = cmp.value() else {
        return false;
    };
    let Some((lower, upper)) = starts_with_component_bounds(prefix, TextPrefixBoundMode::Strict)
    else {
        return false;
    };

    access_bound_ranges_include_lower_bound(cmp.field(), &implied_bounds.ranges, &lower)
        && access_bound_ranges_include_upper_bound(cmp.field(), &implied_bounds.ranges, &upper)
}

fn access_bound_ranges_include_lower_bound(
    field: &str,
    ranges: &[Option<ComparisonRef<'_>>],
    required: &Bound<Value>,
) -> bool {
    let Some(required_clause) = access_bound_lower_range_clause(field, required) else {
        return true;
    };

    ranges
        .iter()
        .flatten()
        .any(|bound| range_bound_implies_required(*bound, required_clause))
}

fn access_bound_ranges_include_upper_bound(
    field: &str,
    ranges: &[Option<ComparisonRef<'_>>],
    required: &Bound<Value>,
) -> bool {
    let Some(required_clause) = access_bound_upper_range_clause(field, required) else {
        return true;
    };

    ranges
        .iter()
        .flatten()
        .any(|bound| range_bound_implies_required(*bound, required_clause))
}

fn equality_bound_implies_required(bound: ComparisonRef<'_>, cmp: &ComparePredicate) -> bool {
    if bound.field != cmp.field() || bound.op != CompareOp::Eq {
        return false;
    }

    match cmp.op() {
        CompareOp::Eq | CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte => {
            compare_clause_supported(cmp.into()) && query_clause_implies_required(bound, cmp.into())
        }
        CompareOp::Ne => !values_equal(bound.value, cmp.value()),
        CompareOp::In => list_contains_value(cmp.value(), bound.value),
        CompareOp::NotIn => !list_contains_value(cmp.value(), bound.value),
        CompareOp::Contains | CompareOp::StartsWith | CompareOp::EndsWith => false,
    }
}

fn range_bound_implies_required(bound: ComparisonRef<'_>, cmp: ComparisonRef<'_>) -> bool {
    if bound.field != cmp.field {
        return false;
    }

    compare_clause_supported(cmp) && query_clause_implies_required(bound, cmp)
}

fn branch_in_clause_implies_required(
    branch_in: Option<&AccessBoundBranchIn<'_>>,
    cmp: &ComparePredicate,
) -> bool {
    let Some(branch_in) = branch_in else {
        return false;
    };
    if cmp.field() != branch_in.field {
        return false;
    }

    match cmp.op() {
        CompareOp::Eq => branch_in
            .values
            .iter()
            .all(|branch_value| values_equal(branch_value, cmp.value())),
        CompareOp::Ne => branch_in
            .values
            .iter()
            .all(|branch_value| !values_equal(branch_value, cmp.value())),
        CompareOp::In => list_contains_all_values(cmp.value(), branch_in.values),
        CompareOp::NotIn => branch_in
            .values
            .iter()
            .all(|branch_value| !list_contains_value(cmp.value(), branch_value)),
        CompareOp::Gt
        | CompareOp::Gte
        | CompareOp::Lt
        | CompareOp::Lte
        | CompareOp::Contains
        | CompareOp::StartsWith
        | CompareOp::EndsWith => false,
    }
}

fn list_contains_value(list: &Value, value: &Value) -> bool {
    let Value::List(values) = list else {
        return false;
    };

    values
        .iter()
        .any(|candidate| values_equal(candidate, value))
}

fn list_contains_all_values(list: &Value, required_values: &[Value]) -> bool {
    let Value::List(values) = list else {
        return false;
    };
    if values == required_values {
        return true;
    }

    required_values.iter().all(|value| {
        values
            .iter()
            .any(|candidate| values_equal(candidate, value))
    })
}

fn values_equal(left: &Value, right: &Value) -> bool {
    compare_values(left, right).is_some_and(Ordering::is_eq)
}

// Both residual-stripping paths share the same recursive AND-collapse contract;
// they differ only in which comparison and exact non-null clauses are already
// guaranteed by the selected access contract.
fn strip_query_clauses<F, N>(
    mut query_predicate: Predicate,
    compare_is_redundant: F,
    non_null_is_redundant: N,
) -> Option<Predicate>
where
    F: Fn(&ComparePredicate) -> bool + Copy,
    N: Fn(&str) -> bool + Copy,
{
    retain_query_clause(
        &mut query_predicate,
        compare_is_redundant,
        non_null_is_redundant,
    )
    .then_some(query_predicate)
}

// Ownership enters once. Removing clauses only compacts existing AND backing;
// retained operands and collapsed children move without copying or allocation.
fn retain_query_clause<F, N>(
    query_predicate: &mut Predicate,
    compare_is_redundant: F,
    non_null_is_redundant: N,
) -> bool
where
    F: Fn(&ComparePredicate) -> bool + Copy,
    N: Fn(&str) -> bool + Copy,
{
    match query_predicate {
        Predicate::And(children) => {
            children.retain_mut(|child| {
                retain_query_clause(child, compare_is_redundant, non_null_is_redundant)
            });
            if children.is_empty() {
                return false;
            }
            if children.len() == 1
                && let Some(only) = children.pop()
            {
                *query_predicate = only;
            }
            true
        }
        Predicate::Compare(cmp) if compare_is_redundant(cmp) => false,
        Predicate::IsNotNull { field } if non_null_is_redundant(field) => false,
        Predicate::True => false,
        Predicate::False
        | Predicate::Or(_)
        | Predicate::Not(_)
        | Predicate::CompareFields(_)
        | Predicate::Compare(_)
        | Predicate::IsNull { .. }
        | Predicate::IsNotNull { .. }
        | Predicate::IsMissing { .. }
        | Predicate::IsEmpty { .. }
        | Predicate::IsNotEmpty { .. }
        | Predicate::TextContains { .. }
        | Predicate::TextContainsCi { .. } => true,
    }
}

/// A supported borrowed clause; no predicate shell or clause vector is needed.
#[derive(Clone, Copy)]
enum ImplicationClause<'a> {
    Compare(&'a ComparePredicate),
    NonNull(&'a str),
}

impl ImplicationClause<'_> {
    fn implies(self, required: Self) -> bool {
        match (self, required) {
            (Self::Compare(query), Self::Compare(required)) => {
                query_clause_implies_required(query.into(), required.into())
            }
            (Self::Compare(query), Self::NonNull(field)) => {
                comparison_proves_field_non_null(query, field)
            }
            (Self::NonNull(query), Self::NonNull(required)) => query == required,
            (Self::NonNull(_), Self::Compare(_)) => false,
        }
    }
}

#[derive(Clone, Copy)]
enum CompareClauseMode {
    Query,
    Required,
}

/// Conservative proof classification. Unknown query children inside AND are
/// ignored; unknown required children invalidate the whole conjunction.
enum ImplicationClassification {
    Known,
    Unsatisfiable,
    Unknown,
}

fn classify_implication_clauses(
    predicate: &Predicate,
    mode: CompareClauseMode,
) -> ImplicationClassification {
    match visit_implication_clauses(predicate, mode, &mut |_| {
        ControlFlow::<Infallible>::Continue(())
    }) {
        ControlFlow::Continue(classification) => classification,
        ControlFlow::Break(never) => match never {},
    }
}

// The caller has classified both predicates before entering proof searches:
// a later FALSE query clause must win over an earlier non-matching clause, and
// an unsupported required clause must fail even against an unsatisfiable query.
fn query_clauses_imply_clause(query: &Predicate, required: ImplicationClause<'_>) -> bool {
    visit_implication_clauses(query, CompareClauseMode::Query, &mut |query| {
        if query.implies(required) {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    })
    .is_break()
}

// Single supported requirements reuse the same classifier/search as whole
// predicates. Filtered guards and sparse-index membership need no copied shell.
fn predicate_implies_clause_for_planner(
    implying: &Predicate,
    required: ImplicationClause<'_>,
) -> bool {
    if let Predicate::Or(children) = implying {
        return children
            .iter()
            .all(|child| predicate_implies_clause_for_planner(child, required));
    }
    match classify_implication_clauses(implying, CompareClauseMode::Query) {
        ImplicationClassification::Unsatisfiable => true,
        ImplicationClassification::Unknown => false,
        ImplicationClassification::Known => query_clauses_imply_clause(implying, required),
    }
}

// One short-circuiting visitor owns supported-clause traversal for validation
// and proof search. It neither collects/deduplicates clauses nor copies values.
fn visit_implication_clauses<'a, B>(
    predicate: &'a Predicate,
    mode: CompareClauseMode,
    visitor: &mut impl FnMut(ImplicationClause<'a>) -> ControlFlow<B>,
) -> ControlFlow<B, ImplicationClassification> {
    let classification = match predicate {
        Predicate::And(children) => {
            for child in children {
                match visit_implication_clauses(child, mode, visitor)? {
                    ImplicationClassification::Known => {}
                    ImplicationClassification::Unsatisfiable => {
                        return ControlFlow::Continue(ImplicationClassification::Unsatisfiable);
                    }
                    ImplicationClassification::Unknown => {
                        if matches!(mode, CompareClauseMode::Required) {
                            return ControlFlow::Continue(ImplicationClassification::Unknown);
                        }
                    }
                }
            }
            ImplicationClassification::Known
        }
        Predicate::Compare(cmp) => {
            if !(compare_clause_supported(cmp.into())
                || matches!(mode, CompareClauseMode::Query)
                    && comparison_proves_field_non_null(cmp, cmp.field()))
            {
                return ControlFlow::Continue(ImplicationClassification::Unknown);
            }
            visitor(ImplicationClause::Compare(cmp))?;
            ImplicationClassification::Known
        }
        Predicate::IsNotNull { field } => {
            visitor(ImplicationClause::NonNull(field))?;
            ImplicationClassification::Known
        }
        Predicate::True => ImplicationClassification::Known,
        Predicate::False => match mode {
            CompareClauseMode::Query => ImplicationClassification::Unsatisfiable,
            CompareClauseMode::Required => ImplicationClassification::Unknown,
        },
        Predicate::CompareFields(_)
        | Predicate::Or(_)
        | Predicate::Not(_)
        | Predicate::IsNull { .. }
        | Predicate::IsMissing { .. }
        | Predicate::IsEmpty { .. }
        | Predicate::IsNotEmpty { .. }
        | Predicate::TextContains { .. }
        | Predicate::TextContainsCi { .. } => ImplicationClassification::Unknown,
    };
    ControlFlow::Continue(classification)
}

// Admit only comparisons whose successful evaluation excludes a null source.
// Keep this separate from scalar implication: IN and text-prefix predicates
// prove membership without proving a particular scalar equality or range.
fn comparison_proves_field_non_null(compare: &ComparePredicate, field: &str) -> bool {
    if compare.field() != field
        || !matches!(
            compare.coercion().id,
            CoercionId::Strict | CoercionId::NumericWiden | CoercionId::TextCasefold
        )
    {
        return false;
    }
    match compare.op() {
        CompareOp::Eq | CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte => {
            !matches!(compare.value(), Value::Null)
        }
        CompareOp::In => matches!(compare.value(), Value::List(values)
            if values.iter().all(|value| !matches!(value, Value::Null))),
        CompareOp::StartsWith => matches!(compare.value(), Value::Text(_)),
        _ => false,
    }
}

const fn compare_clause_supported(cmp: ComparisonRef<'_>) -> bool {
    matches!(
        cmp.op,
        CompareOp::Eq | CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte
    ) && matches!(cmp.coercion, CoercionId::Strict | CoercionId::NumericWiden)
}

fn query_clause_implies_required(query: ComparisonRef<'_>, required: ComparisonRef<'_>) -> bool {
    if query.field != required.field {
        return false;
    }
    if !compare_clause_supported(query) || !compare_clause_supported(required) {
        return false;
    }

    let query_value = query.value;
    let required_value = required.value;

    match required.op {
        CompareOp::Eq => {
            query.op == CompareOp::Eq
                && compare_values(query_value, required_value).is_some_and(Ordering::is_eq)
        }
        CompareOp::Gt => match query.op {
            CompareOp::Eq | CompareOp::Gte => {
                compare_values(query_value, required_value).is_some_and(Ordering::is_gt)
            }
            CompareOp::Gt => compare_values(query_value, required_value)
                .is_some_and(|ordering| ordering.is_gt() || ordering.is_eq()),
            _ => false,
        },
        CompareOp::Gte => match query.op {
            CompareOp::Eq => compare_values(query_value, required_value)
                .is_some_and(|ordering| ordering.is_gt() || ordering.is_eq()),
            CompareOp::Gt | CompareOp::Gte => compare_values(query_value, required_value)
                .is_some_and(|ordering| ordering.is_gt() || ordering.is_eq()),
            _ => false,
        },
        CompareOp::Lt => match query.op {
            CompareOp::Eq | CompareOp::Lte => {
                compare_values(query_value, required_value).is_some_and(Ordering::is_lt)
            }
            CompareOp::Lt => compare_values(query_value, required_value)
                .is_some_and(|ordering| ordering.is_lt() || ordering.is_eq()),
            _ => false,
        },
        CompareOp::Lte => match query.op {
            CompareOp::Eq => compare_values(query_value, required_value)
                .is_some_and(|ordering| ordering.is_lt() || ordering.is_eq()),
            CompareOp::Lt | CompareOp::Lte => compare_values(query_value, required_value)
                .is_some_and(|ordering| ordering.is_lt() || ordering.is_eq()),
            _ => false,
        },
        CompareOp::Ne
        | CompareOp::In
        | CompareOp::NotIn
        | CompareOp::Contains
        | CompareOp::StartsWith
        | CompareOp::EndsWith => false,
    }
}

fn compare_values(left: &Value, right: &Value) -> Option<Ordering> {
    compare_numeric_or_strict_order(left, right)
}

#[cfg(test)]
mod tests {
    use super::{
        ComparisonRef, access_bound_lower_range_clause, access_bound_upper_range_clause,
        predicate_implies_predicate_for_planner, strip_query_clauses_satisfied_by_filtered_guard,
    };
    use crate::{
        db::predicate::{CoercionId, CompareFieldsPredicate, CompareOp, Predicate},
        value::Value,
    };

    fn non_null(field: &str) -> Predicate {
        Predicate::is_not_null(field.to_string())
    }

    fn compare(field: &str, op: CompareOp, value: Value) -> Predicate {
        Predicate::Compare(crate::db::predicate::ComparePredicate::with_coercion(
            field,
            op,
            value,
            CoercionId::Strict,
        ))
    }

    #[test]
    fn residual_bound_comparisons_borrow_operands_and_preserve_coercion() {
        use std::ops::Bound;

        let field = "λ".repeat(32);
        let predicate = crate::db::predicate::ComparePredicate::with_coercion(
            field.clone(),
            CompareOp::Eq,
            Value::Text("payload".repeat(128)),
            CoercionId::NumericWiden,
        );
        let view = ComparisonRef::from(&predicate);
        assert!(std::ptr::eq(view.field, predicate.field()));
        assert!(std::ptr::eq(view.value, predicate.value()));
        assert_eq!(view.coercion, CoercionId::NumericWiden);

        for (bound, lower_op, upper_op) in [
            (
                Bound::Included(predicate.value().clone()),
                CompareOp::Gte,
                CompareOp::Lte,
            ),
            (
                Bound::Excluded(predicate.value().clone()),
                CompareOp::Gt,
                CompareOp::Lt,
            ),
        ] {
            let value = match &bound {
                Bound::Included(value) | Bound::Excluded(value) => value,
                Bound::Unbounded => unreachable!(),
            };
            for (view, op) in [
                (
                    access_bound_lower_range_clause(&field, &bound).unwrap(),
                    lower_op,
                ),
                (
                    access_bound_upper_range_clause(&field, &bound).unwrap(),
                    upper_op,
                ),
            ] {
                assert!(std::ptr::eq(view.field, field.as_str()));
                assert!(std::ptr::eq(view.value, value));
                assert_eq!(view.op, op);
                assert_eq!(view.coercion, CoercionId::Strict);
            }
        }
        assert!(access_bound_lower_range_clause(&field, &Bound::Unbounded).is_none());
        assert!(access_bound_upper_range_clause(&field, &Bound::Unbounded).is_none());
    }

    #[test]
    fn nullable_guard_implication_accepts_exact_and_non_null_scalar_comparisons() {
        let required = non_null("email");
        assert!(predicate_implies_predicate_for_planner(
            &required, &required
        ));

        for op in [
            CompareOp::Eq,
            CompareOp::Gt,
            CompareOp::Gte,
            CompareOp::Lt,
            CompareOp::Lte,
        ] {
            assert!(predicate_implies_predicate_for_planner(
                &compare("email", op, Value::Text("a@example.com".to_string())),
                &required,
            ));
        }
    }

    #[test]
    fn nullable_guard_implication_accepts_membership_and_text_prefix_but_not_null_members() {
        let required = non_null("email");
        let membership = compare(
            "email",
            CompareOp::In,
            Value::List(vec![
                Value::Text("a".to_string()),
                Value::Text("b".to_string()),
            ]),
        );
        let prefix = compare("email", CompareOp::StartsWith, Value::Text("a".to_string()));
        for query in [
            membership.clone(),
            prefix.clone(),
            Predicate::or(vec![membership, prefix]),
        ] {
            assert!(predicate_implies_predicate_for_planner(&query, &required));
        }
        assert!(predicate_implies_predicate_for_planner(
            &compare("unit", CompareOp::Eq, Value::Unit),
            &non_null("unit"),
        ));
        let nullable_membership = compare(
            "email",
            CompareOp::In,
            Value::List(vec![Value::Text("a".to_string()), Value::Null]),
        );
        assert!(!predicate_implies_predicate_for_planner(
            &nullable_membership,
            &required
        ));
        let casefold = Predicate::Compare(crate::db::predicate::ComparePredicate::with_coercion(
            "email",
            CompareOp::Eq,
            Value::Text("A".to_string()),
            CoercionId::TextCasefold,
        ));
        assert!(predicate_implies_predicate_for_planner(
            &casefold, &required
        ));
    }

    #[test]
    fn nullable_guard_implication_requires_every_composite_guard_and_extra_filter() {
        let required = Predicate::and(vec![
            non_null("tenant"),
            non_null("email"),
            compare("active", CompareOp::Eq, Value::Bool(true)),
        ]);
        let complete = Predicate::and(vec![
            compare(
                "email",
                CompareOp::Eq,
                Value::Text("a@example.com".to_string()),
            ),
            non_null("tenant"),
            compare("active", CompareOp::Eq, Value::Bool(true)),
            Predicate::IsNotEmpty {
                field: "display_name".to_string(),
            },
        ]);
        assert!(predicate_implies_predicate_for_planner(
            &complete, &required
        ));

        let missing = Predicate::and(vec![
            non_null("email"),
            compare("active", CompareOp::Eq, Value::Bool(true)),
        ]);
        assert!(!predicate_implies_predicate_for_planner(
            &missing, &required
        ));
    }

    #[test]
    fn nullable_guard_implication_keeps_unsupported_shapes_conservative() {
        let required = non_null("email");
        let cross_field = Predicate::CompareFields(CompareFieldsPredicate::with_coercion(
            "email",
            CompareOp::Eq,
            "backup_email",
            CoercionId::Strict,
        ));
        for query in [
            Predicate::or(vec![required.clone(), non_null("tenant")]),
            Predicate::not(Predicate::IsNull {
                field: "email".to_string(),
            }),
            cross_field,
            compare("email", CompareOp::Eq, Value::Null),
            compare(
                "lower_email",
                CompareOp::Eq,
                Value::Text("a@example.com".to_string()),
            ),
        ] {
            assert!(!predicate_implies_predicate_for_planner(&query, &required));
        }
    }

    #[test]
    fn nullable_guard_implication_treats_unsatisfiable_query_as_vacuous_proof() {
        assert!(predicate_implies_predicate_for_planner(
            &Predicate::False,
            &non_null("email"),
        ));
    }

    #[test]
    fn filtered_guard_stripping_removes_only_guaranteed_non_null_clause() {
        let guard = non_null("email");
        assert_eq!(
            strip_query_clauses_satisfied_by_filtered_guard(guard.clone(), &guard),
            None,
        );

        let query = Predicate::and(vec![guard.clone(), non_null("tenant")]);
        assert_eq!(
            strip_query_clauses_satisfied_by_filtered_guard(query, &guard),
            Some(non_null("tenant")),
        );
    }
}
