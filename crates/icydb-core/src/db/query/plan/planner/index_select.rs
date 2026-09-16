//! Module: db::query::plan::planner::index_select
//! Selects and orders candidate indexes for predicate-backed access planning.

#[cfg(test)]
mod implication_tests;
#[cfg(test)]
mod proof_budget_tests;

use crate::{
    db::{
        access::{AccessPath, SemanticIndexAccessContract},
        index::next_text_prefix,
        numeric::compare_numeric_or_strict_order,
        predicate::{CoercionId, CompareOp, ComparePredicate, Predicate},
        query::construction::ConstructionBudget,
        schema::{FieldType, SchemaInfo, literal_matches_type},
    },
    error::InternalError,
    value::Value,
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;
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
    budget: &dyn ConstructionBudget,
) -> Result<Vec<SemanticIndexAccessContract>, InternalError> {
    // Every visible index can survive filtering. Admit one destination and
    // the outer visits before implication checks; contract clones share backing.
    // Inner proof visits use the same authority, separately from list construction.
    budget.charge(Resource::PredicateExpressionSteps, indexes.len() as u64)?;
    let mut eligible = budget.vec_with_capacity(indexes.len())?;
    debug_assert!(index_contracts_are_sorted(indexes));
    for index in indexes {
        if index_contract_predicate_implied_by_query(index, query_predicate, budget)?
            && index_stream_is_complete_for_query(schema, index, query_predicate, budget)?
        {
            eligible.push(index.clone());
        }
    }
    Ok(eligible)
}

/// Prove that every matching row has all physical index components. Nullable
/// trailing fields and nullable path ancestors can otherwise omit whole rows,
/// even when the constrained leading prefix is non-null.
pub(in crate::db::query::plan) fn index_stream_is_complete_for_query(
    schema: &SchemaInfo,
    index: &SemanticIndexAccessContract,
    query_predicate: &Predicate,
    budget: &dyn ConstructionBudget,
) -> Result<bool, InternalError> {
    for slot in 0..index.key_arity() {
        budget.charge(Resource::PredicateExpressionSteps, 1)?;
        let Some(key_item) = index.key_item_at(slot) else {
            return Ok(false);
        };
        let field = key_item.field();
        if schema
            .accepted_query_field_is_omittable(field)
            .unwrap_or(true)
            && !predicate_implies_clause_for_planner(
                query_predicate,
                ImplicationClause::NonNull(field),
                budget,
            )?
        {
            return Ok(false);
        }
    }
    Ok(true)
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
    budget: &dyn ConstructionBudget,
) -> Result<bool, InternalError> {
    let Some(index_predicate) = index.predicate_semantics() else {
        return Ok(true);
    };

    predicate_implies_predicate_for_planner(query_predicate, index_predicate, budget)
}

pub(in crate::db) fn residual_query_predicate_after_filtered_access_contract(
    index: SemanticIndexAccessContract,
    query_predicate: Predicate,
    budget: &dyn ConstructionBudget,
) -> Result<Option<Predicate>, InternalError> {
    let Some(index_predicate) = index.predicate_semantics() else {
        return Ok(Some(query_predicate));
    };

    if !predicate_implies_predicate_for_planner(&query_predicate, index_predicate, budget)? {
        return Ok(Some(query_predicate));
    }

    strip_query_clauses_satisfied_by_filtered_guard(query_predicate, index_predicate, budget)
}

pub(in crate::db) fn residual_query_predicate_after_access_path_bounds(
    access_path: Option<&AccessPath<Value>>,
    query_predicate: Predicate,
    budget: &dyn ConstructionBudget,
) -> Result<Option<Predicate>, InternalError> {
    let Some(access_path) = access_path else {
        return Ok(Some(query_predicate));
    };

    // Borrow only clauses guaranteed by this concrete path. Proof construction
    // must not copy field labels, operands or comparison-vector backing.
    let Some(implied_bounds) = AccessBoundClauses::from_path(access_path) else {
        return Ok(Some(query_predicate));
    };
    if implied_bounds.is_empty() {
        return Ok(Some(query_predicate));
    }

    // Remove only clauses guaranteed by these bounds, preserving stricter
    // siblings that still require runtime filtering.
    strip_query_clauses_satisfied_by_access_bounds(query_predicate, &implied_bounds, budget)
}

pub(in crate::db::query::plan) fn predicate_implies_predicate_for_planner(
    implying: &Predicate,
    required: &Predicate,
    budget: &dyn ConstructionBudget,
) -> Result<bool, InternalError> {
    budget.charge(Resource::PredicateExpressionSteps, 1)?;
    // Required validity precedes query contradiction; nested FALSE remains unsupported.
    let classification = if matches!(required, Predicate::False) {
        ImplicationClassification::Unsatisfiable
    } else {
        classify_implication_clauses(required, CompareClauseMode::Required, budget)?
    };
    predicate_implies_classified_requirement(implying, required, &classification, budget)
}

// Every OR branch proves the same borrowed requirement. Classify that invariant
// once, but keep empty OR vacuously true even for unsupported requirements.
fn predicate_implies_classified_requirement(
    implying: &Predicate,
    required: &Predicate,
    classification: &ImplicationClassification,
    budget: &dyn ConstructionBudget,
) -> Result<bool, InternalError> {
    budget.charge(Resource::PredicateExpressionSteps, 1)?;
    if let Predicate::Or(children) = implying {
        for child in children {
            if !predicate_implies_classified_requirement(child, required, classification, budget)? {
                return Ok(false);
            }
        }
        return Ok(true);
    }
    if matches!(classification, ImplicationClassification::Unknown) {
        return Ok(false);
    }
    match classify_implication_clauses(implying, CompareClauseMode::Query, budget)? {
        ImplicationClassification::Unsatisfiable => Ok(true),
        ImplicationClassification::Unknown => Ok(false),
        ImplicationClassification::Known => match classification {
            ImplicationClassification::Unsatisfiable | ImplicationClassification::Unknown => {
                Ok(false)
            }
            ImplicationClassification::Known => Ok(visit_implication_clauses(
                required,
                CompareClauseMode::Required,
                budget,
                &mut |required| {
                    Ok(if query_clauses_imply_clause(implying, required, budget)? {
                        ControlFlow::Continue(())
                    } else {
                        ControlFlow::Break(())
                    })
                },
            )?
            .is_continue()),
        },
    }
}

fn strip_query_clauses_satisfied_by_filtered_guard(
    query_predicate: Predicate,
    index_predicate: &Predicate,
    budget: &dyn ConstructionBudget,
) -> Result<Option<Predicate>, InternalError> {
    strip_query_clauses(
        query_predicate,
        |cmp| {
            Ok(compare_clause_supported(cmp.into())
                && predicate_implies_clause_for_planner(
                    index_predicate,
                    ImplicationClause::Compare(cmp),
                    budget,
                )?)
        },
        |field| {
            predicate_implies_clause_for_planner(
                index_predicate,
                ImplicationClause::NonNull(field),
                budget,
            )
        },
        budget,
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
    budget: &dyn ConstructionBudget,
) -> Result<Option<Predicate>, InternalError> {
    strip_query_clauses(
        query_predicate,
        |cmp| access_bound_clauses_imply_required(implied_bounds, cmp, budget),
        |_| Ok(false),
        budget,
    )
}

fn access_bound_clauses_imply_required(
    implied_bounds: &AccessBoundClauses,
    cmp: &ComparePredicate,
    budget: &dyn ConstructionBudget,
) -> Result<bool, InternalError> {
    if access_bound_text_prefix_range_implies_required(&implied_bounds.ranges, cmp, budget)?
        || branch_in_clause_implies_required(implied_bounds.branch_in.as_ref(), cmp, budget)?
    {
        return Ok(true);
    }
    for bound in implied_bounds.equalities() {
        if equality_bound_implies_required(bound, cmp, budget)? {
            return Ok(true);
        }
    }
    for bound in implied_bounds.ranges.iter().flatten() {
        if range_bound_implies_required(*bound, cmp.into(), budget)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn access_bound_text_prefix_range_implies_required(
    ranges: &[Option<ComparisonRef<'_>>],
    cmp: &ComparePredicate,
    budget: &dyn ConstructionBudget,
) -> Result<bool, InternalError> {
    if cmp.op() != CompareOp::StartsWith || cmp.coercion().id != CoercionId::Strict {
        return Ok(false);
    }
    let Value::Text(prefix) = cmp.value() else {
        return Ok(false);
    };
    if prefix.is_empty() {
        return Ok(false);
    }
    let lower = ComparisonRef::strict(cmp.field(), CompareOp::Gte, cmp.value());
    let mut lower_proven = false;
    for bound in ranges.iter().flatten() {
        if range_bound_implies_required(*bound, lower, budget)? {
            lower_proven = true;
            break;
        }
    }
    if !lower_proven {
        return Ok(false);
    }
    // The existing Unicode owner scans at most the input bytes and constructs
    // at most length + 1 bytes (a scalar successor grows by at most one byte).
    budget.charge(
        Resource::PredicateExpressionSteps,
        1 + 2 * prefix.len() as u64,
    )?;
    budget.charge(Resource::TemporaryBytes, 1 + prefix.len() as u64)?;
    let Some(successor) = next_text_prefix(prefix) else {
        return Ok(true);
    };
    let upper = Value::Text(successor);
    let required = ComparisonRef::strict(cmp.field(), CompareOp::Lt, &upper);
    for bound in ranges.iter().flatten() {
        if range_bound_implies_required(*bound, required, budget)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn equality_bound_implies_required(
    bound: ComparisonRef<'_>,
    cmp: &ComparePredicate,
    budget: &dyn ConstructionBudget,
) -> Result<bool, InternalError> {
    if !proof_fields_equal(bound.field, cmp.field(), budget)? || bound.op != CompareOp::Eq {
        return Ok(false);
    }
    Ok(match cmp.op() {
        CompareOp::Eq | CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte => {
            compare_clause_supported(cmp.into())
                && query_clause_implies_required(bound, cmp.into(), budget)?
        }
        CompareOp::Ne => !values_equal(bound.value, cmp.value(), budget)?,
        CompareOp::In => list_contains_value(cmp.value(), bound.value, budget)?,
        CompareOp::NotIn => !list_contains_value(cmp.value(), bound.value, budget)?,
        CompareOp::Contains | CompareOp::StartsWith | CompareOp::EndsWith => false,
    })
}

fn range_bound_implies_required(
    bound: ComparisonRef<'_>,
    cmp: ComparisonRef<'_>,
    budget: &dyn ConstructionBudget,
) -> Result<bool, InternalError> {
    Ok(proof_fields_equal(bound.field, cmp.field, budget)?
        && compare_clause_supported(cmp)
        && query_clause_implies_required(bound, cmp, budget)?)
}

fn branch_in_clause_implies_required(
    branch_in: Option<&AccessBoundBranchIn<'_>>,
    cmp: &ComparePredicate,
    budget: &dyn ConstructionBudget,
) -> Result<bool, InternalError> {
    let Some(branch_in) = branch_in else {
        return Ok(false);
    };
    if !proof_fields_equal(cmp.field(), branch_in.field, budget)? {
        return Ok(false);
    }
    if cmp.op() == CompareOp::In {
        return list_contains_all_values(cmp.value(), branch_in.values, budget);
    }
    if !matches!(cmp.op(), CompareOp::Eq | CompareOp::Ne | CompareOp::NotIn) {
        return Ok(false);
    }
    for value in branch_in.values {
        budget.charge(Resource::PredicateExpressionSteps, 1)?;
        let satisfied = match cmp.op() {
            CompareOp::Eq => values_equal(value, cmp.value(), budget)?,
            CompareOp::Ne => !values_equal(value, cmp.value(), budget)?,
            CompareOp::NotIn => !list_contains_value(cmp.value(), value, budget)?,
            _ => return Err(InternalError::planner_executor_invariant()),
        };
        if !satisfied {
            return Ok(false);
        }
    }
    Ok(true)
}

fn list_contains_value(
    list: &Value,
    value: &Value,
    budget: &dyn ConstructionBudget,
) -> Result<bool, InternalError> {
    let Value::List(values) = list else {
        return Ok(false);
    };
    for candidate in values {
        if values_equal(candidate, value, budget)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn list_contains_all_values(
    list: &Value,
    required_values: &[Value],
    budget: &dyn ConstructionBudget,
) -> Result<bool, InternalError> {
    let Value::List(values) = list else {
        return Ok(false);
    };
    // Preserve structural equality's shortcut (including non-orderable values).
    if values.len() == required_values.len() {
        budget.charge(Resource::PredicateExpressionSteps, 1 + values.len() as u64)?;
        for value in values {
            budget.admit_value_comparison(value)?;
        }
        if values == required_values {
            return Ok(true);
        }
    }
    for value in required_values {
        budget.charge(Resource::PredicateExpressionSteps, 1)?;
        if !list_contains_value(list, value, budget)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn values_equal(
    left: &Value,
    right: &Value,
    budget: &dyn ConstructionBudget,
) -> Result<bool, InternalError> {
    Ok(compare_values(left, right, budget)?.is_some_and(Ordering::is_eq))
}

// Both residual-stripping paths share the same recursive AND-collapse contract;
// they differ only in which comparison and exact non-null clauses are already
// guaranteed by the selected access contract.
fn strip_query_clauses<F, N>(
    mut query_predicate: Predicate,
    compare_is_redundant: F,
    non_null_is_redundant: N,
    budget: &dyn ConstructionBudget,
) -> Result<Option<Predicate>, InternalError>
where
    F: Fn(&ComparePredicate) -> Result<bool, InternalError> + Copy,
    N: Fn(&str) -> Result<bool, InternalError> + Copy,
{
    Ok(retain_query_clause(
        &mut query_predicate,
        compare_is_redundant,
        non_null_is_redundant,
        budget,
    )?
    .then_some(query_predicate))
}

// Keep the in-place compaction owner. On failure retain_mut only finishes its
// backing-store bookkeeping; no later child proof runs and the owned result drops.
fn retain_query_clause<F, N>(
    query_predicate: &mut Predicate,
    compare_is_redundant: F,
    non_null_is_redundant: N,
    budget: &dyn ConstructionBudget,
) -> Result<bool, InternalError>
where
    F: Fn(&ComparePredicate) -> Result<bool, InternalError> + Copy,
    N: Fn(&str) -> Result<bool, InternalError> + Copy,
{
    budget.charge(Resource::PredicateExpressionSteps, 1)?;
    match query_predicate {
        Predicate::And(children) => {
            let mut result = Ok(());
            children.retain_mut(|child| {
                if result.is_err() {
                    return true;
                }
                match retain_query_clause(
                    child,
                    compare_is_redundant,
                    non_null_is_redundant,
                    budget,
                ) {
                    Ok(retain) => retain,
                    Err(error) => {
                        result = Err(error);
                        true
                    }
                }
            });
            result?;
            if children.is_empty() {
                return Ok(false);
            }
            if children.len() == 1
                && let Some(only) = children.pop()
            {
                *query_predicate = only;
            }
            Ok(true)
        }
        Predicate::Compare(cmp) => Ok(!compare_is_redundant(cmp)?),
        Predicate::IsNotNull { field } => Ok(!non_null_is_redundant(field)?),
        Predicate::True => Ok(false),
        Predicate::False
        | Predicate::Or(_)
        | Predicate::Not(_)
        | Predicate::CompareFields(_)
        | Predicate::IsNull { .. }
        | Predicate::IsMissing { .. }
        | Predicate::IsEmpty { .. }
        | Predicate::IsNotEmpty { .. }
        | Predicate::TextContains { .. }
        | Predicate::TextContainsCi { .. } => Ok(true),
    }
}

/// A supported borrowed clause; no predicate shell or clause vector is needed.
#[derive(Clone, Copy)]
enum ImplicationClause<'a> {
    Compare(&'a ComparePredicate),
    NonNull(&'a str),
}

impl ImplicationClause<'_> {
    fn implies(
        self,
        required: Self,
        budget: &dyn ConstructionBudget,
    ) -> Result<bool, InternalError> {
        match (self, required) {
            (Self::Compare(query), Self::Compare(required)) => {
                query_clause_implies_required(query.into(), required.into(), budget)
            }
            (Self::Compare(query), Self::NonNull(field)) => {
                comparison_proves_field_non_null(query, field, budget)
            }
            (Self::NonNull(query), Self::NonNull(required)) => {
                proof_fields_equal(query, required, budget)
            }
            (Self::NonNull(_), Self::Compare(_)) => Ok(false),
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
    budget: &dyn ConstructionBudget,
) -> Result<ImplicationClassification, InternalError> {
    match visit_implication_clauses(predicate, mode, budget, &mut |_| {
        Ok(ControlFlow::<Infallible>::Continue(()))
    })? {
        ControlFlow::Continue(classification) => Ok(classification),
        ControlFlow::Break(never) => match never {},
    }
}

// The caller has classified both predicates before entering proof searches:
// a later FALSE query clause must win over an earlier non-matching clause, and
// an unsupported required clause must fail even against an unsatisfiable query.
fn query_clauses_imply_clause(
    query: &Predicate,
    required: ImplicationClause<'_>,
    budget: &dyn ConstructionBudget,
) -> Result<bool, InternalError> {
    Ok(
        visit_implication_clauses(query, CompareClauseMode::Query, budget, &mut |query| {
            Ok(if query.implies(required, budget)? {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            })
        })?
        .is_break(),
    )
}

// Single supported requirements reuse the same classifier/search as whole
// predicates. Filtered guards and sparse-index membership need no copied shell.
fn predicate_implies_clause_for_planner(
    implying: &Predicate,
    required: ImplicationClause<'_>,
    budget: &dyn ConstructionBudget,
) -> Result<bool, InternalError> {
    budget.charge(Resource::PredicateExpressionSteps, 1)?;
    if let Predicate::Or(children) = implying {
        for child in children {
            if !predicate_implies_clause_for_planner(child, required, budget)? {
                return Ok(false);
            }
        }
        return Ok(true);
    }
    match classify_implication_clauses(implying, CompareClauseMode::Query, budget)? {
        ImplicationClassification::Unsatisfiable => Ok(true),
        ImplicationClassification::Unknown => Ok(false),
        ImplicationClassification::Known => query_clauses_imply_clause(implying, required, budget),
    }
}

// One short-circuiting visitor owns supported-clause traversal for validation
// and proof search. It neither collects/deduplicates clauses nor copies values.
fn visit_implication_clauses<'a, B>(
    predicate: &'a Predicate,
    mode: CompareClauseMode,
    budget: &dyn ConstructionBudget,
    visitor: &mut impl FnMut(ImplicationClause<'a>) -> Result<ControlFlow<B>, InternalError>,
) -> Result<ControlFlow<B, ImplicationClassification>, InternalError> {
    budget.charge(Resource::PredicateExpressionSteps, 1)?;
    let classification = match predicate {
        Predicate::And(children) => {
            for child in children {
                match visit_implication_clauses(child, mode, budget, visitor)? {
                    ControlFlow::Break(value) => return Ok(ControlFlow::Break(value)),
                    ControlFlow::Continue(ImplicationClassification::Known) => {}
                    ControlFlow::Continue(ImplicationClassification::Unsatisfiable) => {
                        return Ok(ControlFlow::Continue(
                            ImplicationClassification::Unsatisfiable,
                        ));
                    }
                    ControlFlow::Continue(ImplicationClassification::Unknown) => {
                        if matches!(mode, CompareClauseMode::Required) {
                            return Ok(ControlFlow::Continue(ImplicationClassification::Unknown));
                        }
                    }
                }
            }
            ImplicationClassification::Known
        }
        Predicate::Compare(cmp) => {
            if !(compare_clause_supported(cmp.into())
                || matches!(mode, CompareClauseMode::Query)
                    && comparison_proves_field_non_null(cmp, cmp.field(), budget)?)
            {
                return Ok(ControlFlow::Continue(ImplicationClassification::Unknown));
            }
            if let ControlFlow::Break(value) = visitor(ImplicationClause::Compare(cmp))? {
                return Ok(ControlFlow::Break(value));
            }
            ImplicationClassification::Known
        }
        Predicate::IsNotNull { field } => {
            if let ControlFlow::Break(value) = visitor(ImplicationClause::NonNull(field))? {
                return Ok(ControlFlow::Break(value));
            }
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
    Ok(ControlFlow::Continue(classification))
}

// Admit only comparisons whose successful evaluation excludes a null source.
// Keep this separate from scalar implication: IN and text-prefix predicates
// prove membership without proving a particular scalar equality or range.
fn comparison_proves_field_non_null(
    compare: &ComparePredicate,
    field: &str,
    budget: &dyn ConstructionBudget,
) -> Result<bool, InternalError> {
    if !proof_fields_equal(compare.field(), field, budget)?
        || !matches!(
            compare.coercion().id,
            CoercionId::Strict | CoercionId::NumericWiden | CoercionId::TextCasefold
        )
    {
        return Ok(false);
    }
    Ok(match compare.op() {
        CompareOp::Eq | CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte => {
            !matches!(compare.value(), Value::Null)
        }
        CompareOp::In => {
            let Value::List(values) = compare.value() else {
                return Ok(false);
            };
            for value in values {
                budget.charge(Resource::PredicateExpressionSteps, 1)?;
                if matches!(value, Value::Null) {
                    return Ok(false);
                }
            }
            true
        }
        CompareOp::StartsWith => matches!(compare.value(), Value::Text(_)),
        _ => false,
    })
}

const fn compare_clause_supported(cmp: ComparisonRef<'_>) -> bool {
    matches!(
        cmp.op,
        CompareOp::Eq | CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte
    ) && matches!(cmp.coercion, CoercionId::Strict | CoercionId::NumericWiden)
}

fn query_clause_implies_required(
    query: ComparisonRef<'_>,
    required: ComparisonRef<'_>,
    budget: &dyn ConstructionBudget,
) -> Result<bool, InternalError> {
    if !proof_fields_equal(query.field, required.field, budget)? {
        return Ok(false);
    }
    if !compare_clause_supported(query) || !compare_clause_supported(required) {
        return Ok(false);
    }

    let query_value = query.value;
    let required_value = required.value;

    Ok(match required.op {
        CompareOp::Eq => {
            query.op == CompareOp::Eq
                && compare_values(query_value, required_value, budget)?.is_some_and(Ordering::is_eq)
        }
        CompareOp::Gt => match query.op {
            CompareOp::Eq | CompareOp::Gte => {
                compare_values(query_value, required_value, budget)?.is_some_and(Ordering::is_gt)
            }
            CompareOp::Gt => compare_values(query_value, required_value, budget)?
                .is_some_and(|ordering| ordering.is_gt() || ordering.is_eq()),
            _ => false,
        },
        CompareOp::Gte => match query.op {
            CompareOp::Eq => compare_values(query_value, required_value, budget)?
                .is_some_and(|ordering| ordering.is_gt() || ordering.is_eq()),
            CompareOp::Gt | CompareOp::Gte => compare_values(query_value, required_value, budget)?
                .is_some_and(|ordering| ordering.is_gt() || ordering.is_eq()),
            _ => false,
        },
        CompareOp::Lt => match query.op {
            CompareOp::Eq | CompareOp::Lte => {
                compare_values(query_value, required_value, budget)?.is_some_and(Ordering::is_lt)
            }
            CompareOp::Lt => compare_values(query_value, required_value, budget)?
                .is_some_and(|ordering| ordering.is_lt() || ordering.is_eq()),
            _ => false,
        },
        CompareOp::Lte => match query.op {
            CompareOp::Eq => compare_values(query_value, required_value, budget)?
                .is_some_and(|ordering| ordering.is_lt() || ordering.is_eq()),
            CompareOp::Lt | CompareOp::Lte => compare_values(query_value, required_value, budget)?
                .is_some_and(|ordering| ordering.is_lt() || ordering.is_eq()),
            _ => false,
        },
        CompareOp::Ne
        | CompareOp::In
        | CompareOp::NotIn
        | CompareOp::Contains
        | CompareOp::StartsWith
        | CompareOp::EndsWith => false,
    })
}

fn compare_values(
    left: &Value,
    right: &Value,
    budget: &dyn ConstructionBudget,
) -> Result<Option<Ordering>, InternalError> {
    // Keep numeric/strict meaning in its existing owner. Variable-sized strict
    // comparisons only descend when root tags agree; one side bounds their
    // lexicographic traversal. Numeric-conversion scratch remains separately owned.
    budget.charge(Resource::PredicateExpressionSteps, 1)?;
    if std::mem::discriminant(left) == std::mem::discriminant(right) {
        budget.admit_value_comparison(left)?;
    }
    Ok(compare_numeric_or_strict_order(left, right))
}

fn proof_fields_equal(
    left: &str,
    right: &str,
    budget: &dyn ConstructionBudget,
) -> Result<bool, InternalError> {
    budget.charge(
        Resource::PredicateExpressionSteps,
        1 + left.len().min(right.len()) as u64,
    )?;
    Ok(left == right)
}

#[cfg(test)]
mod tests {
    use super::{
        ComparisonRef, access_bound_lower_range_clause, access_bound_upper_range_clause,
        predicate_implies_predicate_for_planner, strip_query_clauses_satisfied_by_filtered_guard,
    };
    use crate::db::query::preparation::with_preparation_work;
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
        assert!(
            with_preparation_work(|budget| {
                predicate_implies_predicate_for_planner(&required, &required, budget)
            })
            .unwrap()
        );

        for op in [
            CompareOp::Eq,
            CompareOp::Gt,
            CompareOp::Gte,
            CompareOp::Lt,
            CompareOp::Lte,
        ] {
            assert!(
                with_preparation_work(|budget| {
                    predicate_implies_predicate_for_planner(
                        &compare("email", op, Value::Text("a@example.com".to_string())),
                        &required,
                        budget,
                    )
                })
                .unwrap()
            );
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
            assert!(
                with_preparation_work(|budget| {
                    predicate_implies_predicate_for_planner(&query, &required, budget)
                })
                .unwrap()
            );
        }
        assert!(
            with_preparation_work(|budget| {
                predicate_implies_predicate_for_planner(
                    &compare("unit", CompareOp::Eq, Value::Unit),
                    &non_null("unit"),
                    budget,
                )
            })
            .unwrap()
        );
        let nullable_membership = compare(
            "email",
            CompareOp::In,
            Value::List(vec![Value::Text("a".to_string()), Value::Null]),
        );
        assert!(
            !with_preparation_work(|budget| {
                predicate_implies_predicate_for_planner(&nullable_membership, &required, budget)
            })
            .unwrap()
        );
        let casefold = Predicate::Compare(crate::db::predicate::ComparePredicate::with_coercion(
            "email",
            CompareOp::Eq,
            Value::Text("A".to_string()),
            CoercionId::TextCasefold,
        ));
        assert!(
            with_preparation_work(|budget| {
                predicate_implies_predicate_for_planner(&casefold, &required, budget)
            })
            .unwrap()
        );
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
        assert!(
            with_preparation_work(|budget| {
                predicate_implies_predicate_for_planner(&complete, &required, budget)
            })
            .unwrap()
        );

        let missing = Predicate::and(vec![
            non_null("email"),
            compare("active", CompareOp::Eq, Value::Bool(true)),
        ]);
        assert!(
            !with_preparation_work(|budget| {
                predicate_implies_predicate_for_planner(&missing, &required, budget)
            })
            .unwrap()
        );
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
            assert!(
                !with_preparation_work(|budget| {
                    predicate_implies_predicate_for_planner(&query, &required, budget)
                })
                .unwrap()
            );
        }
    }

    #[test]
    fn nullable_guard_implication_treats_unsatisfiable_query_as_vacuous_proof() {
        assert!(
            with_preparation_work(|budget| {
                predicate_implies_predicate_for_planner(
                    &Predicate::False,
                    &non_null("email"),
                    budget,
                )
            })
            .unwrap()
        );
    }

    #[test]
    fn filtered_guard_stripping_removes_only_guaranteed_non_null_clause() {
        let guard = non_null("email");
        assert_eq!(
            with_preparation_work(|budget| {
                strip_query_clauses_satisfied_by_filtered_guard(guard.clone(), &guard, budget)
            })
            .unwrap(),
            None,
        );

        let query = Predicate::and(vec![guard.clone(), non_null("tenant")]);
        assert_eq!(
            with_preparation_work(|budget| {
                strip_query_clauses_satisfied_by_filtered_guard(query, &guard, budget)
            })
            .unwrap(),
            Some(non_null("tenant")),
        );
    }
}
