//! Module: db::query::plan::planner::index_select
//! Selects and orders candidate indexes for predicate-backed access planning.

use crate::{
    db::{
        access::{AccessPath, SemanticIndexAccessContract},
        numeric::compare_numeric_or_strict_order,
        predicate::{CoercionId, CompareOp, ComparePredicate, Predicate},
        schema::{FieldType, SchemaInfo, literal_matches_type},
    },
    value::Value,
};
use std::cmp::Ordering;

pub(in crate::db::query) fn sorted_index_contracts(
    indexes: &[SemanticIndexAccessContract],
    query_predicate: &Predicate,
) -> Vec<SemanticIndexAccessContract> {
    let mut indexes = indexes.to_vec();
    indexes.sort_unstable_by(|left, right| left.name().cmp(right.name()));
    indexes
        .into_iter()
        .filter(|index| index_contract_predicate_implied_by_query(index, query_predicate))
        .collect()
}

pub(in crate::db::query) fn eligible_sorted_index_contracts(
    indexes: &[SemanticIndexAccessContract],
    query_predicate: &Predicate,
) -> Vec<SemanticIndexAccessContract> {
    debug_assert!(index_contracts_are_sorted(indexes));
    indexes
        .iter()
        .filter(|index| index_contract_predicate_implied_by_query(index, query_predicate))
        .cloned()
        .collect()
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

#[derive(Clone, Copy)]
pub(in crate::db::query) struct IndexFieldLiteralMatcher<'a> {
    field_type: Option<&'a FieldType>,
}

impl IndexFieldLiteralMatcher<'_> {
    #[must_use]
    pub(in crate::db::query) fn matches(self, value: &Value) -> bool {
        self.field_type
            .is_some_and(|field_type| literal_matches_type(value, field_type))
    }
}

#[must_use]
pub(in crate::db::query) fn index_field_literal_matcher<'a>(
    schema: &'a SchemaInfo,
    field: &str,
) -> IndexFieldLiteralMatcher<'a> {
    IndexFieldLiteralMatcher {
        field_type: schema.field(field),
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
    query_predicate: &Predicate,
) -> Option<Predicate> {
    let Some(index_predicate) = index.predicate_semantics() else {
        return Some(query_predicate.clone());
    };

    if !predicate_implies_predicate_for_planner(query_predicate, index_predicate) {
        return Some(query_predicate.clone());
    }

    strip_query_clauses_satisfied_by_filtered_guard(query_predicate, index_predicate)
}

pub(in crate::db) fn residual_query_predicate_after_access_path_bounds(
    access_path: Option<&AccessPath<Value>>,
    query_predicate: &Predicate,
) -> Option<Predicate> {
    let Some(access_path) = access_path else {
        return Some(query_predicate.clone());
    };

    // Phase 1: derive only clauses that the concrete access path already
    // guarantees. Range paths intentionally keep their open bounds as residual
    // semantics unless they appear in the fixed equality prefix.
    let implied_bounds = if let Some((index, values)) = access_path.as_index_prefix_contract() {
        AccessBoundClauses {
            equalities: access_bound_equalities(index, values),
            branch_in: None,
        }
    } else if let Some((index, values)) = access_path.as_index_multi_lookup_contract() {
        AccessBoundClauses {
            equalities: Vec::new(),
            branch_in: access_bound_branch_in(&index, 0, values),
        }
    } else if let Some(spec) = access_path.as_index_branch_set_spec() {
        AccessBoundClauses {
            equalities: access_bound_equalities(spec.index(), spec.fixed_values()),
            branch_in: access_bound_branch_in(
                spec.index_ref(),
                spec.branch_slot(),
                spec.branch_values(),
            ),
        }
    } else if let Some(spec) = access_path.as_index_range() {
        AccessBoundClauses {
            equalities: access_bound_equalities(spec.index(), spec.prefix_values()),
            branch_in: None,
        }
    } else {
        AccessBoundClauses::default()
    };
    if implied_bounds.is_empty() {
        return Some(query_predicate.clone());
    }

    // Phase 2: strip only clauses already implied by those fixed equality
    // bounds so execution does not retain redundant post-access filtering.
    strip_query_clauses_satisfied_by_access_bounds(query_predicate, &implied_bounds)
}

pub(in crate::db::query::plan) fn predicate_implies_predicate_for_planner(
    implying: &Predicate,
    required: &Predicate,
) -> bool {
    let Some(required) = required_compare_clauses(required) else {
        return false;
    };
    let query = query_compare_clauses(implying);

    match query {
        QueryCompareClauses::Unsatisfiable => true,
        QueryCompareClauses::Unknown => false,
        QueryCompareClauses::Clauses(query_clauses) => match required {
            RequiredCompareClauses::Unsatisfiable => false,
            RequiredCompareClauses::Clauses(required_clauses) => {
                required_clauses.iter().all(|required_clause| {
                    query_clauses.iter().any(|query_clause| {
                        query_clause_implies_required(query_clause, required_clause)
                    })
                })
            }
        },
    }
}

fn strip_query_clauses_satisfied_by_filtered_guard(
    query_predicate: &Predicate,
    index_predicate: &Predicate,
) -> Option<Predicate> {
    strip_query_clauses(query_predicate, |cmp| {
        compare_clause_supported(cmp)
            && predicate_implies_predicate_for_planner(
                index_predicate,
                &Predicate::Compare(cmp.clone()),
            )
    })
}

fn access_bound_equalities(
    index: SemanticIndexAccessContract,
    values: &[Value],
) -> Vec<ComparePredicate> {
    (0..values.len())
        .zip(values.iter())
        .filter_map(|(slot, value)| {
            let field = index.key_field_at(slot)?;

            Some(ComparePredicate::with_coercion(
                field,
                CompareOp::Eq,
                value.clone(),
                CoercionId::Strict,
            ))
        })
        .collect()
}

#[derive(Default)]
struct AccessBoundClauses<'a> {
    equalities: Vec<ComparePredicate>,
    branch_in: Option<AccessBoundBranchIn<'a>>,
}

struct AccessBoundBranchIn<'a> {
    field: String,
    values: &'a [Value],
}

impl AccessBoundClauses<'_> {
    const fn is_empty(&self) -> bool {
        self.equalities.is_empty() && self.branch_in.is_none()
    }
}

fn access_bound_branch_in<'values>(
    index: &SemanticIndexAccessContract,
    branch_slot: usize,
    branch_values: &'values [Value],
) -> Option<AccessBoundBranchIn<'values>> {
    let field = index.key_field_at(branch_slot)?;

    Some(AccessBoundBranchIn {
        field: field.to_string(),
        values: branch_values,
    })
}

fn strip_query_clauses_satisfied_by_access_bounds(
    query_predicate: &Predicate,
    implied_bounds: &AccessBoundClauses,
) -> Option<Predicate> {
    strip_query_clauses(query_predicate, |cmp| {
        access_bound_clauses_imply_required(implied_bounds, cmp)
    })
}

fn access_bound_clauses_imply_required(
    implied_bounds: &AccessBoundClauses,
    cmp: &ComparePredicate,
) -> bool {
    branch_in_clause_implies_required(implied_bounds.branch_in.as_ref(), cmp)
        || implied_bounds
            .equalities
            .iter()
            .any(|bound| equality_bound_implies_required(bound, cmp))
}

fn equality_bound_implies_required(bound: &ComparePredicate, cmp: &ComparePredicate) -> bool {
    if bound.field() != cmp.field() || bound.op() != CompareOp::Eq {
        return false;
    }

    match cmp.op() {
        CompareOp::Eq | CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte => {
            compare_clause_supported(cmp) && query_clause_implies_required(bound, cmp)
        }
        CompareOp::Ne => !values_equal(bound.value(), cmp.value()),
        CompareOp::In => list_contains_value(cmp.value(), bound.value()),
        CompareOp::NotIn => !list_contains_value(cmp.value(), bound.value()),
        CompareOp::Contains | CompareOp::StartsWith | CompareOp::EndsWith => false,
    }
}

fn branch_in_clause_implies_required(
    branch_in: Option<&AccessBoundBranchIn<'_>>,
    cmp: &ComparePredicate,
) -> bool {
    let Some(branch_in) = branch_in else {
        return false;
    };
    if cmp.field() != branch_in.field.as_str() {
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
// they only differ in how they decide one compare clause is already implied.
fn strip_query_clauses<F>(query_predicate: &Predicate, compare_is_redundant: F) -> Option<Predicate>
where
    F: Fn(&ComparePredicate) -> bool + Copy,
{
    match query_predicate {
        Predicate::And(children) => {
            let mut residual_children = Vec::with_capacity(children.len());
            for child in children {
                if let Some(residual_child) = strip_query_clauses(child, compare_is_redundant) {
                    residual_children.push(residual_child);
                }
            }

            match residual_children.len() {
                0 => None,
                1 => residual_children.pop(),
                _ => Some(Predicate::And(residual_children)),
            }
        }
        Predicate::Compare(cmp) if compare_is_redundant(cmp) => None,
        Predicate::True => None,
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
        | Predicate::TextContainsCi { .. } => Some(query_predicate.clone()),
    }
}

///
/// QueryCompareClauses
///
/// Compare clauses extracted from one query predicate for implication checks.
///

enum QueryCompareClauses<'a> {
    Clauses(Vec<&'a ComparePredicate>),
    Unsatisfiable,
    Unknown,
}

#[derive(Clone, Copy)]
enum CompareClauseMode {
    Query,
    Required,
}

enum CompareClauseCollect {
    Known,
    Unsatisfiable,
    Unknown,
}

///
/// RequiredCompareClauses
///
/// Compare clauses extracted from one index predicate for implication checks.
///

enum RequiredCompareClauses<'a> {
    Clauses(Vec<&'a ComparePredicate>),
    Unsatisfiable,
}

fn query_compare_clauses(predicate: &Predicate) -> QueryCompareClauses<'_> {
    match predicate {
        Predicate::False => QueryCompareClauses::Unsatisfiable,
        Predicate::True => QueryCompareClauses::Clauses(Vec::new()),
        Predicate::Compare(cmp) => {
            if compare_clause_supported(cmp) {
                QueryCompareClauses::Clauses(vec![cmp])
            } else {
                QueryCompareClauses::Unknown
            }
        }
        Predicate::And(children) => {
            let mut clauses = Vec::new();
            for child in children {
                match collect_compare_clauses(child, &mut clauses, CompareClauseMode::Query) {
                    CompareClauseCollect::Unsatisfiable => {
                        return QueryCompareClauses::Unsatisfiable;
                    }
                    CompareClauseCollect::Known | CompareClauseCollect::Unknown => {}
                }
            }

            QueryCompareClauses::Clauses(clauses)
        }
        Predicate::Or(_)
        | Predicate::Not(_)
        | Predicate::CompareFields(_)
        | Predicate::IsNull { .. }
        | Predicate::IsNotNull { .. }
        | Predicate::IsMissing { .. }
        | Predicate::IsEmpty { .. }
        | Predicate::IsNotEmpty { .. }
        | Predicate::TextContains { .. }
        | Predicate::TextContainsCi { .. } => QueryCompareClauses::Unknown,
    }
}

fn required_compare_clauses(predicate: &Predicate) -> Option<RequiredCompareClauses<'_>> {
    match predicate {
        Predicate::True => Some(RequiredCompareClauses::Clauses(Vec::new())),
        Predicate::False => Some(RequiredCompareClauses::Unsatisfiable),
        _ => {
            let mut clauses = Vec::new();
            match collect_compare_clauses(predicate, &mut clauses, CompareClauseMode::Required) {
                CompareClauseCollect::Known => {}
                CompareClauseCollect::Unsatisfiable => {
                    return Some(RequiredCompareClauses::Unsatisfiable);
                }
                CompareClauseCollect::Unknown => return None,
            }
            Some(RequiredCompareClauses::Clauses(clauses))
        }
    }
}

fn collect_compare_clauses<'a>(
    predicate: &'a Predicate,
    out: &mut Vec<&'a ComparePredicate>,
    mode: CompareClauseMode,
) -> CompareClauseCollect {
    match predicate {
        Predicate::And(children) => {
            for child in children {
                match collect_compare_clauses(child, out, mode) {
                    CompareClauseCollect::Known => {}
                    CompareClauseCollect::Unsatisfiable => {
                        return CompareClauseCollect::Unsatisfiable;
                    }
                    CompareClauseCollect::Unknown => {
                        if matches!(mode, CompareClauseMode::Required) {
                            return CompareClauseCollect::Unknown;
                        }
                    }
                }
            }

            CompareClauseCollect::Known
        }
        Predicate::Compare(cmp) => {
            if !compare_clause_supported(cmp) {
                return CompareClauseCollect::Unknown;
            }
            out.push(cmp);
            CompareClauseCollect::Known
        }
        Predicate::True => CompareClauseCollect::Known,
        Predicate::False => match mode {
            CompareClauseMode::Query => CompareClauseCollect::Unsatisfiable,
            CompareClauseMode::Required => CompareClauseCollect::Unknown,
        },
        Predicate::CompareFields(_)
        | Predicate::Or(_)
        | Predicate::Not(_)
        | Predicate::IsNull { .. }
        | Predicate::IsNotNull { .. }
        | Predicate::IsMissing { .. }
        | Predicate::IsEmpty { .. }
        | Predicate::IsNotEmpty { .. }
        | Predicate::TextContains { .. }
        | Predicate::TextContainsCi { .. } => CompareClauseCollect::Unknown,
    }
}

const fn compare_clause_supported(cmp: &ComparePredicate) -> bool {
    matches!(
        cmp.op(),
        CompareOp::Eq | CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte
    ) && matches!(
        cmp.coercion().id,
        CoercionId::Strict | CoercionId::NumericWiden
    )
}

fn query_clause_implies_required(query: &ComparePredicate, required: &ComparePredicate) -> bool {
    if query.field() != required.field() {
        return false;
    }
    if !compare_clause_supported(query) || !compare_clause_supported(required) {
        return false;
    }

    let query_value = query.value();
    let required_value = required.value();

    match required.op() {
        CompareOp::Eq => {
            query.op() == CompareOp::Eq
                && compare_values(query_value, required_value).is_some_and(Ordering::is_eq)
        }
        CompareOp::Gt => match query.op() {
            CompareOp::Eq | CompareOp::Gte => {
                compare_values(query_value, required_value).is_some_and(Ordering::is_gt)
            }
            CompareOp::Gt => compare_values(query_value, required_value)
                .is_some_and(|ordering| ordering.is_gt() || ordering.is_eq()),
            _ => false,
        },
        CompareOp::Gte => match query.op() {
            CompareOp::Eq => compare_values(query_value, required_value)
                .is_some_and(|ordering| ordering.is_gt() || ordering.is_eq()),
            CompareOp::Gt | CompareOp::Gte => compare_values(query_value, required_value)
                .is_some_and(|ordering| ordering.is_gt() || ordering.is_eq()),
            _ => false,
        },
        CompareOp::Lt => match query.op() {
            CompareOp::Eq | CompareOp::Lte => {
                compare_values(query_value, required_value).is_some_and(Ordering::is_lt)
            }
            CompareOp::Lt => compare_values(query_value, required_value)
                .is_some_and(|ordering| ordering.is_lt() || ordering.is_eq()),
            _ => false,
        },
        CompareOp::Lte => match query.op() {
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
