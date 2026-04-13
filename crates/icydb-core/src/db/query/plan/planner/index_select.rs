//! Module: db::query::plan::planner::index_select
//! Selects and orders candidate indexes for predicate-backed access planning.

use crate::{
    db::{
        access::AccessPath,
        index::canonical_index_predicate,
        numeric::compare_numeric_or_strict_order,
        predicate::{CoercionId, CompareOp, ComparePredicate, Predicate},
        schema::{SchemaInfo, literal_matches_type},
    },
    model::index::IndexModel,
    value::Value,
};
use std::cmp::Ordering;

pub(in crate::db::query::plan) fn sorted_indexes(
    indexes: &[&'static IndexModel],
    query_predicate: &Predicate,
) -> Vec<&'static IndexModel> {
    sorted_model_indexes(indexes)
        .into_iter()
        .filter(|index| index_predicate_implied_by_query(index, query_predicate))
        .collect()
}

pub(in crate::db::query::plan) fn sorted_model_indexes(
    indexes: &[&'static IndexModel],
) -> Vec<&'static IndexModel> {
    let mut indexes = indexes.to_vec();
    // Schema validation rejects duplicate index names, so deterministic
    // lexicographic ordering does not require a stable sort here.
    indexes.sort_unstable_by(|left, right| left.name().cmp(right.name()));

    indexes
}

pub(in crate::db::query::plan) fn index_literal_matches_schema(
    schema: &SchemaInfo,
    field: &str,
    value: &Value,
) -> bool {
    let Some(field_type) = schema.field(field) else {
        return false;
    };
    if !literal_matches_type(value, field_type) {
        return false;
    }

    true
}

// Filtered indexes are eligible only when the full query predicate implies the
// index predicate. This check is intentionally conservative and fail-closed:
// unsupported predicate forms are treated as non-implying.
fn index_predicate_implied_by_query(index: &IndexModel, query_predicate: &Predicate) -> bool {
    if index.predicate().is_none() {
        return true;
    }

    filtered_index_predicate_query_relation(index, query_predicate)
}

pub(in crate::db) fn residual_query_predicate_after_filtered_access(
    index: &IndexModel,
    query_predicate: &Predicate,
) -> Option<Predicate> {
    if index.predicate().is_none() {
        return Some(query_predicate.clone());
    }
    let Some(index_predicate) = canonical_index_predicate(index) else {
        return Some(query_predicate.clone());
    };

    // Phase 1: only strip filtered-guard clauses after the full query has
    // already proven the index predicate. Otherwise execution must keep the
    // original predicate intact and fail closed.
    if !predicate_implies_predicate(query_predicate, index_predicate) {
        return Some(query_predicate.clone());
    }

    // Phase 2: remove only the query clauses the filtered guard itself
    // guarantees. Stronger user predicates must remain, even when they also
    // imply the guard, because index membership alone does not satisfy them.
    strip_query_clauses_satisfied_by_filtered_guard(query_predicate, index_predicate)
}

pub(in crate::db) fn residual_query_predicate_after_access_path_bounds(
    access_path: Option<&AccessPath<Value>>,
    query_predicate: &Predicate,
) -> Option<Predicate> {
    let Some(access_path) = access_path else {
        return Some(query_predicate.clone());
    };

    // Phase 1: derive only equality clauses that the concrete access path
    // already guarantees. Range and multi-lookup paths intentionally keep
    // their open bounds as residual semantics unless they appear in the fixed
    // equality prefix.
    let implied_equalities = if let Some((index, values)) = access_path.as_index_prefix() {
        access_bound_equalities(index, values)
    } else if let Some((index, prefix_values, _, _)) = access_path.as_index_range() {
        access_bound_equalities(index, prefix_values)
    } else {
        Vec::new()
    };
    if implied_equalities.is_empty() {
        return Some(query_predicate.clone());
    }

    // Phase 2: strip only clauses already implied by those fixed equality
    // bounds so execution does not retain redundant post-access filtering.
    strip_query_clauses_satisfied_by_access_bounds(query_predicate, &implied_equalities)
}

fn filtered_index_predicate_query_relation(
    index: &IndexModel,
    query_predicate: &Predicate,
) -> bool {
    if index.predicate().is_none() {
        return false;
    }
    let Some(index_predicate) = canonical_index_predicate(index) else {
        return false;
    };

    predicate_implies_predicate(query_predicate, index_predicate)
}

fn predicate_implies_predicate(implying: &Predicate, required: &Predicate) -> bool {
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
    match query_predicate {
        Predicate::And(children) => {
            let mut residual_children = Vec::with_capacity(children.len());
            for child in children {
                if let Some(residual_child) =
                    strip_query_clauses_satisfied_by_filtered_guard(child, index_predicate)
                {
                    residual_children.push(residual_child);
                }
            }

            match residual_children.len() {
                0 => None,
                1 => residual_children.pop(),
                _ => Some(Predicate::And(residual_children)),
            }
        }
        Predicate::Compare(cmp)
            if compare_clause_supported(cmp)
                && predicate_implies_predicate(
                    index_predicate,
                    &Predicate::Compare(cmp.clone()),
                ) =>
        {
            None
        }
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

fn access_bound_equalities(index: &IndexModel, values: &[Value]) -> Vec<ComparePredicate> {
    index
        .fields()
        .iter()
        .zip(values.iter())
        .map(|(field, value)| {
            ComparePredicate::with_coercion(
                *field,
                CompareOp::Eq,
                value.clone(),
                CoercionId::Strict,
            )
        })
        .collect()
}

fn strip_query_clauses_satisfied_by_access_bounds(
    query_predicate: &Predicate,
    implied_equalities: &[ComparePredicate],
) -> Option<Predicate> {
    match query_predicate {
        Predicate::And(children) => {
            let mut residual_children = Vec::with_capacity(children.len());
            for child in children {
                if let Some(residual_child) =
                    strip_query_clauses_satisfied_by_access_bounds(child, implied_equalities)
                {
                    residual_children.push(residual_child);
                }
            }

            match residual_children.len() {
                0 => None,
                1 => residual_children.pop(),
                _ => Some(Predicate::And(residual_children)),
            }
        }
        Predicate::Compare(cmp)
            if compare_clause_supported(cmp)
                && implied_equalities
                    .iter()
                    .any(|bound| query_clause_implies_required(bound, cmp)) =>
        {
            None
        }
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

///
/// RequiredCompareClauses
///
/// Compare clauses extracted from one index predicate for implication checks.
///

enum RequiredCompareClauses<'a> {
    Clauses(Vec<&'a ComparePredicate>),
    Unsatisfiable,
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

impl IndexModel {
    /// Return true when this index can structurally support the field/operator pair.
    #[must_use]
    pub(in crate::db::query::plan) fn is_field_indexable(
        &self,
        field: &str,
        op: CompareOp,
    ) -> bool {
        // Field-key indexability helper only.
        // Expression-key eligibility is owned by key-item lowering paths.
        if self.has_expression_key_items() {
            return false;
        }
        if !self.fields().contains(&field) {
            return false;
        }

        matches!(
            op,
            CompareOp::Eq
                | CompareOp::In
                | CompareOp::Gt
                | CompareOp::Gte
                | CompareOp::Lt
                | CompareOp::Lte
                | CompareOp::StartsWith
        )
    }
}
