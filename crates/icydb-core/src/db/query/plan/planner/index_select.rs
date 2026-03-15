//! Module: db::query::plan::planner::index_select
//! Responsibility: module-local ownership and contracts for db::query::plan::planner::index_select.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        index::canonical_index_predicate,
        numeric::compare_numeric_or_strict_order,
        predicate::{CoercionId, CompareOp, ComparePredicate, Predicate},
        schema::{SchemaInfo, literal_matches_type},
    },
    model::{entity::EntityModel, index::IndexModel},
    value::Value,
};
use std::cmp::Ordering;

pub(in crate::db::query::plan::planner) fn sorted_indexes(
    model: &EntityModel,
    query_predicate: &Predicate,
) -> Vec<&'static IndexModel> {
    let mut indexes = model
        .indexes
        .iter()
        .copied()
        .filter(|index| index_predicate_implied_by_query(index, query_predicate))
        .collect::<Vec<_>>();
    indexes.sort_by(|left, right| left.name().cmp(right.name()));

    indexes
}

pub(in crate::db::query::plan::planner) fn better_index(
    candidate: (usize, bool, &IndexModel),
    current: (usize, bool, &IndexModel),
) -> bool {
    let (cand_len, cand_exact, cand_index) = candidate;
    let (best_len, best_exact, best_index) = current;

    cand_len > best_len
        || (cand_len == best_len && cand_exact && !best_exact)
        || (cand_len == best_len
            && cand_exact == best_exact
            && cand_index.name() < best_index.name())
}

pub(in crate::db::query::plan::planner) fn index_literal_matches_schema(
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
    let Ok(index_predicate) = canonical_index_predicate(index) else {
        return false;
    };
    let Some(index_predicate) = index_predicate else {
        return true;
    };

    let Some(required) = required_compare_clauses(index_predicate) else {
        return false;
    };
    let query = query_compare_clauses(query_predicate);

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
                match collect_query_compare_clauses(child, &mut clauses) {
                    QueryClauseCollect::Unsatisfiable => {
                        return QueryCompareClauses::Unsatisfiable;
                    }
                    QueryClauseCollect::Known | QueryClauseCollect::Unknown => {}
                }
            }

            QueryCompareClauses::Clauses(clauses)
        }
        Predicate::Or(_)
        | Predicate::Not(_)
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
            collect_required_compare_clauses(predicate, &mut clauses)?;
            Some(RequiredCompareClauses::Clauses(clauses))
        }
    }
}

fn collect_required_compare_clauses<'a>(
    predicate: &'a Predicate,
    out: &mut Vec<&'a ComparePredicate>,
) -> Option<()> {
    match predicate {
        Predicate::And(children) => {
            for child in children {
                collect_required_compare_clauses(child, out)?;
            }

            Some(())
        }
        Predicate::Compare(cmp) => {
            if !compare_clause_supported(cmp) {
                return None;
            }
            out.push(cmp);
            Some(())
        }
        Predicate::True => Some(()),
        Predicate::False
        | Predicate::Or(_)
        | Predicate::Not(_)
        | Predicate::IsNull { .. }
        | Predicate::IsNotNull { .. }
        | Predicate::IsMissing { .. }
        | Predicate::IsEmpty { .. }
        | Predicate::IsNotEmpty { .. }
        | Predicate::TextContains { .. }
        | Predicate::TextContainsCi { .. } => None,
    }
}

enum QueryClauseCollect {
    Known,
    Unsatisfiable,
    Unknown,
}

fn collect_query_compare_clauses<'a>(
    predicate: &'a Predicate,
    out: &mut Vec<&'a ComparePredicate>,
) -> QueryClauseCollect {
    match predicate {
        Predicate::False => QueryClauseCollect::Unsatisfiable,
        Predicate::True => QueryClauseCollect::Known,
        Predicate::And(children) => {
            for child in children {
                if matches!(
                    collect_query_compare_clauses(child, out),
                    QueryClauseCollect::Unsatisfiable
                ) {
                    return QueryClauseCollect::Unsatisfiable;
                }
            }

            QueryClauseCollect::Known
        }
        Predicate::Compare(cmp) => {
            if !compare_clause_supported(cmp) {
                return QueryClauseCollect::Unknown;
            }
            out.push(cmp);
            QueryClauseCollect::Known
        }
        Predicate::Or(_)
        | Predicate::Not(_)
        | Predicate::IsNull { .. }
        | Predicate::IsNotNull { .. }
        | Predicate::IsMissing { .. }
        | Predicate::IsEmpty { .. }
        | Predicate::IsNotEmpty { .. }
        | Predicate::TextContains { .. }
        | Predicate::TextContainsCi { .. } => QueryClauseCollect::Unknown,
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
    pub(in crate::db::query::plan::planner) fn is_field_indexable(
        &self,
        field: &str,
        op: CompareOp,
    ) -> bool {
        // Range/startswith planning remains field-key-only in this release.
        // Expression-key indexes are handled by dedicated Eq/In key-item paths.
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
