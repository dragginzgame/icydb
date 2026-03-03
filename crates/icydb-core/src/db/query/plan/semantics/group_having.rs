use crate::{
    db::{
        predicate::{CoercionId, CoercionSpec, CompareOp, compare_eq, compare_order},
        query::plan::{GroupHavingSpec, GroupPlan},
    },
    value::Value,
};

///
/// GroupedCursorPolicyViolation
///
/// Canonical grouped cursor-policy violations shared by planner and executor
/// boundaries so grouped continuation rules are not reimplemented per layer.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupedCursorPolicyViolation {
    ContinuationRequiresLimit,
    GlobalDistinctContinuationUnsupported,
}

impl GroupedCursorPolicyViolation {
    /// Return canonical invariant message text for grouped cursor policy violations.
    #[must_use]
    pub(in crate::db) const fn invariant_message(self) -> &'static str {
        match self {
            Self::ContinuationRequiresLimit => {
                "grouped continuation cursors require an explicit LIMIT"
            }
            Self::GlobalDistinctContinuationUnsupported => {
                "global DISTINCT grouped aggregates do not support continuation cursors"
            }
        }
    }
}

/// Return whether grouped HAVING supports this compare operator in grouped v1.
#[must_use]
pub(crate) const fn grouped_having_compare_op_supported(op: CompareOp) -> bool {
    grouped_having_compare_kind(op).is_some()
}

/// Return grouped cursor-policy violations for one grouped plan shape.
#[must_use]
pub(in crate::db::query::plan) fn grouped_cursor_policy_violation(
    grouped: &GroupPlan,
    cursor_present: bool,
) -> Option<GroupedCursorPolicyViolation> {
    if !cursor_present {
        return None;
    }
    if grouped
        .scalar
        .page
        .as_ref()
        .and_then(|page| page.limit)
        .is_none()
    {
        return Some(GroupedCursorPolicyViolation::ContinuationRequiresLimit);
    }
    if grouped.is_global_distinct_aggregate_without_group_keys() {
        return Some(GroupedCursorPolicyViolation::GlobalDistinctContinuationUnsupported);
    }

    None
}

/// Evaluate one grouped HAVING comparison under v1 semantic rules.
///
/// Returns `None` when `op` is outside grouped HAVING v1 support.
#[must_use]
pub(crate) fn evaluate_grouped_having_compare_v1(
    actual: &Value,
    op: CompareOp,
    expected: &Value,
) -> Option<bool> {
    let numeric = CoercionSpec::new(CoercionId::NumericWiden);
    let strict = CoercionSpec::default();
    let coercion = if actual.supports_numeric_coercion() || expected.supports_numeric_coercion() {
        &numeric
    } else {
        &strict
    };
    let kind = grouped_having_compare_kind(op)?;

    Some(match kind {
        GroupedHavingCompareKind::Eq => compare_eq(actual, expected, coercion).unwrap_or(false),
        GroupedHavingCompareKind::Ne => {
            compare_eq(actual, expected, coercion).is_some_and(|equal| !equal)
        }
        GroupedHavingCompareKind::Lt => {
            compare_order(actual, expected, coercion).is_some_and(std::cmp::Ordering::is_lt)
        }
        GroupedHavingCompareKind::Lte => {
            compare_order(actual, expected, coercion).is_some_and(std::cmp::Ordering::is_le)
        }
        GroupedHavingCompareKind::Gt => {
            compare_order(actual, expected, coercion).is_some_and(std::cmp::Ordering::is_gt)
        }
        GroupedHavingCompareKind::Gte => {
            compare_order(actual, expected, coercion).is_some_and(std::cmp::Ordering::is_ge)
        }
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GroupedHavingCompareKind {
    Eq,
    Ne,
    Lt,
    Lte,
    Gt,
    Gte,
}

const fn grouped_having_compare_kind(op: CompareOp) -> Option<GroupedHavingCompareKind> {
    match op {
        CompareOp::Eq => Some(GroupedHavingCompareKind::Eq),
        CompareOp::Ne => Some(GroupedHavingCompareKind::Ne),
        CompareOp::Lt => Some(GroupedHavingCompareKind::Lt),
        CompareOp::Lte => Some(GroupedHavingCompareKind::Lte),
        CompareOp::Gt => Some(GroupedHavingCompareKind::Gt),
        CompareOp::Gte => Some(GroupedHavingCompareKind::Gte),
        CompareOp::In
        | CompareOp::NotIn
        | CompareOp::Contains
        | CompareOp::StartsWith
        | CompareOp::EndsWith => None,
    }
}

pub(in crate::db::query::plan::semantics) fn grouped_having_streaming_compatible(
    having: Option<&GroupHavingSpec>,
) -> bool {
    having.is_none_or(|having| {
        having
            .clauses()
            .iter()
            .all(|clause| grouped_having_compare_op_supported(clause.op()))
    })
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::{predicate::CompareOp, query::plan::semantics::evaluate_grouped_having_compare_v1},
        value::Value,
    };

    #[test]
    fn grouped_having_numeric_equality_uses_numeric_widen_semantics() {
        let matched =
            evaluate_grouped_having_compare_v1(&Value::Uint(7), CompareOp::Eq, &Value::Int(7))
                .expect("eq should be supported");

        assert!(matched);
    }

    #[test]
    fn grouped_having_numeric_ordering_uses_numeric_widen_semantics() {
        let matched =
            evaluate_grouped_having_compare_v1(&Value::Uint(2), CompareOp::Lt, &Value::Int(3))
                .expect("lt should be supported");

        assert!(matched);
    }

    #[test]
    fn grouped_having_numeric_vs_non_numeric_is_fail_closed() {
        let matched = evaluate_grouped_having_compare_v1(
            &Value::Uint(7),
            CompareOp::Eq,
            &Value::Text("7".to_string()),
        )
        .expect("eq should be supported");

        assert!(!matched);
    }
}
