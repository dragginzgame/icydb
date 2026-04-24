//! Module: executor::aggregate::value_reducer
//! Responsibility: projected-value aggregate reduction for global aggregate
//! execution lanes.
//! Does not own: SQL projection, aggregate lowering, or post-aggregate result
//! shaping.
//! Boundary: keeps fold/finalize behavior inside executor even when callers
//! provide already-projected aggregate input values.

use crate::{
    db::numeric::{
        add_decimal_terms, average_decimal_terms, coerce_numeric_decimal,
        compare_numeric_or_strict_order,
    },
    error::InternalError,
    types::Decimal,
    value::Value,
};

///
/// ProjectedValueAggregateKind
///
/// ProjectedValueAggregateKind identifies the reducer family used when an
/// outer execution surface has already projected aggregate input values.
/// It deliberately excludes row-count terminals, which stay on the scalar
/// terminal boundary instead of consuming value payloads.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum ProjectedValueAggregateKind {
    CountField,
    Sum,
    Avg,
    Min,
    Max,
}

///
/// ProjectedValueAggregateRequest
///
/// ProjectedValueAggregateRequest is the executor-owned aggregate reducer
/// contract for already-projected input values.
/// It keeps DISTINCT policy and reducer kind together so session surfaces do
/// not reimplement fold/finalize behavior after projection.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct ProjectedValueAggregateRequest {
    kind: ProjectedValueAggregateKind,
    distinct: bool,
}

impl ProjectedValueAggregateRequest {
    /// Build one projected-value aggregate reducer request.
    #[must_use]
    pub(in crate::db) const fn new(kind: ProjectedValueAggregateKind, distinct: bool) -> Self {
        Self { kind, distinct }
    }
}

// Deduplicate one aggregate input vector before executor reduction when the
// prepared aggregate terminal carries DISTINCT semantics.
fn dedup_projected_values(values: Vec<Value>) -> Vec<Value> {
    let mut deduped = Vec::with_capacity(values.len());

    for value in values {
        if deduped.iter().any(|current| current == &value) {
            continue;
        }
        deduped.push(value);
    }

    deduped
}

// Fold one projected COUNT(input) vector, preserving SQL/global aggregate
// semantics that ignore NULL input values.
fn reduce_projected_count(values: Vec<Value>) -> Value {
    let count = values
        .into_iter()
        .filter(|value| !matches!(value, Value::Null))
        .count();

    Value::Uint(u64::try_from(count).unwrap_or(u64::MAX))
}

// Fold one projected SUM/AVG input vector through the shared numeric decimal
// coercion contract used by aggregate reducers.
fn reduce_projected_numeric(
    values: Vec<Value>,
    kind: ProjectedValueAggregateKind,
) -> Result<Value, InternalError> {
    let mut sum = None::<Decimal>;
    let mut row_count = 0_u64;

    for value in values {
        if matches!(value, Value::Null) {
            continue;
        }
        let decimal = coerce_numeric_decimal(&value).ok_or_else(|| {
            InternalError::query_executor_invariant(format!(
                "projected aggregate numeric reducer encountered non-numeric value: {value:?}",
            ))
        })?;
        sum = Some(sum.map_or(decimal, |current| add_decimal_terms(current, decimal)));
        row_count = row_count.saturating_add(1);
    }

    match kind {
        ProjectedValueAggregateKind::Sum => Ok(sum.map_or(Value::Null, Value::Decimal)),
        ProjectedValueAggregateKind::Avg => Ok(sum
            .and_then(|sum| average_decimal_terms(sum, row_count))
            .map_or(Value::Null, Value::Decimal)),
        ProjectedValueAggregateKind::CountField
        | ProjectedValueAggregateKind::Min
        | ProjectedValueAggregateKind::Max => Err(InternalError::query_executor_invariant(
            "projected aggregate numeric reducer kind mismatch",
        )),
    }
}

// Fold one projected MIN/MAX input vector using the same strict comparable
// value contract as scalar expression aggregate reducers.
fn reduce_projected_extrema(
    values: Vec<Value>,
    kind: ProjectedValueAggregateKind,
) -> Result<Value, InternalError> {
    let mut selected = None::<Value>;

    for value in values {
        if matches!(value, Value::Null) {
            continue;
        }

        let replace = match selected.as_ref() {
            None => true,
            Some(current) => {
                let ordering = compare_numeric_or_strict_order(&value, current).ok_or_else(|| {
                    InternalError::query_executor_invariant(format!(
                        "projected aggregate extrema reducer encountered incomparable values: left={value:?} right={current:?}",
                    ))
                })?;

                match kind {
                    ProjectedValueAggregateKind::Min => ordering.is_lt(),
                    ProjectedValueAggregateKind::Max => ordering.is_gt(),
                    ProjectedValueAggregateKind::CountField
                    | ProjectedValueAggregateKind::Sum
                    | ProjectedValueAggregateKind::Avg => {
                        return Err(InternalError::query_executor_invariant(
                            "projected aggregate extrema reducer kind mismatch",
                        ));
                    }
                }
            }
        };

        if replace {
            selected = Some(value);
        }
    }

    Ok(selected.unwrap_or(Value::Null))
}

/// Execute one projected-value aggregate reducer through executor-owned
/// fold/finalize behavior.
pub(in crate::db) fn execute_projected_value_aggregate(
    values: Vec<Value>,
    request: ProjectedValueAggregateRequest,
) -> Result<Value, InternalError> {
    let values = if request.distinct {
        dedup_projected_values(values)
    } else {
        values
    };

    match request.kind {
        ProjectedValueAggregateKind::CountField => Ok(reduce_projected_count(values)),
        ProjectedValueAggregateKind::Sum | ProjectedValueAggregateKind::Avg => {
            reduce_projected_numeric(values, request.kind)
        }
        ProjectedValueAggregateKind::Min | ProjectedValueAggregateKind::Max => {
            reduce_projected_extrema(values, request.kind)
        }
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::executor::aggregate::{
            ProjectedValueAggregateKind, ProjectedValueAggregateRequest,
            execute_projected_value_aggregate,
        },
        value::Value,
    };

    #[test]
    fn projected_count_field_skips_null_inputs() {
        let value = execute_projected_value_aggregate(
            vec![Value::Uint(10), Value::Null, Value::Uint(20)],
            ProjectedValueAggregateRequest::new(ProjectedValueAggregateKind::CountField, false),
        )
        .expect("projected COUNT(input) should reduce");

        assert_eq!(value, Value::Uint(2));
    }

    #[test]
    fn projected_distinct_count_dedupes_before_null_filtering() {
        let value = execute_projected_value_aggregate(
            vec![
                Value::Text("a".to_string()),
                Value::Text("a".to_string()),
                Value::Null,
                Value::Null,
                Value::Text("b".to_string()),
            ],
            ProjectedValueAggregateRequest::new(ProjectedValueAggregateKind::CountField, true),
        )
        .expect("projected COUNT(DISTINCT input) should reduce");

        assert_eq!(value, Value::Uint(2));
    }
}
