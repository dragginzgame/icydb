//! Module: db::executor::aggregate::runtime::grouped_fold::candidate_rows::sink
//! Responsibility: module-local ownership and contracts for db::executor::aggregate::runtime::grouped_fold::candidate_rows::sink.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{db::contracts::canonical_value_compare, error::InternalError, value::Value};

///
/// GroupedCandidateSink
///
/// Strategy selected once per grouped execution to avoid per-row branching on
/// bounded vs unbounded candidate buffering policy.
///

pub(super) enum GroupedCandidateSink {
    Bounded {
        rows: Vec<(Value, Vec<Value>)>,
        selection_bound: usize,
    },
    Unbounded {
        rows: Vec<(Value, Vec<Value>)>,
        max_groups_bound: usize,
    },
}

impl GroupedCandidateSink {
    fn duplicate_canonical_group_key(group_key_value: &Value) -> InternalError {
        InternalError::query_executor_invariant(format!(
            "grouped finalize produced duplicate canonical group key: {group_key_value:?}"
        ))
    }

    fn out_of_order_canonical_group_key(
        previous_group_key_value: &Value,
        group_key_value: &Value,
    ) -> InternalError {
        InternalError::query_executor_invariant(format!(
            "grouped finalize produced out-of-order canonical group keys: previous={previous_group_key_value:?}, current={group_key_value:?}"
        ))
    }

    pub(super) const fn new(selection_bound: Option<usize>, max_groups_bound: usize) -> Self {
        match selection_bound {
            Some(selection_bound) => Self::Bounded {
                rows: Vec::new(),
                selection_bound,
            },
            None => Self::Unbounded {
                rows: Vec::new(),
                max_groups_bound,
            },
        }
    }

    // Push one grouped candidate emitted in canonical grouped-key order.
    // Returns true when the bounded sink has filled its selection window and
    // the caller can stop consuming later finalized rows.
    pub(super) fn push_candidate(
        &mut self,
        group_key_value: Value,
        aggregate_values: Vec<Value>,
    ) -> Result<bool, InternalError> {
        match self {
            Self::Bounded {
                rows,
                selection_bound,
            } => {
                // Finalized grouped rows already arrive in canonical order, so
                // the bounded sink can append directly and stop once the page
                // selection window is full.
                if let Some((last_group_key, _)) = rows.last() {
                    match canonical_value_compare(last_group_key, &group_key_value) {
                        std::cmp::Ordering::Less => {}
                        std::cmp::Ordering::Equal => {
                            return Err(Self::duplicate_canonical_group_key(&group_key_value));
                        }
                        std::cmp::Ordering::Greater => {
                            return Err(Self::out_of_order_canonical_group_key(
                                last_group_key,
                                &group_key_value,
                            ));
                        }
                    }
                }
                if rows.len() >= *selection_bound {
                    return Ok(true);
                }
                rows.push((group_key_value, aggregate_values));
                debug_assert!(
                    rows.len() <= *selection_bound,
                    "bounded grouped candidate rows must stay <= selection_bound",
                );

                Ok(rows.len() >= *selection_bound)
            }
            Self::Unbounded {
                rows,
                max_groups_bound,
            } => {
                rows.push((group_key_value, aggregate_values));
                debug_assert!(
                    rows.len() <= *max_groups_bound,
                    "grouped candidate rows must stay bounded by max_groups",
                );

                Ok(false)
            }
        }
    }

    pub(super) fn into_rows(self) -> Vec<(Value, Vec<Value>)> {
        match self {
            Self::Bounded {
                rows,
                selection_bound,
            } => {
                debug_assert!(
                    rows.len() <= selection_bound,
                    "grouped candidate rows must remain bounded by selection_bound",
                );

                rows
            }
            Self::Unbounded {
                mut rows,
                max_groups_bound,
            } => {
                rows.sort_by(|(left, _), (right, _)| canonical_value_compare(left, right));
                debug_assert!(
                    rows.len() <= max_groups_bound,
                    "grouped candidate rows must remain bounded by max_groups",
                );

                rows
            }
        }
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::executor::aggregate::runtime::grouped_fold::candidate_rows::sink::GroupedCandidateSink,
        value::Value,
    };

    #[test]
    fn bounded_grouped_candidate_sink_stops_once_selection_bound_is_full() {
        let mut sink = GroupedCandidateSink::new(Some(2), 8);

        assert!(
            !sink
                .push_candidate(Value::Uint(1), vec![Value::Uint(10)])
                .expect("first bounded grouped candidate should fit"),
            "first bounded grouped candidate must not stop selection",
        );
        assert!(
            sink.push_candidate(Value::Uint(2), vec![Value::Uint(20)])
                .expect("second bounded grouped candidate should fill selection"),
            "second bounded grouped candidate should saturate selection bound",
        );

        let rows = sink.into_rows();
        assert_eq!(
            rows,
            vec![
                (Value::Uint(1), vec![Value::Uint(10)]),
                (Value::Uint(2), vec![Value::Uint(20)]),
            ],
            "bounded grouped candidate sink must preserve canonical leading rows",
        );
    }

    #[test]
    fn bounded_grouped_candidate_sink_rejects_out_of_order_keys() {
        let mut sink = GroupedCandidateSink::new(Some(3), 8);
        sink.push_candidate(Value::Uint(2), vec![Value::Uint(20)])
            .expect("first bounded grouped candidate should fit");

        let err = sink
            .push_candidate(Value::Uint(1), vec![Value::Uint(10)])
            .expect_err("out-of-order grouped candidate rows must fail");

        assert!(
            err.display_with_class()
                .contains("out-of-order canonical group keys"),
            "bounded grouped candidate sink should fail with the sorted-input invariant message",
        );
    }
}
