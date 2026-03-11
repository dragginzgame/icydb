//! Module: db::executor::load::grouped_fold::candidate_rows::sink
//! Responsibility: module-local ownership and contracts for db::executor::load::grouped_fold::candidate_rows::sink.
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

    pub(super) fn push_candidate(
        &mut self,
        group_key_value: Value,
        aggregate_values: Vec<Value>,
    ) -> Result<(), InternalError> {
        match self {
            Self::Bounded {
                rows,
                selection_bound,
            } => {
                match rows.binary_search_by(|(existing_key, _)| {
                    canonical_value_compare(existing_key, &group_key_value)
                }) {
                    Ok(_) => {
                        return Err(crate::db::error::query_executor_invariant(format!(
                            "grouped finalize produced duplicate canonical group key: {group_key_value:?}"
                        )));
                    }
                    Err(insert_index) => {
                        rows.insert(insert_index, (group_key_value, aggregate_values));
                        if rows.len() > *selection_bound {
                            let _ = rows.pop();
                        }
                        debug_assert!(
                            rows.len() <= *selection_bound,
                            "bounded grouped candidate rows must stay <= selection_bound",
                        );
                    }
                }
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
            }
        }

        Ok(())
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
