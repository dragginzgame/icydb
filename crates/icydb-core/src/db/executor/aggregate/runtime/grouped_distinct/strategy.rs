use crate::db::{
    executor::aggregate::runtime::grouped_distinct::GlobalDistinctFieldAggregateKind,
    query::plan::GroupedDistinctExecutionStrategy,
};

///
/// GlobalDistinctFieldExecutionSpec
///
/// Data-only execution spec for grouped global DISTINCT field reducers.
/// This spec is resolved from planner-owned grouped DISTINCT strategy and does
/// not execute any runtime behavior.
///

pub(in crate::db::executor) struct GlobalDistinctFieldExecutionSpec<'a> {
    pub(in crate::db::executor) target_field: &'a str,
    pub(in crate::db::executor) aggregate_kind: GlobalDistinctFieldAggregateKind,
}

// Resolve one grouped DISTINCT strategy into one optional global field
// execution spec. This helper is data-only and does not execute any fold path.
pub(in crate::db::executor) const fn global_distinct_field_execution_spec(
    strategy: &GroupedDistinctExecutionStrategy,
) -> Option<GlobalDistinctFieldExecutionSpec<'_>> {
    match strategy {
        GroupedDistinctExecutionStrategy::None => None,
        GroupedDistinctExecutionStrategy::GlobalDistinctFieldCount { target_field } => {
            Some(GlobalDistinctFieldExecutionSpec {
                target_field: target_field.as_str(),
                aggregate_kind: GlobalDistinctFieldAggregateKind::Count,
            })
        }
        GroupedDistinctExecutionStrategy::GlobalDistinctFieldSum { target_field } => {
            Some(GlobalDistinctFieldExecutionSpec {
                target_field: target_field.as_str(),
                aggregate_kind: GlobalDistinctFieldAggregateKind::Sum,
            })
        }
        GroupedDistinctExecutionStrategy::GlobalDistinctFieldAvg { target_field } => {
            Some(GlobalDistinctFieldExecutionSpec {
                target_field: target_field.as_str(),
                aggregate_kind: GlobalDistinctFieldAggregateKind::Avg,
            })
        }
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::db::{
        executor::aggregate::runtime::grouped_distinct::{
            GlobalDistinctFieldAggregateKind, global_distinct_field_execution_spec,
        },
        query::plan::GroupedDistinctExecutionStrategy,
    };

    #[test]
    fn grouped_distinct_strategy_none_maps_to_no_global_field_spec() {
        let strategy = GroupedDistinctExecutionStrategy::None;

        assert!(
            global_distinct_field_execution_spec(&strategy).is_none(),
            "grouped distinct None strategy must not resolve to a global field execution spec",
        );
    }

    #[test]
    fn grouped_distinct_count_strategy_maps_to_count_field_spec() {
        let strategy = GroupedDistinctExecutionStrategy::GlobalDistinctFieldCount {
            target_field: "rank".to_string(),
        };
        let spec = global_distinct_field_execution_spec(&strategy)
            .expect("grouped distinct COUNT strategy should resolve");

        assert_eq!(spec.target_field, "rank");
        assert!(matches!(
            spec.aggregate_kind,
            GlobalDistinctFieldAggregateKind::Count
        ));
    }

    #[test]
    fn grouped_distinct_sum_strategy_maps_to_sum_field_spec() {
        let strategy = GroupedDistinctExecutionStrategy::GlobalDistinctFieldSum {
            target_field: "score".to_string(),
        };
        let spec = global_distinct_field_execution_spec(&strategy)
            .expect("grouped distinct SUM strategy should resolve");

        assert_eq!(spec.target_field, "score");
        assert!(matches!(
            spec.aggregate_kind,
            GlobalDistinctFieldAggregateKind::Sum
        ));
    }

    #[test]
    fn grouped_distinct_avg_strategy_maps_to_avg_field_spec() {
        let strategy = GroupedDistinctExecutionStrategy::GlobalDistinctFieldAvg {
            target_field: "score".to_string(),
        };
        let spec = global_distinct_field_execution_spec(&strategy)
            .expect("grouped distinct AVG strategy should resolve");

        assert_eq!(spec.target_field, "score");
        assert!(matches!(
            spec.aggregate_kind,
            GlobalDistinctFieldAggregateKind::Avg
        ));
    }
}
