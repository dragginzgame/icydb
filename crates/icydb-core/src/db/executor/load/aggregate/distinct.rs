use crate::{
    db::{
        executor::{
            AccessStreamBindings, ExecutablePlan,
            aggregate::field::{FieldSlot, extract_orderable_field_value},
            compile_predicate_slots,
            load::LoadExecutor,
            load::execute::{ExecutionInputs, IndexPredicateCompileMode},
            plan::{record_plan_metrics, record_rows_scanned},
            route::ExecutionMode,
        },
        query::plan::validate::validate_executor_plan,
        response::Response,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    value::Value,
};
use std::{cmp::Ordering, collections::BTreeSet};

///
/// CanonicalDistinctValue
///
/// Canonical set key wrapper for `count_distinct_by` value deduplication.
/// Uses `Value::canonical_cmp` to provide a total ordering for `BTreeSet`.
///

#[derive(Clone, Debug, Eq, PartialEq)]
struct CanonicalDistinctValue(Value);

impl Ord for CanonicalDistinctValue {
    fn cmp(&self, other: &Self) -> Ordering {
        let ordering = Value::canonical_cmp(&self.0, &other.0);
        debug_assert!(
            (ordering == Ordering::Equal) == (self.0 == other.0),
            "canonical distinct ordering must preserve Value equality semantics",
        );

        ordering
    }
}

impl PartialOrd for CanonicalDistinctValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    pub(in crate::db) fn aggregate_count_distinct_by(
        &self,
        plan: ExecutablePlan<E>,
        target_field: impl Into<String>,
    ) -> Result<u32, InternalError> {
        let target_field = target_field.into();

        self.execute_count_distinct_field_aggregate(plan, target_field.as_str())
    }

    // Execute one field-target distinct-count aggregate
    // (`count_distinct(field)`) via canonical materialized fallback semantics.
    fn execute_count_distinct_field_aggregate(
        &self,
        plan: ExecutablePlan<E>,
        target_field: &str,
    ) -> Result<u32, InternalError> {
        let field_slot = Self::resolve_any_field_slot(target_field)?;
        validate_executor_plan::<E>(plan.as_inner())?;
        let route_plan =
            Self::build_execution_route_plan_for_load(plan.as_inner(), None, None, None)?;
        let direction = route_plan.direction();
        // Snapshot route-owned execution mode at the orchestration boundary.
        // This remains immutable for the full terminal execution lifecycle.
        let execution_mode = route_plan.execution_mode;
        if matches!(execution_mode, ExecutionMode::Materialized) {
            let response = self.execute(plan)?;
            return Self::aggregate_count_distinct_field_from_materialized(
                response,
                target_field,
                field_slot,
            );
        }

        let continuation_signature = plan.continuation_signature();
        let index_prefix_specs = plan.index_prefix_specs()?.to_vec();
        let index_range_specs = plan.index_range_specs()?.to_vec();
        let logical_plan = plan.into_inner();
        let predicate_slots = compile_predicate_slots::<E>(&logical_plan);
        validate_executor_plan::<E>(&logical_plan)?;
        let ctx = self.db.recovered_context::<E>()?;
        record_plan_metrics(&logical_plan.access);
        let execution_inputs = ExecutionInputs {
            ctx: &ctx,
            plan: &logical_plan,
            stream_bindings: AccessStreamBindings {
                index_prefix_specs: index_prefix_specs.as_slice(),
                index_range_specs: index_range_specs.as_slice(),
                index_range_anchor: None,
                direction,
            },
            predicate_slots: predicate_slots.as_ref(),
        };
        let materialized = Self::materialize_with_optional_residual_retry(
            &execution_inputs,
            &route_plan,
            None,
            continuation_signature,
            IndexPredicateCompileMode::ConservativeSubset,
        )?;
        let page = materialized.page;
        let rows_scanned = materialized.rows_scanned;
        let post_access_rows = materialized.post_access_rows;

        debug_assert!(
            post_access_rows >= page.items.0.len(),
            "count_distinct materialization must not exceed post-access row cardinality",
        );
        record_rows_scanned::<E>(rows_scanned);

        Self::aggregate_count_distinct_field_from_materialized(page.items, target_field, field_slot)
    }

    // Reduce one materialized response into `count_distinct(field)` by
    // counting unique typed field values across the effective response window.
    fn aggregate_count_distinct_field_from_materialized(
        response: Response<E>,
        target_field: &str,
        field_slot: FieldSlot,
    ) -> Result<u32, InternalError> {
        let mut distinct_values: BTreeSet<CanonicalDistinctValue> = BTreeSet::new();
        let mut distinct_count = 0u32;
        for (_, entity) in response {
            let value = extract_orderable_field_value(&entity, target_field, field_slot)
                .map_err(Self::map_aggregate_field_value_error)?;
            if distinct_values.insert(CanonicalDistinctValue(value)) {
                distinct_count = distinct_count.saturating_add(1);
            }
        }

        Ok(distinct_count)
    }
}
