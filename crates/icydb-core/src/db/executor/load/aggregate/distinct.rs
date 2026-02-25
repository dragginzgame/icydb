use crate::{
    db::{
        executor::{
            AccessStreamBindings, DistinctOrderedKeyStream, ExecutablePlan, KeyOrderComparator,
            OrderedKeyStreamBox,
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
        let mut resolved = Self::resolve_execution_key_stream(
            &execution_inputs,
            &route_plan,
            IndexPredicateCompileMode::ConservativeSubset,
        )?;
        let (mut page, keys_scanned, mut post_access_rows) =
            Self::materialize_key_stream_into_page(
                &ctx,
                &logical_plan,
                predicate_slots.as_ref(),
                resolved.key_stream.as_mut(),
                route_plan.scan_hints.load_scan_budget_hint,
                route_plan.streaming_access_shape_safe(),
                None,
                direction,
                continuation_signature,
            )?;
        let mut rows_scanned = resolved.rows_scanned_override.unwrap_or(keys_scanned);
        if Self::index_range_limited_residual_retry_required(
            &logical_plan,
            None,
            &route_plan,
            rows_scanned,
            post_access_rows,
        ) {
            let mut fallback_route_plan = route_plan;
            fallback_route_plan.index_range_limit_spec = None;
            let mut fallback_resolved = Self::resolve_execution_key_stream(
                &execution_inputs,
                &fallback_route_plan,
                IndexPredicateCompileMode::ConservativeSubset,
            )?;
            let (fallback_page, fallback_keys_scanned, fallback_post_access_rows) =
                Self::materialize_key_stream_into_page(
                    &ctx,
                    &logical_plan,
                    predicate_slots.as_ref(),
                    fallback_resolved.key_stream.as_mut(),
                    fallback_route_plan.scan_hints.load_scan_budget_hint,
                    fallback_route_plan.streaming_access_shape_safe(),
                    None,
                    direction,
                    continuation_signature,
                )?;
            let fallback_rows_scanned = fallback_resolved
                .rows_scanned_override
                .unwrap_or(fallback_keys_scanned);
            rows_scanned = rows_scanned.saturating_add(fallback_rows_scanned);
            page = fallback_page;
            post_access_rows = fallback_post_access_rows;
        }

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
        let mut distinct_values: Vec<Value> = Vec::new();
        let mut distinct_count = 0u32;
        for (_, entity) in response {
            let value = extract_orderable_field_value(&entity, target_field, field_slot)
                .map_err(Self::map_aggregate_field_value_error)?;
            if distinct_values.iter().any(|existing| existing == &value) {
                continue;
            }

            distinct_values.push(value);
            distinct_count = distinct_count.saturating_add(1);
        }

        Ok(distinct_count)
    }

    // Wrap fast-path streams with DISTINCT semantics only when requested.
    pub(in crate::db::executor::load::aggregate) fn maybe_wrap_distinct_stream(
        ordered_key_stream: OrderedKeyStreamBox,
        distinct: bool,
        key_comparator: KeyOrderComparator,
    ) -> OrderedKeyStreamBox {
        if distinct {
            return Box::new(DistinctOrderedKeyStream::new(
                ordered_key_stream,
                key_comparator,
            ));
        }

        ordered_key_stream
    }
}
