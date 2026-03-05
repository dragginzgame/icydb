//! Module: executor::aggregate::projection
//! Responsibility: field-value projection terminals over materialized responses.
//! Does not own: grouped key canonicalization internals or route planning logic.
//! Boundary: projection terminal helpers (`values`, `distinct_values`, `first/last value`).
//!
//! `distinct_values_by(field)` here is a non-grouped effective-window helper.
//! Grouped Class B DISTINCT accounting is enforced only through grouped
//! execution context boundaries.

use crate::{
    db::{
        access::AccessPlan,
        data::DataKey,
        direction::Direction,
        executor::{
            ExecutablePlan, ExecutionKernel,
            aggregate::field::{
                FieldSlot, extract_orderable_field_value,
                resolve_any_aggregate_target_slot_from_planner_slot,
            },
            aggregate::materialized_distinct::insert_materialized_distinct_value,
            aggregate::{AggregateKind, AggregateOutput},
            group::GroupKeySet,
            load::LoadExecutor,
        },
        index::IndexScanContinuationInput,
        predicate::MissingRowPolicy,
        query::builder::{
            AggregateExpr,
            aggregate::{count, exists, first, last, max, min},
        },
        query::plan::{FieldSlot as PlannedFieldSlot, OrderDirection, OrderSpec, PageSpec},
        response::EntityResponse,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    types::{Id, Ulid},
    value::{Value, ValueTag},
};

type IdValueProjection<E> = Vec<(Id<E>, Value)>;

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    /// Execute `values_by(field)` over the effective response window using one
    /// planner-resolved field slot.
    pub(in crate::db) fn values_by_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
    ) -> Result<Vec<Value>, InternalError> {
        let field_slot = resolve_any_aggregate_target_slot_from_planner_slot::<E>(&target_field)
            .map_err(Self::map_aggregate_field_value_error)?;
        if let Some(projected_values) =
            self.covering_index_projection_values_if_eligible(&plan, &target_field)?
        {
            return Ok(projected_values);
        }
        if let Some(constant_value) =
            Self::constant_covering_projection_value_if_eligible(&plan, target_field.field())
        {
            let row_count = self.aggregate_count(plan)?;
            let output_len = usize::try_from(row_count).unwrap_or(usize::MAX);
            return Ok(vec![constant_value; output_len]);
        }

        let response = self.execute(plan)?;

        Self::project_field_values_from_materialized(response, target_field.field(), field_slot)
    }

    /// Execute `distinct_values_by(field)` over the effective response window
    /// using one planner-resolved field slot.
    pub(in crate::db) fn distinct_values_by_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
    ) -> Result<Vec<Value>, InternalError> {
        let field_slot = resolve_any_aggregate_target_slot_from_planner_slot::<E>(&target_field)
            .map_err(Self::map_aggregate_field_value_error)?;
        if let Some(covering_projection) =
            self.covering_index_projection_values_with_context_if_eligible(&plan, &target_field)?
        {
            if covering_index_adjacent_distinct_eligible(covering_projection.context) {
                return Ok(dedup_adjacent_values(covering_projection.values));
            }

            return dedup_values_preserving_first(covering_projection.values);
        }
        if let Some(constant_value) =
            Self::constant_covering_projection_value_if_eligible(&plan, target_field.field())
        {
            let has_rows = self.aggregate_exists(plan)?;
            return Ok(if has_rows {
                vec![constant_value]
            } else {
                Vec::new()
            });
        }

        let response = self.execute(plan)?;

        Self::project_distinct_field_values_from_materialized(
            response,
            target_field.field(),
            field_slot,
        )
    }

    /// Execute `values_by_with_ids(field)` over the effective response window
    /// using one planner-resolved field slot.
    pub(in crate::db) fn values_by_with_ids_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
    ) -> Result<IdValueProjection<E>, InternalError> {
        let field_slot = resolve_any_aggregate_target_slot_from_planner_slot::<E>(&target_field)
            .map_err(Self::map_aggregate_field_value_error)?;
        if let Some(projected_values) =
            self.covering_index_projection_values_with_ids_if_eligible(&plan, &target_field)?
        {
            return Ok(projected_values);
        }
        let response = self.execute(plan)?;

        Self::project_field_values_with_ids_from_materialized(
            response,
            target_field.field(),
            field_slot,
        )
    }

    /// Execute `first_value_by(field)` using one planner-resolved field slot.
    pub(in crate::db) fn first_value_by_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
    ) -> Result<Option<Value>, InternalError> {
        let field_slot = resolve_any_aggregate_target_slot_from_planner_slot::<E>(&target_field)
            .map_err(Self::map_aggregate_field_value_error)?;
        if let Some(projected_values) =
            self.covering_index_projection_values_if_eligible(&plan, &target_field)?
        {
            return Ok(projected_values.first().cloned());
        }
        if let Some(constant_value) =
            Self::constant_covering_projection_value_if_eligible(&plan, target_field.field())
        {
            let has_rows = self.aggregate_exists(plan)?;
            return Ok(has_rows.then_some(constant_value));
        }

        self.execute_terminal_value_field_projection_with_slot(
            plan,
            target_field.field(),
            field_slot,
            AggregateKind::First,
        )
    }

    /// Execute `last_value_by(field)` using one planner-resolved field slot.
    pub(in crate::db) fn last_value_by_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
    ) -> Result<Option<Value>, InternalError> {
        let field_slot = resolve_any_aggregate_target_slot_from_planner_slot::<E>(&target_field)
            .map_err(Self::map_aggregate_field_value_error)?;
        if let Some(projected_values) =
            self.covering_index_projection_values_if_eligible(&plan, &target_field)?
        {
            return Ok(projected_values.last().cloned());
        }
        if let Some(constant_value) =
            Self::constant_covering_projection_value_if_eligible(&plan, target_field.field())
        {
            let has_rows = self.aggregate_exists(plan)?;
            return Ok(has_rows.then_some(constant_value));
        }

        self.execute_terminal_value_field_projection_with_slot(
            plan,
            target_field.field(),
            field_slot,
            AggregateKind::Last,
        )
    }

    // Execute one field-target scalar terminal projection (`first_value_by` /
    // `last_value_by`) using a planner-validated slot and route-owned
    // first/last row selection semantics.
    fn execute_terminal_value_field_projection_with_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: &str,
        field_slot: FieldSlot,
        terminal_kind: AggregateKind,
    ) -> Result<Option<Value>, InternalError> {
        if !terminal_kind.supports_terminal_value_projection() {
            return Err(invariant(
                "terminal value projection requires FIRST/LAST aggregate kind",
            ));
        }

        let consistency = plan.consistency();
        let (AggregateOutput::First(selected_id) | AggregateOutput::Last(selected_id)) =
            ExecutionKernel::execute_aggregate_spec(
                self,
                plan,
                terminal_aggregate_expr(terminal_kind),
            )?
        else {
            return Err(invariant("terminal value projection result kind mismatch"));
        };
        let Some(selected_id) = selected_id else {
            return Ok(None);
        };

        let ctx = self.recovered_context()?;
        let key = DataKey::try_new::<E>(selected_id.key())?;
        let Some(entity) = Self::read_entity_for_field_extrema(&ctx, consistency, &key)? else {
            return Ok(None);
        };
        extract_orderable_field_value(&entity, target_field, field_slot)
            .map_err(Self::map_aggregate_field_value_error)
            .map(Some)
    }

    // Project one materialized response into one field value vector while
    // preserving the effective response row order.
    fn project_field_values_from_materialized(
        response: EntityResponse<E>,
        target_field: &str,
        field_slot: FieldSlot,
    ) -> Result<Vec<Value>, InternalError> {
        let mut projected_values = Vec::new();
        for row in response {
            let value = extract_orderable_field_value(row.entity_ref(), target_field, field_slot)
                .map_err(Self::map_aggregate_field_value_error)?;
            projected_values.push(value);
        }

        Ok(projected_values)
    }

    // Project one materialized response into distinct field values while
    // preserving first-observed order within the effective response window.
    // This is value DISTINCT semantics via canonical `GroupKey` equality.
    fn project_distinct_field_values_from_materialized(
        response: EntityResponse<E>,
        target_field: &str,
        field_slot: FieldSlot,
    ) -> Result<Vec<Value>, InternalError> {
        let mut distinct_values = GroupKeySet::default();
        let mut projected_values = Vec::new();
        for row in response {
            let value = extract_orderable_field_value(row.entity_ref(), target_field, field_slot)
                .map_err(Self::map_aggregate_field_value_error)?;
            if !insert_materialized_distinct_value(&mut distinct_values, &value)? {
                continue;
            }
            projected_values.push(value);
        }

        Ok(projected_values)
    }

    // Project one materialized response into id/value pairs while preserving
    // the effective response row order.
    fn project_field_values_with_ids_from_materialized(
        response: EntityResponse<E>,
        target_field: &str,
        field_slot: FieldSlot,
    ) -> Result<IdValueProjection<E>, InternalError> {
        let mut projected_values = Vec::new();
        for row in response {
            let (id, entity) = row.into_parts();
            let value = extract_orderable_field_value(&entity, target_field, field_slot)
                .map_err(Self::map_aggregate_field_value_error)?;
            projected_values.push((id, value));
        }

        Ok(projected_values)
    }

    // Resolve one constant field projection value when access shape guarantees
    // that target-field value is fixed by index-prefix equality bindings.
    //
    // Guard rails:
    // - only enabled for `MissingRowPolicy::Ignore` to preserve strict
    //   missing-row corruption surfacing behavior.
    // - only applies when the target field is bound by index-prefix equality.
    fn constant_covering_projection_value_if_eligible(
        plan: &ExecutablePlan<E>,
        target_field: &str,
    ) -> Option<Value> {
        if !matches!(plan.consistency(), MissingRowPolicy::Ignore) {
            return None;
        }

        constant_projection_value_from_access(plan.access(), target_field)
    }

    // Resolve one index-covered projection value vector for field terminals when
    // planner/runtime shape contracts allow index-only value decoding.
    fn covering_index_projection_values_if_eligible(
        &self,
        plan: &ExecutablePlan<E>,
        target_field: &PlannedFieldSlot,
    ) -> Result<Option<Vec<Value>>, InternalError> {
        let Some(covering_projection) =
            self.covering_index_projection_values_with_context_if_eligible(plan, target_field)?
        else {
            return Ok(None);
        };

        Ok(Some(covering_projection.values))
    }

    // Resolve one index-covered projection value vector with routing metadata
    // so terminal-specific post-processing can choose safe distinct strategy.
    fn covering_index_projection_values_with_context_if_eligible(
        &self,
        plan: &ExecutablePlan<E>,
        target_field: &PlannedFieldSlot,
    ) -> Result<Option<CoveringProjectionValues>, InternalError> {
        if plan.has_predicate() {
            return Ok(None);
        }

        let Some(context) =
            covering_index_projection_context::<E>(plan.access(), plan, target_field.field())
        else {
            return Ok(None);
        };

        let scan_direction = match context.order_contract {
            CoveringProjectionOrder::IndexOrder(direction) => direction,
            CoveringProjectionOrder::PrimaryKeyOrder(_) => Direction::Asc,
        };
        let raw_pairs = self.read_covering_projection_component_pairs(
            plan,
            context.component_index,
            scan_direction,
        )?;

        let mut projected_pairs = Vec::with_capacity(raw_pairs.len());
        let ctx = self.recovered_context()?;
        for (data_key, component_bytes) in raw_pairs {
            match plan.consistency() {
                MissingRowPolicy::Ignore => match ctx.read(&data_key) {
                    Ok(_) => {}
                    Err(err) if err.is_not_found() => continue,
                    Err(err) => return Err(err),
                },
                MissingRowPolicy::Error => {
                    ctx.read_strict(&data_key)?;
                }
            }

            let Some(value) = decode_covering_projection_component(&component_bytes)? else {
                return Ok(None);
            };
            projected_pairs.push((data_key, value));
        }

        match context.order_contract {
            CoveringProjectionOrder::PrimaryKeyOrder(Direction::Asc) => {
                projected_pairs.sort_by(|left, right| left.0.cmp(&right.0));
            }
            CoveringProjectionOrder::PrimaryKeyOrder(Direction::Desc) => {
                projected_pairs.sort_by(|left, right| right.0.cmp(&left.0));
            }
            CoveringProjectionOrder::IndexOrder(Direction::Asc | Direction::Desc) => {}
        }

        let (offset, limit) = scalar_window_for_covering_projection(plan.page_spec());
        let mut values = Vec::new();
        for (_, value) in projected_pairs.into_iter().skip(offset) {
            if let Some(limit) = limit
                && values.len() == limit
            {
                break;
            }
            values.push(value);
        }

        Ok(Some(CoveringProjectionValues { values, context }))
    }

    // Resolve one index-covered `(id, value)` vector for `values_by_with_ids`
    // terminals when planner/runtime shape contracts allow index-only decode.
    fn covering_index_projection_values_with_ids_if_eligible(
        &self,
        plan: &ExecutablePlan<E>,
        target_field: &PlannedFieldSlot,
    ) -> Result<Option<IdValueProjection<E>>, InternalError> {
        if plan.has_predicate() {
            return Ok(None);
        }

        let Some(context) =
            covering_index_projection_context::<E>(plan.access(), plan, target_field.field())
        else {
            return Ok(None);
        };

        // Phase 1: read component pairs in the order implied by the covering contract.
        let scan_direction = match context.order_contract {
            CoveringProjectionOrder::IndexOrder(direction) => direction,
            CoveringProjectionOrder::PrimaryKeyOrder(_) => Direction::Asc,
        };
        let raw_pairs = self.read_covering_projection_component_pairs(
            plan,
            context.component_index,
            scan_direction,
        )?;

        // Phase 2: enforce missing-row policy and decode projection components.
        let mut projected_pairs = Vec::with_capacity(raw_pairs.len());
        let ctx = self.recovered_context()?;
        for (data_key, component_bytes) in raw_pairs {
            match plan.consistency() {
                MissingRowPolicy::Ignore => match ctx.read(&data_key) {
                    Ok(_) => {}
                    Err(err) if err.is_not_found() => continue,
                    Err(err) => return Err(err),
                },
                MissingRowPolicy::Error => {
                    ctx.read_strict(&data_key)?;
                }
            }

            let Some(value) = decode_covering_projection_component(&component_bytes)? else {
                return Ok(None);
            };
            projected_pairs.push((data_key, value));
        }

        // Phase 3: realign to post-access order and apply effective window.
        match context.order_contract {
            CoveringProjectionOrder::PrimaryKeyOrder(Direction::Asc) => {
                projected_pairs.sort_by(|left, right| left.0.cmp(&right.0));
            }
            CoveringProjectionOrder::PrimaryKeyOrder(Direction::Desc) => {
                projected_pairs.sort_by(|left, right| right.0.cmp(&left.0));
            }
            CoveringProjectionOrder::IndexOrder(Direction::Asc | Direction::Desc) => {}
        }

        let (offset, limit) = scalar_window_for_covering_projection(plan.page_spec());
        let mut projected_values = Vec::new();
        for (data_key, value) in projected_pairs.into_iter().skip(offset) {
            if let Some(limit) = limit
                && projected_values.len() == limit
            {
                break;
            }
            let id = Id::from_key(data_key.try_key::<E>()?);
            projected_values.push((id, value));
        }

        Ok(Some(projected_values))
    }

    // Read one index-backed `(data_key, encoded_component)` stream for covering
    // projection decoding.
    fn read_covering_projection_component_pairs(
        &self,
        plan: &ExecutablePlan<E>,
        component_index: usize,
        direction: Direction,
    ) -> Result<Vec<(DataKey, Vec<u8>)>, InternalError> {
        let ctx = self.recovered_context()?;
        let continuation = IndexScanContinuationInput::new(None, direction);

        let prefix_specs = plan.index_prefix_specs()?;
        if let [spec] = prefix_specs {
            let store = ctx
                .db
                .with_store_registry(|registry| registry.try_get_store(spec.index().store))?;
            return store.with_index(|index_store| {
                index_store.resolve_data_values_with_component_in_raw_range_limited::<E>(
                    spec.index(),
                    (spec.lower(), spec.upper()),
                    continuation,
                    usize::MAX,
                    component_index,
                    None,
                )
            });
        }
        if !prefix_specs.is_empty() {
            return Err(invariant(
                "covering projection index-prefix path requires one lowered prefix spec",
            ));
        }

        let range_specs = plan.index_range_specs()?;
        if let [spec] = range_specs {
            let store = ctx
                .db
                .with_store_registry(|registry| registry.try_get_store(spec.index().store))?;
            return store.with_index(|index_store| {
                index_store.resolve_data_values_with_component_in_raw_range_limited::<E>(
                    spec.index(),
                    (spec.lower(), spec.upper()),
                    continuation,
                    usize::MAX,
                    component_index,
                    None,
                )
            });
        }
        if !range_specs.is_empty() {
            return Err(invariant(
                "covering projection index-range path requires one lowered range spec",
            ));
        }

        Err(invariant(
            "covering projection component scans require index-backed access paths",
        ))
    }
}

fn invariant(message: impl Into<String>) -> InternalError {
    InternalError::query_executor_invariant(message)
}

fn terminal_aggregate_expr(kind: AggregateKind) -> AggregateExpr {
    match kind {
        AggregateKind::Count => count(),
        AggregateKind::Sum => {
            unreachable!("terminal aggregate expression helper must not be used for SUM(field)")
        }
        AggregateKind::Exists => exists(),
        AggregateKind::Min => min(),
        AggregateKind::Max => max(),
        AggregateKind::First => first(),
        AggregateKind::Last => last(),
    }
}

// Resolve one constant projection value when access path binds the target
// field through index-prefix equality.
fn constant_projection_value_from_access<K>(
    access: &AccessPlan<K>,
    target_field: &str,
) -> Option<Value> {
    if let Some((index, values)) = access.as_index_prefix_path() {
        return constant_projection_value_from_prefix(index.fields, values, target_field);
    }
    if let Some((index, prefix_values, _, _)) = access.as_index_range_path() {
        return constant_projection_value_from_prefix(index.fields, prefix_values, target_field);
    }

    None
}

// Resolve one constant projection value from index-prefix bindings.
fn constant_projection_value_from_prefix(
    index_fields: &[&'static str],
    prefix_values: &[Value],
    target_field: &str,
) -> Option<Value> {
    index_fields
        .iter()
        .zip(prefix_values.iter())
        .find_map(|(field, value)| (*field == target_field).then(|| value.clone()))
}

#[derive(Clone, Copy)]
enum CoveringProjectionOrder {
    IndexOrder(Direction),
    PrimaryKeyOrder(Direction),
}

///
/// CoveringProjectionContext
///
/// Covering projection metadata derived from one executable access/order shape.
/// This context keeps distinct strategy decisions local to projection runtime
/// and avoids re-deriving index-position contracts across terminal paths.
///

#[derive(Clone, Copy)]
struct CoveringProjectionContext {
    component_index: usize,
    prefix_len: usize,
    order_contract: CoveringProjectionOrder,
}

///
/// CoveringProjectionValues
///
/// Covering projection decoded values plus the context that produced them.
/// Distinct terminals use this bundle to choose between adjacent-key dedupe
/// and first-observed canonical dedupe without recomputing shape checks.
///

struct CoveringProjectionValues {
    values: Vec<Value>,
    context: CoveringProjectionContext,
}

// Derive covering-projection access context (index-field position + output order
// contract) from one index-backed path and scalar ORDER BY shape.
fn covering_index_projection_context<E>(
    access: &AccessPlan<E::Key>,
    plan: &ExecutablePlan<E>,
    target_field: &str,
) -> Option<CoveringProjectionContext>
where
    E: EntityKind + EntityValue,
{
    let (index_fields, prefix_len, path_kind_is_range) =
        if let Some((index, values)) = access.as_index_prefix_path() {
            (index.fields, values.len(), false)
        } else if let Some((index, prefix_values, _, _)) = access.as_index_range_path() {
            (index.fields, prefix_values.len(), true)
        } else {
            return None;
        };
    let component_index = index_fields
        .iter()
        .position(|field| *field == target_field)?;

    let order_contract = covering_projection_order_contract(
        plan.order_spec(),
        index_fields,
        prefix_len,
        E::MODEL.primary_key.name,
        path_kind_is_range,
    )?;

    Some(CoveringProjectionContext {
        component_index,
        prefix_len,
        order_contract,
    })
}

// Resolve one output-order contract that keeps index-projected values aligned
// with load post-access ordering semantics.
fn covering_projection_order_contract(
    order: Option<&OrderSpec>,
    index_fields: &[&'static str],
    prefix_len: usize,
    primary_key_name: &'static str,
    path_kind_is_range: bool,
) -> Option<CoveringProjectionOrder> {
    let Some(order) = order else {
        return Some(CoveringProjectionOrder::PrimaryKeyOrder(Direction::Asc));
    };
    let (first_order_field, first_order_direction) = order.fields.first()?;
    let direction = match first_order_direction {
        OrderDirection::Asc => Direction::Asc,
        OrderDirection::Desc => Direction::Desc,
    };
    if order
        .fields
        .iter()
        .any(|(_, order_direction)| order_direction != first_order_direction)
    {
        return None;
    }

    if order.fields.len() == 1 && first_order_field == primary_key_name {
        return Some(CoveringProjectionOrder::PrimaryKeyOrder(direction));
    }

    let mut expected_suffix = Vec::with_capacity(index_fields.len().saturating_sub(prefix_len) + 1);
    expected_suffix.extend(index_fields.iter().skip(prefix_len).copied());
    expected_suffix.push(primary_key_name);
    let actual_fields = order
        .fields
        .iter()
        .map(|(field, _)| field.as_str())
        .collect::<Vec<_>>();
    if actual_fields == expected_suffix {
        return Some(CoveringProjectionOrder::IndexOrder(direction));
    }

    if path_kind_is_range {
        return None;
    }

    let mut expected_full = Vec::with_capacity(index_fields.len() + 1);
    expected_full.extend(index_fields.iter().copied());
    expected_full.push(primary_key_name);
    (actual_fields == expected_full).then_some(CoveringProjectionOrder::IndexOrder(direction))
}

fn scalar_window_for_covering_projection(page: Option<&PageSpec>) -> (usize, Option<usize>) {
    let Some(page) = page else {
        return (0, None);
    };

    let offset = usize::try_from(page.offset).unwrap_or(usize::MAX);
    let limit = page
        .limit
        .map(|limit| usize::try_from(limit).unwrap_or(usize::MAX));

    (offset, limit)
}

// Return whether one covering distinct projection can use adjacent-key dedupe.
//
// Safety contract:
// - output order must remain in index traversal order (no primary-key reorder),
// - target projection field must be the first unbound index component.
//
// Under this shape, equal projected values are contiguous in the effective
// covering value stream, so adjacent dedupe is equivalent to first-observed
// canonical dedupe.
const fn covering_index_adjacent_distinct_eligible(context: CoveringProjectionContext) -> bool {
    matches!(
        context.order_contract,
        CoveringProjectionOrder::IndexOrder(_)
    ) && context.component_index == context.prefix_len
}

fn dedup_values_preserving_first(values: Vec<Value>) -> Result<Vec<Value>, InternalError> {
    let mut seen = GroupKeySet::default();
    let mut out = Vec::new();
    for value in values {
        if !insert_materialized_distinct_value(&mut seen, &value)? {
            continue;
        }
        out.push(value);
    }

    Ok(out)
}

fn dedup_adjacent_values(values: Vec<Value>) -> Vec<Value> {
    let mut out = Vec::with_capacity(values.len());
    for value in values {
        if out.last().is_some_and(|previous| previous == &value) {
            continue;
        }
        out.push(value);
    }

    out
}

// Decode one canonical encoded index component payload into a runtime `Value`.
// Returns `Ok(None)` when this component kind is not supported by the current
// covering fast-path decoder.
fn decode_covering_projection_component(component: &[u8]) -> Result<Option<Value>, InternalError> {
    let Some((&tag, payload)) = component.split_first() else {
        return Err(InternalError::index_corruption(
            "index component payload is empty during covering projection decode",
        ));
    };

    if tag == ValueTag::Bool.to_u8() {
        return decode_covering_bool(payload);
    }
    if tag == ValueTag::Int.to_u8() {
        return decode_covering_i64(payload);
    }
    if tag == ValueTag::Uint.to_u8() {
        return decode_covering_u64(payload);
    }
    if tag == ValueTag::Text.to_u8() {
        return decode_covering_text(payload);
    }
    if tag == ValueTag::Ulid.to_u8() {
        return decode_covering_ulid(payload);
    }
    if tag == ValueTag::Unit.to_u8() {
        return Ok(Some(Value::Unit));
    }

    Ok(None)
}

fn decode_covering_bool(payload: &[u8]) -> Result<Option<Value>, InternalError> {
    let Some(value) = payload.first() else {
        return Err(InternalError::index_corruption(
            "bool covering component payload is truncated",
        ));
    };
    if payload.len() != 1 {
        return Err(InternalError::index_corruption(
            "bool covering component payload has invalid length",
        ));
    }

    match *value {
        0 => Ok(Some(Value::Bool(false))),
        1 => Ok(Some(Value::Bool(true))),
        _ => Err(InternalError::index_corruption(
            "bool covering component payload has invalid value",
        )),
    }
}

fn decode_covering_i64(payload: &[u8]) -> Result<Option<Value>, InternalError> {
    if payload.len() != 8 {
        return Err(InternalError::index_corruption(
            "int covering component payload has invalid length",
        ));
    }
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(payload);
    let biased = u64::from_be_bytes(bytes);
    let unsigned = biased ^ (1u64 << 63);
    let value = i64::from_be_bytes(unsigned.to_be_bytes());

    Ok(Some(Value::Int(value)))
}

fn decode_covering_u64(payload: &[u8]) -> Result<Option<Value>, InternalError> {
    if payload.len() != 8 {
        return Err(InternalError::index_corruption(
            "uint covering component payload has invalid length",
        ));
    }
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(payload);

    Ok(Some(Value::Uint(u64::from_be_bytes(bytes))))
}

fn decode_covering_text(payload: &[u8]) -> Result<Option<Value>, InternalError> {
    let mut bytes = Vec::new();
    let mut i = 0usize;
    while i < payload.len() {
        let byte = payload[i];
        if byte != 0 {
            bytes.push(byte);
            i = i.saturating_add(1);
            continue;
        }

        let Some(next) = payload.get(i.saturating_add(1)).copied() else {
            return Err(InternalError::index_corruption(
                "text covering component payload has invalid terminator",
            ));
        };
        match next {
            0 => {
                i = i.saturating_add(2);
                if i != payload.len() {
                    return Err(InternalError::index_corruption(
                        "text covering component payload contains trailing bytes",
                    ));
                }

                let text = String::from_utf8(bytes).map_err(|_| {
                    InternalError::index_corruption(
                        "text covering component payload is not valid utf-8",
                    )
                })?;
                return Ok(Some(Value::Text(text)));
            }
            0xFF => {
                bytes.push(0);
                i = i.saturating_add(2);
            }
            _ => {
                return Err(InternalError::index_corruption(
                    "text covering component payload has invalid escape sequence",
                ));
            }
        }
    }

    Err(InternalError::index_corruption(
        "text covering component payload is missing terminator",
    ))
}

fn decode_covering_ulid(payload: &[u8]) -> Result<Option<Value>, InternalError> {
    if payload.len() != 16 {
        return Err(InternalError::index_corruption(
            "ulid covering component payload has invalid length",
        ));
    }

    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(payload);

    Ok(Some(Value::Ulid(Ulid::from_bytes(bytes))))
}
