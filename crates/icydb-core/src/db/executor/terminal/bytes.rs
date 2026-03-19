//! Module: db::executor::terminal::bytes
//! Responsibility: module-local ownership and contracts for db::executor::terminal::bytes.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        access::{ExecutableAccessPathDispatch, dispatch_executable_access_path},
        cursor::IndexScanContinuationInput,
        data::DataKey,
        direction::Direction,
        executor::{
            AccessScanContinuationInput, AccessStreamBindings, BytesByProjectionMode,
            ExecutableAccess, ExecutablePlan,
            aggregate::field::{
                AggregateFieldValueError, FieldSlot, extract_orderable_field_value,
                resolve_any_aggregate_target_slot_from_planner_slot,
            },
            pipeline::{contracts::LoadExecutor, entrypoints::PreparedScalarMaterializedBoundary},
            route::BytesTerminalFastPathContract,
        },
        predicate::MissingRowPolicy,
        query::plan::{
            CoveringProjectionOrder, FieldSlot as PlannedFieldSlot,
            constant_covering_projection_value_from_access, covering_index_projection_context,
        },
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    types::Ulid,
    value::{Value, ValueTag},
};

use crate::db::executor::terminal::{
    bytes_page_window_state, saturating_add_payload_len, serialized_value_len,
};

// Typed boundary request for one scalar bytes terminal family call.
enum BytesTerminalBoundaryRequest {
    Total,
    BySlot { target_field: PlannedFieldSlot },
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Classify canonical `bytes_by(field)` execution mode from one neutral
    // prepared scalar boundary without reintroducing plan-owned execution.
    fn bytes_by_projection_mode_from_prepared(
        prepared: &PreparedScalarMaterializedBoundary<'_, E>,
        target_field: &str,
    ) -> BytesByProjectionMode {
        if !matches!(prepared.consistency(), MissingRowPolicy::Ignore) {
            return BytesByProjectionMode::Materialized;
        }

        if constant_covering_projection_value_from_access(prepared.access(), target_field).is_some()
        {
            return BytesByProjectionMode::CoveringConstant;
        }

        if prepared.has_predicate() {
            return BytesByProjectionMode::Materialized;
        }

        if covering_index_projection_context(
            prepared.access(),
            prepared.order_spec(),
            target_field,
            E::MODEL.primary_key.name,
        )
        .is_some()
        {
            return BytesByProjectionMode::CoveringIndex;
        }

        BytesByProjectionMode::Materialized
    }

    // Derive one route-owned `bytes()` fast-path contract from the neutral
    // non-aggregate scalar materialized boundary.
    fn derive_bytes_terminal_fast_path_contract_from_prepared(
        prepared: &PreparedScalarMaterializedBoundary<'_, E>,
    ) -> Option<BytesTerminalFastPathContract> {
        prepared.has_no_predicate_or_distinct().then_some(())?;

        let direction = prepared.unordered_or_primary_key_order_direction()?;
        let access_strategy = prepared.access().resolve_strategy();
        let capabilities = access_strategy
            .as_path()
            .map(crate::db::access::single_path_capabilities)?;

        capabilities
            .supports_bytes_terminal_primary_key_window()
            .then_some(BytesTerminalFastPathContract::PrimaryKeyWindow(direction))
            .or_else(|| {
                capabilities
                    .supports_bytes_terminal_ordered_key_stream_window()
                    .then_some(BytesTerminalFastPathContract::OrderedKeyStreamWindow(
                        direction,
                    ))
            })
    }

    // Execute one scalar bytes terminal family request from the typed API
    // boundary and immediately hand off to shared bytes execution logic.
    fn execute_bytes_terminal_boundary(
        &self,
        plan: ExecutablePlan<E>,
        request: BytesTerminalBoundaryRequest,
    ) -> Result<u64, InternalError> {
        let prepared = self.prepare_scalar_materialized_boundary(plan)?;

        self.execute_prepared_bytes_terminal_boundary(prepared, request)
    }

    // Execute one scalar bytes terminal family request from the neutral
    // non-aggregate prepared boundary payload.
    fn execute_prepared_bytes_terminal_boundary(
        &self,
        prepared: PreparedScalarMaterializedBoundary<'_, E>,
        request: BytesTerminalBoundaryRequest,
    ) -> Result<u64, InternalError> {
        match request {
            BytesTerminalBoundaryRequest::Total => {
                if let Some(contract) =
                    Self::derive_bytes_terminal_fast_path_contract_from_prepared(&prepared)
                {
                    return match contract {
                        BytesTerminalFastPathContract::PrimaryKeyWindow(direction) => {
                            self.bytes_from_pk_store_window(&prepared, direction)
                        }
                        BytesTerminalFastPathContract::OrderedKeyStreamWindow(direction) => {
                            self.bytes_from_ordered_key_stream_window(&prepared, direction)
                        }
                    };
                }

                let response = self.execute_scalar_materialized_rows_boundary(prepared)?;
                let ctx = self.recovered_context()?;
                let mut total = 0u64;

                // Sum persisted row payload sizes for the effective response window.
                for id in response.ids() {
                    let key = DataKey::try_new::<E>(id.key())?;
                    let row = ctx.read(&key)?;
                    total = saturating_add_payload_len(total, row.len());
                }

                Ok(total)
            }
            BytesTerminalBoundaryRequest::BySlot { target_field } => {
                let projection_mode =
                    Self::bytes_by_projection_mode_from_prepared(&prepared, target_field.field());
                match projection_mode {
                    BytesByProjectionMode::CoveringConstant => {
                        let constant_value = constant_covering_projection_value_from_access(
                            prepared.access(),
                            target_field.field(),
                        )
                        .ok_or_else(|| {
                            crate::db::error::query_executor_invariant(
                                "bytes_by covering-constant mode selected without constant value",
                            )
                        })?;
                        let value_len = serialized_value_len(&constant_value)?;
                        let response = self.execute_scalar_materialized_rows_boundary(prepared)?;
                        let row_count = u64::try_from(response.len()).unwrap_or(u64::MAX);

                        Ok(crate::db::executor::saturating_row_len(value_len)
                            .saturating_mul(row_count))
                    }
                    BytesByProjectionMode::CoveringIndex => {
                        if let Some(total) =
                            Self::bytes_by_covering_index_if_eligible(&prepared, &target_field)?
                        {
                            return Ok(total);
                        }

                        let field_slot =
                            resolve_any_aggregate_target_slot_from_planner_slot::<E>(&target_field)
                                .map_err(AggregateFieldValueError::into_internal_error)?;
                        let response = self.execute_scalar_materialized_rows_boundary(prepared)?;

                        Self::bytes_by_materialized_response(
                            response,
                            target_field.field(),
                            field_slot,
                        )
                    }
                    BytesByProjectionMode::Materialized => {
                        let field_slot =
                            resolve_any_aggregate_target_slot_from_planner_slot::<E>(&target_field)
                                .map_err(AggregateFieldValueError::into_internal_error)?;
                        let response = self.execute_scalar_materialized_rows_boundary(prepared)?;

                        Self::bytes_by_materialized_response(
                            response,
                            target_field.field(),
                            field_slot,
                        )
                    }
                }
            }
        }
    }

    // Fold `bytes(field)` over one already materialized canonical response window.
    fn bytes_by_materialized_response(
        response: crate::db::response::EntityResponse<E>,
        target_field: &str,
        field_slot: FieldSlot,
    ) -> Result<u64, InternalError> {
        let mut total = 0u64;

        // Fold serialized field payload sizes over the effective response window.
        for row in response {
            let value = extract_orderable_field_value(row.entity_ref(), target_field, field_slot)
                .map_err(AggregateFieldValueError::into_internal_error)?;
            total = saturating_add_payload_len(total, serialized_value_len(&value)?);
        }

        Ok(total)
    }

    // Resolve one `bytes(field)` total from an index-covered projection when
    // the neutral prepared scalar boundary still satisfies the covering contract.
    fn bytes_by_covering_index_if_eligible(
        prepared: &PreparedScalarMaterializedBoundary<'_, E>,
        target_field: &PlannedFieldSlot,
    ) -> Result<Option<u64>, InternalError> {
        let Some(context) = covering_index_projection_context(
            prepared.access(),
            prepared.order_spec(),
            target_field.field(),
            E::MODEL.primary_key.name,
        ) else {
            return Ok(None);
        };

        // Phase 1: read component bytes in covering-order scan direction.
        let scan_direction = match context.order_contract {
            CoveringProjectionOrder::IndexOrder(direction) => direction,
            CoveringProjectionOrder::PrimaryKeyOrder(_) => Direction::Asc,
        };
        let raw_pairs = Self::read_bytes_covering_projection_component_pairs(
            prepared,
            context.component_index,
            scan_direction,
        )?;

        // Phase 2: enforce existing-row policy and decode component payloads.
        let mut projected_rows = Vec::with_capacity(raw_pairs.len());
        let ctx = &prepared.ctx;
        for (data_key, component_bytes) in raw_pairs {
            match prepared.consistency() {
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
            projected_rows.push((data_key, serialized_value_len(&value)?));
        }

        // Phase 3: reapply the effective output order before page-window folding.
        match context.order_contract {
            CoveringProjectionOrder::PrimaryKeyOrder(Direction::Asc) => {
                projected_rows.sort_by(|left, right| left.0.cmp(&right.0));
            }
            CoveringProjectionOrder::PrimaryKeyOrder(Direction::Desc) => {
                projected_rows.sort_by(|left, right| right.0.cmp(&left.0));
            }
            CoveringProjectionOrder::IndexOrder(Direction::Asc | Direction::Desc) => {}
        }

        let (offset, limit) = bytes_page_window_state(prepared.page_spec());
        let total = match limit {
            Some(limit) => projected_rows
                .into_iter()
                .skip(offset)
                .take(limit)
                .fold(0u64, |total, (_, value_len)| {
                    saturating_add_payload_len(total, value_len)
                }),
            None => projected_rows
                .into_iter()
                .skip(offset)
                .fold(0u64, |total, (_, value_len)| {
                    saturating_add_payload_len(total, value_len)
                }),
        };

        Ok(Some(total))
    }

    // Resolve one raw `(data_key, component_bytes)` stream for an eligible
    // covering-index `bytes(field)` path from the neutral scalar boundary.
    fn read_bytes_covering_projection_component_pairs(
        prepared: &PreparedScalarMaterializedBoundary<'_, E>,
        component_index: usize,
        direction: Direction,
    ) -> Result<Vec<(DataKey, Vec<u8>)>, InternalError> {
        let continuation = IndexScanContinuationInput::new(None, direction);
        let prefix_specs = prepared.index_prefix_specs.as_slice();

        if let [spec] = prefix_specs {
            return Self::read_bytes_covering_projection_component_pairs_for_index_bounds(
                &prepared.ctx,
                spec.index(),
                (spec.lower(), spec.upper()),
                continuation,
                component_index,
            );
        }
        if !prefix_specs.is_empty() {
            return Err(crate::db::error::query_executor_invariant(
                "covering projection index-prefix path requires one lowered prefix spec",
            ));
        }

        let range_specs = prepared.index_range_specs.as_slice();
        if let [spec] = range_specs {
            return Self::read_bytes_covering_projection_component_pairs_for_index_bounds(
                &prepared.ctx,
                spec.index(),
                (spec.lower(), spec.upper()),
                continuation,
                component_index,
            );
        }
        if !range_specs.is_empty() {
            return Err(crate::db::error::query_executor_invariant(
                "covering projection index-range path requires one lowered range spec",
            ));
        }

        Err(crate::db::error::query_executor_invariant(
            "covering projection component scans require index-backed access paths",
        ))
    }

    // Resolve one bounded covering projection component stream from one
    // lowered index-bound contract.
    fn read_bytes_covering_projection_component_pairs_for_index_bounds(
        ctx: &crate::db::executor::Context<'_, E>,
        index: &crate::model::index::IndexModel,
        bounds: (
            &std::ops::Bound<crate::db::index::RawIndexKey>,
            &std::ops::Bound<crate::db::index::RawIndexKey>,
        ),
        continuation: IndexScanContinuationInput<'_>,
        component_index: usize,
    ) -> Result<Vec<(DataKey, Vec<u8>)>, InternalError> {
        let store = ctx
            .db
            .with_store_registry(|registry| registry.try_get_store(index.store()))?;
        store.with_index(|index_store| {
            index_store.resolve_data_values_with_component_in_raw_range_limited(
                E::ENTITY_TAG,
                index,
                bounds,
                continuation,
                usize::MAX,
                component_index,
                None,
            )
        })
    }

    /// Execute one `bytes()` terminal over the canonical load response.
    pub(in crate::db) fn bytes(&self, plan: ExecutablePlan<E>) -> Result<u64, InternalError> {
        self.execute_bytes_terminal_boundary(plan, BytesTerminalBoundaryRequest::Total)
    }

    /// Execute one `bytes(field)` terminal over the canonical load response
    /// window using one planner-resolved field slot.
    pub(in crate::db) fn bytes_by_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
    ) -> Result<u64, InternalError> {
        self.execute_bytes_terminal_boundary(
            plan,
            BytesTerminalBoundaryRequest::BySlot { target_field },
        )
    }

    // Fold `bytes()` directly from persisted primary rows over the canonical
    // page window for safe PK full-scan/key-range shapes.
    fn bytes_from_pk_store_window(
        &self,
        prepared: &PreparedScalarMaterializedBoundary<'_, E>,
        direction: Direction,
    ) -> Result<u64, InternalError> {
        // Phase 1: snapshot paging + executable payload before store traversal.
        let page = prepared.page_spec().cloned();
        let access_strategy = prepared.access().resolve_strategy();
        let Some(path) = access_strategy.as_path() else {
            return Err(crate::db::error::query_executor_invariant(
                "bytes PK fast path requires single-path access strategy",
            ));
        };
        let (offset, limit) = bytes_page_window_state(page.as_ref());
        let ctx = self.recovered_context()?;

        // Phase 2: fold payload bytes through context traversal adapters.
        match dispatch_executable_access_path(path) {
            ExecutableAccessPathDispatch::FullScan => {
                ctx.sum_row_payload_bytes_full_scan_window(direction, offset, limit)
            }
            ExecutableAccessPathDispatch::KeyRange { start, end } => {
                let start_key = DataKey::try_new::<E>(*start)?;
                let end_key = DataKey::try_new::<E>(*end)?;
                ctx.sum_row_payload_bytes_key_range_window(
                    &start_key, &end_key, direction, offset, limit,
                )
            }
            _ => Err(crate::db::error::query_executor_invariant(
                "bytes PK fast path requires full-scan or key-range access",
            )),
        }
    }

    // Fold `bytes()` from an ordered key stream over the canonical page window
    // for unordered scalar shapes where row materialization is unnecessary.
    fn bytes_from_ordered_key_stream_window(
        &self,
        prepared: &PreparedScalarMaterializedBoundary<'_, E>,
        direction: Direction,
    ) -> Result<u64, InternalError> {
        // Phase 1: materialize immutable stream bindings before stream resolution.
        let page = prepared.page_spec().cloned();
        let consistency = prepared.consistency();
        let access = ExecutableAccess::new(
            prepared.access(),
            AccessStreamBindings::new(
                prepared.index_prefix_specs.as_slice(),
                prepared.index_range_specs.as_slice(),
                AccessScanContinuationInput::new(None, direction),
            ),
            None,
            None,
        );
        let (offset, limit) = bytes_page_window_state(page.as_ref());

        // Phase 2: stream keys and sum persisted payload lengths over the page window.
        let ctx = self.recovered_context()?;
        let mut key_stream = ctx.ordered_key_stream_from_runtime_access(access)?;

        ctx.sum_row_payload_bytes_from_ordered_key_stream(
            key_stream.as_mut(),
            consistency,
            offset,
            limit,
        )
    }
}

// Decode one canonical encoded covering-index component into one runtime
// `Value` so `bytes(field)` can reuse index-only projection payloads.
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
                        "text covering component payload is not valid UTF-8",
                    )
                })?;

                return Ok(Some(Value::Text(text)));
            }
            0xff => {
                bytes.push(0);
                i = i.saturating_add(2);
            }
            _ => {
                return Err(InternalError::index_corruption(
                    "text covering component payload has invalid escape byte",
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
