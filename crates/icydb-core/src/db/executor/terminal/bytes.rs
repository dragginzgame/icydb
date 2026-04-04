//! Module: db::executor::terminal::bytes
//! Responsibility: module-local ownership and contracts for db::executor::terminal::bytes.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        access::{ExecutableAccessPathDispatch, dispatch_executable_access_path},
        cursor::IndexScanContinuationInput,
        data::{DataKey, DataRow},
        direction::Direction,
        executor::{
            AccessScanContinuationInput, AccessStreamBindings, BytesByProjectionMode,
            ExecutableAccess, ExecutablePlan, PreparedLoadPlan, TraversalRuntime,
            aggregate::field::{
                AggregateFieldValueError, FieldSlot,
                extract_orderable_field_value_with_slot_reader,
                resolve_any_aggregate_target_slot_from_planner_slot_with_model,
            },
            decode_covering_projection_component,
            pipeline::{contracts::LoadExecutor, entrypoints::PreparedScalarMaterializedBoundary},
            read_row_with_consistency_from_store,
            route::BytesTerminalFastPathContract,
            sum_row_payload_bytes_from_ordered_key_stream_with_store,
            sum_row_payload_bytes_full_scan_window_with_store,
            sum_row_payload_bytes_key_range_window_with_store,
            terminal::{RowDecoder, RowLayout},
        },
        predicate::MissingRowPolicy,
        query::plan::{
            CoveringProjectionOrder, FieldSlot as PlannedFieldSlot,
            constant_covering_projection_value_from_access, covering_index_projection_context,
        },
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
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
        prepared: &PreparedScalarMaterializedBoundary<'_>,
        target_field: &str,
    ) -> BytesByProjectionMode {
        if !matches!(prepared.consistency(), MissingRowPolicy::Ignore) {
            return BytesByProjectionMode::Materialized;
        }

        if constant_covering_projection_value_from_access(
            &prepared.logical_plan.access,
            target_field,
        )
        .is_some()
        {
            return BytesByProjectionMode::CoveringConstant;
        }

        if prepared.has_predicate() {
            return BytesByProjectionMode::Materialized;
        }

        if covering_index_projection_context(
            &prepared.logical_plan.access,
            prepared.order_spec(),
            target_field,
            prepared.authority.model().primary_key.name,
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
        prepared: &PreparedScalarMaterializedBoundary<'_>,
    ) -> Option<BytesTerminalFastPathContract> {
        prepared.has_no_predicate_or_distinct().then_some(())?;

        let direction = prepared.unordered_or_primary_key_order_direction()?;
        let access_strategy = prepared.logical_plan.access.resolve_strategy();
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
        plan: PreparedLoadPlan,
        request: BytesTerminalBoundaryRequest,
    ) -> Result<u64, InternalError> {
        let prepared = self.prepare_scalar_materialized_boundary(plan)?;

        self.execute_prepared_bytes_terminal_boundary(prepared, request)
    }

    // Execute one scalar bytes terminal family request from the neutral
    // non-aggregate prepared boundary payload.
    fn execute_prepared_bytes_terminal_boundary(
        &self,
        prepared: PreparedScalarMaterializedBoundary<'_>,
        request: BytesTerminalBoundaryRequest,
    ) -> Result<u64, InternalError> {
        match request {
            BytesTerminalBoundaryRequest::Total => {
                if let Some(contract) =
                    Self::derive_bytes_terminal_fast_path_contract_from_prepared(&prepared)
                {
                    return match contract {
                        BytesTerminalFastPathContract::PrimaryKeyWindow(direction) => {
                            Self::bytes_from_pk_store_window(&prepared, direction)
                        }
                        BytesTerminalFastPathContract::OrderedKeyStreamWindow(direction) => {
                            Self::bytes_from_ordered_key_stream_window(&prepared, direction)
                        }
                    };
                }

                let page = self.execute_scalar_materialized_page_boundary(prepared)?;

                Ok(page.data_rows().iter().fold(0u64, |total, (_, row)| {
                    saturating_add_payload_len(total, row.len())
                }))
            }
            BytesTerminalBoundaryRequest::BySlot { target_field } => {
                let projection_mode =
                    Self::bytes_by_projection_mode_from_prepared(&prepared, target_field.field());
                match projection_mode {
                    BytesByProjectionMode::CoveringConstant => {
                        let constant_value = constant_covering_projection_value_from_access(
                            &prepared.logical_plan.access,
                            target_field.field(),
                        )
                        .ok_or_else(|| {
                            InternalError::query_executor_invariant(
                                "bytes_by covering-constant mode selected without constant value",
                            )
                        })?;
                        let value_len = serialized_value_len(&constant_value)?;
                        let page = self.execute_scalar_materialized_page_boundary(prepared)?;
                        let row_count = u64::try_from(page.data_rows().len()).unwrap_or(u64::MAX);

                        Ok(crate::db::executor::saturating_row_len(value_len)
                            .saturating_mul(row_count))
                    }
                    BytesByProjectionMode::CoveringIndex => {
                        if let Some(total) =
                            Self::bytes_by_covering_index_if_eligible(&prepared, &target_field)?
                        {
                            return Ok(total);
                        }

                        let row_layout = RowLayout::from_model(prepared.authority.model());
                        let field_slot =
                            resolve_any_aggregate_target_slot_from_planner_slot_with_model(
                                prepared.authority.model(),
                                &target_field,
                            )
                            .map_err(AggregateFieldValueError::into_internal_error)?;
                        let page = self.execute_scalar_materialized_page_boundary(prepared)?;

                        Self::bytes_by_materialized_rows(
                            page.data_rows(),
                            &row_layout,
                            target_field.field(),
                            field_slot,
                        )
                    }
                    BytesByProjectionMode::Materialized => {
                        let row_layout = RowLayout::from_model(prepared.authority.model());
                        let field_slot =
                            resolve_any_aggregate_target_slot_from_planner_slot_with_model(
                                prepared.authority.model(),
                                &target_field,
                            )
                            .map_err(AggregateFieldValueError::into_internal_error)?;
                        let page = self.execute_scalar_materialized_page_boundary(prepared)?;

                        Self::bytes_by_materialized_rows(
                            page.data_rows(),
                            &row_layout,
                            target_field.field(),
                            field_slot,
                        )
                    }
                }
            }
        }
    }

    // Fold `bytes(field)` over one already materialized structural row window.
    fn bytes_by_materialized_rows(
        rows: &[DataRow],
        row_layout: &RowLayout,
        target_field: &str,
        field_slot: FieldSlot,
    ) -> Result<u64, InternalError> {
        let row_decoder = RowDecoder::structural();
        let mut total = 0u64;

        // Fold serialized field payload sizes over the effective response
        // window without rebuilding typed entity responses.
        for (data_key, raw_row) in rows {
            let row = row_decoder.decode(row_layout, (data_key.clone(), raw_row.clone()))?;
            let value = extract_orderable_field_value_with_slot_reader(
                target_field,
                field_slot,
                &mut |index| row.slot(index),
            )
            .map_err(AggregateFieldValueError::into_internal_error)?;
            total = saturating_add_payload_len(total, serialized_value_len(&value)?);
        }

        Ok(total)
    }

    // Resolve one `bytes(field)` total from an index-covered projection when
    // the neutral prepared scalar boundary still satisfies the covering contract.
    fn bytes_by_covering_index_if_eligible(
        prepared: &PreparedScalarMaterializedBoundary<'_>,
        target_field: &PlannedFieldSlot,
    ) -> Result<Option<u64>, InternalError> {
        let Some(context) = covering_index_projection_context(
            &prepared.logical_plan.access,
            prepared.order_spec(),
            target_field.field(),
            prepared.authority.model().primary_key.name,
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
        for (data_key, component_bytes) in raw_pairs {
            if read_row_with_consistency_from_store(
                prepared.store,
                &data_key,
                prepared.consistency(),
            )?
            .is_none()
            {
                continue;
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
        prepared: &PreparedScalarMaterializedBoundary<'_>,
        component_index: usize,
        direction: Direction,
    ) -> Result<Vec<(DataKey, Vec<u8>)>, InternalError> {
        let continuation = IndexScanContinuationInput::new(None, direction);
        let prefix_specs = prepared.index_prefix_specs.as_slice();

        if let [spec] = prefix_specs {
            return Self::read_bytes_covering_projection_component_pairs_for_index_bounds(
                prepared.store_resolver,
                prepared.authority.entity_tag(),
                spec.index(),
                (spec.lower(), spec.upper()),
                continuation,
                component_index,
            );
        }
        if !prefix_specs.is_empty() {
            return Err(InternalError::query_executor_invariant(
                "covering projection index-prefix path requires one lowered prefix spec",
            ));
        }

        let range_specs = prepared.index_range_specs.as_slice();
        if let [spec] = range_specs {
            return Self::read_bytes_covering_projection_component_pairs_for_index_bounds(
                prepared.store_resolver,
                prepared.authority.entity_tag(),
                spec.index(),
                (spec.lower(), spec.upper()),
                continuation,
                component_index,
            );
        }
        if !range_specs.is_empty() {
            return Err(InternalError::query_executor_invariant(
                "covering projection index-range path requires one lowered range spec",
            ));
        }

        Err(InternalError::query_executor_invariant(
            "covering projection component scans require index-backed access paths",
        ))
    }

    // Resolve one bounded covering projection component stream from one
    // lowered index-bound contract.
    fn read_bytes_covering_projection_component_pairs_for_index_bounds(
        store_resolver: crate::db::executor::StoreResolver<'_>,
        entity_tag: crate::types::EntityTag,
        index: &crate::model::index::IndexModel,
        bounds: (
            &std::ops::Bound<crate::db::index::RawIndexKey>,
            &std::ops::Bound<crate::db::index::RawIndexKey>,
        ),
        continuation: IndexScanContinuationInput<'_>,
        component_index: usize,
    ) -> Result<Vec<(DataKey, Vec<u8>)>, InternalError> {
        let store = store_resolver.try_get_store(index.store())?;
        store.with_index(|index_store| {
            index_store.resolve_data_values_with_component_in_raw_range_limited(
                entity_tag,
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
        self.execute_bytes_terminal_boundary(
            plan.into_prepared_load_plan(),
            BytesTerminalBoundaryRequest::Total,
        )
    }

    /// Execute one `bytes(field)` terminal over the canonical load response
    /// window using one planner-resolved field slot.
    pub(in crate::db) fn bytes_by_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
    ) -> Result<u64, InternalError> {
        self.execute_bytes_terminal_boundary(
            plan.into_prepared_load_plan(),
            BytesTerminalBoundaryRequest::BySlot { target_field },
        )
    }

    // Fold `bytes()` directly from persisted primary rows over the canonical
    // page window for safe PK full-scan/key-range shapes.
    fn bytes_from_pk_store_window(
        prepared: &PreparedScalarMaterializedBoundary<'_>,
        direction: Direction,
    ) -> Result<u64, InternalError> {
        // Phase 1: snapshot paging + executable payload before store traversal.
        let page = prepared.page_spec().cloned();
        let access_strategy = prepared.logical_plan.access.resolve_strategy();
        let Some(path) = access_strategy.as_path() else {
            return Err(InternalError::query_executor_invariant(
                "bytes PK fast path requires single-path access strategy",
            ));
        };
        let (offset, limit) = bytes_page_window_state(page.as_ref());

        // Phase 2: fold payload bytes through structural store traversal helpers.
        match dispatch_executable_access_path(path) {
            ExecutableAccessPathDispatch::FullScan => {
                Ok(sum_row_payload_bytes_full_scan_window_with_store(
                    prepared.store,
                    direction,
                    offset,
                    limit,
                ))
            }
            ExecutableAccessPathDispatch::KeyRange { start, end } => {
                let start_key =
                    DataKey::try_from_structural_key(prepared.authority.entity_tag(), start)?;
                let end_key =
                    DataKey::try_from_structural_key(prepared.authority.entity_tag(), end)?;
                sum_row_payload_bytes_key_range_window_with_store(
                    prepared.store,
                    &start_key,
                    &end_key,
                    direction,
                    offset,
                    limit,
                )
            }
            _ => Err(InternalError::query_executor_invariant(
                "bytes PK fast path requires full-scan or key-range access",
            )),
        }
    }

    // Fold `bytes()` from an ordered key stream over the canonical page window
    // for unordered scalar shapes where row materialization is unnecessary.
    fn bytes_from_ordered_key_stream_window(
        prepared: &PreparedScalarMaterializedBoundary<'_>,
        direction: Direction,
    ) -> Result<u64, InternalError> {
        // Phase 1: materialize immutable stream bindings before stream resolution.
        let page = prepared.page_spec().cloned();
        let consistency = prepared.consistency();
        let access = ExecutableAccess::new(
            &prepared.logical_plan.access,
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
        let runtime = TraversalRuntime::new(prepared.store, prepared.authority.entity_tag());
        let mut key_stream = runtime.ordered_key_stream_from_runtime_access(access)?;

        sum_row_payload_bytes_from_ordered_key_stream_with_store(
            prepared.store,
            key_stream.as_mut(),
            consistency,
            offset,
            limit,
        )
    }
}
