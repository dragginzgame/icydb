use crate::{
    db::{
        Context,
        data::DataKey,
        entity_decode::{decode_and_validate_entity_key, format_entity_key_for_mismatch},
        executor::load::{CursorPage, FastLoadResult, LoadExecutor},
        query::plan::{
            AccessPath, ContinuationSignature, CursorBoundary, Direction, LogicalPlan,
            OrderDirection, decode_pk_cursor_boundary,
        },
        response::Response,
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    traits::{EntityKind, EntityValue},
    types::Id,
};
use std::ops::Bound;

///
/// PkStreamScanConfig
///
/// Fast-path scan configuration derived from access-path bounds.
/// Used to drive store-range traversal for PK-ordered scans.
///

struct PkStreamScanConfig<K> {
    range_start_key: Option<K>,
    range_end_key: Option<K>,
}

///
/// PkStreamScanResult
///
/// Fast-path access scan output before canonical post-access semantics.
/// Captures decoded rows and low-level scan volume.
///

struct PkStreamScanResult<E: EntityKind> {
    rows: Vec<(Id<E>, E)>,
    rows_scanned: usize,
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Fast path for canonical primary-key ordering over full scans.
    pub(super) fn try_execute_pk_order_stream(
        ctx: &Context<'_, E>,
        plan: &LogicalPlan<E::Key>,
        cursor_boundary: Option<&CursorBoundary>,
        direction: Direction,
        continuation_signature: ContinuationSignature,
    ) -> Result<Option<FastLoadResult<E>>, InternalError> {
        // Phase 1: derive a fast-path scan config from the canonical plan + cursor.
        let Some(config) = Self::build_pk_stream_scan_config(plan, cursor_boundary)? else {
            return Ok(None);
        };
        let stream_direction = Self::pk_stream_direction(plan);
        if Self::pk_scan_range_is_empty(config.range_start_key, config.range_end_key) {
            return Ok(Some(FastLoadResult {
                page: CursorPage {
                    items: Response(Vec::new()),
                    next_cursor: None,
                },
                rows_scanned: 0,
                post_access_rows: 0,
            }));
        }

        // Phase 2: stream rows directly from the store in primary-key order.
        let mut scan = Self::scan_pk_stream_rows(ctx, &config, stream_direction)?;

        // Phase 3: apply canonical post-access semantics and derive continuation.
        let page = Self::finalize_rows_into_page(
            plan,
            &mut scan.rows,
            cursor_boundary,
            direction,
            continuation_signature,
        )?;
        Ok(Some(FastLoadResult {
            post_access_rows: page.items.0.len(),
            page,
            rows_scanned: scan.rows_scanned,
        }))
    }

    // Build the fast-path scan config for canonical PK-ordered streaming.
    fn build_pk_stream_scan_config(
        plan: &LogicalPlan<E::Key>,
        cursor_boundary: Option<&CursorBoundary>,
    ) -> Result<Option<PkStreamScanConfig<E::Key>>, InternalError> {
        if !Self::is_pk_order_stream_eligible(plan) {
            return Ok(None);
        }

        // Keep malformed boundary classification stable on PK fast-path execution.
        let _cursor_key = decode_pk_cursor_boundary::<E>(cursor_boundary)?;
        let Some((range_start_key, range_end_key)) =
            plan.access.as_path().and_then(AccessPath::pk_stream_bounds)
        else {
            return Ok(None);
        };

        Ok(Some(PkStreamScanConfig {
            range_start_key,
            range_end_key,
        }))
    }

    // Execute the store-range streaming phase for the PK fast path.
    fn scan_pk_stream_rows(
        ctx: &Context<'_, E>,
        config: &PkStreamScanConfig<E::Key>,
        direction: Direction,
    ) -> Result<PkStreamScanResult<E>, InternalError> {
        ctx.with_store(|store| {
            let lower_raw = match config.range_start_key {
                Some(start) => DataKey::try_new::<E>(start)?.to_raw()?,
                None => DataKey::lower_bound::<E>().to_raw()?,
            };
            let lower_bound = Bound::Included(lower_raw);
            let upper_raw = match config.range_end_key {
                Some(end) => DataKey::try_new::<E>(end)?.to_raw()?,
                None => DataKey::upper_bound::<E>().to_raw()?,
            };

            let mut rows_scanned = 0usize;
            let mut rows = Vec::new();
            match direction {
                Direction::Asc => {
                    for entry in store.range((lower_bound, Bound::Included(upper_raw))) {
                        rows_scanned = rows_scanned.saturating_add(1);
                        let data_key = DataKey::try_from_raw(entry.key()).map_err(|err| {
                            InternalError::new(
                                ErrorClass::Corruption,
                                ErrorOrigin::Store,
                                format!("ordered scan encountered corrupted data key: {err}"),
                            )
                        })?;
                        let expected_key = data_key.try_key::<E>()?;
                        let entity = decode_and_validate_entity_key::<E, _, _, _, _>(
                            expected_key,
                            || entry.value().try_decode::<E>(),
                            |err| {
                                InternalError::new(
                                    ErrorClass::Corruption,
                                    ErrorOrigin::Serialize,
                                    format!(
                                        "ordered scan failed to decode row for {data_key}: {err}"
                                    ),
                                )
                            },
                            |expected_key, actual_key| {
                                let expected = format_entity_key_for_mismatch::<E>(expected_key);
                                let found = format_entity_key_for_mismatch::<E>(actual_key);
                                InternalError::new(
                                    ErrorClass::Corruption,
                                    ErrorOrigin::Store,
                                    format!("row key mismatch: expected {expected}, found {found}"),
                                )
                            },
                        )?;
                        rows.push((Id::from_key(expected_key), entity));
                    }
                }
                Direction::Desc => {
                    for entry in store.range((lower_bound, Bound::Included(upper_raw))).rev() {
                        rows_scanned = rows_scanned.saturating_add(1);
                        let data_key = DataKey::try_from_raw(entry.key()).map_err(|err| {
                            InternalError::new(
                                ErrorClass::Corruption,
                                ErrorOrigin::Store,
                                format!("ordered scan encountered corrupted data key: {err}"),
                            )
                        })?;
                        let expected_key = data_key.try_key::<E>()?;
                        let entity = decode_and_validate_entity_key::<E, _, _, _, _>(
                            expected_key,
                            || entry.value().try_decode::<E>(),
                            |err| {
                                InternalError::new(
                                    ErrorClass::Corruption,
                                    ErrorOrigin::Serialize,
                                    format!(
                                        "ordered scan failed to decode row for {data_key}: {err}"
                                    ),
                                )
                            },
                            |expected_key, actual_key| {
                                let expected = format_entity_key_for_mismatch::<E>(expected_key);
                                let found = format_entity_key_for_mismatch::<E>(actual_key);
                                InternalError::new(
                                    ErrorClass::Corruption,
                                    ErrorOrigin::Store,
                                    format!("row key mismatch: expected {expected}, found {found}"),
                                )
                            },
                        )?;
                        rows.push((Id::from_key(expected_key), entity));
                    }
                }
            }

            Ok(PkStreamScanResult { rows, rows_scanned })
        })?
    }

    fn is_pk_order_stream_eligible(plan: &LogicalPlan<E::Key>) -> bool {
        if !plan.mode.is_load() {
            return false;
        }

        let supports_pk_stream_access = plan
            .access
            .as_path()
            .is_some_and(AccessPath::is_full_scan_or_key_range);
        if !supports_pk_stream_access {
            return false;
        }

        let Some(order) = plan.order.as_ref() else {
            return false;
        };

        order.fields.len() == 1 && order.fields[0].0 == E::MODEL.primary_key.name
    }

    fn pk_scan_range_is_empty(
        range_start_key: Option<E::Key>,
        range_end_key: Option<E::Key>,
    ) -> bool {
        let Some(start) = range_start_key else {
            return false;
        };
        let Some(end) = range_end_key else {
            return false;
        };

        start > end
    }

    fn pk_stream_direction(plan: &LogicalPlan<E::Key>) -> Direction {
        let Some(order) = plan.order.as_ref() else {
            return Direction::Asc;
        };

        match order.fields.first().map(|(_, direction)| direction) {
            Some(OrderDirection::Desc) => Direction::Desc,
            _ => Direction::Asc,
        }
    }
}
