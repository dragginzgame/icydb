use crate::{
    db::{
        Context,
        data::DataKey,
        executor::{
            VecOrderedKeyStream,
            load::{ExecutionOptimization, FastPathKeyResult, LoadExecutor},
        },
        query::plan::{
            AccessPath, Direction, LogicalPlan, SlotSelectionPolicy, derive_scan_direction,
        },
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
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
/// Captures ordered keys and low-level scan volume.
///

struct PkStreamScanResult {
    keys: Vec<DataKey>,
    rows_scanned: usize,
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Fast path for canonical primary-key ordering over full scans.
    // Produces ordered keys only; shared row materialization happens in load/mod.rs.
    pub(super) fn try_execute_pk_order_stream(
        ctx: &Context<'_, E>,
        plan: &LogicalPlan<E::Key>,
        probe_fetch_hint: Option<usize>,
    ) -> Result<Option<FastPathKeyResult>, InternalError> {
        // Phase 1: derive a fast-path scan config from the canonical plan.
        let config = Self::build_pk_stream_scan_config(plan)?;
        let stream_direction = Self::pk_stream_direction(plan);
        if Self::pk_scan_range_is_empty(config.range_start_key, config.range_end_key) {
            return Ok(Some(FastPathKeyResult {
                ordered_key_stream: Box::new(VecOrderedKeyStream::new(Vec::new())),
                rows_scanned: 0,
                optimization: ExecutionOptimization::PrimaryKey,
            }));
        }

        // Phase 2: stream ordered keys directly from the store.
        let scan = Self::scan_pk_stream_keys(ctx, &config, stream_direction, probe_fetch_hint)?;

        Ok(Some(FastPathKeyResult {
            ordered_key_stream: Box::new(VecOrderedKeyStream::new(scan.keys)),
            rows_scanned: scan.rows_scanned,
            optimization: ExecutionOptimization::PrimaryKey,
        }))
    }

    // Build the fast-path scan config for canonical PK-ordered streaming.
    fn build_pk_stream_scan_config(
        plan: &LogicalPlan<E::Key>,
    ) -> Result<PkStreamScanConfig<E::Key>, InternalError> {
        let (range_start_key, range_end_key) = plan
            .access
            .as_path()
            .and_then(AccessPath::pk_stream_bounds)
            .ok_or_else(|| {
                InternalError::query_executor_invariant(
                    "pk stream fast-path requires full-scan/key-range access path",
                )
            })?;

        Ok(PkStreamScanConfig {
            range_start_key,
            range_end_key,
        })
    }

    // Execute the store-range key streaming phase for the PK fast path.
    fn scan_pk_stream_keys(
        ctx: &Context<'_, E>,
        config: &PkStreamScanConfig<E::Key>,
        direction: Direction,
        probe_fetch_hint: Option<usize>,
    ) -> Result<PkStreamScanResult, InternalError> {
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
            let mut keys = Vec::new();
            let range = (lower_bound, Bound::Included(upper_raw));
            let fetch_cap = probe_fetch_hint.unwrap_or(usize::MAX);
            if fetch_cap == 0 {
                return Ok(PkStreamScanResult { keys, rows_scanned });
            }

            match direction {
                Direction::Asc => {
                    for entry in store.range(range) {
                        rows_scanned = rows_scanned.saturating_add(1);
                        let data_key = DataKey::try_from_raw(entry.key()).map_err(|err| {
                            InternalError::store_corruption(format!(
                                "ordered scan encountered corrupted data key: {err}"
                            ))
                        })?;
                        keys.push(data_key);
                        if keys.len() == fetch_cap {
                            break;
                        }
                    }
                }
                Direction::Desc => {
                    for entry in store.range(range).rev() {
                        rows_scanned = rows_scanned.saturating_add(1);
                        let data_key = DataKey::try_from_raw(entry.key()).map_err(|err| {
                            InternalError::store_corruption(format!(
                                "ordered scan encountered corrupted data key: {err}"
                            ))
                        })?;
                        keys.push(data_key);
                        if keys.len() == fetch_cap {
                            break;
                        }
                    }
                }
            }

            Ok(PkStreamScanResult { keys, rows_scanned })
        })?
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
        plan.order.as_ref().map_or(Direction::Asc, |order| {
            derive_scan_direction(order, SlotSelectionPolicy::First)
        })
    }
}
