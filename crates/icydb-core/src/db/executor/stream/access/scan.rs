//! Module: executor::stream::access::scan
//! Responsibility: low-level primary/index scan adapters over store/index handles.
//! Does not own: access routing decisions or planner spec construction.
//! Boundary: direct scan primitives used by access-stream resolver.

use crate::{
    db::{
        cursor::IndexScanContinuationInput,
        data::{DataKey, RawDataKey},
        direction::Direction,
        executor::{LoweredIndexPrefixSpec, LoweredIndexRangeSpec, LoweredKey},
        index::{IndexDataKeyScanChunk, predicate::IndexPredicateExecution},
        registry::StoreHandle,
    },
    error::InternalError,
    model::index::IndexModel,
    types::EntityTag,
};
use std::ops::Bound;

///
/// PrimaryScan
///
/// Executor-owned adapter for primary data-store iteration.
/// The physical stream resolver must request scans through this boundary instead of
/// traversing store handles directly.
///

pub(in crate::db::executor) struct PrimaryScan;

impl PrimaryScan {
    // Decode one raw data key through the canonical corruption mapping.
    pub(in crate::db::executor) fn decode_data_key(
        raw: &RawDataKey,
    ) -> Result<DataKey, InternalError> {
        DataKey::try_from_raw(raw).map_err(|err| {
            InternalError::identity_corruption(format!("failed to decode data key: {err}"))
        })
    }
}

///
/// IndexScan
///
/// Executor-owned adapter for secondary-index iteration.
/// The physical stream resolver must request index traversal via this adapter so routing
/// stays decoupled from store-registry/index-handle internals.
///

pub(in crate::db::executor) struct IndexScan;

impl IndexScan {
    /// Resolve one lowered index-prefix envelope through structural store authority.
    pub(in crate::db::executor) fn prefix_structural(
        store: StoreHandle,
        entity_tag: EntityTag,
        spec: &LoweredIndexPrefixSpec,
        direction: Direction,
        limit: usize,
        predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<Vec<DataKey>, InternalError> {
        Self::resolve_limited(
            store,
            entity_tag,
            spec.index(),
            spec.lower(),
            spec.upper(),
            IndexScanContinuationInput::new(None, direction),
            limit,
            predicate_execution,
        )
    }

    /// Resolve one lowered index-range envelope through structural store authority.
    pub(in crate::db::executor) fn range_structural(
        store: StoreHandle,
        entity_tag: EntityTag,
        spec: &LoweredIndexRangeSpec,
        continuation: IndexScanContinuationInput<'_>,
        limit: usize,
        predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<Vec<DataKey>, InternalError> {
        Self::resolve_limited(
            store,
            entity_tag,
            spec.index(),
            spec.lower(),
            spec.upper(),
            continuation,
            limit,
            predicate_execution,
        )
    }

    /// Resolve one bounded lowered-index chunk through structural store authority.
    #[expect(clippy::too_many_arguments)]
    pub(in crate::db::executor) fn chunk_structural(
        store: StoreHandle,
        entity_tag: EntityTag,
        index: &IndexModel,
        lower: &Bound<LoweredKey>,
        upper: &Bound<LoweredKey>,
        continuation: IndexScanContinuationInput<'_>,
        max_entries: usize,
        output_limit: Option<usize>,
    ) -> Result<IndexDataKeyScanChunk, InternalError> {
        Self::resolve_chunk(
            store,
            entity_tag,
            index,
            lower,
            upper,
            continuation,
            max_entries,
            output_limit,
        )
    }

    // Resolve one index range via store registry and index-store iterator boundary.
    #[expect(clippy::too_many_arguments)]
    fn resolve_limited(
        store: StoreHandle,
        entity_tag: EntityTag,
        index: &IndexModel,
        lower: &Bound<LoweredKey>,
        upper: &Bound<LoweredKey>,
        continuation: IndexScanContinuationInput<'_>,
        limit: usize,
        predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<Vec<DataKey>, InternalError> {
        let keys = store.with_index(|index_store| {
            index_store.resolve_data_values_in_raw_range_limited(
                entity_tag,
                index,
                (lower, upper),
                continuation,
                limit,
                predicate_execution,
            )
        })?;

        Ok(keys)
    }

    // Resolve one index range chunk via store registry and index-store iterator boundary.
    #[expect(clippy::too_many_arguments)]
    fn resolve_chunk(
        store: StoreHandle,
        entity_tag: EntityTag,
        index: &IndexModel,
        lower: &Bound<LoweredKey>,
        upper: &Bound<LoweredKey>,
        continuation: IndexScanContinuationInput<'_>,
        max_entries: usize,
        output_limit: Option<usize>,
    ) -> Result<IndexDataKeyScanChunk, InternalError> {
        let chunk = store.with_index(|index_store| {
            index_store.resolve_data_values_in_raw_range_chunk(
                entity_tag,
                index,
                (lower, upper),
                continuation,
                max_entries,
                output_limit,
            )
        })?;

        Ok(chunk)
    }
}
