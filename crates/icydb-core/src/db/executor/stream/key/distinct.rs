//! Module: executor::stream::key::distinct
//! Responsibility: DISTINCT-adapter over ordered key streams.
//! Does not own: upstream key generation or route eligibility policy.
//! Boundary: enforces monotonicity and suppresses adjacent duplicate keys.

use crate::{
    db::{
        data::{DecodedDataStoreKey, RawRow, StoreVisit},
        executor::{
            OrderedKeyStreamBox,
            stream::key::{KeyOrderComparator, OrderedKeyStream},
        },
    },
    error::InternalError,
};

///
/// DistinctOrderedKeyStream
///
/// Ordered-key stream adapter that suppresses adjacent duplicate `DecodedDataStoreKey`
/// values while preserving upstream monotonic key order invariants.
///

pub(in crate::db::executor) struct DistinctOrderedKeyStream<S> {
    inner: S,
    last_emitted: Option<DecodedDataStoreKey>,
    comparator: KeyOrderComparator,
}

impl<S> DistinctOrderedKeyStream<S> {
    /// Construct one distinct stream adapter.
    #[must_use]
    pub(in crate::db::executor) const fn new(inner: S, comparator: KeyOrderComparator) -> Self {
        Self {
            inner,
            last_emitted: None,
            comparator,
        }
    }
}

impl DistinctOrderedKeyStream<Box<OrderedKeyStreamBox>> {
    // Preserve DISTINCT monotonicity and progress while transparently
    // forwarding an eligible primary-row visitor to the wrapped leaf.
    pub(in crate::db::executor) fn try_visit_primary_rows_direct(
        &mut self,
        begin_row: &mut dyn FnMut() -> Result<bool, InternalError>,
        visit_row: &mut dyn for<'row> FnMut(
            DecodedDataStoreKey,
            &'row RawRow,
        ) -> Result<StoreVisit, InternalError>,
    ) -> Result<Option<()>, InternalError> {
        // Direct visitation is exposed only by one ASC primary-map leaf. Its
        // keys are already strictly ordered and unique, and the leaf advances
        // its exclusive lower bound even when the visitor stops early. The
        // adapter therefore has no duplicate state to maintain on this route.
        self.inner
            .try_visit_primary_rows_direct(begin_row, visit_row)
    }
}

impl<S> OrderedKeyStream for DistinctOrderedKeyStream<S>
where
    S: OrderedKeyStream,
{
    fn next_key(&mut self) -> Result<Option<DecodedDataStoreKey>, InternalError> {
        loop {
            let Some(next) = self.inner.next_key()? else {
                return Ok(None);
            };

            if let Some(last) = self.last_emitted.as_ref() {
                // Keep ordering and equality semantics split:
                // - ordering comparator enforces monotonic stream contract
                // - exact key equality controls DISTINCT suppression
                if self.comparator.compare_data_keys(last, &next).is_gt() {
                    return Err(InternalError::query_executor_invariant());
                }
                if last == &next {
                    continue;
                }
            }

            self.last_emitted = Some(next.clone());

            return Ok(Some(next));
        }
    }
}
