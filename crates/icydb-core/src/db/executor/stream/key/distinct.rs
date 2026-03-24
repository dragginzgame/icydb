//! Module: executor::stream::key::distinct
//! Responsibility: DISTINCT-adapter over ordered key streams.
//! Does not own: upstream key generation or route eligibility policy.
//! Boundary: enforces monotonicity and suppresses adjacent duplicate keys.

use crate::{
    db::{
        data::DataKey,
        executor::stream::key::{KeyOrderComparator, OrderedKeyStream},
    },
    error::InternalError,
};
use std::{cell::Cell, rc::Rc};

///
/// DistinctOrderedKeyStream
///
/// Ordered-key stream adapter that suppresses adjacent duplicate `DataKey`
/// values while preserving upstream monotonic key order invariants.
///

pub(in crate::db::executor) struct DistinctOrderedKeyStream<S> {
    inner: S,
    last_emitted: Option<DataKey>,
    comparator: KeyOrderComparator,
    deduped_keys_counter: Option<Rc<Cell<u64>>>,
}

impl<S> DistinctOrderedKeyStream<S> {
    /// Construct one distinct stream adapter.
    #[must_use]
    pub(in crate::db::executor) const fn new(inner: S, comparator: KeyOrderComparator) -> Self {
        Self {
            inner,
            last_emitted: None,
            comparator,
            deduped_keys_counter: None,
        }
    }

    /// Construct one distinct stream adapter with external dedup observability counter.
    #[must_use]
    pub(in crate::db::executor) const fn new_with_dedup_counter(
        inner: S,
        comparator: KeyOrderComparator,
        deduped_keys_counter: Rc<Cell<u64>>,
    ) -> Self {
        Self {
            inner,
            last_emitted: None,
            comparator,
            deduped_keys_counter: Some(deduped_keys_counter),
        }
    }
}

impl<S> OrderedKeyStream for DistinctOrderedKeyStream<S>
where
    S: OrderedKeyStream,
{
    fn next_key(&mut self) -> Result<Option<DataKey>, InternalError> {
        loop {
            let Some(next) = self.inner.next_key()? else {
                return Ok(None);
            };

            if let Some(last) = self.last_emitted.as_ref() {
                // Keep ordering and equality semantics split:
                // - ordering comparator enforces monotonic stream contract
                // - exact key equality controls DISTINCT suppression
                if self.comparator.compare_data_keys(last, &next).is_gt() {
                    return Err(InternalError::query_executor_invariant(
                        "distinct ordered stream received non-monotonic key order",
                    ));
                }
                if last == &next {
                    if let Some(counter) = self.deduped_keys_counter.as_ref() {
                        counter.set(counter.get().saturating_add(1));
                    }
                    continue;
                }
            }

            self.last_emitted = Some(next.clone());

            return Ok(Some(next));
        }
    }
}
