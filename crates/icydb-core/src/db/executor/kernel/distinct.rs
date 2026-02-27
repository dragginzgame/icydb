use crate::{
    db::{
        data::DataKey,
        direction::Direction,
        executor::{
            KeyOrderComparator, OrderedKeyStream, OrderedKeyStreamBox,
            load::{ResolvedExecutionKeyStream, key_stream_comparator_from_direction},
        },
        query::plan::AccessPlannedQuery,
    },
    error::InternalError,
};
use std::{cell::Cell, rc::Rc};

///
/// DistinctOrderedKeyStream
///
/// Kernel-local ordered stream wrapper that suppresses adjacent duplicate keys.
/// Row DISTINCT semantics are identity-based at this boundary:
/// duplicates are defined as identical `DataKey` values (entity + primary key).
/// Correct DISTINCT requires contiguous equal keys in the upstream stream order.
///

pub(super) struct DistinctOrderedKeyStream<S> {
    inner: S,
    last_emitted: Option<DataKey>,
    comparator: KeyOrderComparator,
    deduped_keys_counter: Option<Rc<Cell<u64>>>,
}

impl<S> DistinctOrderedKeyStream<S> {
    #[must_use]
    pub(super) const fn new(inner: S, comparator: KeyOrderComparator) -> Self {
        Self {
            inner,
            last_emitted: None,
            comparator,
            deduped_keys_counter: None,
        }
    }

    #[must_use]
    pub(super) const fn new_with_dedup_counter(
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

fn wrap_distinct_ordered_key_stream(
    ordered_key_stream: OrderedKeyStreamBox,
    distinct: bool,
    key_comparator: KeyOrderComparator,
    dedup_counter: Option<Rc<Cell<u64>>>,
) -> (OrderedKeyStreamBox, Option<Rc<Cell<u64>>>) {
    if !distinct {
        return (ordered_key_stream, None);
    }

    if let Some(counter) = dedup_counter {
        let wrapped = Box::new(DistinctOrderedKeyStream::new_with_dedup_counter(
            ordered_key_stream,
            key_comparator,
            counter.clone(),
        ));
        return (wrapped, Some(counter));
    }

    (
        Box::new(DistinctOrderedKeyStream::new(
            ordered_key_stream,
            key_comparator,
        )),
        None,
    )
}

pub(super) fn decorate_resolved_execution_key_stream<K>(
    mut resolved: ResolvedExecutionKeyStream,
    plan: &AccessPlannedQuery<K>,
    direction: Direction,
) -> ResolvedExecutionKeyStream {
    let key_comparator = key_stream_comparator_from_direction(direction);
    let distinct = plan.scalar_plan().distinct;
    let dedup_counter = distinct.then(|| Rc::new(Cell::new(0u64)));
    let (key_stream, dedup_counter) = wrap_distinct_ordered_key_stream(
        resolved.key_stream,
        distinct,
        key_comparator,
        dedup_counter,
    );
    resolved.key_stream = key_stream;
    resolved.distinct_keys_deduped_counter = dedup_counter;

    resolved
}

pub(in crate::db::executor) fn decorate_key_stream_for_plan<K>(
    ordered_key_stream: OrderedKeyStreamBox,
    plan: &AccessPlannedQuery<K>,
    direction: Direction,
) -> OrderedKeyStreamBox {
    let key_comparator = key_stream_comparator_from_direction(direction);

    wrap_distinct_ordered_key_stream(
        ordered_key_stream,
        plan.scalar_plan().distinct,
        key_comparator,
        None,
    )
    .0
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::{
            data::{DataKey, StorageKey},
            direction::Direction,
            executor::{
                KeyOrderComparator, OrderedKeyStream, VecOrderedKeyStream,
                kernel::distinct::DistinctOrderedKeyStream,
            },
            identity::EntityName,
        },
        error::{ErrorClass, ErrorOrigin, InternalError},
    };
    use std::{cell::Cell, rc::Rc};

    fn data_key(value: u64) -> DataKey {
        let raw = DataKey::raw_from_parts(
            EntityName::try_from_str("kernel_distinct_tests")
                .expect("test entity name should be valid"),
            StorageKey::Uint(value),
        )
        .expect("test key encoding should succeed");

        DataKey::try_from_raw(&raw).expect("test key decode should succeed")
    }

    struct StaticOrderedKeyStream {
        keys: Vec<DataKey>,
        index: usize,
        fail_at: Option<usize>,
    }

    impl StaticOrderedKeyStream {
        fn new(keys: Vec<DataKey>) -> Self {
            Self {
                keys,
                index: 0,
                fail_at: None,
            }
        }

        fn with_fail_at(keys: Vec<DataKey>, fail_at: usize) -> Self {
            Self {
                keys,
                index: 0,
                fail_at: Some(fail_at),
            }
        }
    }

    impl OrderedKeyStream for StaticOrderedKeyStream {
        fn next_key(&mut self) -> Result<Option<DataKey>, InternalError> {
            if self.fail_at.is_some_and(|idx| self.index == idx) {
                return Err(InternalError::query_internal("forced stream failure"));
            }
            if self.index >= self.keys.len() {
                return Ok(None);
            }

            let key = self.keys[self.index].clone();
            self.index = self.index.saturating_add(1);

            Ok(Some(key))
        }
    }

    fn collect_stream(stream: &mut impl OrderedKeyStream) -> Result<Vec<DataKey>, InternalError> {
        let mut out = Vec::new();
        while let Some(key) = stream.next_key()? {
            out.push(key);
        }

        Ok(out)
    }

    #[test]
    fn distinct_stream_suppresses_consecutive_duplicates() {
        let inner = StaticOrderedKeyStream::new(vec![
            data_key(1),
            data_key(1),
            data_key(2),
            data_key(2),
            data_key(2),
            data_key(3),
        ]);
        let mut stream = DistinctOrderedKeyStream::new(
            inner,
            KeyOrderComparator::from_direction(Direction::Asc),
        );

        let out = collect_stream(&mut stream).expect("distinct stream should succeed");
        assert_eq!(out, vec![data_key(1), data_key(2), data_key(3)]);
    }

    #[test]
    fn distinct_stream_identity_equality_never_emits_same_datakey_twice() {
        let inner = StaticOrderedKeyStream::new(vec![data_key(7), data_key(7), data_key(7)]);
        let dedup_counter = Rc::new(Cell::new(0u64));
        let mut stream = DistinctOrderedKeyStream::new_with_dedup_counter(
            inner,
            KeyOrderComparator::from_direction(Direction::Asc),
            dedup_counter.clone(),
        );

        let out = collect_stream(&mut stream).expect("distinct stream should succeed");
        assert_eq!(
            out,
            vec![data_key(7)],
            "identical DataKeys must collapse to one row under kernel row DISTINCT",
        );
        assert_eq!(
            dedup_counter.get(),
            2,
            "every repeated identical DataKey should be counted as deduped",
        );
    }

    #[test]
    fn distinct_stream_records_deduped_key_count() {
        let inner = StaticOrderedKeyStream::new(vec![
            data_key(1),
            data_key(1),
            data_key(2),
            data_key(2),
            data_key(2),
            data_key(3),
        ]);
        let dedup_counter = Rc::new(Cell::new(0u64));
        let mut stream = DistinctOrderedKeyStream::new_with_dedup_counter(
            inner,
            KeyOrderComparator::from_direction(Direction::Asc),
            dedup_counter.clone(),
        );

        let out = collect_stream(&mut stream).expect("distinct stream should succeed");
        assert_eq!(out, vec![data_key(1), data_key(2), data_key(3)]);
        assert_eq!(
            dedup_counter.get(),
            3,
            "dedup counter should include every suppressed adjacent duplicate key"
        );
    }

    #[test]
    fn distinct_stream_propagates_underlying_errors() {
        let inner = StaticOrderedKeyStream::with_fail_at(vec![data_key(1), data_key(1)], 1);
        let mut stream = DistinctOrderedKeyStream::new(
            inner,
            KeyOrderComparator::from_direction(Direction::Asc),
        );

        let err = collect_stream(&mut stream).expect_err("distinct stream should fail");
        assert_eq!(err.class, ErrorClass::Internal);
        assert_eq!(err.origin, ErrorOrigin::Query);
    }

    #[test]
    fn distinct_stream_rejects_non_monotonic_keys_for_both_directions() {
        for (direction, values) in [
            (Direction::Asc, vec![data_key(1), data_key(3), data_key(2)]),
            (Direction::Desc, vec![data_key(3), data_key(1), data_key(2)]),
        ] {
            let inner = StaticOrderedKeyStream::new(values);
            let mut stream =
                DistinctOrderedKeyStream::new(inner, KeyOrderComparator::from_direction(direction));

            let err = collect_stream(&mut stream)
                .expect_err("non-monotonic distinct stream input should be rejected");
            assert_eq!(
                err.class,
                ErrorClass::InvariantViolation,
                "distinct monotonicity failures must classify as invariant violations"
            );
            assert!(
                err.message.contains("non-monotonic key order"),
                "distinct monotonicity failure should expose a clear invariant reason"
            );
        }
    }

    #[test]
    fn distinct_stream_accepts_exact_hint_from_inner_stream() {
        let inner = VecOrderedKeyStream::new(vec![data_key(1), data_key(1), data_key(2)]);
        let stream = DistinctOrderedKeyStream::new(
            inner,
            KeyOrderComparator::from_direction(Direction::Asc),
        );

        assert_eq!(
            stream.exact_key_count_hint(),
            None,
            "distinct stream should preserve unknown exact-count semantics"
        );
    }
}
