use crate::{
    db::{
        data::{DataKey, StorageKey},
        direction::Direction,
        executor::stream::key::{
            BudgetedOrderedKeyStream, IntersectOrderedKeyStream, MergeOrderedKeyStream,
            OrderedKeyStream, VecOrderedKeyStream,
        },
        identity::EntityName,
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
};

fn data_key(value: u64) -> DataKey {
    let raw = DataKey::raw_from_parts(
        EntityName::try_from_str("ordered_key_stream_tests")
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
fn vec_ordered_key_stream_yields_keys_in_input_order() {
    let mut stream =
        VecOrderedKeyStream::new(vec![data_key(3), data_key(1), data_key(2), data_key(1)]);
    let mut out = Vec::new();

    while let Some(key) = stream.next_key().expect("stream next must succeed") {
        out.push(key);
    }

    assert_eq!(
        out,
        vec![data_key(3), data_key(1), data_key(2), data_key(1)]
    );
}

#[test]
fn vec_ordered_key_stream_returns_none_after_exhaustion() {
    let mut stream = VecOrderedKeyStream::new(vec![data_key(9)]);
    let first = stream.next_key().expect("first next must succeed");
    let second = stream.next_key().expect("second next must succeed");
    let third = stream.next_key().expect("third next must succeed");

    assert_eq!(first, Some(data_key(9)));
    assert_eq!(second, None);
    assert_eq!(third, None);
}

#[test]
fn vec_stream_reports_exact_key_count_hint() {
    let mut stream = VecOrderedKeyStream::new(vec![data_key(1), data_key(2)]);
    assert_eq!(
        stream.exact_key_count_hint(),
        Some(2),
        "vec stream must report exact total key count before consumption"
    );

    let _ = stream.next_key().expect("first next must succeed");
    assert_eq!(
        stream.exact_key_count_hint(),
        Some(2),
        "vec stream exact key-count hint must remain stable after consumption"
    );
}

#[test]
fn budgeted_stream_does_not_claim_exact_key_count_hint() {
    let inner = StaticOrderedKeyStream::new(vec![data_key(1), data_key(2)]);
    let stream = BudgetedOrderedKeyStream::new(inner, 1);

    assert_eq!(
        stream.exact_key_count_hint(),
        None,
        "budgeted stream must not claim exact counts when inner stream does not provide them"
    );
}

#[test]
fn budgeted_stream_reports_stable_total_hint_when_inner_is_exact() {
    let inner = VecOrderedKeyStream::new(vec![data_key(1), data_key(2), data_key(3)]);
    let mut stream = BudgetedOrderedKeyStream::new(inner, 2);

    assert_eq!(
        stream.exact_key_count_hint(),
        Some(2),
        "budgeted stream must report min(inner_total, budget) as exact total output"
    );

    let _ = stream.next_key().expect("first key should be available");
    assert_eq!(
        stream.exact_key_count_hint(),
        Some(2),
        "budgeted stream exact key-count hint must remain stable after consumption"
    );
}

#[test]
fn budgeted_stream_total_hint_is_min_of_inner_total_and_budget() {
    let inner = VecOrderedKeyStream::new(vec![data_key(1), data_key(2), data_key(3)]);
    let budget_limited = BudgetedOrderedKeyStream::new(inner, 2);
    assert_eq!(
        budget_limited.exact_key_count_hint(),
        Some(2),
        "budget-limited stream must report budget when budget is smaller than inner total"
    );

    let inner = VecOrderedKeyStream::new(vec![data_key(1), data_key(2), data_key(3)]);
    let inner_limited = BudgetedOrderedKeyStream::new(inner, 10);
    assert_eq!(
        inner_limited.exact_key_count_hint(),
        Some(3),
        "budget-limited stream must report inner total when budget exceeds inner total"
    );
}

#[test]
fn budgeted_stream_stops_after_budget_without_polling_inner() {
    let inner =
        StaticOrderedKeyStream::with_fail_at(vec![data_key(1), data_key(2), data_key(3)], 1);
    let mut stream = BudgetedOrderedKeyStream::new(inner, 1);

    assert_eq!(
        stream.next_key().expect("first key should be available"),
        Some(data_key(1))
    );
    assert_eq!(
        stream
            .next_key()
            .expect("exhausted budget should return None"),
        None
    );
    assert_eq!(
        stream
            .next_key()
            .expect("exhausted budget should keep returning None"),
        None
    );
}

#[test]
fn budgeted_stream_with_zero_budget_is_immediately_exhausted() {
    let inner = StaticOrderedKeyStream::with_fail_at(vec![data_key(1)], 0);
    let mut stream = BudgetedOrderedKeyStream::new(inner, 0);

    assert_eq!(
        stream
            .next_key()
            .expect("zero-budget stream should not poll inner"),
        None
    );
    assert_eq!(
        stream
            .next_key()
            .expect("zero-budget stream should stay exhausted"),
        None
    );
}

#[test]
fn merge_stream_asc_interleaves_two_ordered_streams() {
    let left = StaticOrderedKeyStream::new(vec![data_key(1), data_key(3), data_key(5)]);
    let right = StaticOrderedKeyStream::new(vec![data_key(2), data_key(4), data_key(6)]);
    let mut merged = MergeOrderedKeyStream::new(left, right, Direction::Asc);

    let out = collect_stream(&mut merged).expect("merge should succeed");
    assert_eq!(
        out,
        vec![
            data_key(1),
            data_key(2),
            data_key(3),
            data_key(4),
            data_key(5),
            data_key(6)
        ]
    );
}

#[test]
fn merge_stream_deduplicates_shared_keys() {
    let left = StaticOrderedKeyStream::new(vec![data_key(1), data_key(2), data_key(3)]);
    let right = StaticOrderedKeyStream::new(vec![data_key(2), data_key(3), data_key(4)]);
    let mut merged = MergeOrderedKeyStream::new(left, right, Direction::Asc);

    let out = collect_stream(&mut merged).expect("merge should succeed");
    assert_eq!(
        out,
        vec![data_key(1), data_key(2), data_key(3), data_key(4)]
    );
}

#[test]
fn merge_stream_equal_key_discard_path_remains_stable() {
    let left = StaticOrderedKeyStream::new(vec![
        data_key(1),
        data_key(2),
        data_key(3),
        data_key(4),
        data_key(5),
    ]);
    let right = StaticOrderedKeyStream::new(vec![
        data_key(1),
        data_key(2),
        data_key(3),
        data_key(4),
        data_key(5),
    ]);
    let mut merged = MergeOrderedKeyStream::new(left, right, Direction::Asc);

    let out = collect_stream(&mut merged).expect("merge should succeed");
    assert_eq!(
        out,
        vec![
            data_key(1),
            data_key(2),
            data_key(3),
            data_key(4),
            data_key(5)
        ],
        "merge output should remain stable when repeatedly discarding equal right-side lookahead"
    );
}

#[test]
fn merge_stream_desc_interleaves_two_descending_streams() {
    let left = StaticOrderedKeyStream::new(vec![data_key(6), data_key(4), data_key(2)]);
    let right = StaticOrderedKeyStream::new(vec![data_key(5), data_key(3), data_key(1)]);
    let mut merged = MergeOrderedKeyStream::new(left, right, Direction::Desc);

    let out = collect_stream(&mut merged).expect("merge should succeed");
    assert_eq!(
        out,
        vec![
            data_key(6),
            data_key(5),
            data_key(4),
            data_key(3),
            data_key(2),
            data_key(1)
        ]
    );
}

#[test]
fn merge_stream_propagates_underlying_errors() {
    let left = StaticOrderedKeyStream::with_fail_at(vec![data_key(1), data_key(3)], 1);
    let right = StaticOrderedKeyStream::new(vec![data_key(2), data_key(4)]);
    let mut merged = MergeOrderedKeyStream::new(left, right, Direction::Asc);

    let err = collect_stream(&mut merged).expect_err("merge should fail");
    assert_eq!(err.class, ErrorClass::Internal);
    assert_eq!(err.origin, ErrorOrigin::Query);
}

#[test]
fn merge_stream_rejects_child_direction_mismatch() {
    let left = StaticOrderedKeyStream::new(vec![data_key(1), data_key(2)]);
    let right = StaticOrderedKeyStream::new(vec![data_key(3), data_key(4)]);
    let mut merged = MergeOrderedKeyStream::new(left, right, Direction::Desc);

    let err = collect_stream(&mut merged).expect_err("merge should fail on direction mismatch");
    assert_eq!(err.class, ErrorClass::InvariantViolation);
    assert_eq!(err.origin, ErrorOrigin::Query);
}

#[test]
fn intersect_stream_asc_yields_shared_keys() {
    let left = StaticOrderedKeyStream::new(vec![data_key(1), data_key(3), data_key(5)]);
    let right = StaticOrderedKeyStream::new(vec![data_key(3), data_key(4), data_key(5)]);
    let mut intersected = IntersectOrderedKeyStream::new(left, right, Direction::Asc);

    let out = collect_stream(&mut intersected).expect("intersection should succeed");
    assert_eq!(out, vec![data_key(3), data_key(5)]);
}

#[test]
fn intersect_stream_desc_yields_shared_keys() {
    let left = StaticOrderedKeyStream::new(vec![data_key(5), data_key(3), data_key(1)]);
    let right = StaticOrderedKeyStream::new(vec![data_key(6), data_key(5), data_key(3)]);
    let mut intersected = IntersectOrderedKeyStream::new(left, right, Direction::Desc);

    let out = collect_stream(&mut intersected).expect("intersection should succeed");
    assert_eq!(out, vec![data_key(5), data_key(3)]);
}

#[test]
fn intersect_stream_returns_empty_when_no_overlap() {
    let left = StaticOrderedKeyStream::new(vec![data_key(1), data_key(2)]);
    let right = StaticOrderedKeyStream::new(vec![data_key(3), data_key(4)]);
    let mut intersected = IntersectOrderedKeyStream::new(left, right, Direction::Asc);

    let out = collect_stream(&mut intersected).expect("intersection should succeed");
    assert!(out.is_empty());
}

#[test]
fn intersect_stream_deduplicates_internal_duplicates() {
    let left = StaticOrderedKeyStream::new(vec![
        data_key(1),
        data_key(1),
        data_key(2),
        data_key(3),
        data_key(3),
    ]);
    let right = StaticOrderedKeyStream::new(vec![data_key(1), data_key(2), data_key(3)]);
    let mut intersected = IntersectOrderedKeyStream::new(left, right, Direction::Asc);

    let out = collect_stream(&mut intersected).expect("intersection should succeed");
    assert_eq!(out, vec![data_key(1), data_key(2), data_key(3)]);
}

#[test]
fn intersect_stream_deduplicates_when_both_sides_duplicate() {
    let left =
        StaticOrderedKeyStream::new(vec![data_key(1), data_key(1), data_key(2), data_key(3)]);
    let right =
        StaticOrderedKeyStream::new(vec![data_key(1), data_key(1), data_key(2), data_key(3)]);
    let mut intersected = IntersectOrderedKeyStream::new(left, right, Direction::Asc);

    let out = collect_stream(&mut intersected).expect("intersection should succeed");
    assert_eq!(out, vec![data_key(1), data_key(2), data_key(3)]);
}

#[test]
fn intersect_stream_propagates_underlying_errors() {
    let left = StaticOrderedKeyStream::with_fail_at(vec![data_key(1), data_key(3)], 1);
    let right = StaticOrderedKeyStream::new(vec![data_key(1), data_key(3)]);
    let mut intersected = IntersectOrderedKeyStream::new(left, right, Direction::Asc);

    let err = collect_stream(&mut intersected).expect_err("intersection should fail");
    assert_eq!(err.class, ErrorClass::Internal);
    assert_eq!(err.origin, ErrorOrigin::Query);
}

#[test]
fn intersect_stream_rejects_child_direction_mismatch() {
    let left = StaticOrderedKeyStream::new(vec![data_key(1), data_key(2)]);
    let right = StaticOrderedKeyStream::new(vec![data_key(3), data_key(4)]);
    let mut intersected = IntersectOrderedKeyStream::new(left, right, Direction::Desc);

    let err = collect_stream(&mut intersected).expect_err("intersection should fail on mismatch");
    assert_eq!(err.class, ErrorClass::InvariantViolation);
    assert_eq!(err.origin, ErrorOrigin::Query);
}

#[test]
fn intersect_stream_rejects_non_monotonic_child_sequence() {
    let left = StaticOrderedKeyStream::new(vec![data_key(1), data_key(3), data_key(2)]);
    let right = StaticOrderedKeyStream::new(vec![data_key(1), data_key(2), data_key(3)]);
    let mut intersected = IntersectOrderedKeyStream::new(left, right, Direction::Asc);

    let err = collect_stream(&mut intersected)
        .expect_err("intersection should fail when child emits non-monotonic keys");
    assert_eq!(err.class, ErrorClass::InvariantViolation);
    assert_eq!(err.origin, ErrorOrigin::Query);
}

#[test]
fn intersect_stream_rejects_non_monotonic_child_sequence_when_discard_updates_witness() {
    let left = StaticOrderedKeyStream::new(vec![data_key(1), data_key(3), data_key(2)]);
    let right = StaticOrderedKeyStream::new(vec![data_key(10), data_key(10), data_key(10)]);
    let mut intersected = IntersectOrderedKeyStream::new(left, right, Direction::Asc);

    let err = collect_stream(&mut intersected).expect_err(
        "intersection should fail when repeated discard advances are followed by non-monotonic keys",
    );
    assert_eq!(err.class, ErrorClass::InvariantViolation);
    assert_eq!(err.origin, ErrorOrigin::Query);
}
