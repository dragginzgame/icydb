use crate::{
    db::{
        data::{DecodedDataStoreKey, PrimaryKeyComponent},
        direction::Direction,
        executor::stream::key::{
            HeldHeadKeyStream, HeldHeadSeekOutcome, HeldHeadSeekWork, KeyOrderComparator,
            OrderedKeyStream, OrderedKeyStreamBox, RepeatedPullHeldHeadKeyStream,
        },
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    types::EntityTag,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource, DiagnosticExecutionBudgetScope, DiagnosticExecutionLane,
};
use std::{cell::Cell, rc::Rc};

fn data_key(value: u64) -> DecodedDataStoreKey {
    entity_data_key(1, value)
}

fn entity_data_key(entity: u64, value: u64) -> DecodedDataStoreKey {
    let raw = DecodedDataStoreKey::new(
        EntityTag::new(entity),
        &PrimaryKeyComponent::Nat64(value).into(),
    )
    .to_raw()
    .expect("test key encoding should succeed");

    DecodedDataStoreKey::try_from_raw(&raw).expect("test key decode should succeed")
}

#[derive(Clone, Copy)]
enum ForcedFailure {
    HardBudget,
    Corruption,
}

struct StaticOrderedKeyStream {
    keys: Vec<DecodedDataStoreKey>,
    index: usize,
    fail_at: Option<(usize, ForcedFailure)>,
    pull_attempts: Rc<Cell<u64>>,
}

impl StaticOrderedKeyStream {
    fn new(keys: Vec<DecodedDataStoreKey>) -> Self {
        Self {
            keys,
            index: 0,
            fail_at: None,
            pull_attempts: Rc::new(Cell::new(0)),
        }
    }

    fn observed(keys: Vec<DecodedDataStoreKey>) -> (Self, Rc<Cell<u64>>) {
        let pull_attempts = Rc::new(Cell::new(0));
        (
            Self {
                keys,
                index: 0,
                fail_at: None,
                pull_attempts: Rc::clone(&pull_attempts),
            },
            pull_attempts,
        )
    }

    fn with_failure(
        keys: Vec<DecodedDataStoreKey>,
        fail_at: usize,
        failure: ForcedFailure,
    ) -> Self {
        Self {
            keys,
            index: 0,
            fail_at: Some((fail_at, failure)),
            pull_attempts: Rc::new(Cell::new(0)),
        }
    }
}

impl OrderedKeyStream for StaticOrderedKeyStream {
    fn next_key(&mut self) -> Result<Option<DecodedDataStoreKey>, InternalError> {
        self.pull_attempts
            .set(self.pull_attempts.get().saturating_add(1));
        if let Some((fail_at, failure)) = self.fail_at
            && self.index == fail_at
        {
            return Err(match failure {
                ForcedFailure::HardBudget => InternalError::execution_budget_exceeded(
                    DiagnosticExecutionBudgetResource::CursorSteps,
                    1,
                    2,
                    DiagnosticExecutionBudgetScope::Execution,
                    DiagnosticExecutionLane::TrustedRead,
                    0,
                ),
                ForcedFailure::Corruption => InternalError::executor_invariant(),
            });
        }
        if self.index >= self.keys.len() {
            return Ok(None);
        }

        let key = self.keys[self.index].clone();
        self.index = self.index.saturating_add(1);
        Ok(Some(key))
    }
}

fn reference_stream(
    values: &[u64],
    direction: Direction,
) -> RepeatedPullHeldHeadKeyStream<StaticOrderedKeyStream> {
    RepeatedPullHeldHeadKeyStream::new(
        StaticOrderedKeyStream::new(values.iter().copied().map(data_key).collect()),
        KeyOrderComparator::from_direction(direction),
    )
}

fn assert_held(outcome: HeldHeadSeekOutcome<'_>, expected: u64) {
    let HeldHeadSeekOutcome::Held(key) = outcome else {
        panic!("expected held head")
    };
    assert_eq!(key, &data_key(expected));
}

fn collect_remaining<S>(
    stream: &mut RepeatedPullHeldHeadKeyStream<S>,
    work: &mut HeldHeadSeekWork,
) -> Result<Vec<DecodedDataStoreKey>, InternalError>
where
    S: OrderedKeyStream,
{
    let mut keys = Vec::new();
    loop {
        match stream.ensure_head(work)? {
            HeldHeadSeekOutcome::Held(_) => keys.push(
                stream
                    .consume_head(work)?
                    .ok_or_else(InternalError::executor_invariant)?,
            ),
            HeldHeadSeekOutcome::Exhausted => return Ok(keys),
            HeldHeadSeekOutcome::PageStop => return Err(InternalError::executor_invariant()),
        }
    }
}

fn next_key_with_work<S>(
    stream: &mut RepeatedPullHeldHeadKeyStream<S>,
    work: &mut HeldHeadSeekWork,
) -> Result<Option<DecodedDataStoreKey>, InternalError>
where
    S: OrderedKeyStream,
{
    match stream.ensure_head(work)? {
        HeldHeadSeekOutcome::Held(_) => stream.consume_head(work),
        HeldHeadSeekOutcome::Exhausted => Ok(None),
        HeldHeadSeekOutcome::PageStop => Err(InternalError::executor_invariant()),
    }
}

#[test]
fn concrete_stream_envelope_exposes_the_reference_protocol() {
    let stream = OrderedKeyStreamBox::materialized(vec![data_key(2), data_key(4)]);
    let mut held = RepeatedPullHeldHeadKeyStream::new(
        stream,
        KeyOrderComparator::from_direction(Direction::Asc),
    );
    let mut work = HeldHeadSeekWork::unbounded();

    assert_held(
        held.seek_head_at_or_after(&data_key(3), &mut work)
            .expect("concrete stream reference seek should succeed"),
        4,
    );
    assert_eq!(work.skipped_occurrences(), 1);
}

#[test]
fn asc_seek_keeps_behind_and_equal_targets_then_stops_before_repull() {
    let mut stream = reference_stream(&[20, 30, 40], Direction::Asc);
    let mut work = HeldHeadSeekWork::with_pull_attempt_limit(1);

    assert_held(
        stream.ensure_head(&mut work).expect("ensure should hold"),
        20,
    );
    assert_held(
        stream
            .seek_head_at_or_after(&data_key(10), &mut work)
            .expect("behind target should hold"),
        20,
    );
    assert_held(
        stream
            .seek_head_at_or_after(&data_key(20), &mut work)
            .expect("equal target should hold"),
        20,
    );
    assert_eq!(
        stream
            .seek_head_at_or_after(&data_key(30), &mut work)
            .expect("envelope stop should succeed"),
        HeldHeadSeekOutcome::PageStop,
    );
    assert_eq!(work.pull_attempts(), 1);
    assert_eq!(work.skipped_occurrences(), 1);
    assert_eq!(work.consumed_occurrences(), 1);

    let mut resumed_work = HeldHeadSeekWork::unbounded();
    assert_held(
        stream
            .seek_head_at_or_after(&data_key(30), &mut resumed_work)
            .expect("resume should hold target"),
        30,
    );
    assert_eq!(
        stream
            .consume_head(&mut resumed_work)
            .expect("held target should consume"),
        Some(data_key(30)),
    );
    assert_eq!(
        stream
            .consume_head(&mut resumed_work)
            .expect("unpositioned consume should succeed"),
        None,
    );
}

#[test]
fn desc_seek_uses_traversal_order_instead_of_numeric_greatest() {
    let mut stream = reference_stream(&[20, 10, 0], Direction::Desc);
    let mut work = HeldHeadSeekWork::with_pull_attempt_limit(1);

    assert_held(
        stream.ensure_head(&mut work).expect("ensure should hold"),
        20,
    );
    assert_held(
        stream
            .seek_head_at_or_after(&data_key(30), &mut work)
            .expect("behind DESC target should hold"),
        20,
    );
    assert_held(
        stream
            .seek_head_at_or_after(&data_key(20), &mut work)
            .expect("equal DESC target should hold"),
        20,
    );
    assert_eq!(
        stream
            .seek_head_at_or_after(&data_key(10), &mut work)
            .expect("DESC envelope stop should succeed"),
        HeldHeadSeekOutcome::PageStop,
    );

    let mut resumed_work = HeldHeadSeekWork::unbounded();
    assert_held(
        stream
            .seek_head_at_or_after(&data_key(10), &mut resumed_work)
            .expect("DESC resume should hold target"),
        10,
    );
}

#[test]
fn duplicate_occurrences_remain_distinct_until_consumed() {
    let mut stream = reference_stream(&[1, 1, 2], Direction::Asc);
    let mut work = HeldHeadSeekWork::unbounded();

    for expected in [1, 1, 2] {
        assert_held(
            stream
                .seek_head_at_or_after(&data_key(expected), &mut work)
                .expect("duplicate seek should hold"),
            expected,
        );
        assert_eq!(
            stream
                .consume_head(&mut work)
                .expect("duplicate head should consume"),
            Some(data_key(expected)),
        );
    }

    assert_eq!(work.skipped_occurrences(), 0);
    assert_eq!(work.consumed_occurrences(), 3);
    assert_eq!(
        stream
            .ensure_head(&mut work)
            .expect("stream should exhaust"),
        HeldHeadSeekOutcome::Exhausted,
    );
}

#[test]
fn page_stop_preserves_monotonic_progress_and_reconciles_pull_attempts() {
    let (inner, observed_pulls) =
        StaticOrderedKeyStream::observed(vec![data_key(1), data_key(3), data_key(5)]);
    let mut stream = RepeatedPullHeldHeadKeyStream::new(
        inner,
        KeyOrderComparator::from_direction(Direction::Asc),
    );
    let mut first_page = HeldHeadSeekWork::with_pull_attempt_limit(2);

    assert_eq!(
        stream
            .seek_head_at_or_after(&data_key(5), &mut first_page)
            .expect("first page should stop successfully"),
        HeldHeadSeekOutcome::PageStop,
    );
    assert_eq!(first_page.pull_attempts(), 2);
    assert_eq!(first_page.skipped_occurrences(), 2);
    assert_eq!(first_page.consumed_occurrences(), 2);
    assert_eq!(observed_pulls.get(), first_page.pull_attempts());

    let mut second_page = HeldHeadSeekWork::with_pull_attempt_limit(1);
    assert_held(
        stream
            .seek_head_at_or_after(&data_key(5), &mut second_page)
            .expect("second page should hold target"),
        5,
    );
    assert_held(
        stream
            .ensure_head(&mut second_page)
            .expect("held head should survive interruption"),
        5,
    );
    assert_eq!(second_page.pull_attempts(), 1);
    assert_eq!(observed_pulls.get(), 3);
}

#[test]
fn hard_failure_returns_no_page_and_keeps_completed_work_charged() {
    let inner = StaticOrderedKeyStream::with_failure(
        vec![data_key(1), data_key(3)],
        1,
        ForcedFailure::HardBudget,
    );
    let mut stream = RepeatedPullHeldHeadKeyStream::new(
        inner,
        KeyOrderComparator::from_direction(Direction::Asc),
    );
    let mut work = HeldHeadSeekWork::unbounded();

    let error = stream
        .seek_head_at_or_after(&data_key(5), &mut work)
        .expect_err("hard failure must not return a page outcome");
    assert_eq!(error.class, ErrorClass::Unsupported);
    assert_eq!(error.origin, ErrorOrigin::Executor);
    assert_eq!(work.pull_attempts(), 2);
    assert_eq!(work.skipped_occurrences(), 1);
    assert_eq!(work.consumed_occurrences(), 1);
}

#[test]
fn corruption_and_non_monotonic_input_are_typed_failures() {
    let direct_failure =
        StaticOrderedKeyStream::with_failure(vec![data_key(1)], 0, ForcedFailure::Corruption);
    let mut direct_stream = RepeatedPullHeldHeadKeyStream::new(
        direct_failure,
        KeyOrderComparator::from_direction(Direction::Asc),
    );
    let mut direct_work = HeldHeadSeekWork::unbounded();
    let error = direct_stream
        .ensure_head(&mut direct_work)
        .expect_err("corruption should propagate");
    assert_eq!(error.class, ErrorClass::InvariantViolation);
    assert_eq!(error.origin, ErrorOrigin::Executor);

    let mut stream = reference_stream(&[1, 3, 2], Direction::Asc);
    let mut work = HeldHeadSeekWork::unbounded();
    assert_eq!(
        next_key_with_work(&mut stream, &mut work).expect("first key should succeed"),
        Some(data_key(1)),
    );
    assert_eq!(
        next_key_with_work(&mut stream, &mut work).expect("second key should succeed"),
        Some(data_key(3)),
    );
    let error =
        next_key_with_work(&mut stream, &mut work).expect_err("non-monotonic key should fail");
    assert_eq!(error.class, ErrorClass::InvariantViolation);
    assert_eq!(error.origin, ErrorOrigin::Executor);
}

#[test]
fn entity_drift_and_counter_overflow_preserve_typed_invariants() {
    let inner = StaticOrderedKeyStream::new(vec![entity_data_key(1, 1), entity_data_key(2, 2)]);
    let mut drifting_stream = RepeatedPullHeldHeadKeyStream::new(
        inner,
        KeyOrderComparator::from_direction(Direction::Asc),
    );
    let mut drifting_work = HeldHeadSeekWork::unbounded();
    assert_eq!(
        next_key_with_work(&mut drifting_stream, &mut drifting_work)
            .expect("first entity key should succeed"),
        Some(entity_data_key(1, 1)),
    );
    let error = drifting_stream
        .ensure_head(&mut drifting_work)
        .expect_err("entity drift should fail");
    assert_eq!(error.class, ErrorClass::InvariantViolation);
    assert_eq!(error.origin, ErrorOrigin::Executor);

    let mut stream = reference_stream(&[1, 2], Direction::Asc);
    let mut initial_work = HeldHeadSeekWork::unbounded();
    assert_held(
        stream
            .ensure_head(&mut initial_work)
            .expect("head should load before overflow"),
        1,
    );

    let mut comparison_overflow =
        HeldHeadSeekWork::with_observed_for_tests(u64::MAX, 0, u64::MAX, 0, 0);
    let error = stream
        .seek_head_at_or_after(&data_key(1), &mut comparison_overflow)
        .expect_err("comparison overflow should fail");
    assert_eq!(error.class, ErrorClass::InvariantViolation);

    let mut consumption_overflow =
        HeldHeadSeekWork::with_observed_for_tests(u64::MAX, 0, 0, 0, u64::MAX);
    let error = stream
        .consume_head(&mut consumption_overflow)
        .expect_err("consumption overflow should fail");
    assert_eq!(error.class, ErrorClass::InvariantViolation);

    let mut recovery_work = HeldHeadSeekWork::unbounded();
    assert_eq!(
        stream
            .consume_head(&mut recovery_work)
            .expect("failed accounting must leave the head held"),
        Some(data_key(1)),
    );
}

#[test]
fn held_head_survives_interruption_and_is_consumed_exactly_once() {
    let mut stream = reference_stream(&[3, 5], Direction::Asc);
    let mut work = HeldHeadSeekWork::with_pull_attempt_limit(1);

    assert_held(stream.ensure_head(&mut work).expect("head should load"), 3);
    assert_held(
        stream
            .ensure_head(&mut work)
            .expect("re-entered ensure should reuse head"),
        3,
    );
    assert_held(
        stream
            .seek_head_at_or_after(&data_key(3), &mut work)
            .expect("equal seek should reuse head"),
        3,
    );
    assert_eq!(work.pull_attempts(), 1);
    assert_eq!(
        stream
            .consume_head(&mut work)
            .expect("head should consume once"),
        Some(data_key(3)),
    );
    assert_eq!(
        stream
            .consume_head(&mut work)
            .expect("second consume should be empty"),
        None,
    );
}

#[test]
fn randomized_seek_matches_a_simple_filtered_suffix_oracle() {
    for direction in [Direction::Asc, Direction::Desc] {
        for seed in 1_u64..=64 {
            let mut state = seed;
            let mut values = (0..48)
                .map(|_| {
                    state = state
                        .wrapping_mul(6_364_136_223_846_793_005)
                        .wrapping_add(1);
                    (state >> 32) % 24
                })
                .collect::<Vec<_>>();
            values.sort_unstable();
            if direction == Direction::Desc {
                values.reverse();
            }

            let consumed_prefix = usize::try_from(seed % 12).expect("prefix should fit");
            let target = (seed.wrapping_mul(17).wrapping_add(5)) % 28;
            let comparator = KeyOrderComparator::from_direction(direction);
            let expected_start = values[consumed_prefix..]
                .iter()
                .position(|value| {
                    !comparator
                        .compare_data_keys(&data_key(*value), &data_key(target))
                        .is_lt()
                })
                .map(|offset| consumed_prefix.saturating_add(offset));

            let mut stream = reference_stream(values.as_slice(), direction);
            let mut work = HeldHeadSeekWork::unbounded();
            for expected in &values[..consumed_prefix] {
                assert_eq!(
                    next_key_with_work(&mut stream, &mut work)
                        .expect("prefix consumption should succeed"),
                    Some(data_key(*expected)),
                );
            }

            let outcome = stream
                .seek_head_at_or_after(&data_key(target), &mut work)
                .expect("randomized seek should succeed");
            match expected_start {
                Some(start) => {
                    assert_held(outcome, values[start]);
                    let mut actual = vec![
                        stream
                            .consume_head(&mut work)
                            .expect("held randomized head should consume")
                            .expect("held randomized head should exist"),
                    ];
                    actual.extend(
                        collect_remaining(&mut stream, &mut work)
                            .expect("randomized suffix should collect"),
                    );
                    let expected = values[start..]
                        .iter()
                        .copied()
                        .map(data_key)
                        .collect::<Vec<_>>();
                    assert_eq!(actual, expected, "seed={seed} direction={direction:?}");
                }
                None => assert_eq!(outcome, HeldHeadSeekOutcome::Exhausted),
            }
            assert!(work.comparisons() >= work.skipped_occurrences());
        }
    }
}
