//! Public generated-write acceptance without a Canic endpoint dependency.

use super::generated;
use crate::actor_schema::{ActorHygieneEntity as Row, ActorHygieneEntityInsert as Insert};
use runtime_api::{
    db::{TypedBulkWriteError, TypedBulkWriteLimits, TypedWriteAdapter},
    prelude::Id,
    value::OutputValue,
};

fn input(
    binding: &runtime_api::db::TypedEntityBinding,
    label: String,
) -> runtime_api::db::TypedWrite {
    Insert::from(Row {
        id: 999,
        label,
        nickname: None,
    })
    .encode_write(binding)
    .unwrap()
}

#[test]
fn bulk_byte_splitting_keeps_assigned_keys_order_and_single_row_refusals() {
    generated::__drive_native_database_for_tests().unwrap();
    runtime_api::db::with_request_execution(|| {
        let session = generated::db().unwrap();
        let binding = Row::typed_binding(&session).unwrap();
        let sample = session
            .execute_trusted_typed_write_batch(vec![input(&binding, "x".repeat(32))])
            .unwrap();
        let one_result = runtime_api::wire::encode_one(&sample).unwrap().len();
        let writes: Vec<_> = (0..5)
            .map(|index| input(&binding, format!("{index:032}")))
            .collect();
        let limits = TypedBulkWriteLimits {
            max_batch_rows: 4,
            max_batch_result_bytes: one_result,
            max_attempted_rows: 32,
            ..Default::default()
        };
        let progress = session
            .execute_trusted_typed_write_bulk(&writes, limits)
            .unwrap();
        assert_eq!(progress.next_input, 5);
        assert_eq!(progress.attempted_rows, 11); // refused 4 and 2, then five single rows
        assert_eq!(progress.batches.len(), 5);
        for (index, batch) in progress.batches.iter().enumerate() {
            assert_eq!(batch.first_input, index);
            assert_eq!(batch.results.len(), 1);
            let result = &batch.results[0];
            assert_eq!(result.affected_rows, 1);
            let id = result.columns.iter().position(|name| name == "id").unwrap();
            let label = result
                .columns
                .iter()
                .position(|name| name == "label")
                .unwrap();
            let expected_id = u64::try_from(index).unwrap() + 2;
            assert_eq!(result.rows[0][id], OutputValue::nat64(expected_id));
            assert_eq!(
                result.rows[0][label],
                OutputValue::text(format!("{index:032}"))
            );
            assert!(
                session
                    .get::<Row>(Id::from_key(expected_id))
                    .unwrap()
                    .is_some()
            );
        }

        let failure = session
            .execute_trusted_typed_write_bulk(
                &writes[..1],
                TypedBulkWriteLimits {
                    max_batch_rows: 1,
                    max_batch_result_bytes: one_result - 1,
                    ..limits
                },
            )
            .unwrap_err();
        assert!(matches!(
            failure.error,
            TypedBulkWriteError::InputTooLarge(_)
        ));
        assert_eq!(failure.progress.next_input, 0);
        assert_eq!(failure.progress.attempted_rows, 1);
        let good = session
            .execute_trusted_typed_write(input(&binding, "next".into()))
            .unwrap();
        assert_eq!(good.rows[0][0], OutputValue::nat64(7));
    });
}

#[test]
fn bulk_preserves_committed_prefix_and_never_splits_constraint_errors_or_atomic_calls() {
    generated::__drive_native_database_for_tests().unwrap();
    runtime_api::db::with_request_execution(|| {
        let session = generated::db().unwrap();
        let binding = Row::typed_binding(&session).unwrap();
        let writes = vec![
            input(&binding, "first".into()),
            input(&binding, "second".into()),
            input(&binding, "x".repeat(33)),
            input(&binding, "fourth".into()),
        ];
        assert!(
            session
                .execute_trusted_typed_write_batch(writes.clone())
                .is_err()
        );
        assert!(session.get::<Row>(Id::from_key(1_u64)).unwrap().is_none());
        let failure = session
            .execute_trusted_typed_write_bulk(
                &writes,
                TypedBulkWriteLimits {
                    max_batch_rows: 2,
                    ..Default::default()
                },
            )
            .unwrap_err();
        assert!(matches!(failure.error, TypedBulkWriteError::Operation(_)));
        assert_eq!(failure.progress.next_input, 2);
        assert_eq!(failure.progress.attempted_rows, 4);
        assert_eq!(failure.progress.batches.len(), 1);
        assert_eq!(
            session
                .get::<Row>(Id::from_key(1_u64))
                .unwrap()
                .unwrap()
                .label,
            "first"
        );
        assert_eq!(
            session
                .get::<Row>(Id::from_key(2_u64))
                .unwrap()
                .unwrap()
                .label,
            "second"
        );
        assert!(session.get::<Row>(Id::from_key(3_u64)).unwrap().is_none());
    });
}

#[test]
fn bulk_refused_work_exhaustion_does_not_consume_identity_or_retry() {
    generated::__drive_native_database_for_tests().unwrap();
    runtime_api::db::with_request_execution(|| {
        let session = generated::db().unwrap();
        let binding = Row::typed_binding(&session).unwrap();
        let writes: Vec<_> = (0..4).map(|_| input(&binding, "wide".repeat(8))).collect();
        let failure = session
            .execute_trusted_typed_write_bulk(
                &writes,
                TypedBulkWriteLimits {
                    max_batch_rows: 4,
                    max_batch_result_bytes: 1,
                    max_attempted_rows: 4,
                    ..Default::default()
                },
            )
            .unwrap_err();
        assert!(matches!(failure.error, TypedBulkWriteError::WorkLimit));
        assert_eq!(failure.progress.next_input, 0);
        assert_eq!(failure.progress.attempted_rows, 4);
        let next = session
            .execute_trusted_typed_write(writes[0].clone())
            .unwrap();
        assert_eq!(next.rows[0][0], OutputValue::nat64(1));
    });
}
