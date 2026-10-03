//! Public request observations retain the execution owner's accounting.

use crate::design::FacadePlayer;
use runtime_api::{
    ErrorCode,
    db::{
        TypedOperationError, with_request_execution, with_request_execution_async,
        with_request_execution_root,
    },
    diagnostic::{
        DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionBudgetScope,
        DiagnosticExecutionLane, DiagnosticFactTag,
    },
    types::{Id, Ulid},
};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};

fn read_one() {
    crate::db()
        .unwrap()
        .get::<FacadePlayer>(Id::from_key(Ulid::MIN))
        .unwrap();
}

#[test]
fn public_headroom_stops_between_reads_and_preserves_exhaustion_facts() {
    crate::__icydb_generated::__drive_native_database_for_tests().unwrap();
    with_request_execution_root(|root| {
        let session = crate::db().unwrap();
        let initial = session.request_budget();
        assert_eq!(initial, root.request_budget());
        let mut completed = 0;
        // This fixture's work unit is one exact-key read, not an arbitrary
        // validation/write bundle. Other resource limits remain enforced.
        for _ in 0..=initial.limit(Resource::QueryExecutions) {
            if session
                .request_budget()
                .remaining(Resource::QueryExecutions)
                == 0
            {
                break;
            }
            read_one();
            completed += 1;
        }
        assert_eq!(completed, initial.limit(Resource::QueryExecutions));
        let full = session.request_budget();
        assert_eq!(full.observed(Resource::QueryExecutions), completed);
        assert_eq!(full.remaining(Resource::QueryExecutions), 0);
        assert_eq!(
            full,
            session.request_budget(),
            "observation does not charge"
        );
        assert_eq!(full, root.request_budget());

        let TypedOperationError::Database(error) = session
            .get::<FacadePlayer>(Id::from_key(Ulid::MIN))
            .unwrap_err()
        else {
            panic!("expected a database budget failure");
        };
        assert_eq!(
            error.code(),
            ErrorCode::RUNTIME_BOUNDARY_EXECUTION_BUDGET_EXCEEDED
        );
        for (tag, expected) in [
            (
                DiagnosticFactTag::BudgetResource,
                Resource::QueryExecutions.raw(),
            ),
            (DiagnosticFactTag::Limit, completed),
            (DiagnosticFactTag::Actual, completed + 1),
            (
                DiagnosticFactTag::ExecutionBudgetScope,
                DiagnosticExecutionBudgetScope::Request.raw(),
            ),
            (
                DiagnosticFactTag::ExecutionLane,
                DiagnosticExecutionLane::PublicRead.raw(),
            ),
        ] {
            assert_eq!(
                error
                    .facts()
                    .iter()
                    .find(|fact| fact.tag() == tag.raw())
                    .unwrap()
                    .value(),
                expected
            );
        }
        assert!(
            error
                .facts()
                .iter()
                .any(|fact| fact.tag() == DiagnosticFactTag::QueryShapeFingerprintPrefix.raw())
        );
        let wire = candid::encode_one(&error).unwrap();
        assert_eq!(
            candid::decode_one::<runtime_api::Error>(&wire).unwrap(),
            error
        );
        let failed = session.request_budget();
        assert_eq!(failed.observed(Resource::QueryExecutions), completed + 1);
        assert_eq!(failed.remaining(Resource::QueryExecutions), 0);
        with_request_execution(|| {
            let nested = crate::db().unwrap();
            assert_eq!(nested.request_budget(), failed);
            assert!(nested.get::<FacadePlayer>(Id::from_key(Ulid::MIN)).is_err());
        });
        assert_eq!(
            session.request_budget().observed(Resource::QueryExecutions),
            completed + 2
        );
        assert_eq!(initial.observed(Resource::QueryExecutions), 0);
    });
    with_request_execution(|| {
        assert_eq!(
            crate::db()
                .unwrap()
                .request_budget()
                .observed(Resource::QueryExecutions),
            0
        );
        read_one();
    });
}

#[test]
fn retained_sessions_observe_their_owner_after_root_drop_and_during_other_requests() {
    crate::__icydb_generated::__drive_native_database_for_tests().unwrap();
    let retained = with_request_execution_root(|root| {
        let first = crate::db().unwrap();
        let sibling = crate::db().unwrap();
        with_request_execution(|| {
            read_one();
        });
        assert_eq!(
            first.request_budget().observed(Resource::QueryExecutions),
            1
        );
        assert_eq!(first.request_budget(), sibling.request_budget());
        assert_eq!(first.request_budget(), root.request_budget());
        first
    });
    let before = retained.request_budget();
    with_request_execution(|| {
        read_one();
        read_one();
        assert_eq!(
            crate::db()
                .unwrap()
                .request_budget()
                .observed(Resource::QueryExecutions),
            2
        );
        assert_eq!(retained.request_budget(), before);
    });
    assert_eq!(retained.request_budget(), before);
}

struct YieldOnce(bool);

impl Future for YieldOnce {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.0 {
            Poll::Ready(())
        } else {
            self.0 = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

#[test]
fn interleaved_async_polls_share_nested_work_but_isolate_request_headroom() {
    crate::__icydb_generated::__drive_native_database_for_tests().unwrap();
    let mut first = Box::pin(with_request_execution_async(async {
        let session = crate::db().unwrap();
        read_one();
        YieldOnce(false).await;
        assert_eq!(
            session.request_budget().observed(Resource::QueryExecutions),
            1
        );
        with_request_execution_async(async {
            read_one();
        })
        .await;
        assert_eq!(
            session.request_budget().observed(Resource::QueryExecutions),
            2
        );
        session.request_budget()
    }));
    let mut second = Box::pin(with_request_execution_async(async {
        read_one();
        read_one();
        YieldOnce(false).await;
        let session = crate::db().unwrap();
        assert_eq!(
            session.request_budget().observed(Resource::QueryExecutions),
            2
        );
        read_one();
        session.request_budget()
    }));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(first.as_mut().poll(&mut cx).is_pending());
    assert!(
        crate::db().is_err(),
        "suspended polls leave no ambient owner"
    );
    assert!(second.as_mut().poll(&mut cx).is_pending());
    let Poll::Ready(first_budget) = first.as_mut().poll(&mut cx) else {
        panic!("first should finish")
    };
    let Poll::Ready(second_budget) = second.as_mut().poll(&mut cx) else {
        panic!("second should finish")
    };
    assert_eq!(first_budget.observed(Resource::QueryExecutions), 2);
    assert_eq!(second_budget.observed(Resource::QueryExecutions), 3);
    assert!(crate::db().is_err());
}
