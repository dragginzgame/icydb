use super::*;
use crate::{
    db::{
        MutationJobAdvanceRequest, MutationJobIdempotencyKey, MutationJobPhase,
        MutationJobRestartReason, MutationJobStatus, RequestExecutionRoot, StoreRegistry,
        mutation_job::{MutationJobRecord, MutationJobTransition},
    },
    traits::Path,
};
#[cfg(feature = "sql")]
use crate::{
    db::{
        data::{AcceptedFixedUpdatePatch, FieldSlot},
        query::plan::expr::{BinaryOp, Expr, FieldId},
        resource::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
    },
    types::Timestamp,
    value::Value,
};

struct TestCanister;

impl Path for TestCanister {
    const PATH: &'static str = "db::session::mutation_job::tests::Canister";
}

impl CanisterKind for TestCanister {
    fn commit_memory_id() -> Result<u64, ic_storage::RuntimeOpenError> {
        Ok(244)
    }
    const COMMIT_STABLE_KEY: &'static str = "icydb.test.mutation_job.commit.v1";
    fn startup_memory_id() -> Result<u64, ic_storage::RuntimeOpenError> {
        Ok(246)
    }
    const STARTUP_STABLE_KEY: &'static str = "icydb.test.mutation_job.startup.control.v1";
    fn integrity_progress_memory_id() -> Result<u64, ic_storage::RuntimeOpenError> {
        Ok(245)
    }
    const INTEGRITY_PROGRESS_STABLE_KEY: &'static str = "icydb.test.mutation_job.progress.v1";
}

thread_local! {
    static STORE_REGISTRY: StoreRegistry = StoreRegistry::new();
}

fn job_id() -> MutationJobId {
    MutationJobId::try_from_bytes([19; 32]).expect("nonzero mutation job id should admit")
}

fn session() -> DbSession<TestCanister> {
    let root = RequestExecutionRoot::__new_runtime_root();
    let session = DbSession::new(&STORE_REGISTRY, &root);
    assert!(session.db.drive_startup_recovery_page().unwrap());
    session
}

#[cfg(feature = "sql")]
fn exhausted_session() -> DbSession<TestCanister> {
    let budget =
        HardExecutionBudget::uniform_for_tests(0, HardExecutionFailureHeadroom::new(500, 256));
    let root = RequestExecutionRoot::new_for_tests(budget);
    DbSession::new(&STORE_REGISTRY, &root)
}

#[test]
fn session_load_and_terminal_acknowledgement_preserve_the_store_contract() {
    let initial = MutationJobRecord::new(job_id(), vec![1, 2], vec![3])
        .expect("bounded initial record should admit");
    with_mutation_progress_store::<TestCanister, _>(|store| {
        store.insert_mutation(&initial).map(|_| ())
    })
    .expect("initial record should insert");

    let session = session();
    assert_eq!(
        session.mutation_job_state(job_id()),
        Ok(initial.state().clone())
    );
    assert_eq!(
        session.acknowledge_mutation_job(job_id(), 0),
        Err(MutationJobError::Active),
    );

    let request = MutationJobAdvanceRequest::new(
        job_id(),
        0,
        MutationJobIdempotencyKey::new("authority-drift")
            .expect("bounded idempotency key should admit"),
    );
    let (terminal, _) = initial
        .apply_transition(
            &request,
            MutationJobTransition::new(
                MutationJobStatus::RestartRequired(
                    MutationJobRestartReason::ManagedTimestampRegression,
                ),
                MutationJobPhase::Forward,
                Vec::new(),
                0,
                0,
                0,
            ),
        )
        .expect("terminal restart receipt should admit");
    with_mutation_progress_store::<TestCanister, _>(|store| store.replace_mutation(&terminal))
        .expect("terminal record should replace active state");

    assert_eq!(
        session.acknowledge_mutation_job(job_id(), 0),
        Err(MutationJobError::StaleSequence {
            expected: 0,
            actual: 1,
        }),
    );
    assert_eq!(session.acknowledge_mutation_job(job_id(), 1), Ok(()));
    assert_eq!(session.acknowledge_mutation_job(job_id(), 1), Ok(()));
    assert_eq!(
        session.mutation_job_state(job_id()),
        Err(MutationJobError::NotFound),
    );
}

#[cfg(feature = "sql")]
fn canonical_intent(authority: u8, scope_value: u64, timestamp: i64) -> CanonicalMutationIntent {
    let scope = Expr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(Expr::Field(FieldId::new("collection_id"))),
        right: Box::new(Expr::Literal(Value::Nat64(scope_value))),
    };
    let patch = AcceptedFixedUpdatePatch::from_canonical_fields(vec![(
        FieldSlot::from_validated_index(1),
        vec![3, 4, 5],
    )])
    .expect("fixed patch should admit");
    CanonicalMutationIntent::new(
        [authority; 16],
        [authority; 32],
        "journaled".to_string(),
        "schema::Token".to_string(),
        7,
        11,
        1,
        [authority; 16],
        &scope,
        &patch,
        Timestamp::from_millis(timestamp),
        17,
    )
    .expect("canonical intent should admit")
}

#[cfg(feature = "sql")]
#[test]
fn occupied_start_distinguishes_replay_authority_drift_and_identity_conflict() {
    let retained_intent = canonical_intent(1, 7, 100);
    let retained = MutationJobRecord::new(
        job_id(),
        retained_intent.encode().expect("intent should encode"),
        vec![9],
    )
    .expect("record should admit");

    assert_eq!(
        resolve_occupied_mutation_job_start(&retained, &canonical_intent(1, 7, 200)),
        Ok(retained.state().clone()),
    );
    assert_eq!(
        resolve_occupied_mutation_job_start(&retained, &canonical_intent(2, 7, 200)),
        Err(MutationJobError::AuthorityMismatch),
    );
    assert_eq!(
        resolve_occupied_mutation_job_start(&retained, &canonical_intent(1, 8, 200)),
        Err(MutationJobError::IdentityConflict),
    );
}

#[cfg(feature = "sql")]
#[test]
fn aggregate_budget_exhaustion_does_not_advance_durable_state() {
    let initial = MutationJobRecord::new(job_id(), vec![1, 2], vec![3])
        .expect("bounded initial record should admit");
    with_mutation_progress_store::<TestCanister, _>(|store| {
        store.insert_mutation(&initial).map(|_| ())
    })
    .expect("initial record should insert");
    let request = MutationJobAdvanceRequest::new(
        job_id(),
        0,
        MutationJobIdempotencyKey::new("budget-exhausted")
            .expect("bounded idempotency key should admit"),
    );

    assert!(matches!(
        exhausted_session().advance_trusted_mutation_job(&request),
        Err(MutationJobError::ExecutionBudgetExceeded {
            limit: 0,
            observed: 1,
            ..
        })
    ));
    assert_eq!(
        session().mutation_job_state(job_id()),
        Ok(initial.state().clone()),
    );
}

#[cfg(feature = "sql")]
#[test]
fn exact_replay_precedes_advance_budget_accounting() {
    let initial = MutationJobRecord::new(job_id(), vec![1, 2], vec![3])
        .expect("bounded initial record should admit");
    let request = MutationJobAdvanceRequest::new(
        job_id(),
        0,
        MutationJobIdempotencyKey::new("lost-response")
            .expect("bounded idempotency key should admit"),
    );
    let (advanced, receipt) = initial
        .apply_transition(
            &request,
            MutationJobTransition::new(
                MutationJobStatus::Active,
                MutationJobPhase::Forward,
                vec![4],
                1,
                1,
                0,
            ),
        )
        .expect("bounded successor should admit");
    with_mutation_progress_store::<TestCanister, _>(|store| {
        store.insert_mutation(&advanced).map(|_| ())
    })
    .expect("advanced record should insert");

    assert_eq!(
        exhausted_session().advance_trusted_mutation_job(&request),
        Ok(receipt),
    );
    assert_eq!(
        session().mutation_job_state(job_id()),
        Ok(advanced.state().clone()),
    );
}
