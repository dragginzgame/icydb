//! Module: session::mutation_job
//! Responsibility: charged mutation-job start, state load, phase dispatch, and terminal acknowledgement.
//! Does not own: SQL lowering, Forward/Verify execution, or authorization.
//! Boundary: trusted session API -> excluded mutation progress record.

use crate::{
    db::{
        DbSession, MutationJobError, MutationJobId, MutationJobState,
        executor::budget::{ExecutionBudgetExceeded, HardExecutionContext},
        integrity::with_mutation_progress_store,
    },
    traits::CanisterKind,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource, DiagnosticExecutionBudgetScope, DiagnosticExecutionLane,
};

#[cfg(feature = "sql")]
use crate::db::{
    MutationJobAdvanceReceipt, MutationJobAdvanceRequest, MutationJobPhase,
    integrity::InsertMutationJobResult,
    mutation_job::{CanonicalMutationIntent, MutationJobRecord},
};

#[cfg(feature = "sql")]
const MUTATION_JOB_START_SHAPE: u64 = 0x6d75_7461_7465_0100;
const MUTATION_JOB_LOAD_SHAPE: u64 = 0x6d75_7461_7465_0101;
const MUTATION_JOB_ACKNOWLEDGE_SHAPE: u64 = 0x6d75_7461_7465_0102;
#[cfg(feature = "sql")]
const MUTATION_JOB_ADVANCE_SHAPE: u64 = 0x6d75_7461_7465_0103;

impl<C: CanisterKind> DbSession<C> {
    /// Start one durable trusted fixed SQL mutation job.
    ///
    /// SQL is parsed and admitted exactly once for a new identity. The
    /// catalog-native intent and initial engine checkpoint are durably retained
    /// before this method returns, and no target row is read or mutated.
    /// Repeating the same canonical request returns the retained state without
    /// replacing its operation timestamp or resetting progress.
    #[cfg(feature = "sql")]
    pub fn start_trusted_sql_mutation_job(
        &self,
        job_id: MutationJobId,
        sql: &str,
    ) -> Result<MutationJobState, MutationJobError> {
        job_id.validate()?;
        self.charge_mutation_job_operation(
            DiagnosticExecutionLane::Mutation,
            MUTATION_JOB_START_SHAPE,
        )?;
        let prepared = self.prepare_mutation_job_start(job_id, sql)?;
        let submitted_intent = CanonicalMutationIntent::decode(&prepared.canonical_intent)?;
        let submitted = MutationJobRecord::new(
            job_id,
            prepared.canonical_intent,
            prepared.engine_continuation,
        )?;
        with_mutation_progress_store::<C, _>(|store| match store.insert_mutation(&submitted)? {
            InsertMutationJobResult::Inserted => Ok(submitted.state().clone()),
            InsertMutationJobResult::Occupied(retained) => {
                resolve_occupied_mutation_job_start(&retained, &submitted_intent)
            }
        })
    }

    /// Load the bounded public state for one retained mutation job.
    pub fn mutation_job_state(
        &self,
        job_id: MutationJobId,
    ) -> Result<MutationJobState, MutationJobError> {
        self.charge_mutation_job_operation(
            DiagnosticExecutionLane::TrustedRead,
            MUTATION_JOB_LOAD_SHAPE,
        )?;
        with_mutation_progress_store::<C, _>(|store| store.load_mutation(job_id))
            .map(|record| record.state().clone())
    }

    /// Advance one durable mutation job through one bounded engine-owned step.
    ///
    /// The request carries only job identity, expected sequence, and a replay
    /// key. SQL and continuation bytes remain private IcyDB custody.
    #[cfg(feature = "sql")]
    pub fn advance_trusted_mutation_job(
        &self,
        request: &MutationJobAdvanceRequest,
    ) -> Result<MutationJobAdvanceReceipt, MutationJobError> {
        let retained =
            with_mutation_progress_store::<C, _>(|store| store.load_mutation(request.job_id))?;
        if let Some(receipt) = retained.exact_replay(request)? {
            return Ok(receipt.clone());
        }
        self.charge_mutation_job_operation(
            DiagnosticExecutionLane::Mutation,
            MUTATION_JOB_ADVANCE_SHAPE,
        )?;
        retained.ensure_can_advance(request)?;
        match retained.state().phase {
            MutationJobPhase::Forward => self.advance_mutation_job_forward(&retained, request),
            MutationJobPhase::Verify => self.advance_mutation_job_verify(&retained, request),
        }
    }

    /// Remove one terminal mutation job after its result has been consumed.
    ///
    /// Repeating acknowledgement after a lost response succeeds when the job
    /// is already absent. Active jobs and stale terminal sequences fail closed.
    pub fn acknowledge_mutation_job(
        &self,
        job_id: MutationJobId,
        expected_terminal_sequence: u64,
    ) -> Result<(), MutationJobError> {
        self.charge_mutation_job_operation(
            DiagnosticExecutionLane::Mutation,
            MUTATION_JOB_ACKNOWLEDGE_SHAPE,
        )?;
        with_mutation_progress_store::<C, _>(|store| {
            store.acknowledge_mutation(job_id, expected_terminal_sequence)
        })
    }

    fn charge_mutation_job_operation(
        &self,
        lane: DiagnosticExecutionLane,
        shape: u64,
    ) -> Result<(), MutationJobError> {
        self.db
            .request_execution_scope()
            .charge(
                HardExecutionContext::new(DiagnosticExecutionBudgetScope::Execution, lane, shape),
                DiagnosticExecutionBudgetResource::QueryExecutions,
                1,
            )
            .map_err(mutation_job_execution_budget_error)
    }
}

#[cfg(feature = "sql")]
fn resolve_occupied_mutation_job_start(
    retained: &MutationJobRecord,
    submitted_intent: &CanonicalMutationIntent,
) -> Result<MutationJobState, MutationJobError> {
    let retained_intent = CanonicalMutationIntent::decode(retained.canonical_intent())?;
    if retained_intent.same_start_request(submitted_intent) {
        return Ok(retained.state().clone());
    }
    if !retained_intent.same_authority(submitted_intent) {
        return Err(MutationJobError::AuthorityMismatch);
    }
    Err(MutationJobError::IdentityConflict)
}

const fn mutation_job_execution_budget_error(error: ExecutionBudgetExceeded) -> MutationJobError {
    MutationJobError::ExecutionBudgetExceeded {
        resource: error.resource().raw(),
        limit: error.limit(),
        observed: error.observed(),
        scope: error.scope().raw(),
        lane: error.lane().raw(),
        normalized_shape_fingerprint_prefix: error.normalized_shape_fingerprint_prefix(),
    }
}

#[cfg(test)]
mod tests {
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
            executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
            query::plan::expr::{BinaryOp, Expr, FieldId},
        },
        types::Timestamp,
        value::Value,
    };

    struct TestCanister;

    impl Path for TestCanister {
        const PATH: &'static str = "db::session::mutation_job::tests::Canister";
    }

    impl CanisterKind for TestCanister {
        const COMMIT_MEMORY_ID: u8 = 244;
        const COMMIT_STABLE_KEY: &'static str = "icydb.test.mutation-job.commit.v1";
        const INTEGRITY_PROGRESS_MEMORY_ID: u8 = 245;
        const INTEGRITY_PROGRESS_STABLE_KEY: &'static str = "icydb.test.mutation-job.progress.v1";
    }

    thread_local! {
        static STORE_REGISTRY: StoreRegistry = StoreRegistry::new();
    }

    fn job_id() -> MutationJobId {
        MutationJobId::try_from_bytes([19; 32]).expect("nonzero mutation job id should admit")
    }

    fn session() -> DbSession<TestCanister> {
        let root = RequestExecutionRoot::__new_runtime_root();
        DbSession::new(&STORE_REGISTRY, &root)
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
                        MutationJobRestartReason::AcceptedSchemaChanged,
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
    fn canonical_intent(
        authority: u8,
        scope_value: u64,
        timestamp: i64,
    ) -> CanonicalMutationIntent {
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
}
