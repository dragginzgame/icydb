//! Test-only helpers for the parent module.

use super::*;

pub(in crate::db::integrity) fn current_job_codec_fixture() -> IntegrityJob {
    let id = IntegrityJobId::try_from_bytes([1; 32]).expect("fixture job ID is nonzero");
    let database_incarnation_id = DatabaseIncarnationId::for_tests(2);
    let entity = IntegrityEntityIdentity {
        entity_tag: 3,
        entity_path: "fixture::entity".to_string(),
        store_path: "fixture::store".to_string(),
    };
    let checkpoint = IntegrityCheckpoint::QuickMetadata;
    let captured_proof_vector = IntegrityProofVector {
        database_incarnation_id,
        accepted_schema_version: 1,
        accepted_schema_fingerprint: [4; 16],
        inspection_plan_fingerprint: [5; 32],
        database_control_fingerprint: [6; 32],
        allocation_registry_generation: 7,
        stores: vec![IntegrityStoreProof {
            store_path: "fixture::store".to_string(),
            data_generation: 8,
            index_generation: 9,
            journal: JournalTailProofIdentity::from_persisted_parts(1, 0, 0, 1, 2),
        }],
        index_generations: Vec::new(),
        relation_generations: Vec::new(),
    };
    let receipt = IntegrityJobReceipt::Page(DeepIntegrityPage {
        job_id: id,
        page_sequence: 0,
        phase: IntegrityPhase::QuickMetadata,
        status: DeepIntegrityPageStatus::InProgress,
        pages_completed: 0,
        findings_seen: 0,
        findings: Vec::new(),
        blocked_verifier_families: Vec::new(),
    });
    let job = IntegrityJob {
        id,
        database_incarnation_id,
        owner: IntegrityJobOwner::new("fixture-owner").expect("fixture owner is valid"),
        submission_key: IntegritySubmissionKey::new("fixture-submission")
            .expect("fixture submission key is valid"),
        entity,
        accepted_schema_version: 1,
        accepted_schema_fingerprint: [4; 16],
        inspection_plan_fingerprint: [5; 32],
        checkpoint,
        captured_proof_vector,
        state: IntegrityJobState::InProgress,
        lease_deadline_nanos: 10,
        findings_seen: 0,
        pages_completed: 0,
        blocked_verifier_families: Vec::new(),
        last_receipt: IntegrityReceiptEnvelope {
            replay_key: IntegrityReceiptReplayKey::Start,
            receipt,
        },
    };
    job.validate().expect("codec fixture is internally valid");
    job
}
