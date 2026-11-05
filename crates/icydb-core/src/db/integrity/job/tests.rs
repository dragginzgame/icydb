use super::*;
use crate::error::{ErrorClass, ErrorOrigin};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

#[test]
fn inspection_resource_failures_keep_their_typed_terminal_and_wire_code() {
    use crate::db::{
        resource::budget::MaintenanceConstructionBudget, resource::construction::ConstructionBudget,
    };
    let work = MaintenanceConstructionBudget::with_limit_for_tests(Resource::TemporaryBytes, 0);
    for error in [
        work.charge(Resource::TemporaryBytes, 1).unwrap_err(),
        InternalError::relation_budget_exceeded(Resource::NestedValueSteps, 10, 11),
        InternalError::relation_budget_exceeded(Resource::TemporaryBytes, 10, 11),
        InternalError::page_unit_too_large(Resource::TemporaryBytes, 10, 11),
    ] {
        let expected = error.diagnostic();
        let outcome = IntegrityTerminalOutcome::from_internal(&error);
        let IntegrityTerminalOutcome::ResourceLimited(diagnostic) = &outcome else {
            panic!("resource exhaustion must not become an authority failure");
        };
        assert_eq!(
            diagnostic.diagnostic_code(),
            error.diagnostic_code().error_code().raw(),
        );
        let bytes = icydb_schema::wire::encode_one(&outcome).expect("terminal should encode");
        let decoded: IntegrityTerminalOutcome =
            icydb_schema::wire::decode_one(&bytes).expect("terminal should decode");
        assert_eq!(decoded, outcome);
        let IntegrityDeepError::Internal(error) = IntegrityDeepError::from_plan_load(error) else {
            panic!("plan-load exhaustion must retain its operational error");
        };
        assert_eq!(error.diagnostic(), expected);
    }
}

#[test]
fn inspection_authority_failures_are_not_classified_as_exhaustion() {
    for class in [
        ErrorClass::Corruption,
        ErrorClass::IncompatiblePersistedFormat,
        ErrorClass::InvariantViolation,
        ErrorClass::Unsupported,
        ErrorClass::NotFound,
        ErrorClass::Conflict,
        ErrorClass::Internal,
    ] {
        let error = InternalError::classified(class, ErrorOrigin::Store);
        assert_eq!(
            IntegrityTerminalOutcome::from_internal(&error),
            IntegrityTerminalOutcome::Uninspectable(IntegrityAuthorityDiagnostic::from_internal(
                &error
            ),),
        );
        let expected = IntegrityAuthorityDiagnostic::from_internal(&error);
        let IntegrityDeepError::Uninspectable(diagnostic) =
            IntegrityDeepError::from_plan_load(error)
        else {
            panic!("invalid authority keeps its established plan-load classification");
        };
        assert_eq!(diagnostic, expected);
    }
}

#[test]
fn public_job_inputs_revalidate_after_wire_decode() {
    let owner_bytes = icydb_schema::wire::encode_one(IntegrityJobOwner(String::new()))
        .expect("owner should encode");
    let owner: IntegrityJobOwner =
        icydb_schema::wire::decode_one(&owner_bytes).expect("owner should decode");
    assert_eq!(owner.validate(), Err(IntegrityJobError::InvalidOwner));

    let submission_bytes = icydb_schema::wire::encode_one(IntegritySubmissionKey(String::new()))
        .expect("submission key should encode");
    let submission: IntegritySubmissionKey =
        icydb_schema::wire::decode_one(&submission_bytes).expect("submission key should decode");
    assert_eq!(
        submission.validate(),
        Err(IntegrityJobError::InvalidSubmissionKey),
    );

    let job_id_bytes =
        icydb_schema::wire::encode_one(IntegrityJobId([0; 32])).expect("job id should encode");
    let job_id: IntegrityJobId =
        icydb_schema::wire::decode_one(&job_id_bytes).expect("job id should decode");
    assert_eq!(job_id.validate(), Err(IntegrityJobError::InvalidJobId),);
}

#[test]
fn persisted_constraint_value_paths_require_current_accepted_id_shape() {
    let valid = ConstraintValuePath::new(vec![
        ConstraintValuePathComponent::RootField { field_id: 1 },
        ConstraintValuePathComponent::RecordMember {
            composite_type_id: 2,
            member_id: 3,
        },
        ConstraintValuePathComponent::ListElement { index: 0 },
    ]);
    assert!(constraint_value_path_is_well_formed(&valid));

    for malformed in [
        ConstraintValuePath::new(Vec::new()),
        ConstraintValuePath::new(vec![ConstraintValuePathComponent::RootField {
            field_id: 0,
        }]),
        ConstraintValuePath::new(vec![
            ConstraintValuePathComponent::RootField { field_id: 1 },
            ConstraintValuePathComponent::RootField { field_id: 2 },
        ]),
        ConstraintValuePath::new(vec![
            ConstraintValuePathComponent::RootField { field_id: 1 },
            ConstraintValuePathComponent::Newtype {
                composite_type_id: 0,
            },
        ]),
    ] {
        assert!(!constraint_value_path_is_well_formed(&malformed));
    }
}

#[test]
fn public_job_id_hex_round_trip_is_exact_and_fail_closed() {
    let mut bytes = [0_u8; 32];
    bytes[0] = 0x01;
    bytes[31] = 0xfe;
    let job_id = IntegrityJobId::try_from_bytes(bytes).expect("job id should admit");
    let encoded = job_id.to_hex();

    assert_eq!(encoded.len(), 64);
    assert_eq!(IntegrityJobId::try_from_hex(encoded.as_str()), Ok(job_id),);
    assert_eq!(
        IntegrityJobId::try_from_hex(encoded.to_uppercase().as_str()),
        Ok(job_id),
    );
    for malformed in [
        "",
        "01",
        "0000000000000000000000000000000000000000000000000000000000000000",
        "g001000000000000000000000000000000000000000000000000000000000000",
    ] {
        assert_eq!(
            IntegrityJobId::try_from_hex(malformed),
            Err(IntegrityJobError::InvalidJobId),
        );
    }
}

#[test]
fn persisted_checkpoint_families_stay_phase_owned() {
    let journal_in_row = PhysicalUnitCheckpoint::Within {
        physical_key: vec![1],
        verifier_family: IntegrityVerifierFamily::JournalEnvelope,
        ordinal: 0,
    };
    let row_in_index = PhysicalUnitCheckpoint::Within {
        physical_key: vec![1],
        verifier_family: IntegrityVerifierFamily::FieldValue,
        ordinal: 0,
    };
    let reverse_in_reverse = PhysicalUnitCheckpoint::Within {
        physical_key: vec![1],
        verifier_family: IntegrityVerifierFamily::ReverseRelationEntry,
        ordinal: 0,
    };

    assert!(!row_checkpoint_is_well_formed(&journal_in_row));
    assert!(!index_checkpoint_is_well_formed(&row_in_index));
    assert!(reverse_checkpoint_is_well_formed(&reverse_in_reverse));
}

#[test]
fn persisted_journal_checkpoint_cannot_skip_the_captured_tail_interval() {
    assert!(journal_checkpoint_is_well_formed(
        &JournalInspectionCheckpoint::BeforeFirst,
        4,
        8,
    ));
    assert!(journal_checkpoint_is_well_formed(
        &JournalInspectionCheckpoint::BeforeBatch { sequence: 7 },
        4,
        8,
    ));
    assert!(journal_checkpoint_is_well_formed(
        &JournalInspectionCheckpoint::AfterBatch { sequence: 4 },
        4,
        8,
    ));
    assert!(!journal_checkpoint_is_well_formed(
        &JournalInspectionCheckpoint::BeforeBatch { sequence: 4 },
        4,
        8,
    ));
    assert!(!journal_checkpoint_is_well_formed(
        &JournalInspectionCheckpoint::AfterBatch { sequence: 8 },
        4,
        8,
    ));
    assert!(!journal_checkpoint_is_well_formed(
        &JournalInspectionCheckpoint::CheckingBatchIdentity {
            sequence: 7,
            batch_id: [1; 16],
            next_prior_sequence: 4,
        },
        4,
        8,
    ));
}
