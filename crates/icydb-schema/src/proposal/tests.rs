use super::*;
use crate::decode_schema_proposal;
use std::str::FromStr;

fn empty_proposal_with(capabilities: Vec<SchemaCapability>) -> SchemaProposal {
    SchemaProposal::try_compose(
        capabilities,
        TargetDatabaseIdentity::from_bytes([1; 32]),
        SchemaSubmissionKey::try_new("proposal-version-test").expect("submission key should admit"),
        ExpectedAcceptedHead::Empty,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    )
    .expect("empty proposal should compose")
}

#[test]
fn decoded_future_contract_version_fails_typed() {
    let proposal = empty_proposal_with(Vec::new());
    let mut bytes = crate::encode_schema_proposal(&proposal).expect("proposal should encode");
    bytes[5] = 2;

    assert_eq!(
        decode_schema_proposal(&bytes),
        Err(SchemaContractError::UnsupportedVersion {
            found: 2,
            supported: 1,
        }),
    );
}

#[test]
fn decoded_unknown_capability_fails_typed() {
    let mut proposal = empty_proposal_with(vec![SchemaCapability::EXACT_COMPOSITE_TYPES]);
    proposal.capabilities[0] = SchemaCapability::from_raw(u16::MAX);
    let bytes = crate::wire::encode_one((*b"ICWP\x01", proposal)).unwrap();

    assert_eq!(
        decode_schema_proposal(&bytes),
        Err(SchemaContractError::UnsupportedCapability),
    );
}

#[test]
fn targeted_numeric_operations_require_exact_target_literal_admission() {
    let decimal = |value| {
        crate::ScalarLiteral::Decimal(
            crate::Decimal::from_str(value).expect("decimal fixture should parse"),
        )
    };
    assert!(operation_matches_target(
        &SourceRuleOperation::MultipleOf {
            divisor: decimal("0.25"),
        },
        RuleTargetShape::Scalar(ScalarType::Decimal { scale: 2 }),
    ));
    assert!(!operation_matches_target(
        &SourceRuleOperation::MultipleOf {
            divisor: decimal("0.251"),
        },
        RuleTargetShape::Scalar(ScalarType::Decimal { scale: 2 }),
    ));
    assert!(!operation_matches_target(
        &SourceRuleOperation::MultipleOf {
            divisor: crate::ScalarLiteral::Float64(
                crate::Float64::try_new(2.0).expect("finite float"),
            ),
        },
        RuleTargetShape::Scalar(ScalarType::Float64),
    ));
    assert!(operation_matches_target(
        &SourceRuleOperation::NumericMaximumInclusive {
            value: crate::ScalarLiteral::Float64(
                crate::Float64::try_new(2.0).expect("finite float"),
            ),
        },
        RuleTargetShape::Scalar(ScalarType::Float64),
    ));
}
