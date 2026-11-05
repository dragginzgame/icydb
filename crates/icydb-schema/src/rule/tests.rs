use super::SourceRuleOperation;
use crate::{ScalarLiteral, SchemaContractError};

#[test]
fn source_rule_operation_rejects_reversed_and_mixed_ranges() {
    assert_eq!(
        SourceRuleOperation::LengthRangeInclusive { min: 2, max: 1 }.validate(),
        Err(SchemaContractError::InvalidRuleOperation),
    );
    assert_eq!(
        SourceRuleOperation::NumericRangeInclusive {
            min: ScalarLiteral::Nat(2),
            max: ScalarLiteral::Nat(1),
        }
        .validate(),
        Err(SchemaContractError::InvalidRuleOperation),
    );
    assert_eq!(
        SourceRuleOperation::NumericRangeInclusive {
            min: ScalarLiteral::Int(0),
            max: ScalarLiteral::Nat(1),
        }
        .validate(),
        Err(SchemaContractError::InvalidRuleOperation),
    );
}

#[test]
fn source_rule_operation_accepts_ordered_exact_ranges() {
    assert!(
        SourceRuleOperation::LengthRangeInclusive { min: 1, max: 2 }
            .validate()
            .is_ok()
    );
    assert!(
        SourceRuleOperation::NumericRangeInclusive {
            min: ScalarLiteral::Int(-1),
            max: ScalarLiteral::Int(1),
        }
        .validate()
        .is_ok()
    );
}

#[test]
fn source_rule_operation_accepts_exact_nonzero_multiple_and_maximum() {
    assert!(
        SourceRuleOperation::NumericMaximumInclusive {
            value: ScalarLiteral::Nat(10),
        }
        .validate()
        .is_ok()
    );
    assert!(
        SourceRuleOperation::MultipleOf {
            divisor: ScalarLiteral::Int(-5),
        }
        .validate()
        .is_ok()
    );
}

#[test]
fn source_rule_operation_rejects_zero_and_float_multiple() {
    for divisor in [
        ScalarLiteral::Nat(0),
        ScalarLiteral::Float64(crate::Float64::try_new(1.0).expect("finite float")),
    ] {
        assert_eq!(
            SourceRuleOperation::MultipleOf { divisor }.validate(),
            Err(SchemaContractError::InvalidRuleOperation),
        );
    }
}
