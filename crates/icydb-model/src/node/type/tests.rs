use super::{RuleNumber, SourceRuleAuthoringOperation};

#[test]
fn directly_constructed_rule_numbers_validate_before_lowering() {
    assert_eq!(
        SourceRuleAuthoringOperation::MultipleOf {
            divisor: RuleNumber::Decimal("not-a-decimal"),
        }
        .validate_shape(),
        Err("multiple_of divisor must be a valid numeric literal"),
    );
    assert_eq!(
        SourceRuleAuthoringOperation::NumericMaximumInclusive {
            value: RuleNumber::Float64(f64::NAN),
        }
        .validate_shape(),
        Err("numeric rule value must be a valid numeric literal"),
    );
}
