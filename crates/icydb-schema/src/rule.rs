//! Bounded source-side durable-rule operations.
//!
//! These values are proposal facts. Accepted schema resolves their nominal
//! target and owns every runtime evaluator.

use candid::CandidType;
use serde::{Deserialize, Serialize};

use crate::{ScalarKind, ScalarLiteral, SchemaContractError};

/// One closed durable operation applied to every selected nominal value.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum SourceRuleOperation {
    /// Inclusive Unicode-scalar, octet, or collection-cardinality range.
    LengthRangeInclusive {
        /// Inclusive minimum length.
        min: u64,
        /// Inclusive maximum length.
        max: u64,
    },
    /// Exact nonzero integer or fixed-scale decimal divisor.
    MultipleOf {
        /// Exact divisor admitted against the target kind.
        divisor: ScalarLiteral,
    },
    /// Inclusive upper bound for one exact numeric kind.
    NumericMaximumInclusive {
        /// Exact upper-bound literal.
        value: ScalarLiteral,
    },
    /// Inclusive lower bound for one exact numeric kind.
    NumericMinimumInclusive {
        /// Exact lower-bound literal.
        value: ScalarLiteral,
    },
    /// Inclusive lower and upper bounds for one exact numeric kind.
    NumericRangeInclusive {
        /// Exact lower-bound literal.
        min: ScalarLiteral,
        /// Exact upper-bound literal.
        max: ScalarLiteral,
    },
}

impl SourceRuleOperation {
    /// Validate the bounded operation independently of its eventual target.
    pub(crate) fn validate(&self) -> Result<(), SchemaContractError> {
        match self {
            Self::LengthRangeInclusive { min, max } if min <= max => Ok(()),
            Self::MultipleOf { divisor }
                if exact_multiple_literal(divisor) && !scalar_literal_is_zero(divisor) =>
            {
                divisor.validate()
            }
            Self::NumericMaximumInclusive { value } | Self::NumericMinimumInclusive { value }
                if numeric_literal(value) =>
            {
                value.validate()
            }
            Self::NumericRangeInclusive { min, max }
                if numeric_literal(min)
                    && min.kind() == max.kind()
                    && scalar_literal_le(min, max) =>
            {
                min.validate()?;
                max.validate()
            }
            Self::LengthRangeInclusive { .. }
            | Self::MultipleOf { .. }
            | Self::NumericMaximumInclusive { .. }
            | Self::NumericMinimumInclusive { .. }
            | Self::NumericRangeInclusive { .. } => Err(SchemaContractError::InvalidRuleOperation),
        }
    }
}

pub(crate) const fn exact_multiple_literal(literal: &ScalarLiteral) -> bool {
    matches!(
        literal.kind(),
        ScalarKind::Decimal
            | ScalarKind::Int128
            | ScalarKind::IntBig
            | ScalarKind::Nat128
            | ScalarKind::NatBig
    )
}

const fn numeric_literal(literal: &ScalarLiteral) -> bool {
    matches!(
        literal.kind(),
        ScalarKind::Decimal
            | ScalarKind::Float32
            | ScalarKind::Float64
            | ScalarKind::Int128
            | ScalarKind::IntBig
            | ScalarKind::Nat128
            | ScalarKind::NatBig
    )
}

fn scalar_literal_le(left: &ScalarLiteral, right: &ScalarLiteral) -> bool {
    match (left, right) {
        (ScalarLiteral::Decimal(left), ScalarLiteral::Decimal(right)) => left <= right,
        (ScalarLiteral::Float32(left), ScalarLiteral::Float32(right)) => left <= right,
        (ScalarLiteral::Float64(left), ScalarLiteral::Float64(right)) => left <= right,
        (ScalarLiteral::Int(left), ScalarLiteral::Int(right)) => left <= right,
        (ScalarLiteral::IntBig(left), ScalarLiteral::IntBig(right)) => left <= right,
        (ScalarLiteral::Nat(left), ScalarLiteral::Nat(right)) => left <= right,
        (ScalarLiteral::NatBig(left), ScalarLiteral::NatBig(right)) => left <= right,
        _ => false,
    }
}

fn scalar_literal_is_zero(literal: &ScalarLiteral) -> bool {
    match literal {
        ScalarLiteral::Decimal(value) => value.is_zero(),
        ScalarLiteral::Int(value) => *value == 0,
        ScalarLiteral::IntBig(value) => value == &crate::IntBig::default(),
        ScalarLiteral::Nat(value) => *value == 0,
        ScalarLiteral::NatBig(value) => value == &crate::NatBig::default(),
        ScalarLiteral::Account(_)
        | ScalarLiteral::Blob(_)
        | ScalarLiteral::Bool(_)
        | ScalarLiteral::Date(_)
        | ScalarLiteral::Duration(_)
        | ScalarLiteral::EnumUnit { .. }
        | ScalarLiteral::Float32(_)
        | ScalarLiteral::Float64(_)
        | ScalarLiteral::Principal(_)
        | ScalarLiteral::Subaccount(_)
        | ScalarLiteral::Text(_)
        | ScalarLiteral::Timestamp(_)
        | ScalarLiteral::Ulid(_)
        | ScalarLiteral::Unit(_) => false,
    }
}

#[cfg(test)]
mod tests {
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
}
