use crate::prelude::*;

///
/// Type
///
/// Canonical runtime type descriptor for one schema node's attached normalizers
/// and validators.
///

#[derive(Clone, Debug, Serialize)]
pub struct Type {
    #[serde(skip_serializing_if = "<[_]>::is_empty")]
    normalizers: &'static [TypeNormalizer],

    #[serde(skip_serializing_if = "<[_]>::is_empty")]
    validators: &'static [TypeValidator],

    #[serde(skip_serializing_if = "<[_]>::is_empty")]
    rules: &'static [SourceRule],
}

impl Type {
    #[must_use]
    pub const fn new(
        normalizers: &'static [TypeNormalizer],
        validators: &'static [TypeValidator],
        rules: &'static [SourceRule],
    ) -> Self {
        Self {
            normalizers,
            validators,
            rules,
        }
    }

    #[must_use]
    pub const fn normalizers(&self) -> &'static [TypeNormalizer] {
        self.normalizers
    }

    #[must_use]
    pub const fn validators(&self) -> &'static [TypeValidator] {
        self.validators
    }

    /// Borrow explicitly declared durable rules.
    #[must_use]
    pub const fn rules(&self) -> &'static [SourceRule] {
        self.rules
    }
}

impl ValidateNode for Type {}

impl VisitableNode for Type {
    fn drive<V: Visitor>(&self, v: &mut V) {
        for node in self.normalizers() {
            node.accept(v);
        }
        for node in self.validators() {
            node.accept(v);
        }
        for node in self.rules() {
            node.accept(v);
        }
    }
}

///
/// SourceRule
///
/// Compiler-authored durable rule template carried by one reusable type.
/// Fragment lowering instantiates it for each persisted field use; it is not
/// an application callback or a database runtime evaluator.
///

#[derive(Clone, Debug, Serialize)]
pub struct SourceRule {
    name: &'static str,
    operation: SourceRuleAuthoringOperation,
}

impl SourceRule {
    /// Construct one explicit reusable rule template.
    #[must_use]
    pub const fn new(name: &'static str, operation: SourceRuleAuthoringOperation) -> Self {
        Self { name, operation }
    }

    /// Return the current declared rule name.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        self.name
    }

    /// Borrow the frozen rule operation and its named operands.
    #[must_use]
    pub const fn operation(&self) -> &SourceRuleAuthoringOperation {
        &self.operation
    }
}

impl ValidateNode for SourceRule {
    fn validate(&self) -> Result<(), ErrorTree> {
        let mut errs = ErrorTree::new();
        validate_source_name(
            &mut errs,
            "rule",
            self.name(),
            icydb_schema::RuleSourceKey::try_new,
        );
        if let Err(message) = self.operation().validate_shape() {
            err!(errs, "rule '{}': {message}", self.name());
        }
        errs.result()
    }
}

impl VisitableNode for SourceRule {}

///
/// SourceRuleAuthoringOperation
///
/// Closed authoring vocabulary with operation-specific named operands.
/// Accepted schema owns runtime evaluation after fragment lowering.
///

#[derive(Clone, Debug, Serialize)]
pub enum SourceRuleAuthoringOperation {
    /// Inclusive character/octet/collection length range.
    LengthRangeInclusive {
        /// Inclusive minimum logical length.
        min: RuleNumber,
        /// Inclusive maximum logical length.
        max: RuleNumber,
    },
    /// Exact integer or decimal multiple-of divisor.
    MultipleOf {
        /// Nonzero exact divisor admitted against the target kind during lowering.
        divisor: RuleNumber,
    },
    /// Inclusive numeric maximum.
    NumericMaximumInclusive {
        /// Exact upper-bound literal admitted against the target kind during lowering.
        value: RuleNumber,
    },
    /// Inclusive numeric minimum.
    NumericMinimumInclusive {
        /// Exact lower-bound literal admitted against the target kind during lowering.
        value: RuleNumber,
    },
    /// Inclusive numeric range.
    NumericRangeInclusive {
        /// Exact lower-bound literal admitted against the target kind during lowering.
        min: RuleNumber,
        /// Exact upper-bound literal admitted against the target kind during lowering.
        max: RuleNumber,
    },
}

impl SourceRuleAuthoringOperation {
    fn validate_shape(&self) -> Result<(), &'static str> {
        match self {
            Self::LengthRangeInclusive { min, max } => {
                let min = rule_length_bound(min).ok_or(
                    "length_range_inclusive operands must be nonnegative integers within u64",
                )?;
                let max = rule_length_bound(max).ok_or(
                    "length_range_inclusive operands must be nonnegative integers within u64",
                )?;
                if min > max {
                    return Err("length_range_inclusive requires min <= max");
                }
            }
            Self::MultipleOf { divisor } => {
                if !rule_number_is_valid(divisor) {
                    return Err("multiple_of divisor must be a valid numeric literal");
                }
                if rule_number_is_zero(divisor) {
                    return Err("multiple_of divisor must be nonzero");
                }
            }
            Self::NumericMaximumInclusive { value } | Self::NumericMinimumInclusive { value } => {
                if !rule_number_is_valid(value) {
                    return Err("numeric rule value must be a valid numeric literal");
                }
            }
            Self::NumericRangeInclusive { min, max } => {
                if !rule_number_is_valid(min) || !rule_number_is_valid(max) {
                    return Err("numeric range operands must be valid numeric literals");
                }
            }
        }
        Ok(())
    }
}

/// One exact, operation-owned numeric literal emitted by the derive parser.
///
/// Unsuffixed decimal text stays textual until it is bound to the declared
/// primitive. This prevents decimal rules from passing through binary float
/// conversion before accepted-schema admission.
#[derive(Clone, Debug, Serialize)]
pub enum RuleNumber {
    /// Canonical signed or unsigned integer text.
    Integer(&'static str),
    /// Exact unsuffixed base-10 decimal text.
    Decimal(&'static str),
    /// Explicit `f32` literal.
    Float32(f32),
    /// Explicit `f64` literal.
    Float64(f64),
}

fn rule_length_bound(value: &RuleNumber) -> Option<u64> {
    match value {
        RuleNumber::Integer(value) => value.parse().ok(),
        RuleNumber::Decimal(_) | RuleNumber::Float32(_) | RuleNumber::Float64(_) => None,
    }
}

fn rule_number_is_zero(value: &RuleNumber) -> bool {
    match value {
        RuleNumber::Integer(value) => {
            value.parse::<i128>().is_ok_and(|value| value == 0)
                || value.parse::<u128>().is_ok_and(|value| value == 0)
        }
        RuleNumber::Decimal(value) => value
            .parse::<icydb_schema::Decimal>()
            .is_ok_and(|value| value.is_zero()),
        RuleNumber::Float32(value) => *value == 0.0,
        RuleNumber::Float64(value) => *value == 0.0,
    }
}

fn rule_number_is_valid(value: &RuleNumber) -> bool {
    match value {
        RuleNumber::Integer(value) => {
            value.parse::<i128>().is_ok() || value.parse::<u128>().is_ok()
        }
        RuleNumber::Decimal(value) => value.parse::<icydb_schema::Decimal>().is_ok(),
        RuleNumber::Float32(value) => value.is_finite(),
        RuleNumber::Float64(value) => value.is_finite(),
    }
}

///
/// TypeNormalizer
///
/// Reference to one normalizer node plus its bound argument list.
///

#[derive(Clone, Debug, Serialize)]
pub struct TypeNormalizer {
    path: &'static str,
    args: Args,
}

impl TypeNormalizer {
    #[must_use]
    pub const fn new(path: &'static str, args: Args) -> Self {
        Self { path, args }
    }

    #[must_use]
    pub const fn path(&self) -> &'static str {
        self.path
    }

    #[must_use]
    pub const fn args(&self) -> &Args {
        &self.args
    }
}

impl ValidateNode for TypeNormalizer {
    fn validate(&self) -> Result<(), ErrorTree> {
        let mut errs = ErrorTree::new();

        // Resolve the referenced normalizer path against the schema graph.
        let res = schema_read().check_node_as::<Normalizer>(self.path());
        if let Err(e) = res {
            errs.add(e.to_string());
        }

        errs.result()
    }
}

impl VisitableNode for TypeNormalizer {}

///
/// TypeValidator
///
/// Reference to one validator node plus its bound argument list.
///

#[derive(Clone, Debug, Serialize)]
pub struct TypeValidator {
    path: &'static str,
    args: Args,
}

impl TypeValidator {
    #[must_use]
    pub const fn new(path: &'static str, args: Args) -> Self {
        Self { path, args }
    }

    #[must_use]
    pub const fn path(&self) -> &'static str {
        self.path
    }

    #[must_use]
    pub const fn args(&self) -> &Args {
        &self.args
    }
}

impl ValidateNode for TypeValidator {
    fn validate(&self) -> Result<(), ErrorTree> {
        let mut errs = ErrorTree::new();

        // Resolve the referenced validator path against the schema graph.
        let res = schema_read().check_node_as::<Validator>(self.path());
        if let Err(e) = res {
            errs.add(e.to_string());
        }

        errs.result()
    }
}

impl VisitableNode for TypeValidator {}

#[cfg(test)]
mod tests {
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
}
