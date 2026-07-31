//! Module: node::type
//! Responsibility: derive-side node parsing.
//! Does not own: runtime schema semantics.
//! Boundary: macro metadata to node models.

use std::collections::BTreeSet;

use crate::prelude::*;

///
/// Type
///

#[derive(Clone, Debug, Default, FromMeta)]
#[darling(and_then = "Type::validate_rules")]
pub struct Type {
    #[darling(multiple, rename = "normalizer")]
    pub(crate) normalizers: Vec<TypeNormalizer>,

    #[darling(multiple, rename = "validator")]
    pub(crate) validators: Vec<TypeValidator>,

    #[darling(multiple, rename = "rule")]
    pub(crate) rules: Vec<SourceRule>,
}

impl Type {
    fn validate_rules(self) -> Result<Self, DarlingError> {
        let mut names = BTreeSet::new();
        for rule in &self.rules {
            let name = rule.name.value();
            if !names.insert(name.clone()) {
                return Err(DarlingError::custom(format!(
                    "duplicate durable rule name '{name}' on one type"
                ))
                .with_span(&rule.name));
            }
        }
        Ok(self)
    }
}

impl HasSchemaPart for Type {
    fn schema_part(&self) -> TokenStream {
        let normalizers = quote_slice(&self.normalizers, TypeNormalizer::schema_part);
        let validators = quote_slice(&self.validators, TypeValidator::schema_part);
        let rules = quote_slice(&self.rules, SourceRule::schema_part);

        // quote
        quote! {
            ::icydb_model::node::Type::new(#normalizers, #validators, #rules)
        }
    }
}

///
/// SourceRule
///

#[derive(Clone, Debug)]
pub struct SourceRule {
    pub(crate) name: LitStr,
    pub(crate) operation: SourceRuleOperation,
}

impl FromMeta for SourceRule {
    fn from_list(items: &[darling::ast::NestedMeta]) -> Result<Self, DarlingError> {
        let input = SourceRuleInput::from_list(items)?;
        icydb_schema::RuleSourceKey::try_new(input.name.value()).map_err(|error| {
            DarlingError::custom(format!("invalid durable rule name: {error}"))
                .with_span(&input.name)
        })?;

        let operations = [
            input
                .length_range_inclusive
                .map(|bounds| SourceRuleOperation::LengthRangeInclusive {
                    min: bounds.min,
                    max: bounds.max,
                }),
            input
                .multiple_of
                .map(|operand| SourceRuleOperation::MultipleOf {
                    divisor: operand.divisor,
                }),
            input.numeric_maximum_inclusive.map(|operand| {
                SourceRuleOperation::NumericMaximumInclusive {
                    value: operand.value,
                }
            }),
            input.numeric_minimum_inclusive.map(|operand| {
                SourceRuleOperation::NumericMinimumInclusive {
                    value: operand.value,
                }
            }),
            input.numeric_range_inclusive.map(|bounds| {
                SourceRuleOperation::NumericRangeInclusive {
                    min: bounds.min,
                    max: bounds.max,
                }
            }),
        ];
        let mut operations = operations.into_iter().flatten();
        let operation = operations.next().ok_or_else(|| {
            DarlingError::custom("rule(...) requires exactly one typed operation")
                .with_span(&input.name)
        })?;
        if operations.next().is_some() {
            return Err(
                DarlingError::custom("rule(...) accepts exactly one typed operation")
                    .with_span(&input.name),
            );
        }
        operation.validate_shape(&input.name)?;

        Ok(Self {
            name: input.name,
            operation,
        })
    }
}

impl HasSchemaPart for SourceRule {
    fn schema_part(&self) -> TokenStream {
        let name = &self.name;
        let operation = self.operation.schema_part();

        quote! {
            ::icydb_model::node::SourceRule::new(#name, #operation)
        }
    }
}

#[derive(Clone, Debug, FromMeta)]
struct SourceRuleInput {
    name: LitStr,

    #[darling(default)]
    length_range_inclusive: Option<RangeOperands>,

    #[darling(default)]
    multiple_of: Option<MultipleOfOperand>,

    #[darling(default)]
    numeric_maximum_inclusive: Option<ValueOperand>,

    #[darling(default)]
    numeric_minimum_inclusive: Option<ValueOperand>,

    #[darling(default)]
    numeric_range_inclusive: Option<RangeOperands>,
}

#[derive(Clone, Debug, FromMeta)]
struct RangeOperands {
    min: RuleNumber,
    max: RuleNumber,
}

#[derive(Clone, Debug, FromMeta)]
struct ValueOperand {
    value: RuleNumber,
}

#[derive(Clone, Debug, FromMeta)]
struct MultipleOfOperand {
    divisor: RuleNumber,
}

#[derive(Clone, Debug)]
pub(crate) struct RuleNumber {
    literal: String,
    kind: RuleNumberKind,
}

#[derive(Clone, Copy, Debug)]
enum RuleNumberKind {
    Integer,
    Decimal,
    Float32(f32),
    Float64(f64),
}

impl FromMeta for RuleNumber {
    fn from_value(value: &syn::Lit) -> Result<Self, DarlingError> {
        let literal = match value {
            syn::Lit::Int(value) => value.to_string(),
            syn::Lit::Float(value) => value.to_string(),
            syn::Lit::Str(value) => value.value(),
            _ => return Err(DarlingError::custom("expected numeric literal")),
        };
        Self::parse(literal.as_str())
    }
}

impl RuleNumber {
    fn parse(literal: &str) -> Result<Self, DarlingError> {
        let literal = literal.replace('_', "");
        let parsed = ArgNumber::parse_numeric_string(literal.as_str())?;
        let (literal, kind) = match parsed {
            ArgNumber::Float32(value) if value.is_finite() => {
                (value.to_string(), RuleNumberKind::Float32(value))
            }
            ArgNumber::Float64(value) if literal.ends_with("f64") => {
                if !value.is_finite() {
                    return Err(DarlingError::custom("numeric literal must be finite"));
                }
                (value.to_string(), RuleNumberKind::Float64(value))
            }
            ArgNumber::Float64(value) if value.is_finite() => (literal, RuleNumberKind::Decimal),
            ArgNumber::Float32(_) | ArgNumber::Float64(_) => {
                return Err(DarlingError::custom("numeric literal must be finite"));
            }
            ArgNumber::Int8(value) => (value.to_string(), RuleNumberKind::Integer),
            ArgNumber::Int16(value) => (value.to_string(), RuleNumberKind::Integer),
            ArgNumber::Int32(value) => (value.to_string(), RuleNumberKind::Integer),
            ArgNumber::Int64(value) => (value.to_string(), RuleNumberKind::Integer),
            ArgNumber::Int128(value) => (value.to_string(), RuleNumberKind::Integer),
            ArgNumber::Nat8(value) => (value.to_string(), RuleNumberKind::Integer),
            ArgNumber::Nat16(value) => (value.to_string(), RuleNumberKind::Integer),
            ArgNumber::Nat32(value) => (value.to_string(), RuleNumberKind::Integer),
            ArgNumber::Nat64(value) => (value.to_string(), RuleNumberKind::Integer),
            ArgNumber::Nat128(value) => (value.to_string(), RuleNumberKind::Integer),
        };
        Ok(Self { literal, kind })
    }

    fn schema_part(&self) -> TokenStream {
        let literal = &self.literal;
        match self.kind {
            RuleNumberKind::Integer => {
                quote!(::icydb_model::node::RuleNumber::Integer(#literal))
            }
            RuleNumberKind::Decimal => {
                quote!(::icydb_model::node::RuleNumber::Decimal(#literal))
            }
            RuleNumberKind::Float32(value) => {
                quote!(::icydb_model::node::RuleNumber::Float32(#value))
            }
            RuleNumberKind::Float64(value) => {
                quote!(::icydb_model::node::RuleNumber::Float64(#value))
            }
        }
    }

    fn as_u64(&self) -> Option<u64> {
        matches!(self.kind, RuleNumberKind::Integer)
            .then(|| self.literal.parse::<u64>().ok())
            .flatten()
    }

    fn is_zero(&self) -> bool {
        match self.kind {
            RuleNumberKind::Integer => {
                self.literal.parse::<i128>().is_ok_and(|value| value == 0)
                    || self.literal.parse::<u128>().is_ok_and(|value| value == 0)
            }
            RuleNumberKind::Decimal => self
                .literal
                .parse::<icydb_schema::Decimal>()
                .is_ok_and(|value| value.is_zero()),
            RuleNumberKind::Float32(value) => value == 0.0,
            RuleNumberKind::Float64(value) => value == 0.0,
        }
    }
}

#[derive(Clone, Debug)]
pub enum SourceRuleOperation {
    LengthRangeInclusive { min: RuleNumber, max: RuleNumber },
    MultipleOf { divisor: RuleNumber },
    NumericMaximumInclusive { value: RuleNumber },
    NumericMinimumInclusive { value: RuleNumber },
    NumericRangeInclusive { min: RuleNumber, max: RuleNumber },
}

impl SourceRuleOperation {
    fn validate_shape(&self, span: &LitStr) -> Result<(), DarlingError> {
        match self {
            Self::LengthRangeInclusive { min, max } => {
                let min = rule_length_bound(min).ok_or_else(|| {
                    DarlingError::custom(
                        "length_range_inclusive operands must be nonnegative integers within u64",
                    )
                    .with_span(span)
                })?;
                let max = rule_length_bound(max).ok_or_else(|| {
                    DarlingError::custom(
                        "length_range_inclusive operands must be nonnegative integers within u64",
                    )
                    .with_span(span)
                })?;
                if min > max {
                    return Err(
                        DarlingError::custom("length_range_inclusive requires min <= max")
                            .with_span(span),
                    );
                }
            }
            Self::MultipleOf { divisor } => {
                if rule_number_is_zero(divisor) {
                    return Err(
                        DarlingError::custom("multiple_of divisor must be nonzero").with_span(span)
                    );
                }
            }
            Self::NumericMaximumInclusive { .. }
            | Self::NumericMinimumInclusive { .. }
            | Self::NumericRangeInclusive { .. } => {}
        }
        Ok(())
    }
}

impl HasSchemaPart for SourceRuleOperation {
    fn schema_part(&self) -> TokenStream {
        match self {
            Self::LengthRangeInclusive { min, max } => {
                let min = min.schema_part();
                let max = max.schema_part();
                quote! {
                    ::icydb_model::node::SourceRuleAuthoringOperation::LengthRangeInclusive {
                        min: #min,
                        max: #max,
                    }
                }
            }
            Self::MultipleOf { divisor } => {
                let divisor = divisor.schema_part();
                quote! {
                    ::icydb_model::node::SourceRuleAuthoringOperation::MultipleOf {
                        divisor: #divisor,
                    }
                }
            }
            Self::NumericMaximumInclusive { value } => {
                let value = value.schema_part();
                quote! {
                    ::icydb_model::node::SourceRuleAuthoringOperation::NumericMaximumInclusive {
                        value: #value,
                    }
                }
            }
            Self::NumericMinimumInclusive { value } => {
                let value = value.schema_part();
                quote! {
                    ::icydb_model::node::SourceRuleAuthoringOperation::NumericMinimumInclusive {
                        value: #value,
                    }
                }
            }
            Self::NumericRangeInclusive { min, max } => {
                let min = min.schema_part();
                let max = max.schema_part();
                quote! {
                    ::icydb_model::node::SourceRuleAuthoringOperation::NumericRangeInclusive {
                        min: #min,
                        max: #max,
                    }
                }
            }
        }
    }
}

fn rule_length_bound(value: &RuleNumber) -> Option<u64> {
    value.as_u64()
}

fn rule_number_is_zero(value: &RuleNumber) -> bool {
    value.is_zero()
}

///
/// TypeNormalizer
///

#[derive(Clone, Debug, FromMeta)]
pub struct TypeNormalizer {
    pub(crate) path: Path,

    #[darling(default)]
    pub(crate) args: Args,
}

impl TypeNormalizer {
    pub fn quote_constructor(&self) -> TokenStream {
        let path = &self.path;
        let args = self.args.iter();

        if self.args.is_empty() {
            quote! { #path }
        } else {
            quote! { #path::new(#(#args),*) }
        }
    }
}

impl HasSchemaPart for TypeNormalizer {
    fn schema_part(&self) -> TokenStream {
        let path = quote_one(&self.path, to_path);
        let args = &self.args.schema_part();

        // quote
        quote! {
            ::icydb_model::node::TypeNormalizer::new(#path, #args)
        }
    }
}

///
/// TypeValidator
///

#[derive(Clone, Debug, FromMeta)]
pub struct TypeValidator {
    pub(crate) path: Path,

    #[darling(default)]
    pub(crate) args: Args,
}

impl TypeValidator {
    pub fn quote_constructor(&self) -> TokenStream {
        let path = &self.path;
        let args = self.args.iter();

        if self.args.is_empty() {
            quote! { #path }
        } else {
            quote! { #path::new(#(#args),*) }
        }
    }
}

impl HasSchemaPart for TypeValidator {
    fn schema_part(&self) -> TokenStream {
        let path = quote_one(&self.path, to_path);
        let args = &self.args.schema_part();

        // quote
        quote! {
            ::icydb_model::node::TypeValidator::new(#path, #args)
        }
    }
}

#[cfg(test)]
mod tests {
    use darling::{FromMeta, ast::NestedMeta};
    use quote::quote;

    use super::{SourceRule, SourceRuleOperation, Type};

    fn parse_rule(tokens: proc_macro2::TokenStream) -> Result<SourceRule, darling::Error> {
        let args = NestedMeta::parse_meta_list(tokens)?;
        SourceRule::from_list(args.as_slice())
    }

    fn parse_type(tokens: proc_macro2::TokenStream) -> Result<Type, darling::Error> {
        let args = NestedMeta::parse_meta_list(tokens)?;
        Type::from_list(args.as_slice())
    }

    #[test]
    fn parses_each_closed_typed_rule_operation() {
        assert!(matches!(
            parse_rule(quote!(
                name = "length",
                length_range_inclusive(min = 1, max = 40)
            ))
            .expect("length range should parse")
            .operation,
            SourceRuleOperation::LengthRangeInclusive { .. }
        ));
        assert!(matches!(
            parse_rule(quote!(name = "step", multiple_of(divisor = 5)))
                .expect("multiple-of should parse")
                .operation,
            SourceRuleOperation::MultipleOf { .. }
        ));
        assert!(matches!(
            parse_rule(quote!(
                name = "maximum",
                numeric_maximum_inclusive(value = 100)
            ))
            .expect("numeric maximum should parse")
            .operation,
            SourceRuleOperation::NumericMaximumInclusive { .. }
        ));
        assert!(matches!(
            parse_rule(quote!(
                name = "minimum",
                numeric_minimum_inclusive(value = 0)
            ))
            .expect("numeric minimum should parse")
            .operation,
            SourceRuleOperation::NumericMinimumInclusive { .. }
        ));
        assert!(matches!(
            parse_rule(quote!(
                name = "range",
                numeric_range_inclusive(min = 0, max = 100)
            ))
            .expect("numeric range should parse")
            .operation,
            SourceRuleOperation::NumericRangeInclusive { .. }
        ));
    }

    #[test]
    fn rejects_missing_multiple_and_malformed_typed_operations() {
        assert!(parse_rule(quote!(name = "missing")).is_err());
        assert!(parse_rule(quote!(name = "unknown", unsupported_operation(value = 0))).is_err());
        assert!(
            parse_rule(quote!(
                name = "multiple",
                numeric_minimum_inclusive(value = 0),
                numeric_maximum_inclusive(value = 100)
            ))
            .is_err()
        );
        assert!(
            parse_rule(quote!(
                name = "missing_operand",
                numeric_range_inclusive(min = 0)
            ))
            .is_err()
        );
        assert!(
            parse_rule(quote!(
                name = "wrong_operand",
                numeric_minimum_inclusive(min = 0)
            ))
            .is_err()
        );
        assert!(
            parse_rule(quote!(
                name = "repeated_operand",
                numeric_minimum_inclusive(value = 0, value = 1)
            ))
            .is_err()
        );
        assert!(
            parse_rule(quote!(
                name = "reversed",
                length_range_inclusive(min = 2, max = 1)
            ))
            .is_err()
        );
        assert!(parse_rule(quote!(name = "zero", multiple_of(divisor = 0))).is_err());
    }

    #[test]
    fn rejects_invalid_and_duplicate_local_rule_names() {
        assert!(parse_rule(quote!(name = "", numeric_minimum_inclusive(value = 0))).is_err());
        assert!(
            parse_rule(quote!(
                name = "invalid name",
                numeric_minimum_inclusive(value = 0)
            ))
            .is_err()
        );
        assert!(
            parse_type(quote!(
                rule(name = "range", numeric_minimum_inclusive(value = 0)),
                rule(name = "range", numeric_maximum_inclusive(value = 100))
            ))
            .is_err()
        );
    }
}
