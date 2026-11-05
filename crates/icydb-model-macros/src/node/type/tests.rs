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
