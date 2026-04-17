mod field;
mod text;

pub(in crate::db::predicate::parser) use field::{
    PredicateFieldOperand, TextPredicateWrapper, parse_predicate_field_operand,
};
pub(in crate::db::predicate::parser) use text::{
    eat_prefix_text_predicate_operator, parse_prefix_text_predicate, parse_starts_with_predicate,
    predicate_literal_starts,
};
