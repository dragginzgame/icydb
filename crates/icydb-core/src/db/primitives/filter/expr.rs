use crate::{
    db::primitives::filter::{Cmp, IntoFilterExpr},
    traits::FieldValue,
    value::Value,
};
use candid::CandidType;
use derive_more::{Deref, DerefMut};
use serde::{Deserialize, Serialize};
use std::ops::{BitAnd, BitOr, Not};

///
/// FilterExpr
///
/// Represents logical expressions for querying/filtering data.
///
/// Expressions can be:
/// - `True` or `False` constants
/// - Single clauses comparing a field with a value
/// - Composite expressions: `And`, `Or`, and negation `Not`.
///

#[derive(CandidType, Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub enum FilterExpr {
    #[default]
    True,
    False,
    Clause(FilterClause),
    And(Vec<Self>),
    Or(Vec<Self>),
    Not(Box<Self>),
}

impl FilterExpr {
    // --- Clause ---

    /// Create a single clause: `field cmp value`.
    pub fn clause(field: impl Into<String>, cmp: Cmp, value: impl FieldValue) -> Self {
        Self::Clause(FilterClause::new(field, cmp, value))
    }

    // --- Equality ---

    pub fn eq(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::clause(field, Cmp::Eq, value)
    }

    pub fn eq_ci(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::clause(field, Cmp::EqCi, value)
    }

    pub fn ne(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::clause(field, Cmp::Ne, value)
    }

    pub fn ne_ci(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::clause(field, Cmp::NeCi, value)
    }

    // --- Ordering ---

    pub fn lt(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::clause(field, Cmp::Lt, value)
    }

    pub fn lte(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::clause(field, Cmp::Lte, value)
    }

    pub fn gt(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::clause(field, Cmp::Gt, value)
    }

    pub fn gte(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::clause(field, Cmp::Gte, value)
    }

    // --- Text / Collection ---

    pub fn contains(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::clause(field, Cmp::Contains, value)
    }

    pub fn contains_ci(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::clause(field, Cmp::ContainsCi, value)
    }

    pub fn starts_with(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::clause(field, Cmp::StartsWith, value)
    }

    pub fn starts_with_ci(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::clause(field, Cmp::StartsWithCi, value)
    }

    pub fn ends_with(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::clause(field, Cmp::EndsWith, value)
    }

    pub fn ends_with_ci(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::clause(field, Cmp::EndsWithCi, value)
    }

    // --- Presence / Empty ---

    pub fn is_empty(field: impl Into<String>) -> Self {
        Self::clause(field, Cmp::IsEmpty, ())
    }

    pub fn is_not_empty(field: impl Into<String>) -> Self {
        Self::clause(field, Cmp::IsNotEmpty, ())
    }

    pub fn is_some(field: impl Into<String>) -> Self {
        Self::clause(field, Cmp::IsSome, ())
    }

    pub fn is_none(field: impl Into<String>) -> Self {
        Self::clause(field, Cmp::IsNone, ())
    }

    // --- Membership ---

    pub fn in_iter<I>(field: impl Into<String>, vals: I) -> Self
    where
        I: IntoIterator,
        I::Item: FieldValue,
    {
        Self::clause(
            field,
            Cmp::In,
            vals.into_iter().map(|v| v.to_value()).collect::<Vec<_>>(),
        )
    }

    pub fn in_ci_iter<I>(field: impl Into<String>, vals: I) -> Self
    where
        I: IntoIterator,
        I::Item: FieldValue,
    {
        Self::clause(
            field,
            Cmp::InCi,
            vals.into_iter().map(|v| v.to_value()).collect::<Vec<_>>(),
        )
    }

    pub fn not_in_iter<I>(field: impl Into<String>, vals: I) -> Self
    where
        I: IntoIterator,
        I::Item: FieldValue,
    {
        Self::clause(
            field,
            Cmp::NotIn,
            vals.into_iter().map(|v| v.to_value()).collect::<Vec<_>>(),
        )
    }

    pub fn any_in<I>(field: impl Into<String>, vals: I) -> Self
    where
        I: IntoIterator,
        I::Item: FieldValue,
    {
        Self::clause(
            field,
            Cmp::AnyIn,
            vals.into_iter().map(|v| v.to_value()).collect::<Vec<_>>(),
        )
    }

    pub fn all_in<I>(field: impl Into<String>, vals: I) -> Self
    where
        I: IntoIterator,
        I::Item: FieldValue,
    {
        Self::clause(
            field,
            Cmp::AllIn,
            vals.into_iter().map(|v| v.to_value()).collect::<Vec<_>>(),
        )
    }

    pub fn any_in_ci<I>(field: impl Into<String>, vals: I) -> Self
    where
        I: IntoIterator,
        I::Item: FieldValue,
    {
        Self::clause(
            field,
            Cmp::AnyInCi,
            vals.into_iter().map(|v| v.to_value()).collect::<Vec<_>>(),
        )
    }

    pub fn all_in_ci<I>(field: impl Into<String>, vals: I) -> Self
    where
        I: IntoIterator,
        I::Item: FieldValue,
    {
        Self::clause(
            field,
            Cmp::AllInCi,
            vals.into_iter().map(|v| v.to_value()).collect::<Vec<_>>(),
        )
    }

    // --- Map ---

    pub fn map_contains_key(field: impl Into<String>, key: impl FieldValue) -> Self {
        Self::clause(field, Cmp::MapContainsKey, key)
    }

    pub fn map_not_contains_key(field: impl Into<String>, key: impl FieldValue) -> Self {
        Self::clause(field, Cmp::MapNotContainsKey, key)
    }

    pub fn map_contains_value(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::clause(field, Cmp::MapContainsValue, value)
    }

    pub fn map_not_contains_value(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::clause(field, Cmp::MapNotContainsValue, value)
    }

    pub fn map_contains_entry(
        field: impl Into<String>,
        key: impl FieldValue,
        value: impl FieldValue,
    ) -> Self {
        Self::clause(
            field,
            Cmp::MapContainsEntry,
            vec![key.to_value(), value.to_value()],
        )
    }

    pub fn map_not_contains_entry(
        field: impl Into<String>,
        key: impl FieldValue,
        value: impl FieldValue,
    ) -> Self {
        Self::clause(
            field,
            Cmp::MapNotContainsEntry,
            vec![key.to_value(), value.to_value()],
        )
    }

    /// Combine two expressions into an `And` expression.
    ///
    /// This flattens nested `And`s to avoid deep nesting (e.g., `(a AND b) AND c` becomes `AND[a,b,c]`).
    #[must_use]
    pub fn and(self, other: Self) -> Self {
        match (self, other) {
            (Self::And(mut a), Self::And(mut b)) => {
                a.append(&mut b);
                Self::And(a)
            }
            (Self::And(mut a), b) => {
                a.push(b);
                Self::And(a)
            }
            (a, Self::And(mut b)) => {
                let mut list = vec![a];
                list.append(&mut b);
                Self::And(list)
            }
            (a, b) => Self::And(vec![a, b]),
        }
    }

    #[must_use]
    pub fn and_option(self, other: Option<Self>) -> Self {
        match other {
            Some(f) => self.and(f),
            None => self,
        }
    }

    /// Negate this expression.
    #[must_use]
    #[allow(clippy::should_implement_trait)]
    pub fn not(self) -> Self {
        Self::Not(Box::new(self))
    }

    /// Combine two expressions into an `Or` expression,
    /// flattening nested `Or`s similarly to `and`.
    #[must_use]
    pub fn or(self, other: Self) -> Self {
        match (self, other) {
            (Self::Or(mut a), Self::Or(mut b)) => {
                a.append(&mut b);
                Self::Or(a)
            }
            (Self::Or(mut a), b) => {
                a.push(b);
                Self::Or(a)
            }
            (a, Self::Or(mut b)) => {
                let mut list = vec![a];
                list.append(&mut b);
                Self::Or(list)
            }
            (a, b) => Self::Or(vec![a, b]),
        }
    }

    #[must_use]
    pub fn or_option(self, other: Option<Self>) -> Self {
        match other {
            Some(f) => self.or(f),
            None => self,
        }
    }

    /// Simplifies the logical expression recursively, applying rules like:
    /// - Eliminate double negation `NOT NOT x` -> `x`
    /// - Apply De Morgan's laws:
    ///   - `NOT (AND [a, b])` -> `OR [NOT a, NOT b]`
    ///   - `NOT (OR [a, b])` -> `AND [NOT a, NOT b]`
    /// - Flatten nested `And` and `Or` expressions
    /// - Remove neutral elements:
    ///   - `AND [True, x]` -> `x`
    ///   - `OR [False, x]` -> `x`
    /// - Short circuit on constants:
    ///   - `AND` with `False` -> `False`
    ///   - `OR` with `True` -> `True`
    #[must_use]
    pub fn simplify(self) -> Self {
        match self {
            Self::Not(inner) => match *inner {
                Self::True => Self::False,
                Self::False => Self::True,
                Self::Not(inner2) => (*inner2).simplify(),
                Self::And(children) => {
                    // De Morgan's: NOT(AND(...)) == OR(NOT(...))
                    Self::Or(children.into_iter().map(|c| c.not().simplify()).collect())
                }
                Self::Or(children) => {
                    // De Morgan's: NOT(OR(...)) == AND(NOT(...))
                    Self::And(children.into_iter().map(|c| c.not().simplify()).collect())
                }
                x @ Self::Clause(_) => Self::Not(Box::new(x.simplify())),
            },

            Self::And(children) => {
                // Recursively simplify and flatten `And` children
                let flat = Self::simplify_children(children, |e| matches!(e, Self::And(_)));

                // If any child is `False`, whole AND is False (short circuit)
                if flat.iter().any(|e| matches!(e, Self::False)) {
                    Self::False
                } else {
                    // Remove neutral elements `True`
                    let filtered: Vec<_> = flat
                        .into_iter()
                        .filter(|e| !matches!(e, Self::True))
                        .collect();

                    // If empty after filtering, all were True -> return True
                    match filtered.len() {
                        0 => Self::True,
                        1 => filtered.into_iter().next().unwrap(),
                        _ => Self::And(filtered),
                    }
                }
            }

            Self::Or(children) => {
                // Recursively simplify and flatten `Or` children
                let flat = Self::simplify_children(children, |e| matches!(e, Self::Or(_)));

                // If any child is `True`, whole OR is True (short circuit)
                if flat.iter().any(|e| matches!(e, Self::True)) {
                    Self::True
                } else {
                    // Remove neutral elements `False`
                    let filtered: Vec<_> = flat
                        .into_iter()
                        .filter(|e| !matches!(e, Self::False))
                        .collect();

                    // If empty after filtering, all were False -> return False
                    match filtered.len() {
                        0 => Self::False,
                        1 => filtered.into_iter().next().unwrap(),
                        _ => Self::Or(filtered),
                    }
                }
            }

            // Clauses and constants are already simplest forms
            x => x,
        }
    }

    /// Helper to simplify and flatten nested `And` or `Or` children.
    ///
    /// - `children`: the children expressions to simplify and flatten
    /// - `flatten_if`: a predicate to decide if the child should be flattened
    fn simplify_children(children: Vec<Self>, flatten_if: fn(&Self) -> bool) -> Vec<Self> {
        let mut flat = Vec::with_capacity(children.len());

        for child in children {
            let simplified = child.simplify();
            if flatten_if(&simplified) {
                if let Self::And(nested) | Self::Or(nested) = simplified {
                    flat.extend(nested);
                }
            } else {
                flat.push(simplified);
            }
        }

        flat
    }
}

impl IntoFilterExpr for FilterExpr {
    fn into_expr(self) -> FilterExpr {
        self
    }
}

///
/// Bit Operations
/// allow us to do | & and ^ on expressions
///

impl BitAnd for FilterExpr {
    type Output = Self;

    fn bitand(self, rhs: Self) -> Self::Output {
        self.and(rhs)
    }
}

impl BitOr for FilterExpr {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        self.or(rhs)
    }
}

impl Not for FilterExpr {
    type Output = Self;

    fn not(self) -> Self::Output {
        Self::Not(Box::new(self))
    }
}

///
/// FilterExprOpt
///

#[repr(transparent)]
#[derive(Clone, Debug, Deref, DerefMut, Eq, PartialEq)]
pub struct FilterExprOpt(pub Option<FilterExpr>);

impl BitAnd for FilterExprOpt {
    type Output = Self;

    fn bitand(self, rhs: Self) -> Self::Output {
        match (self.0, rhs.0) {
            (Some(a), Some(b)) => Self(Some(a & b)),
            (Some(a), None) => Self(Some(a)),
            (None, Some(b)) => Self(Some(b)),
            (None, None) => Self(None),
        }
    }
}

impl BitOr for FilterExprOpt {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        match (self.0, rhs.0) {
            (Some(a), Some(b)) => Self(Some(a | b)),
            (Some(a), None) => Self(Some(a)),
            (None, Some(b)) => Self(Some(b)),
            (None, None) => Self(None),
        }
    }
}

impl Not for FilterExprOpt {
    type Output = Self;

    fn not(self) -> Self::Output {
        Self(self.0.map(|a| !a))
    }
}

impl From<Option<FilterExpr>> for FilterExprOpt {
    fn from(opt: Option<FilterExpr>) -> Self {
        Self(opt)
    }
}

impl From<FilterExprOpt> for Option<FilterExpr> {
    fn from(opt: FilterExprOpt) -> Self {
        opt.0
    }
}

///
/// FilterClause
/// represents a basic comparison expression: `field cmp value`
///

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FilterClause {
    pub field: String,
    pub cmp: Cmp,
    pub value: Value,
}

impl FilterClause {
    #[must_use]
    pub fn new(field: impl Into<String>, cmp: Cmp, value: impl FieldValue) -> Self {
        Self {
            field: field.into(),
            cmp,
            value: value.to_value(),
        }
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;

    fn clause(field: &str) -> FilterExpr {
        FilterExpr::Clause(FilterClause::new(field, Cmp::Eq, "foo"))
    }

    #[test]
    fn base_case_constructors_cover_all_cmps() {
        fn assert_clause(expr: FilterExpr, field: &str, cmp: Cmp, value: Value) {
            match expr {
                FilterExpr::Clause(c) => {
                    assert_eq!(c.field, field);
                    assert_eq!(c.cmp, cmp);
                    assert_eq!(c.value, value);
                }
                _ => panic!("expected Clause"),
            }
        }

        assert_clause(FilterExpr::eq("a", 1), "a", Cmp::Eq, Value::Int(1));
        assert_clause(FilterExpr::ne("a", 1), "a", Cmp::Ne, Value::Int(1));
        assert_clause(
            FilterExpr::eq_ci("a", "Hello"),
            "a",
            Cmp::EqCi,
            Value::Text("Hello".to_string()),
        );
        assert_clause(
            FilterExpr::ne_ci("a", "Hello"),
            "a",
            Cmp::NeCi,
            Value::Text("Hello".to_string()),
        );
        assert_clause(FilterExpr::lt("a", 1), "a", Cmp::Lt, Value::Int(1));
        assert_clause(FilterExpr::lte("a", 1), "a", Cmp::Lte, Value::Int(1));
        assert_clause(FilterExpr::gt("a", 1), "a", Cmp::Gt, Value::Int(1));
        assert_clause(FilterExpr::gte("a", 1), "a", Cmp::Gte, Value::Int(1));

        assert_clause(
            FilterExpr::contains("a", "Hello"),
            "a",
            Cmp::Contains,
            Value::Text("Hello".to_string()),
        );
        assert_clause(
            FilterExpr::contains_ci("a", "Hello"),
            "a",
            Cmp::ContainsCi,
            Value::Text("Hello".to_string()),
        );
        assert_clause(
            FilterExpr::starts_with("a", "Hello"),
            "a",
            Cmp::StartsWith,
            Value::Text("Hello".to_string()),
        );
        assert_clause(
            FilterExpr::starts_with_ci("a", "Hello"),
            "a",
            Cmp::StartsWithCi,
            Value::Text("Hello".to_string()),
        );
        assert_clause(
            FilterExpr::ends_with("a", "Hello"),
            "a",
            Cmp::EndsWith,
            Value::Text("Hello".to_string()),
        );
        assert_clause(
            FilterExpr::ends_with_ci("a", "Hello"),
            "a",
            Cmp::EndsWithCi,
            Value::Text("Hello".to_string()),
        );

        assert_clause(FilterExpr::is_empty("a"), "a", Cmp::IsEmpty, Value::Unit);
        assert_clause(
            FilterExpr::is_not_empty("a"),
            "a",
            Cmp::IsNotEmpty,
            Value::Unit,
        );
        assert_clause(FilterExpr::is_some("a"), "a", Cmp::IsSome, Value::Unit);
        assert_clause(FilterExpr::is_none("a"), "a", Cmp::IsNone, Value::Unit);

        let list = Value::List(vec![Value::Int(1), Value::Int(2)]);
        assert_clause(FilterExpr::in_iter("a", [1, 2]), "a", Cmp::In, list.clone());
        assert_clause(
            FilterExpr::in_ci_iter("a", ["A", "B"]),
            "a",
            Cmp::InCi,
            Value::List(vec![
                Value::Text("A".to_string()),
                Value::Text("B".to_string()),
            ]),
        );
        assert_clause(
            FilterExpr::not_in_iter("a", [1, 2]),
            "a",
            Cmp::NotIn,
            list.clone(),
        );
        assert_clause(
            FilterExpr::any_in("a", [1, 2]),
            "a",
            Cmp::AnyIn,
            list.clone(),
        );
        assert_clause(
            FilterExpr::all_in("a", [1, 2]),
            "a",
            Cmp::AllIn,
            list.clone(),
        );
        assert_clause(
            FilterExpr::any_in_ci("a", ["A", "B"]),
            "a",
            Cmp::AnyInCi,
            Value::List(vec![
                Value::Text("A".to_string()),
                Value::Text("B".to_string()),
            ]),
        );
        assert_clause(
            FilterExpr::all_in_ci("a", ["A", "B"]),
            "a",
            Cmp::AllInCi,
            Value::List(vec![
                Value::Text("A".to_string()),
                Value::Text("B".to_string()),
            ]),
        );

        assert_clause(
            FilterExpr::map_contains_key("m", "k"),
            "m",
            Cmp::MapContainsKey,
            Value::Text("k".to_string()),
        );
        assert_clause(
            FilterExpr::map_not_contains_key("m", "k"),
            "m",
            Cmp::MapNotContainsKey,
            Value::Text("k".to_string()),
        );
        assert_clause(
            FilterExpr::map_contains_value("m", 1),
            "m",
            Cmp::MapContainsValue,
            Value::Int(1),
        );
        assert_clause(
            FilterExpr::map_not_contains_value("m", 1),
            "m",
            Cmp::MapNotContainsValue,
            Value::Int(1),
        );
        assert_clause(
            FilterExpr::map_contains_entry("m", "k", 1),
            "m",
            Cmp::MapContainsEntry,
            Value::List(vec![Value::Text("k".to_string()), Value::Int(1)]),
        );
        assert_clause(
            FilterExpr::map_not_contains_entry("m", "k", 1),
            "m",
            Cmp::MapNotContainsEntry,
            Value::List(vec![Value::Text("k".to_string()), Value::Int(1)]),
        );
    }

    #[test]
    fn test_simplify_and_true() {
        let expr = FilterExpr::And(vec![FilterExpr::True, clause("a")]);
        assert!(matches!(expr.simplify(), FilterExpr::Clause(_)));
    }

    #[test]
    fn test_simplify_and_false() {
        let expr = FilterExpr::And(vec![clause("a"), FilterExpr::False]);
        assert_eq!(expr.simplify(), FilterExpr::False);
    }

    #[test]
    fn test_double_negation() {
        let expr = FilterExpr::Not(Box::new(FilterExpr::Not(Box::new(clause("x")))));
        let simplified = expr.simplify();
        assert!(matches!(simplified, FilterExpr::Clause(_)));
    }

    #[test]
    fn test_nested_and_or_flatten() {
        let expr = FilterExpr::And(vec![
            clause("a"),
            FilterExpr::And(vec![clause("b"), clause("c")]),
        ]);
        let simplified = expr.simplify();

        if let FilterExpr::And(children) = simplified {
            assert_eq!(children.len(), 3);
        } else {
            panic!("Expected And");
        }
    }

    #[test]
    fn test_demorgan_not_and() {
        let expr = FilterExpr::Not(Box::new(FilterExpr::And(vec![clause("a"), clause("b")])));
        let simplified = expr.simplify();
        if let FilterExpr::Or(children) = simplified {
            assert_eq!(children.len(), 2);
        } else {
            panic!("Expected Or");
        }
    }

    #[test]
    fn test_and_with_only_true() {
        let expr = FilterExpr::And(vec![FilterExpr::True, FilterExpr::True]);
        assert_eq!(expr.simplify(), FilterExpr::True);
    }

    #[test]
    fn test_or_with_only_false() {
        let expr = FilterExpr::Or(vec![FilterExpr::False, FilterExpr::False]);
        assert_eq!(expr.simplify(), FilterExpr::False);
    }

    #[test]
    fn test_demorgan_not_or() {
        let expr = FilterExpr::Not(Box::new(FilterExpr::Or(vec![clause("a"), clause("b")])));
        let simplified = expr.simplify();
        if let FilterExpr::And(children) = simplified {
            assert_eq!(children.len(), 2);
        } else {
            panic!("Expected And");
        }
    }

    #[test]
    fn test_double_negation_complex() {
        let inner = FilterExpr::Or(vec![clause("a"), clause("b")]);
        let expr = FilterExpr::Not(Box::new(FilterExpr::Not(Box::new(inner.clone()))));
        assert_eq!(expr.simplify(), inner);
    }

    #[test]
    fn test_not_clause() {
        let expr = FilterExpr::Not(Box::new(clause("foo")));
        let simplified = expr.simplify();
        match simplified {
            FilterExpr::Not(boxed) => {
                assert!(matches!(*boxed, FilterExpr::Clause(_)));
            }
            _ => panic!("Expected Not"),
        }
    }

    #[test]
    fn test_complex_nested_expression() {
        let expr = FilterExpr::Not(Box::new(FilterExpr::And(vec![
            FilterExpr::Or(vec![
                clause("a"),
                FilterExpr::False,
                FilterExpr::Not(Box::new(clause("b"))),
                FilterExpr::Or(vec![
                    clause("c"),
                    FilterExpr::True,
                    FilterExpr::Not(Box::new(FilterExpr::False)),
                ]),
            ]),
            FilterExpr::And(vec![
                clause("d"),
                FilterExpr::True,
                FilterExpr::Not(Box::new(FilterExpr::Or(vec![
                    clause("e"),
                    FilterExpr::False,
                ]))),
            ]),
            FilterExpr::Not(Box::new(FilterExpr::Not(Box::new(clause("f"))))),
        ])));

        let simplified = expr.simplify();

        assert!(
            matches!(simplified, FilterExpr::Or(_)),
            "Expected top-level Or"
        );
        assert!(
            contains_clause_f(&simplified),
            "Simplified expression must contain clause(\"f\")"
        );
    }

    fn contains_clause_f(expr: &FilterExpr) -> bool {
        match expr {
            FilterExpr::Clause(c) => c.field == "f",
            FilterExpr::And(children) | FilterExpr::Or(children) => {
                children.iter().any(contains_clause_f)
            }
            FilterExpr::Not(inner) => contains_clause_f(inner),
            _ => false,
        }
    }

    // --- Operators: &, |, ! ---

    #[test]
    fn ops_bitor_bitand_not() {
        let f = (clause("a") & clause("b")) | !clause("c");
        match f {
            FilterExpr::Or(children) => {
                assert_eq!(children.len(), 2);
                match &children[0] {
                    FilterExpr::And(left) => assert_eq!(left.len(), 2),
                    _ => panic!("left should be And"),
                }
                assert!(matches!(&children[1], FilterExpr::Not(_)));
            }
            _ => panic!("expected Or at root"),
        }
    }

    // --- and/or flattening via operators ---

    #[test]
    fn and_flattening_via_ops() {
        let f = (clause("a") & (clause("b") & clause("c"))) & clause("d");
        match f {
            FilterExpr::And(children) => assert_eq!(children.len(), 4),
            _ => panic!("expected And"),
        }
    }

    #[test]
    fn or_flattening_via_ops() {
        let f = (clause("x") | (clause("y") | clause("z"))) | clause("w");
        match f {
            FilterExpr::Or(children) => assert_eq!(children.len(), 4),
            _ => panic!("expected Or"),
        }
    }

    // --- and_option / or_option behavior ---

    #[test]
    fn and_option_includes_when_some() {
        let base = clause("a");
        let out = base.clone().and_option(Some(clause("b")));

        match out {
            FilterExpr::And(children) => {
                assert_eq!(children.len(), 2);
                // sanity: base unchanged when using and_option on a clone
                assert!(matches!(base, FilterExpr::Clause(_)));
            }
            _ => panic!("expected And"),
        }
    }

    #[test]
    fn and_option_skips_when_none() {
        let base = clause("a");
        let out = base.clone().and_option(None);
        assert_eq!(
            format!("{out:?}"),
            format!("{base:?}"),
            "and_option(None) should be identity"
        );
    }

    #[test]
    fn or_option_includes_when_some() {
        let base = clause("x");
        let out = base.or_option(Some(clause("y")));
        match out {
            FilterExpr::Or(children) => assert_eq!(children.len(), 2),
            _ => panic!("expected Or"),
        }
    }

    #[test]
    fn or_option_skips_when_none() {
        let base = clause("x");
        let out = base.clone().or_option(None);
        assert_eq!(
            format!("{out:?}"),
            format!("{base:?}"),
            "or_option(None) should be identity"
        );
    }

    // --- constant rules under NOT ---

    #[test]
    fn not_true_false_rules() {
        assert_eq!(
            FilterExpr::Not(Box::new(FilterExpr::True)).simplify(),
            FilterExpr::False
        );
        assert_eq!(
            FilterExpr::Not(Box::new(FilterExpr::False)).simplify(),
            FilterExpr::True
        );
    }

    // --- FilterExprOpt operators ---

    #[test]
    fn opt_and_both_some() {
        let f1 = FilterExprOpt(Some(clause("a")));
        let f2 = FilterExprOpt(Some(clause("b")));
        let out = f1 & f2;
        match out.0 {
            Some(FilterExpr::And(children)) => assert_eq!(children.len(), 2),
            _ => panic!("expected Some(And)"),
        }
    }

    #[test]
    fn opt_and_left_some_right_none() {
        let f1 = FilterExprOpt(Some(clause("a")));
        let f2 = FilterExprOpt(None);
        let out = f1 & f2;
        assert!(matches!(out.0, Some(FilterExpr::Clause(_))));
    }

    #[test]
    fn opt_and_left_none_right_some() {
        let f1 = FilterExprOpt(None);
        let f2 = FilterExprOpt(Some(clause("b")));
        let out = f1 & f2;
        assert!(matches!(out.0, Some(FilterExpr::Clause(_))));
    }

    #[test]
    fn opt_and_both_none() {
        let f1 = FilterExprOpt(None);
        let f2 = FilterExprOpt(None);
        let out = f1 & f2;
        assert!(out.0.is_none());
    }

    #[test]
    fn opt_or_both_some() {
        let f1 = FilterExprOpt(Some(clause("x")));
        let f2 = FilterExprOpt(Some(clause("y")));
        let out = f1 | f2;
        match out.0 {
            Some(FilterExpr::Or(children)) => assert_eq!(children.len(), 2),
            _ => panic!("expected Some(Or)"),
        }
    }

    #[test]
    fn opt_or_left_some_right_none() {
        let f1 = FilterExprOpt(Some(clause("x")));
        let f2 = FilterExprOpt(None);
        let out = f1 | f2;
        assert!(matches!(out.0, Some(FilterExpr::Clause(_))));
    }

    #[test]
    fn opt_or_left_none_right_some() {
        let f1 = FilterExprOpt(None);
        let f2 = FilterExprOpt(Some(clause("y")));
        let out = f1 | f2;
        assert!(matches!(out.0, Some(FilterExpr::Clause(_))));
    }

    #[test]
    fn opt_or_both_none() {
        let f1 = FilterExprOpt(None);
        let f2 = FilterExprOpt(None);
        let out = f1 | f2;
        assert!(out.0.is_none());
    }

    #[test]
    fn opt_not_some() {
        let f = FilterExprOpt(Some(clause("n")));
        let out = !f;
        match out.0 {
            Some(FilterExpr::Not(inner)) => assert!(matches!(*inner, FilterExpr::Clause(_))),
            _ => panic!("expected Some(Not(Clause))"),
        }
    }

    #[test]
    fn opt_not_none() {
        let f = FilterExprOpt(None);
        let out = !f;
        assert!(out.0.is_none(), "Negating None should stay None");
    }
}
