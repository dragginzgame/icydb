use crate::{
    db::primitives::filter::Cmp,
    types::{Account, Principal, Ulid},
    value::{Value, ValueFamily, ValueFamilyExt},
};
use std::{cmp::Ordering, convert::TryFrom, str::FromStr};

use super::text::coerce_text;

///
/// FamilyPair
///
pub struct FamilyPair {
    pub left: ValueFamily,
    pub right: ValueFamily,
}

impl FamilyPair {
    #[must_use]
    pub fn new(left: &Value, right: &Value) -> Self {
        Self {
            left: left.family(),
            right: right.family(),
        }
    }
}

///
/// Basic coercion dispatcher (no NEW behaviour yet)
///
#[must_use]
pub fn coerce_basic(left: &Value, right: &Value, cmp: Cmp) -> Option<bool> {
    let pair = FamilyPair::new(left, right);

    match (pair.left, pair.right) {
        // numeric <-> numeric
        (ValueFamily::Numeric, ValueFamily::Numeric) => {
            if let Some(ord) = left.cmp_numeric(right) {
                return Some(cmp.compare_order(ord));
            }

            // existing length-based ordering for collections
            if let Some(ord) = cmp_collection_len(left, right) {
                return Some(cmp.compare_order(ord));
            }

            None
        }

        // text <-> text (CS/CI handled in text module)
        (ValueFamily::Textual, ValueFamily::Textual) => coerce_text(left, right, cmp),

        // enum <-> enum
        (ValueFamily::Enum, ValueFamily::Enum) => coerce_enum(left, right, cmp),

        // collection membership
        (ValueFamily::Collection, _) | (_, ValueFamily::Collection) => {
            coerce_collection(left, right, cmp)
        }

        // identifier/text special case — ULID <-> Text only (existing behaviour)
        (ValueFamily::Identifier, ValueFamily::Textual)
        | (ValueFamily::Textual, ValueFamily::Identifier) => {
            coerce_identifier_text(left, right, cmp)
        }

        _ => None,
    }
}

///
/// Collection length comparison (existing behaviour)
///
fn cmp_collection_len(left: &Value, right: &Value) -> Option<Ordering> {
    match left {
        Value::List(items) => {
            let len = i64::try_from(items.len()).ok()?;
            Value::Int(len).cmp_numeric(right)
        }
        _ => None,
    }
}

///
/// Identifier <-> Text equality/inequality (Ulid/Principal/Account)
///
fn coerce_identifier_text(left: &Value, right: &Value, cmp: Cmp) -> Option<bool> {
    let parse_ulid = |s: &str| Ulid::from_str(s).ok();
    let parse_principal = |s: &str| Principal::from_str(s).ok();
    let parse_account = |s: &str| Account::from_str(s).ok();

    let parsed_eq = match (left, right) {
        (Value::Ulid(lhs), Value::Text(s)) | (Value::Text(s), Value::Ulid(lhs)) => {
            parse_ulid(s).map(|rhs| rhs == *lhs)
        }
        (Value::Principal(lhs), Value::Text(s)) | (Value::Text(s), Value::Principal(lhs)) => {
            parse_principal(s).map(|rhs| rhs == *lhs)
        }
        (Value::Account(lhs), Value::Text(s)) | (Value::Text(s), Value::Account(lhs)) => {
            parse_account(s).map(|rhs| rhs == *lhs)
        }
        _ => None,
    }?;

    match cmp {
        Cmp::Eq => Some(parsed_eq),
        Cmp::Ne => Some(!parsed_eq),
        _ => None,
    }
}

///
/// Enum checking
///
fn coerce_enum(left: &Value, right: &Value, cmp: Cmp) -> Option<bool> {
    let paths_compatible = |l: &Option<String>, r: &Option<String>| match (l, r) {
        (Some(lp), Some(rp)) => lp == rp, // strict: both specified and must match
        _ => true,                        // loose: either side missing path → match on variant only
    };

    match (left, right) {
        (Value::Enum(l), Value::Enum(r)) if paths_compatible(&l.path, &r.path) => match cmp {
            Cmp::Eq => Some(l.variant == r.variant),
            Cmp::Ne => Some(l.variant != r.variant),
            _ => None,
        },
        _ => None,
    }
}

///
/// Collection membership using Value helpers
///
fn coerce_collection(actual: &Value, expected: &Value, cmp: Cmp) -> Option<bool> {
    match cmp {
        Cmp::AllIn => actual.contains_all(expected),
        Cmp::AnyIn => actual.contains_any(expected),
        Cmp::Contains => actual.contains(expected),
        Cmp::In => actual.in_list(expected),

        // Negated membership
        Cmp::NotIn => actual.in_list(expected).map(|v| !v),

        // CI variants
        Cmp::AllInCi => actual.contains_all_ci(expected),
        Cmp::AnyInCi => actual.contains_any_ci(expected),
        Cmp::InCi => actual.in_list_ci(expected),

        Cmp::IsEmpty => actual.is_empty(),
        Cmp::IsNotEmpty => actual.is_not_empty(),
        _ => None,
    }
}
