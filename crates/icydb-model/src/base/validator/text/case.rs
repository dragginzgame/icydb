//! Module: base::validator::text::case
//!
//! Responsibility: base validator definitions.
//! Does not own: normalization policy, persistence, or schema mutation semantics.
//! Boundary: reports typed visitor issues for facade schema values.

use crate::{prelude::*, visitor::Validator};

use convert_case::{Case, Casing};

///
/// Camel
///

#[validator]
pub struct Camel;

impl Validator<str> for Camel {
    fn validate(&self, s: &str, ctx: &mut dyn VisitorContext) {
        if s.to_case(Case::Camel) != s {
            ctx.issue(format!("'{s}' is not camelCase"));
        }
    }
}

///
/// Kebab
///

#[validator]
pub struct Kebab;

impl Validator<str> for Kebab {
    fn validate(&self, s: &str, ctx: &mut dyn VisitorContext) {
        if s.to_case(Case::Kebab) != s {
            ctx.issue(format!("'{s}' is not kebab-case"));
        }
    }
}

///
/// Lower
///

#[validator]
pub struct Lower;

impl Validator<str> for Lower {
    fn validate(&self, s: &str, ctx: &mut dyn VisitorContext) {
        if s.to_case(Case::Lower) != s {
            ctx.issue(format!("'{s}' is not lower case"));
        }
    }
}

///
/// LowerUscore
///

#[validator]
pub struct LowerUscore;

impl Validator<str> for LowerUscore {
    fn validate(&self, s: &str, ctx: &mut dyn VisitorContext) {
        if !s.chars().all(|c| c.is_lowercase() || c == '_') {
            ctx.issue(format!("'{s}' is not lower case with underscores"));
        }
    }
}

///
/// Sentence
///

#[validator]
pub struct Sentence;

impl Validator<str> for Sentence {
    fn validate(&self, s: &str, ctx: &mut dyn VisitorContext) {
        if s.to_case(Case::Sentence) != s {
            ctx.issue(format!("'{s}' is not Sentence case"));
        }
    }
}

///
/// Snake
///

#[validator]
pub struct Snake;

impl Validator<str> for Snake {
    fn validate(&self, s: &str, ctx: &mut dyn VisitorContext) {
        if s.to_case(Case::Snake) != s {
            ctx.issue(format!("'{s}' is not snake_case"));
        }
    }
}

///
/// Title
///

#[validator]
pub struct Title;

impl Validator<str> for Title {
    fn validate(&self, s: &str, ctx: &mut dyn VisitorContext) {
        if s.to_case(Case::Title) != s {
            ctx.issue(format!("'{s}' is not Title Case"));
        }
    }
}

///
/// Upper
///

#[validator]
pub struct Upper;

impl Validator<str> for Upper {
    fn validate(&self, s: &str, ctx: &mut dyn VisitorContext) {
        if s.to_case(Case::Upper) != s {
            ctx.issue(format!("'{s}' is not UPPER CASE"));
        }
    }
}

///
/// UpperCamel
///

#[validator]
pub struct UpperCamel;

impl Validator<str> for UpperCamel {
    fn validate(&self, s: &str, ctx: &mut dyn VisitorContext) {
        if s.to_case(Case::UpperCamel) != s {
            ctx.issue(format!("'{s}' is not UpperCamelCase"));
        }
    }
}

///
/// UpperKebab
///

#[validator]
pub struct UpperKebab;

impl Validator<str> for UpperKebab {
    fn validate(&self, s: &str, ctx: &mut dyn VisitorContext) {
        if s.to_case(Case::UpperKebab) != s {
            ctx.issue(format!("'{s}' is not UPPER-KEBAB-CASE"));
        }
    }
}

///
/// UpperSnake
///

#[validator]
pub struct UpperSnake;

impl Validator<str> for UpperSnake {
    fn validate(&self, s: &str, ctx: &mut dyn VisitorContext) {
        if s.to_case(Case::UpperSnake) != s {
            ctx.issue(format!("'{s}' is not UPPER_SNAKE_CASE"));
        }
    }
}
