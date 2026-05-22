use crate::{design::prelude::*, traits::Validator};
use icydb_utils::{Case, Casing};

///
/// Camel
///

#[validator]
pub struct Camel;

impl Validator<str> for Camel {
    fn validate(&self, s: &str, ctx: &mut dyn VisitorContext) {
        if !s.is_case(Case::Camel) {
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
        if !s.is_case(Case::Kebab) {
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
        if !s.is_case(Case::Lower) {
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
        if !s.is_case(Case::Sentence) {
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
        if !s.is_case(Case::Snake) {
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
        if !s.is_case(Case::Title) {
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
        if !s.is_case(Case::Upper) {
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
        if !s.is_case(Case::UpperCamel) {
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
        if !s.is_case(Case::UpperKebab) {
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
        if !s.is_case(Case::UpperSnake) {
            ctx.issue(format!("'{s}' is not UPPER_SNAKE_CASE"));
        }
    }
}
