pub mod case;
pub mod color;

use crate::{
    core::{traits::Validator, visitor::VisitorContext},
    design::prelude::*,
};

///
/// AlphaUscore
/// this doesn't force ASCII; it uses Unicode `is_alphabetic`
/// ASCII is handled by a separate validator
///

#[validator]
pub struct AlphaUscore;

impl Validator<str> for AlphaUscore {
    fn validate(&self, s: &str, ctx: &mut dyn VisitorContext) {
        if !s.chars().all(|c| c.is_alphabetic() || c == '_') {
            ctx.issue(format!("'{s}' is not alphabetic with underscores"));
        }
    }
}

///
/// AlphanumUscore
///

#[validator]
pub struct AlphanumUscore;

impl Validator<str> for AlphanumUscore {
    fn validate(&self, s: &str, ctx: &mut dyn VisitorContext) {
        if !s.chars().all(|c| c.is_alphanumeric() || c == '_') {
            ctx.issue(format!("'{s}' is not alphanumeric with underscores"));
        }
    }
}

///
/// Ascii
///

#[validator]
pub struct Ascii;

impl Validator<str> for Ascii {
    fn validate(&self, s: &str, ctx: &mut dyn VisitorContext) {
        if !s.is_ascii() {
            ctx.issue("string contains non-ascii characters".to_string());
        }
    }
}
