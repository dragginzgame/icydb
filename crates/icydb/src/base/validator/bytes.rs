use crate::{
    core::{traits::Validator, visitor::VisitorContext},
    design::prelude::*,
};

///
/// Utf8
///

#[validator]
pub struct Utf8;

impl Validator<[u8]> for Utf8 {
    fn validate(&self, bytes: &[u8], ctx: &mut dyn VisitorContext) {
        if std::str::from_utf8(bytes).is_err() {
            ctx.issue("invalid UTF-8 data".to_string());
        }
    }
}
