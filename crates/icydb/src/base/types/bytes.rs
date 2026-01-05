use crate::design::prelude::*;

///
/// Utf8
///

#[newtype(
    primitive = "Blob",
    item(prim = "Blob"),
    traits(remove(ValidateCustom))
)]
pub struct Utf8;

impl ValidateCustom for Utf8 {
    fn validate_custom(&self, ctx: &mut dyn VisitorContext) {
        if let Err(msg) = base::validator::bytes::Utf8.validate(self) {
            ctx.issue(msg);
        }
    }
}
