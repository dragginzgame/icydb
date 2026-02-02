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
        base::validator::bytes::Utf8.validate(self.0.as_slice(), ctx);
    }
}
