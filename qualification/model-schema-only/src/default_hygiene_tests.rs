use model_api::{enum_, newtype, tuple};
use std::marker::Send as Default;

const _: ::std::option::Option<&dyn Default> = ::std::option::Option::None;

trait CallerInto {
    fn into(self) -> Self;
}

impl CallerInto for &'static str {
    fn into(self) -> Self {
        self
    }
}

#[newtype(
    item(prim = "Text", max_len = 32),
    default = "expected",
    traits(add(Default))
)]
pub struct HygieneDefaultText {}

#[enum_(
    variant(name = "Count", value(item(prim = "Nat64")), default),
    traits(add(Default))
)]
pub struct DefaultCount {}

#[tuple(
    value(item(prim = "Nat64")),
    value(item(prim = "Text", max_len = 32)),
    traits(add(Default))
)]
pub struct DefaultPair {}

#[test]
fn generated_defaults_ignore_shadowed_trait_names() {
    let count = <DefaultCount as ::std::default::Default>::default();
    assert!(matches!(count, DefaultCount::Count(0)));
    let pair = <DefaultPair as ::std::default::Default>::default();
    assert_eq!(pair.0, 0);
    assert!(pair.1.is_empty());
    let text = <HygieneDefaultText as ::std::default::Default>::default();
    assert_eq!(text.0, "expected");
    assert_eq!(CallerInto::into("caller"), "caller");
}
