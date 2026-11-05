use super::{Int64N, Nat64N};
use icydb_model::Inner as _;

#[icydb_model::newtype(item(prim = "Text", unbounded), default = "::1", traits(add(Default)))]
pub struct QuotedPathDefault;

#[icydb_model::newtype(
    item(prim = "Blob", unbounded),
    default = "DeAdBeEf",
    traits(add(Default))
)]
pub struct HexBlobDefault;

#[test]
fn generated_blob_default_uses_hex_bytes() {
    assert_eq!(
        HexBlobDefault::default().inner().as_bytes(),
        &[0xde, 0xad, 0xbe, 0xef]
    );
}

#[test]
fn quoted_default_is_text_and_bare_default_paths_still_execute() {
    assert_eq!(QuotedPathDefault::default().inner(), "::1");
    assert_eq!(super::DateD::default().inner(), &icydb::types::Date::EPOCH);
    assert_eq!(
        super::PrincipalD::default().inner(),
        &icydb::types::Principal::anonymous()
    );
}

#[test]
fn generated_exact_operators_delegate_to_the_wrapped_type() {
    let mut remainder = Nat64N::from(17_u64);
    remainder %= Nat64N::from(5_u64);
    assert_eq!(*remainder.inner(), 2);
    remainder %= 2_u64;
    assert_eq!(*remainder.inner(), 0);

    let product: Nat64N = [2_u64, 3, 4].into_iter().map(Nat64N::from).product();
    assert_eq!(*product.inner(), 24);
    let inner_product: Nat64N = [2_u64, 3, 4].into_iter().product();
    assert_eq!(*inner_product.inner(), 24);

    let negative = -Int64N::from(7_i64);
    assert_eq!(*negative.inner(), -7);
}
