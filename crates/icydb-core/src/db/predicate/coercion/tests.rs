use crate::{
    db::predicate::{CoercionId, coercion::supports_coercion},
    value::CoercionFamily,
};

#[test]
fn supports_coercion_matches_canonical_family_matrix() {
    assert!(supports_coercion(
        CoercionFamily::Numeric,
        CoercionFamily::Textual,
        CoercionId::Strict,
    ));
    assert!(supports_coercion(
        CoercionFamily::Textual,
        CoercionFamily::Numeric,
        CoercionId::CollectionElement,
    ));

    assert!(supports_coercion(
        CoercionFamily::Numeric,
        CoercionFamily::Numeric,
        CoercionId::NumericWiden,
    ));
    assert!(!supports_coercion(
        CoercionFamily::Numeric,
        CoercionFamily::Textual,
        CoercionId::NumericWiden,
    ));

    assert!(supports_coercion(
        CoercionFamily::Textual,
        CoercionFamily::Textual,
        CoercionId::TextCasefold,
    ));
    assert!(!supports_coercion(
        CoercionFamily::Textual,
        CoercionFamily::Numeric,
        CoercionId::TextCasefold,
    ));
}
