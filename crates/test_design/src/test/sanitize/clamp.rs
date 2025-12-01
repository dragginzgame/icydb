use crate::prelude::*;

///
/// ClampEntity
///

#[entity(
    store = "TestDataStore",
    pk = "id",
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "cint32", value(item(is = "ClampInt32"))),
        field(ident = "cint32_opt", value(opt, item(is = "ClampInt32"))),
        field(ident = "cdec", value(item(is = "ClampDecimal"))),
        field(ident = "cdec_opt", value(opt, item(is = "ClampDecimal"))),
    )
)]
pub struct SanitizeTest {}

///
/// ClampList
///

#[list(item(is = "ClampDecimal"))]
pub struct ClampListDecimal {}

///
/// ClampInt32
///

#[newtype(
    primitive = "Int32",
    item(prim = "Int32"),
    ty(sanitizer(path = "base::sanitizer::num::Clamp", args(10, 20)))
)]
pub struct ClampInt32 {}

///
/// ClampDecimal
///

#[newtype(
    primitive = "Decimal",
    item(prim = "Decimal"),
    ty(sanitizer(path = "base::sanitizer::num::Clamp", args(0.5, 5.5)))
)]
pub struct ClampDecimal {}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use icydb::core::sanitize;

    #[test]
    fn test_clamp_int32() {
        let mut v = ClampInt32::from(5);
        sanitize(&mut v);
        assert_eq!(*v, 10, "should clamp up to min");

        let mut v = ClampInt32::from(25);
        sanitize(&mut v);
        assert_eq!(*v, 20, "should clamp down to max");

        let mut v = ClampInt32::from(15);
        sanitize(&mut v);
        assert_eq!(*v, 15, "in-range value should be unchanged");
    }

    #[test]
    fn test_clamp_decimal() {
        let mut v = ClampDecimal::from(Decimal::from(0.1));
        sanitize(&mut v);
        assert_eq!(*v, Decimal::from(0.5), "should clamp up to min");

        let mut v = ClampDecimal::from(Decimal::from(10));
        sanitize(&mut v);
        assert_eq!(*v, Decimal::from(5.5), "should clamp down to max");

        let mut v = ClampDecimal::from(Decimal::from(2));
        sanitize(&mut v);
        assert_eq!(*v, Decimal::from(2.0), "in-range value should be unchanged");
    }

    #[test]
    fn test_clamp_option_fields() {
        let mut opt: Option<ClampInt32> = Some(ClampInt32::from(5));
        sanitize(&mut opt);
        assert_eq!(
            opt.unwrap(),
            ClampInt32::from(10),
            "option should clamp inner"
        );

        let mut none: Option<ClampInt32> = None;
        sanitize(&mut none);
        assert!(none.is_none(), "None should remain untouched");
    }

    #[test]
    fn test_clamp_list_decimal() {
        let mut list = ClampListDecimal::from(vec![
            Decimal::from(0.1),
            Decimal::from(2.0),
            Decimal::from(10.0),
        ]);
        sanitize(&mut list);

        let expected = vec![Decimal::from(0.5), Decimal::from(2.0), Decimal::from(5.5)];
        assert_eq!(
            *list, expected,
            "list values should be clamped element-wise"
        );
    }

    #[test]
    fn test_sanitize_entity() {
        let mut e = SanitizeTest {
            cint32: ClampInt32::from(5),
            cint32_opt: Some(ClampInt32::from(25)),
            cdec: ClampDecimal::from(10),
            cdec_opt: Some(ClampDecimal::from(0.1)),
            ..Default::default()
        };

        sanitize(&mut e);

        assert_eq!(e.cint32, ClampInt32::from(10), "clamped up");
        assert_eq!(e.cint32_opt.unwrap(), ClampInt32::from(20), "clamped down");
        assert_eq!(e.cdec, ClampDecimal::from(5.5), "clamped down");
        assert_eq!(e.cdec_opt.unwrap(), ClampDecimal::from(0.5), "clamped up");
    }
}
