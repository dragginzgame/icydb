use super::{Primitive, primitive_scalar_kind};
use icydb_schema::ScalarKind;

macro_rules! define_primitive_cases {
    ( @entries $( ($primitive:ident, $scalar:ident) ),* $(,)? ) => {
        const PRIMITIVE_CASES: &[(&str, Primitive, ScalarKind)] = &[
            $(
                (
                    stringify!($primitive),
                    Primitive::$primitive,
                    ScalarKind::$scalar,
                ),
            )*
        ];
    };
}

icydb_schema::authoring_primitive_registry!(define_primitive_cases);

#[test]
fn primitive_catalog_parses_and_maps_to_scalar_kinds() {
    for &(name, primitive, scalar_kind) in PRIMITIVE_CASES {
        assert_eq!(name.parse(), Ok(primitive));
        assert_eq!(primitive_scalar_kind(primitive), scalar_kind);
    }

    assert_eq!("Unknown".parse::<Primitive>(), Err("unknown Primitive"));
}
