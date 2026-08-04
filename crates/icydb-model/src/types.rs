use crate::prelude::*;
use icydb_schema::ScalarKind;
use std::str::FromStr;

//
// Cardinality
//
// Schema-level multiplicity marker used by codegen and validation passes.
// `One` means a required single value.
// `Opt` means an optional slot (nullable / absent is valid).
// `Many` means repeated values (for list/set-like shapes).
//

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub enum Cardinality {
    #[default]
    One,
    Opt,
    Many,
}

impl FromStr for Cardinality {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "One" => Ok(Self::One),
            "Opt" => Ok(Self::Opt),
            "Many" => Ok(Self::Many),
            _ => Err("unknown Cardinality"),
        }
    }
}

//
// Primitive
//
// Width-preserving scalar vocabulary used by the application authoring graph.
// `icydb-schema` owns the catalog and its canonical `ScalarKind` mapping;
// accepted runtime capability policy belongs to `ScalarKind` metadata.
//

macro_rules! define_primitive {
    ( @entries $( ($primitive:ident, $scalar:ident) ),* $(,)? ) => {
        #[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
        #[remain::sorted]
        pub enum Primitive {
            $( $primitive, )*
        }

        impl FromStr for Primitive {
            type Err = &'static str;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                match value {
                    $( stringify!($primitive) => Ok(Self::$primitive), )*
                    _ => Err("unknown Primitive"),
                }
            }
        }

        const fn primitive_scalar_kind(primitive: Primitive) -> ScalarKind {
            match primitive {
                $( Primitive::$primitive => ScalarKind::$scalar, )*
            }
        }
    };
}

icydb_schema::authoring_primitive_registry!(define_primitive);

impl Primitive {
    #[must_use]
    pub const fn is_primary_key_component_encodable(self) -> bool {
        primitive_scalar_kind(self).is_primary_key_component_encodable()
    }
}

#[cfg(test)]
mod tests {
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
}
