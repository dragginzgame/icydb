//! Module: base
//!
//! Responsibility: facade module surface.
//! Does not own: database runtime authority.
//! Boundary: keeps public facade shape stable for downstream code.

pub(crate) mod helper;
pub mod normalizer;
pub mod types;
pub mod validator;

#[cfg(test)]
mod tests {
    use crate::{
        Path as _,
        base::types::{
            color::{RgbHex, RgbaHex},
            finance::{E8s, E18s, Usd},
            geo::{AddressLine, CityName, PostalCode, RegionName},
            hash::Sha256,
            ident::{Constant, Field, Function, Variable, Variant},
            intl::{CountryCode, LanguageCode, PhoneNumber},
            lang::Code,
            num::{Degrees, Percent, PercentModifier},
            web::{MimeType, Url},
        },
        build::schema_read,
        node::Newtype,
    };

    #[test]
    fn built_in_durable_rules_and_application_validators_have_disjoint_owners() {
        let schema = schema_read();
        for path in [
            Degrees::PATH,
            Percent::PATH,
            PercentModifier::PATH,
            Usd::PATH,
            E8s::PATH,
            E18s::PATH,
            AddressLine::PATH,
            CityName::PATH,
            PostalCode::PATH,
            RegionName::PATH,
        ] {
            let newtype = schema
                .cast_node::<Newtype>(path)
                .expect("durable built-in newtype should be registered");
            assert_eq!(newtype.ty().rules().len(), 1, "{path}");
            assert!(newtype.ty().validators().is_empty(), "{path}");
        }

        for path in [
            Constant::PATH,
            Field::PATH,
            Function::PATH,
            Variable::PATH,
            Variant::PATH,
        ] {
            let newtype = schema
                .cast_node::<Newtype>(path)
                .expect("identifier built-in newtype should be registered");
            assert_eq!(newtype.ty().rules().len(), 1, "{path}");
            assert_eq!(newtype.ty().validators().len(), 1, "{path}");
        }

        for path in [Url::PATH, MimeType::PATH, PhoneNumber::PATH] {
            let newtype = schema
                .cast_node::<Newtype>(path)
                .expect("application-only built-in newtype should be registered");
            assert!(newtype.ty().rules().is_empty(), "{path}");
            assert!(!newtype.ty().validators().is_empty(), "{path}");
        }

        for path in [
            RgbHex::PATH,
            RgbaHex::PATH,
            Sha256::PATH,
            CountryCode::PATH,
            LanguageCode::PATH,
            Code::PATH,
        ] {
            let newtype = schema
                .cast_node::<Newtype>(path)
                .expect("application-only built-in newtype should be registered");
            assert!(newtype.ty().rules().is_empty(), "{path}");
            assert!(!newtype.ty().validators().is_empty(), "{path}");
        }

        let mut classified = [
            Degrees::PATH,
            Percent::PATH,
            PercentModifier::PATH,
            Usd::PATH,
            E8s::PATH,
            E18s::PATH,
            AddressLine::PATH,
            CityName::PATH,
            PostalCode::PATH,
            RegionName::PATH,
            Constant::PATH,
            Field::PATH,
            Function::PATH,
            Variable::PATH,
            Variant::PATH,
            Url::PATH,
            MimeType::PATH,
            PhoneNumber::PATH,
            RgbHex::PATH,
            RgbaHex::PATH,
            Sha256::PATH,
            CountryCode::PATH,
            LanguageCode::PATH,
            Code::PATH,
        ];
        classified.sort_unstable();
        let attached = schema
            .get_nodes::<Newtype>()
            .filter(|(_, newtype)| {
                !newtype.ty().rules().is_empty() || !newtype.ty().validators().is_empty()
            })
            .map(|(path, _)| path)
            .collect::<Vec<_>>();
        assert_eq!(attached, classified);

        for (path, expected_scale) in [(Usd::PATH, 2), (E8s::PATH, 8), (E18s::PATH, 18)] {
            let newtype = schema
                .cast_node::<Newtype>(path)
                .expect("decimal built-in newtype should be registered");
            assert_eq!(newtype.item().scale(), Some(expected_scale), "{path}");
        }
        drop(schema);
    }
}
