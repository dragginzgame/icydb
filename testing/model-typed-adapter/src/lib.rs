//! Renamed-dependency typed-adapter compile fixture.

mod schema;

#[cfg(test)]
mod tests {
    use model_api::base::types::{
        bytes::Utf8,
        color::{Rgb, RgbHex, Rgba, RgbaHex},
        finance::{E8s, E18s, Usd},
        geo::{AddressLine, CityName, PostalCode, RegionName},
        hash::Sha256,
        ic::{
            Memo,
            icp::{Payment as IcpPayment, Tokens as IcpTokens},
            icrc1::{Payment as Icrc1Payment, TokenAmount, Tokens as Icrc1Tokens},
            icrc3::Value as Icrc3Value,
        },
        ident::{Constant, Field, Function, Variable, Variant},
        intl::{CountryCode, LanguageCode, PhoneNumber},
        lang::Code,
        num::{
            DecimalRange, Degrees, DurationRange, Int32Range, Nat32Range, Percent, PercentModifier,
        },
        time::{Milliseconds, Minutes, Seconds},
        web::{MimeType, Url},
    };
    use model_api::visitor::{ApplicationOperation, CallbackKind, Visitable};
    use model_api::{Inner as _, NormalizeAndValidate as _, normalize, validate};
    use runtime_api::db::{TypedRowAdapter, TypedWriteAdapter, WriteCell};

    use super::schema::{
        AdapterChoice, AdapterList, AdapterMap, AdapterRecord, AdapterSet, AdapterTuple,
        RecursiveRecord, TypedAdapterEntity, TypedAdapterEntityInsert, TypedAdapterEntityPatch,
        TypedAdapterEntityReplace, X, XEntity,
    };

    fn assert_row_adapter<T: TypedRowAdapter>() {}

    fn assert_write_adapter<T: TypedWriteAdapter>() {}

    fn assert_model_behavior<T: Visitable>() {}

    fn assert_named_adapter<T>()
    where
        T: runtime_api::__macro::TypedInputValue
            + runtime_api::__macro::TypedNamedType
            + runtime_api::__macro::TypedOutputValue,
    {
    }

    #[test]
    fn renamed_dependencies_compile_default_adapters() {
        let insert_without_database_owned_id = TypedAdapterEntityInsert {
            name: WriteCell::Omitted,
            nickname: WriteCell::Omitted,
            profile: WriteCell::Omitted,
            list: WriteCell::Omitted,
            set: WriteCell::Omitted,
            map: WriteCell::Omitted,
            tuple: WriteCell::Omitted,
            recursive: WriteCell::Omitted,
        };
        assert!(matches!(
            insert_without_database_owned_id.name,
            WriteCell::Omitted
        ));
        assert!(matches!(
            insert_without_database_owned_id.nickname,
            WriteCell::Omitted
        ));
        assert_row_adapter::<TypedAdapterEntity>();
        assert_write_adapter::<TypedAdapterEntityInsert>();
        assert_write_adapter::<TypedAdapterEntityPatch>();
        assert_write_adapter::<TypedAdapterEntityReplace>();
        assert_named_adapter::<X>();
        assert_named_adapter::<XEntity>();
        assert_named_adapter::<AdapterChoice>();
        assert_named_adapter::<AdapterRecord>();
        assert_named_adapter::<AdapterList>();
        assert_named_adapter::<AdapterSet>();
        assert_named_adapter::<AdapterMap>();
        assert_named_adapter::<AdapterTuple>();
        assert_named_adapter::<RecursiveRecord>();
        assert_named_adapter::<Utf8>();
        assert_named_adapter::<Rgb>();
        assert_named_adapter::<RgbHex>();
        assert_named_adapter::<Rgba>();
        assert_named_adapter::<RgbaHex>();
        assert_named_adapter::<E8s>();
        assert_named_adapter::<E18s>();
        assert_named_adapter::<Usd>();
        assert_named_adapter::<AddressLine>();
        assert_named_adapter::<CityName>();
        assert_named_adapter::<PostalCode>();
        assert_named_adapter::<RegionName>();
        assert_named_adapter::<Sha256>();
        assert_named_adapter::<Memo>();
        assert_named_adapter::<IcpPayment>();
        assert_named_adapter::<IcpTokens>();
        assert_named_adapter::<Icrc1Payment>();
        assert_named_adapter::<TokenAmount>();
        assert_named_adapter::<Icrc1Tokens>();
        assert_named_adapter::<Icrc3Value>();
        assert_named_adapter::<Constant>();
        assert_named_adapter::<Field>();
        assert_named_adapter::<Function>();
        assert_named_adapter::<Variable>();
        assert_named_adapter::<Variant>();
        assert_named_adapter::<CountryCode>();
        assert_named_adapter::<LanguageCode>();
        assert_named_adapter::<PhoneNumber>();
        assert_named_adapter::<Code>();
        assert_named_adapter::<DecimalRange>();
        assert_named_adapter::<Degrees>();
        assert_named_adapter::<DurationRange>();
        assert_named_adapter::<Int32Range>();
        assert_named_adapter::<Nat32Range>();
        assert_named_adapter::<Percent>();
        assert_named_adapter::<PercentModifier>();
        assert_named_adapter::<Milliseconds>();
        assert_named_adapter::<Minutes>();
        assert_named_adapter::<Seconds>();
        assert_named_adapter::<MimeType>();
        assert_named_adapter::<Url>();
        assert_model_behavior::<runtime_api::types::Ulid>();
    }

    #[test]
    fn generated_application_behavior_is_explicit_and_composable() {
        let normalized = MimeType::from("  Text/HTML  ")
            .normalize_and_validate()
            .expect("normalized MIME type should validate");
        assert_eq!(normalized.inner(), "text/html");

        let authored = MimeType::from("  Text/HTML  ");
        let error = validate(&authored).expect_err("direct validation must not normalize");
        assert_eq!(error.operation(), ApplicationOperation::Validate);
        assert_eq!(authored.inner(), "  Text/HTML  ");

        let mut unsupported_url = Url::from("ftp://example.com");
        let error = normalize(&mut unsupported_url)
            .expect_err("unsupported URL scheme should fail normalization");
        assert_eq!(error.operation(), ApplicationOperation::Normalize);
        let issue = error
            .issues()
            .get("")
            .and_then(|issues| issues.first())
            .expect("normalizer error should retain its root path");
        let callback = issue
            .callback()
            .expect("normalizer error should retain callback identity");
        assert_eq!(callback.kind(), CallbackKind::Normalizer);
        assert!(callback.type_path().ends_with("base::normalizer::web::Url"));
    }

    #[test]
    fn composed_validation_error_retains_declared_validator_identity() {
        let error = MimeType::from("not a mime type")
            .normalize_and_validate()
            .expect_err("invalid normalized MIME type should fail validation");

        assert_eq!(error.operation(), ApplicationOperation::Validate);
        let issue = error
            .issues()
            .get("")
            .and_then(|issues| issues.first())
            .expect("validator error should retain its root path");
        let callback = issue
            .callback()
            .expect("validator error should retain callback identity");
        assert_eq!(callback.kind(), CallbackKind::Validator);
        assert!(
            callback
                .type_path()
                .ends_with("base::validator::web::MimeType")
        );
    }
}
