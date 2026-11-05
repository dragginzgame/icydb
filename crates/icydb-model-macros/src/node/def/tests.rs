use super::Def;

#[test]
fn schema_declarations_reject_authored_fields_before_emission() {
    for item in [
        syn::parse_quote!(
            pub struct Empty {}
        ),
        syn::parse_quote!(
            pub struct Empty();
        ),
        syn::parse_quote!(
            pub struct Empty;
        ),
    ] {
        assert!(Def::validate_declaration(&item).is_ok());
    }
    for item in [
        syn::parse_quote!(
            pub struct Named {
                pub note: String,
            }
        ),
        syn::parse_quote!(
            pub struct Private {
                note: String,
            }
        ),
        syn::parse_quote!(
            pub struct Tuple(pub String, u64);
        ),
    ] {
        let error = Def::validate_declaration(&item).unwrap_err();
        assert_eq!(error.len(), 1);
        assert!(!error.write_errors().is_empty());
    }
}

#[test]
fn schema_declarations_reject_generic_parameters_before_emission() {
    for item in [
        syn::parse_quote!(
            pub struct Concrete {}
        ),
        syn::parse_quote!(
            pub struct Concrete;
        ),
        syn::parse_quote!(
            pub struct Concrete
            where
                String: Clone, {}
        ),
    ] {
        assert!(Def::validate_declaration(&item).is_ok());
    }
    for item in [
        syn::parse_quote!(
            pub struct Generic<T> {}
        ),
        syn::parse_quote!(
            pub struct Generic<'a> {}
        ),
        syn::parse_quote!(
            pub struct Generic<const N: usize> {}
        ),
        syn::parse_quote!(
            pub struct Generic<T: Clone = String> {}
        ),
        syn::parse_quote!(
            pub struct Generic<T>
            where
                T: Clone, {}
        ),
    ] {
        let error = Def::validate_declaration(&item).unwrap_err();
        assert_eq!(error.len(), 1);
        assert!(!error.write_errors().is_empty());
    }
}
